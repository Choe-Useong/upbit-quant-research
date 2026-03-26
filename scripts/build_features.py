#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import heapq
import json
import sys
import tempfile
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.dataframes import read_wide_frames_from_cache
from lib.features import (
    CompareSpec,
    FeatureSpec,
    LogicalSpec,
    ScoreComponentSpec,
    StateSpec,
    TransformSpec,
    build_feature_table,
    feature_columns,
)
from lib.storage import read_candles_csv, write_table
from lib.upbit_collector import CandleRow

MARKET_STREAM_UNSUPPORTED_TRANSFORMS = {"cross_rank", "cross_percentile"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build feature table CSV from candle CSV files and a JSON feature spec."
    )
    parser.add_argument(
        "--candle-dir",
        "--daily-dir",
        dest="candle_dir",
        default="data/upbit/daily",
        help="Directory containing per-market candle CSV files",
    )
    parser.add_argument("--spec-json", required=True, help="JSON file containing feature spec list")
    parser.add_argument(
        "--output-csv",
        "--output-path",
        dest="output_csv",
        default="data/upbit/features/features.csv",
        help="Output feature table path (.csv or .parquet)",
    )
    parser.add_argument(
        "--engine",
        choices=["auto", "legacy", "market_stream"],
        default="auto",
        help="Feature build engine. auto uses market_stream when the spec is market-local only.",
    )
    parser.add_argument(
        "--max-markets",
        type=int,
        default=None,
        help="Optional limit on the number of market CSV files to read, for smoke validation.",
    )
    parser.add_argument(
        "--tail-hours",
        type=int,
        default=None,
        help="Optional per-market tail length to keep, assuming one row per hour.",
    )
    parser.add_argument(
        "--source-cache-dir",
        default="",
        help="Optional wide-parquet source cache directory, e.g. data/upbit_research_cache/60",
    )
    return parser


def load_feature_specs(path: Path) -> list[FeatureSpec]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    specs: list[FeatureSpec] = []
    for item in payload:
        steps = tuple(
            TransformSpec(kind=step["kind"], params=step.get("params", {}))
            for step in item.get("steps", [])
        )
        components = tuple(
            ScoreComponentSpec(
                feature_column=component["feature_column"],
                weight=float(component.get("weight", 1.0)),
            )
            for component in item.get("components", [])
        )
        compare = None
        if "compare" in item:
            compare_payload = item["compare"]
            compare = CompareSpec(
                left_feature=compare_payload["left_feature"],
                operator=compare_payload["operator"],
                right_feature=compare_payload.get("right_feature"),
                right_value=(
                    None
                    if compare_payload.get("right_value") is None
                    else float(compare_payload["right_value"])
                ),
            )
        logical = None
        if "logical" in item:
            logical_payload = item["logical"]
            logical = LogicalSpec(
                operator=logical_payload["operator"],
                features=tuple(logical_payload["features"]),
            )
        state = None
        if "state" in item:
            state_payload = item["state"]
            state = StateSpec(
                entry_feature=state_payload["entry_feature"],
                exit_feature=state_payload["exit_feature"],
            )
        specs.append(
            FeatureSpec(
                source=item.get("source"),
                steps=steps,
                components=components,
                combine=item.get("combine"),
                compare=compare,
                logical=logical,
                state=state,
                column_name=item.get("column_name"),
            )
        )
    return specs


def list_candle_paths(candle_dir: Path, max_markets: int | None = None) -> list[Path]:
    csv_paths = sorted(candle_dir.glob("*.csv"))
    if max_markets is not None:
        csv_paths = csv_paths[:max_markets]
    return csv_paths


def load_market_candles(csv_path: Path, tail_hours: int | None = None) -> list:
    rows = read_candles_csv(csv_path)
    if tail_hours is not None:
        rows = rows[-tail_hours:]
    return rows


def load_market_candles_from_cache(
    cache_dir: Path,
    market: str,
    *,
    tail_hours: int | None = None,
    cache_frames: dict[str, pd.DataFrame] | None = None,
    market_meta: pd.DataFrame | None = None,
    market_warning: pd.DataFrame | None = None,
) -> list[CandleRow]:
    required_columns = (
        "opening_price",
        "high_price",
        "low_price",
        "trade_price",
        "candle_acc_trade_volume",
        "candle_acc_trade_price",
        "timestamp",
    )
    if cache_frames is None:
        cache_frames = read_wide_frames_from_cache(
            cache_dir,
            required_columns,
            tail_rows=tail_hours,
        )
    if market_meta is None:
        market_meta = pd.read_parquet(cache_dir / "market_meta.parquet")
    if market_warning is None:
        warning_path = cache_dir / "market_warning.parquet"
        market_warning = pd.read_parquet(warning_path) if warning_path.exists() else pd.DataFrame()

    meta_rows = market_meta.loc[market_meta["market"] == market]
    if meta_rows.empty:
        return []
    meta_row = meta_rows.iloc[0]

    base_frame = cache_frames["trade_price"]
    if market not in base_frame.columns:
        return []
    market_index = base_frame[market].dropna().index
    if tail_hours is not None and len(market_index) > tail_hours:
        market_index = market_index[-tail_hours:]

    rows: list[CandleRow] = []
    for timestamp in market_index:
        trade_price = cache_frames["trade_price"].at[timestamp, market]
        if pd.isna(trade_price):
            continue
        warning_value = "NONE"
        if not market_warning.empty and market in market_warning.columns and timestamp in market_warning.index:
            warning_cell = market_warning.at[timestamp, market]
            if not pd.isna(warning_cell) and str(warning_cell):
                warning_value = str(warning_cell).upper()
        date_utc = pd.Timestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%S")
        date_kst = (pd.Timestamp(timestamp) + pd.Timedelta(hours=9)).strftime("%Y-%m-%dT%H:%M:%S")
        rows.append(
            CandleRow(
                market=market,
                korean_name=str(meta_row["korean_name"]),
                english_name=str(meta_row["english_name"]),
                market_warning=warning_value,
                date_utc=date_utc,
                date_kst=date_kst,
                opening_price=float(cache_frames["opening_price"].at[timestamp, market]),
                high_price=float(cache_frames["high_price"].at[timestamp, market]),
                low_price=float(cache_frames["low_price"].at[timestamp, market]),
                trade_price=float(trade_price),
                candle_acc_trade_volume=float(cache_frames["candle_acc_trade_volume"].at[timestamp, market]),
                candle_acc_trade_price=float(cache_frames["candle_acc_trade_price"].at[timestamp, market]),
                timestamp=(
                    None
                    if pd.isna(cache_frames["timestamp"].at[timestamp, market])
                    else int(float(cache_frames["timestamp"].at[timestamp, market]))
                ),
            )
        )
    return rows


def load_all_candles(
    candle_dir: Path,
    max_markets: int | None = None,
    tail_hours: int | None = None,
) -> list:
    rows = []
    for csv_path in list_candle_paths(candle_dir, max_markets=max_markets):
        rows.extend(load_market_candles(csv_path, tail_hours=tail_hours))
    return rows


def _is_market_stream_compatible(feature_specs: list[FeatureSpec]) -> bool:
    resolved_columns: set[str] = set()
    for spec in feature_specs:
        if spec.source is not None and spec.source.startswith("market:"):
            return False
        if any(step.kind in MARKET_STREAM_UNSUPPORTED_TRANSFORMS for step in spec.steps):
            return False
        if spec.compare is not None:
            if (
                spec.compare.left_feature.startswith("market:")
                or (spec.compare.right_feature is not None and spec.compare.right_feature.startswith("market:"))
            ):
                return False
        if spec.logical is not None and any(feature.startswith("market:") for feature in spec.logical.features):
            return False
        if spec.state is not None and (
            spec.state.entry_feature.startswith("market:")
            or spec.state.exit_feature.startswith("market:")
        ):
            return False
        if any(component.feature_column.startswith("market:") for component in spec.components):
            return False
        resolved_columns.add(spec.resolved_column_name())
    return True


def _row_sort_key(row: dict[str, str]) -> tuple[str, str]:
    return (row["date_utc"], row["market"])


def _iter_temp_rows(path: Path) -> Iterator[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def _iter_merged_market_feature_rows(temp_paths: list[Path]) -> Iterator[dict[str, str]]:
    iterators: list[Iterator[dict[str, str]]] = []
    heap: list[tuple[tuple[str, str], int, dict[str, str]]] = []

    for temp_path in temp_paths:
        iterator = _iter_temp_rows(temp_path)
        iterators.append(iterator)
        first_row = next(iterator, None)
        if first_row is not None:
            heapq.heappush(heap, (_row_sort_key(first_row), len(iterators) - 1, first_row))

    while heap:
        _, iterator_idx, row = heapq.heappop(heap)
        yield row
        next_row = next(iterators[iterator_idx], None)
        if next_row is not None:
            heapq.heappush(heap, (_row_sort_key(next_row), iterator_idx, next_row))


def _merge_market_feature_tables(temp_paths: list[Path], output_path: Path, columns: list[str]) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    row_iter = _iter_merged_market_feature_rows(temp_paths)
    if output_path.suffix.lower() != ".parquet":
        with output_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            for row in row_iter:
                writer.writerow(row)
                row_count += 1
        return row_count

    writer: pq.ParquetWriter | None = None
    buffers = {column: [] for column in columns}
    batch_size = 50000
    try:
        for row in row_iter:
            for column in columns:
                buffers[column].append(row.get(column, ""))
            row_count += 1
            if len(buffers[columns[0]]) >= batch_size:
                table = pa.table(buffers)
                if writer is None:
                    writer = pq.ParquetWriter(output_path, table.schema, compression="zstd")
                writer.write_table(table)
                buffers = {column: [] for column in columns}
        if buffers[columns[0]]:
            table = pa.table(buffers)
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema, compression="zstd")
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

    return row_count


def build_feature_table_market_stream(
    candle_dir: Path,
    feature_specs: list[FeatureSpec],
    output_path: Path,
    max_markets: int | None = None,
    tail_hours: int | None = None,
    source_cache_dir: Path | None = None,
) -> int:
    columns = feature_columns(feature_specs)
    temp_paths: list[Path] = []
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="build_features_", dir=output_path.parent) as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        cache_frames = None
        market_meta = None
        market_warning = None
        candle_paths = list_candle_paths(candle_dir, max_markets=max_markets)
        if source_cache_dir is not None:
            cache_frames = read_wide_frames_from_cache(
                source_cache_dir,
                (
                    "opening_price",
                    "high_price",
                    "low_price",
                    "trade_price",
                    "candle_acc_trade_volume",
                    "candle_acc_trade_price",
                    "timestamp",
                ),
                max_markets=max_markets,
            )
            market_meta = pd.read_parquet(source_cache_dir / "market_meta.parquet")
            warning_path = source_cache_dir / "market_warning.parquet"
            market_warning = pd.read_parquet(warning_path) if warning_path.exists() else pd.DataFrame()
        for csv_path in candle_paths:
            if source_cache_dir is not None:
                candle_rows = load_market_candles_from_cache(
                    source_cache_dir,
                    csv_path.stem.upper(),
                    tail_hours=tail_hours,
                    cache_frames=cache_frames,
                    market_meta=market_meta,
                    market_warning=market_warning,
                )
            else:
                candle_rows = load_market_candles(csv_path, tail_hours=tail_hours)
            if not candle_rows:
                continue
            feature_rows = build_feature_table(candle_rows, feature_specs)
            if not feature_rows:
                continue
            temp_path = temp_dir / csv_path.name
            write_table(temp_path, feature_rows, columns)
            temp_paths.append(temp_path)
        if not temp_paths:
            raise SystemExit(f"No candle rows found in {candle_dir}")
        return _merge_market_feature_tables(temp_paths, output_path, columns)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    feature_specs = load_feature_specs(Path(args.spec_json))
    candle_dir = Path(args.candle_dir)
    output_csv = Path(args.output_csv)
    source_cache_dir = Path(args.source_cache_dir) if args.source_cache_dir else None

    selected_engine = args.engine
    if selected_engine == "auto":
        selected_engine = "market_stream" if _is_market_stream_compatible(feature_specs) else "legacy"

    if selected_engine == "market_stream":
        if not _is_market_stream_compatible(feature_specs):
            raise SystemExit("market_stream engine only supports market-local feature specs")
        row_count = build_feature_table_market_stream(
            candle_dir,
            feature_specs,
            output_csv,
            max_markets=args.max_markets,
            tail_hours=args.tail_hours,
            source_cache_dir=source_cache_dir,
        )
    else:
        candle_rows = load_all_candles(
            candle_dir,
            max_markets=args.max_markets,
            tail_hours=args.tail_hours,
        )
        if not candle_rows:
            raise SystemExit(f"No candle rows found in {args.candle_dir}")
        feature_rows = build_feature_table(candle_rows, feature_specs)
        write_table(output_csv, feature_rows, feature_columns(feature_specs))
        row_count = len(feature_rows)

    print(f"Wrote {row_count} feature rows to {args.output_csv}")


if __name__ == "__main__":
    main()
