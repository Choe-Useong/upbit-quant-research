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

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

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
from lib.storage import read_candles_csv, write_table_csv

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
    parser.add_argument("--output-csv", default="data/upbit/features/features.csv", help="Output feature CSV path")
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


def _merge_market_feature_csvs(temp_paths: list[Path], output_csv: Path, columns: list[str]) -> int:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    iterators: list[Iterator[dict[str, str]]] = []
    heap: list[tuple[tuple[str, str], int, dict[str, str]]] = []

    for temp_path in temp_paths:
        iterator = _iter_temp_rows(temp_path)
        iterators.append(iterator)
        first_row = next(iterator, None)
        if first_row is not None:
            heapq.heappush(heap, (_row_sort_key(first_row), len(iterators) - 1, first_row))

    with output_csv.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        while heap:
            _, iterator_idx, row = heapq.heappop(heap)
            writer.writerow(row)
            row_count += 1
            next_row = next(iterators[iterator_idx], None)
            if next_row is not None:
                heapq.heappush(heap, (_row_sort_key(next_row), iterator_idx, next_row))

    return row_count


def build_feature_table_market_stream(
    candle_dir: Path,
    feature_specs: list[FeatureSpec],
    output_csv: Path,
    max_markets: int | None = None,
    tail_hours: int | None = None,
) -> int:
    columns = feature_columns(feature_specs)
    temp_paths: list[Path] = []
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="build_features_", dir=output_csv.parent) as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        for csv_path in list_candle_paths(candle_dir, max_markets=max_markets):
            candle_rows = load_market_candles(csv_path, tail_hours=tail_hours)
            if not candle_rows:
                continue
            feature_rows = build_feature_table(candle_rows, feature_specs)
            if not feature_rows:
                continue
            temp_path = temp_dir / csv_path.name
            write_table_csv(temp_path, feature_rows, columns)
            temp_paths.append(temp_path)
        if not temp_paths:
            raise SystemExit(f"No candle rows found in {candle_dir}")
        return _merge_market_feature_csvs(temp_paths, output_csv, columns)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    feature_specs = load_feature_specs(Path(args.spec_json))
    candle_dir = Path(args.candle_dir)
    output_csv = Path(args.output_csv)

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
        write_table_csv(output_csv, feature_rows, feature_columns(feature_specs))
        row_count = len(feature_rows)

    print(f"Wrote {row_count} feature rows to {args.output_csv}")


if __name__ == "__main__":
    main()
