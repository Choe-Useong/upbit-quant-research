#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.legacy.features import build_feature_table, feature_columns
from lib.storage import read_candles_csv, read_table_csv, write_table_csv
from lib.upbit_collector import CandleRow, Market, collect_minute_candles
from lib.legacy.universe import build_universe_table, universe_columns
from lib.legacy.weights import build_weight_table, weight_columns
from lib.spec_io import load_feature_specs
from scripts.build_universe import load_universe_spec
from scripts.build_weights import load_weight_spec


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build portfolio weights by running features -> market scores -> universe -> weights."
    )
    parser.add_argument("--preset", default="", help="Preset portfolio name, e.g. main_strategies_60m_equal_weight_4core")
    parser.add_argument("--candle-dir", default="", help="Override candle directory")
    parser.add_argument("--features-spec-json", default="", help="Feature spec JSON path")
    parser.add_argument("--market-scores-spec-json", default="", help="Market score spec JSON path")
    parser.add_argument("--universe-spec-json", default="", help="Universe spec JSON path")
    parser.add_argument("--weights-spec-json", default="", help="Weights spec JSON path")
    parser.add_argument("--output-dir", default="", help="Output portfolio directory")
    parser.add_argument(
        "--history-bars-override",
        "--required-history-bars",
        dest="history_bars_override",
        type=int,
        default=0,
        help="Optional manual history-bar override; omitted means infer from specs",
    )
    return parser


def _as_binary(value: str) -> float:
    normalized = (value or "").strip().lower()
    return 1.0 if normalized in {"1", "1.0", "true"} else 0.0


def _load_market_score_rules(path: Path) -> tuple[str, dict[str, dict[str, object]]]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return (
        payload.get("output_column", "custom_score"),
        {
            item["market"]: {
                "mode": item.get("mode", "weighted_sum"),
                "components": item.get("components", []),
            }
            for item in payload.get("rules", [])
        },
    )


def _score_feature_rows(
    rows: list[dict[str, str]],
    score_column: str,
    market_rules: dict[str, dict[str, object]],
) -> tuple[list[dict[str, str]], list[str]]:
    scored_rows: list[dict[str, str]] = []
    columns = list(rows[0].keys()) if rows else []
    if score_column not in columns:
        columns.append(score_column)
    for row in rows:
        rule = market_rules.get(row["market"], {"mode": "weighted_sum", "components": []})
        components = rule["components"]
        score = 0.0
        if rule["mode"] == "all_true":
            score = 1.0 if components and all(_as_binary(row.get(component["feature_column"], "")) > 0.0 for component in components) else 0.0
        else:
            for component in components:
                score += float(component.get("weight", 1.0)) * _as_binary(row.get(component["feature_column"], ""))
        next_row = dict(row)
        next_row[score_column] = f"{score:.12g}"
        scored_rows.append(next_row)
    return scored_rows, columns


def _resolve_path(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = ROOT_DIR / path
    return path.resolve()


def _resolve_feature_spec_for_preset(preset: str) -> Path:
    exact = ROOT_DIR / "configs" / "portfolio" / f"features_{preset}.json"
    if exact.exists():
        return exact
    if "60m" in preset:
        return (ROOT_DIR / "configs" / "portfolio" / "features_main_strategies_60m_common.json").resolve()
    if "240m" in preset:
        return (ROOT_DIR / "configs" / "portfolio" / "features_main_strategies_240m_common.json").resolve()
    raise FileNotFoundError(f"Could not infer feature spec for preset: {preset}")


def _resolve_from_preset(preset: str) -> dict[str, Path]:
    base = ROOT_DIR / "configs" / "portfolio"
    return {
        "features_spec_json": _resolve_feature_spec_for_preset(preset),
        "market_scores_spec_json": (base / f"market_scores_{preset}.json").resolve(),
        "universe_spec_json": (base / f"universe_{preset}.json").resolve(),
        "weights_spec_json": (base / f"weights_{preset}.json").resolve(),
        "output_dir": (ROOT_DIR / "data" / "portfolio" / preset).resolve(),
    }


def _history_param_bars(key: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    bars = int(value)
    if bars <= 0:
        return 0
    normalized = str(key).strip().lower()
    if normalized in {"window", "period", "periods", "lag", "lookback", "lookback_bars", "bars", "span"}:
        return bars
    if normalized.endswith(("_window", "_period", "_periods", "_lag", "_lookback", "_bars", "_span")):
        return bars
    return 0


def _feature_history_bars(spec) -> tuple[int, bool]:
    uses_state = getattr(spec, "state", None) is not None
    steps = getattr(spec, "steps", ()) or ()
    if not steps:
        return 1, uses_state

    bars = 1
    for step in steps:
        step_bars = 1
        for key, value in (step.params or {}).items():
            step_bars = max(step_bars, _history_param_bars(key, value))
        bars += max(step_bars - 1, 0)
    return bars, uses_state


def _infer_required_history_bars(feature_specs: list, universe_spec, weight_spec) -> tuple[int, dict[str, object]]:
    max_feature_bars = 1
    uses_stateful_feature = False
    for spec in feature_specs:
        feature_bars, uses_state = _feature_history_bars(spec)
        max_feature_bars = max(max_feature_bars, feature_bars)
        uses_stateful_feature = uses_stateful_feature or uses_state

    lag_values = [int(getattr(universe_spec, "lag", 0) or 0)]
    lag_values.extend(int(getattr(item, "lag", 0) or 0) for item in getattr(universe_spec, "value_filters", ()) or ())
    lag_values.extend(int(getattr(item, "lag", 0) or 0) for item in getattr(universe_spec, "rank_filters", ()) or ())
    max_lag = max(lag_values) if lag_values else 0

    rebalance_frequency = str(getattr(weight_spec, "rebalance_frequency", "") or "").strip().lower()
    rebalance_buffer = 1 if rebalance_frequency in {"every_bar", ""} else 5
    safety_buffer = max(20, min(max_feature_bars // 10, 100))
    inferred_bars = max_feature_bars + max_lag + rebalance_buffer + safety_buffer
    if uses_stateful_feature:
        inferred_bars = max(inferred_bars, max_feature_bars + 250)

    return inferred_bars, {
        "max_feature_bars": max_feature_bars,
        "max_universe_lag": max_lag,
        "rebalance_buffer": rebalance_buffer,
        "safety_buffer": safety_buffer,
        "uses_stateful_feature": uses_stateful_feature,
    }


def _read_tail_candles_csv(path: Path, tail_rows: int) -> list[CandleRow]:
    frame = pd.read_csv(path, encoding="utf-8-sig")
    if tail_rows > 0 and len(frame) > tail_rows:
        frame = frame.tail(tail_rows).copy()
    frame = frame.sort_values("date_utc")
    rows: list[CandleRow] = []
    for row in frame.to_dict(orient="records"):
        rows.append(
            CandleRow(
                market=str(row["market"]),
                korean_name=str(row["korean_name"]),
                english_name=str(row["english_name"]),
                market_warning=str(row["market_warning"]),
                date_utc=str(row["date_utc"]),
                date_kst=str(row["date_kst"]),
                opening_price=float(row["opening_price"]),
                high_price=float(row["high_price"]),
                low_price=float(row["low_price"]),
                trade_price=float(row["trade_price"]),
                candle_acc_trade_volume=float(row["candle_acc_trade_volume"]),
                candle_acc_trade_price=float(row["candle_acc_trade_price"]),
                timestamp=None if row.get("timestamp") in {"", None} or pd.isna(row.get("timestamp")) else int(row["timestamp"]),
            )
        )
    return rows


def _merge_latest_minute_rows(local_rows: list[CandleRow], required_history_bars: int) -> list[CandleRow]:
    if not local_rows or required_history_bars <= 0:
        return local_rows
    meta = local_rows[-1]
    market_meta = Market(
        market=meta.market,
        korean_name=meta.korean_name,
        english_name=meta.english_name,
        market_warning=meta.market_warning,
    )
    latest_rows = collect_minute_candles(
        market=market_meta,
        unit=60,
        candles=required_history_bars,
    )
    merged_by_date = {row.date_utc: row for row in local_rows}
    for row in latest_rows:
        merged_by_date[row.date_utc] = row
    return [merged_by_date[key] for key in sorted(merged_by_date)]


def _load_candle_rows_with_refresh(
    candle_dir: Path,
    allowed_markets: tuple[str, ...],
    required_history_bars: int,
) -> tuple[list, list[str]]:
    rows = []
    refreshed_markets: list[str] = []
    if allowed_markets:
        for market in allowed_markets:
            csv_path = candle_dir / f"{market}.csv"
            if not csv_path.exists():
                raise FileNotFoundError(f"Missing candle file for allowed market: {csv_path}")
            local_rows = _read_tail_candles_csv(csv_path, required_history_bars)
            merged_rows = _merge_latest_minute_rows(local_rows, required_history_bars)
            rows.extend(merged_rows)
            if required_history_bars > 0:
                refreshed_markets.append(market)
        return rows, refreshed_markets

    for csv_path in sorted(candle_dir.glob("*.csv")):
        rows.extend(read_candles_csv(csv_path))
    return rows, refreshed_markets


def _merge_recent_rows(existing_csv: Path, new_rows: list[dict[str, str]], columns: list[str], earliest_date_utc: str | None) -> list[dict[str, str]]:
    if not existing_csv.exists():
        return new_rows
    if not earliest_date_utc:
        return read_table_csv(existing_csv)
    existing_rows = read_table_csv(existing_csv)
    kept_rows = [row for row in existing_rows if str(row.get("date_utc", "")) < earliest_date_utc]
    return kept_rows + new_rows


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.preset:
        resolved = _resolve_from_preset(args.preset)
        features_spec_json = resolved["features_spec_json"]
        market_scores_spec_json = resolved["market_scores_spec_json"]
        universe_spec_json = resolved["universe_spec_json"]
        weights_spec_json = resolved["weights_spec_json"]
        output_dir = _resolve_path(args.output_dir) if args.output_dir else resolved["output_dir"]
    else:
        if not all(
            [
                args.features_spec_json,
                args.market_scores_spec_json,
                args.universe_spec_json,
                args.weights_spec_json,
                args.output_dir,
            ]
        ):
            raise SystemExit(
                "Either --preset or all of --features-spec-json, --market-scores-spec-json, "
                "--universe-spec-json, --weights-spec-json, --output-dir must be provided"
            )
        features_spec_json = _resolve_path(args.features_spec_json)
        market_scores_spec_json = _resolve_path(args.market_scores_spec_json)
        universe_spec_json = _resolve_path(args.universe_spec_json)
        weights_spec_json = _resolve_path(args.weights_spec_json)
        output_dir = _resolve_path(args.output_dir)

    candle_dir = _resolve_path(args.candle_dir) if args.candle_dir else (ROOT_DIR / "data" / "upbit" / "minutes" / "60").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    build_dir = output_dir / "_build"
    build_dir.mkdir(parents=True, exist_ok=True)

    feature_specs = load_feature_specs(features_spec_json)
    universe_spec = load_universe_spec(universe_spec_json)
    weight_spec = load_weight_spec(weights_spec_json)
    score_column, market_rules = _load_market_score_rules(market_scores_spec_json)
    inferred_history_bars, history_inference = _infer_required_history_bars(
        feature_specs,
        universe_spec,
        weight_spec,
    )
    required_history_bars = int(args.history_bars_override) if int(args.history_bars_override) > 0 else inferred_history_bars

    candle_rows, refreshed_markets = _load_candle_rows_with_refresh(
        candle_dir,
        tuple(universe_spec.allowed_markets or ()),
        required_history_bars,
    )
    if not candle_rows:
        raise SystemExit(f"No candle rows found in {candle_dir}")

    feature_rows = build_feature_table(candle_rows, feature_specs)
    write_table_csv(build_dir / "features.csv", feature_rows, feature_columns(feature_specs))

    scored_rows, scored_columns = _score_feature_rows(feature_rows, score_column, market_rules)
    write_table_csv(build_dir / "features_scored.csv", scored_rows, scored_columns)

    universe_rows = build_universe_table(scored_rows, universe_spec)
    earliest_date_utc = min((row["date_utc"] for row in scored_rows), default=None)
    merged_universe_rows = _merge_recent_rows(
        output_dir / "universe.csv",
        universe_rows,
        universe_columns(),
        earliest_date_utc,
    )
    write_table_csv(output_dir / "universe.csv", merged_universe_rows, universe_columns())

    weight_rows = build_weight_table(universe_rows, weight_spec)
    merged_weight_rows = _merge_recent_rows(
        output_dir / "weights.csv",
        weight_rows,
        weight_columns(),
        earliest_date_utc,
    )
    write_table_csv(output_dir / "weights.csv", merged_weight_rows, weight_columns())

    metadata = {
        "preset": args.preset,
        "candle_dir": str(candle_dir),
        "features_spec_json": str(features_spec_json),
        "market_scores_spec_json": str(market_scores_spec_json),
        "universe_spec_json": str(universe_spec_json),
        "weights_spec_json": str(weights_spec_json),
        "history_bars_mode": "manual_override" if int(args.history_bars_override) > 0 else "auto_inferred",
        "history_bars_override": int(args.history_bars_override),
        "inferred_history_bars": inferred_history_bars,
        "required_history_bars": required_history_bars,
        "history_inference": history_inference,
        "refreshed_markets": refreshed_markets,
        "feature_rows": len(feature_rows),
        "universe_rows": len(universe_rows),
        "weight_rows": len(weight_rows),
        "merged_universe_rows": len(merged_universe_rows),
        "merged_weight_rows": len(merged_weight_rows),
        "earliest_recalculated_date_utc": earliest_date_utc,
    }
    (output_dir / "build_metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8-sig")

    print(f"Wrote {len(merged_weight_rows)} weight rows to {output_dir / 'weights.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
