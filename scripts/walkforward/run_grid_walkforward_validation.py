#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
import sys

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.legacy.features import build_feature_table
from lib.legacy.universe import build_universe_table
from lib.vectorbt_adapter import build_price_frame, build_target_weight_frame, run_portfolio_from_target_weights
from lib.legacy.weights import build_weight_table
from scripts.run_grid import (
    _grid_combinations,
    _load_feature_specs_from_payload,
    _load_universe_spec_from_payload,
    _load_vectorbt_spec_from_payload,
    _load_weight_spec_from_payload,
    _passes_constraints,
    _preferred_summary_fields,
    _render_value,
)
from scripts.run_vectorbt import (
    benchmark_summary,
    build_benchmark_curve,
    compute_annualized_return,
    compute_drawdown_recovery_stats,
    compute_excess_curves,
    compute_information_ratio,
    compute_recent_1y_stats,
    compute_recent_2y_stats,
    compute_return_series,
    infer_periods_per_year,
    timeframe_to_pandas_freq,
    load_all_candles,
)
from scripts.walkforward.run_walkforward_validation import _summarize_candidates, _summarize_winners


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run full-grid walk-forward validation for a grid config.")
    parser.add_argument("--grid-config-json", required=True, help="Grid configuration JSON path")
    parser.add_argument("--out-dir", required=True, help="Directory to write fold and summary CSVs")
    parser.add_argument("--is-months", type=int, default=18, help="In-sample months")
    parser.add_argument("--oos-months", type=int, default=6, help="Out-of-sample months")
    parser.add_argument("--step-months", type=int, default=3, help="Step months")
    parser.add_argument(
        "--ranking-metric",
        default="Annualized Information Ratio",
        help="IS metric used to choose each fold winner",
    )
    return parser


def _parse_timestamp(value: str) -> pd.Timestamp:
    return pd.Timestamp(value)


def _filter_rows_by_window(candle_rows: list, start: pd.Timestamp | None, end: pd.Timestamp | None) -> list:
    filtered = []
    for row in candle_rows:
        timestamp = _parse_timestamp(row.date_utc)
        if start is not None and timestamp < start:
            continue
        if end is not None and timestamp >= end:
            continue
        filtered.append(row)
    return filtered


def _available_timestamps(candle_rows: list, market: str) -> pd.DatetimeIndex:
    timestamps = sorted({_parse_timestamp(row.date_utc) for row in candle_rows if row.market == market})
    return pd.DatetimeIndex(timestamps)


def _build_folds(
    timestamps: pd.DatetimeIndex,
    is_months: int,
    oos_months: int,
    step_months: int,
) -> list[dict[str, pd.Timestamp]]:
    if timestamps.empty:
        return []
    folds: list[dict[str, pd.Timestamp]] = []
    current_start = pd.Timestamp(timestamps.min())
    max_timestamp = pd.Timestamp(timestamps.max())
    while True:
        is_end = current_start + pd.DateOffset(months=is_months)
        oos_end = is_end + pd.DateOffset(months=oos_months)
        if oos_end > max_timestamp:
            break
        folds.append(
            {
                "is_start": current_start,
                "is_end": is_end,
                "oos_start": is_end,
                "oos_end": oos_end,
            }
        )
        current_start = current_start + pd.DateOffset(months=step_months)
    return folds


def _safe_float(value: Any) -> float:
    if value in ("", None):
        return float("nan")
    return float(value)


def _candidate_from_combo(config: dict[str, Any], combo: dict[str, Any]) -> dict[str, Any]:
    context = dict(combo)
    context["run_name"] = config["run_name_template"].format(**combo)
    return {
        "label": context["run_name"],
        "feature_specs": _render_value(config["feature_spec_template"], context),
        "universe_spec": _render_value(config["universe_spec_template"], context),
        "weight_spec": _render_value(config["weight_spec_template"], context),
        "vectorbt_spec": _render_value(config["vectorbt_spec_template"], context),
    }


def _run_candidate_window(
    candidate: dict[str, Any],
    candle_rows: list,
    eval_start: pd.Timestamp,
    eval_end: pd.Timestamp,
    benchmark_market: str,
    timeframe: str,
) -> dict[str, Any]:
    feature_specs = _load_feature_specs_from_payload(candidate["feature_specs"])
    universe_spec = _load_universe_spec_from_payload(candidate["universe_spec"])
    weight_spec = _load_weight_spec_from_payload(candidate["weight_spec"])
    vectorbt_spec, payload_benchmark_market = _load_vectorbt_spec_from_payload(
        {**candidate.get("vectorbt_spec", {}), "benchmark_market": benchmark_market}
    )
    benchmark_market = payload_benchmark_market
    periods_per_year = infer_periods_per_year(timeframe)
    pandas_freq = timeframe_to_pandas_freq(timeframe)
    vectorbt_spec = replace(vectorbt_spec, freq=pandas_freq)

    feature_rows = build_feature_table(candle_rows, feature_specs)
    universe_rows = build_universe_table(feature_rows, universe_spec)
    weight_rows = build_weight_table(universe_rows, weight_spec)
    eval_weight_rows = [
        row
        for row in weight_rows
        if eval_start <= _parse_timestamp(row["date_utc"]) < eval_end
    ]
    if not eval_weight_rows:
        return {"status": "empty_weights"}

    allowed_markets = set(candidate["universe_spec"].get("allowed_markets", []))
    if benchmark_market:
        allowed_markets.add(benchmark_market)
    price_rows = [row for row in candle_rows if row.market in allowed_markets]
    price_frame = build_price_frame(price_rows, price_column=vectorbt_spec.price_column)
    if price_frame.empty:
        return {"status": "empty_prices"}
    eval_price_frame = price_frame.loc[(price_frame.index >= eval_start) & (price_frame.index < eval_end)]
    if eval_price_frame.empty:
        return {"status": "empty_prices"}

    target_weight_frame = build_target_weight_frame(eval_weight_rows, eval_price_frame)
    portfolio = run_portfolio_from_target_weights(
        price_frame=eval_price_frame,
        target_weight_frame=target_weight_frame,
        spec=vectorbt_spec,
    )
    equity_curve = portfolio.value()
    benchmark_curve = build_benchmark_curve(eval_price_frame, benchmark_market, vectorbt_spec.init_cash)
    aligned_benchmark_curve = benchmark_curve.reindex(equity_curve.index).ffill()
    strategy_returns = compute_return_series(equity_curve)
    benchmark_returns = compute_return_series(aligned_benchmark_curve)
    ir_stats = compute_information_ratio(
        strategy_returns,
        benchmark_returns,
        annualization_factor=periods_per_year,
    )
    excess_returns, _ = compute_excess_curves(
        equity_curve,
        aligned_benchmark_curve,
        vectorbt_spec.init_cash,
    )
    del excess_returns
    summary = portfolio.stats(settings={"freq": pandas_freq})
    summary = pd.concat(
        [
            summary,
            benchmark_summary(
                aligned_benchmark_curve,
                vectorbt_spec.init_cash,
                benchmark_market,
                annualization_factor=periods_per_year,
            ),
            ir_stats,
            compute_recent_1y_stats(
                equity_curve,
                aligned_benchmark_curve,
                annualization_factor=periods_per_year,
            ),
            compute_recent_2y_stats(
                equity_curve,
                aligned_benchmark_curve,
                annualization_factor=periods_per_year,
            ),
            compute_drawdown_recovery_stats(equity_curve),
        ]
    )
    summary.loc["CAGR [%]"] = compute_annualized_return(
        equity_curve,
        annualization_factor=periods_per_year,
    ) * 100.0
    summary.loc["Timeframe"] = timeframe
    summary.loc["Periods Per Year"] = periods_per_year
    result = {"status": "ok"}
    result.update(_preferred_summary_fields(summary))
    return result


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = json.loads(Path(args.grid_config_json).read_text(encoding="utf-8"))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    timeframe = str(config.get("timeframe", "60m"))
    candle_dir = Path(config.get("candle_dir", "data/upbit/minutes/60"))
    ranking_metric = str(args.ranking_metric)
    vectorbt_spec_template = config["vectorbt_spec_template"]
    benchmark_market = str(vectorbt_spec_template.get("benchmark_market", "KRW-BTC"))
    allowed_markets = tuple(config["universe_spec_template"].get("allowed_markets", []))
    if not allowed_markets:
        raise ValueError("Grid config must have at least one allowed market")
    primary_market = str(allowed_markets[0])

    raw_combinations = _grid_combinations(config.get("grid", {}))
    constraints = list(config.get("constraints", []))
    combinations = [combo for combo in raw_combinations if _passes_constraints(combo, constraints)]
    candidates = [_candidate_from_combo(config, combo) for combo in combinations]

    candle_rows = load_all_candles(candle_dir)
    market_rows = [row for row in candle_rows if row.market in {primary_market, benchmark_market}]
    timestamps = _available_timestamps(market_rows, primary_market)
    folds = _build_folds(timestamps, args.is_months, args.oos_months, args.step_months)
    if not folds:
        raise ValueError("No folds available for the requested window sizes")

    fold_candidate_rows: list[dict[str, Any]] = []
    fold_winner_rows: list[dict[str, Any]] = []

    for fold_index, fold in enumerate(folds, start=1):
        is_rows = _filter_rows_by_window(market_rows, None, fold["is_end"])
        oos_rows = _filter_rows_by_window(market_rows, None, fold["oos_end"])
        is_eval_start = fold["is_start"]
        is_eval_end = fold["is_end"]
        oos_eval_start = fold["oos_start"]
        oos_eval_end = fold["oos_end"]

        fold_rows: list[dict[str, Any]] = []
        for candidate in candidates:
            is_result = _run_candidate_window(
                candidate,
                is_rows,
                is_eval_start,
                is_eval_end,
                benchmark_market,
                timeframe,
            )
            oos_result = _run_candidate_window(
                candidate,
                oos_rows,
                oos_eval_start,
                oos_eval_end,
                benchmark_market,
                timeframe,
            )
            row = {
                "asset": primary_market.replace("KRW-", ""),
                "fold": fold_index,
                "candidate_label": candidate["label"],
                "is_start": is_eval_start.isoformat(),
                "is_end": is_eval_end.isoformat(),
                "oos_start": oos_eval_start.isoformat(),
                "oos_end": oos_eval_end.isoformat(),
            }
            row.update({f"is_{key}": value for key, value in is_result.items()})
            row.update({f"oos_{key}": value for key, value in oos_result.items()})
            fold_rows.append(row)
        fold_candidate_rows.extend(fold_rows)

        ok_is_rows = [
            row
            for row in fold_rows
            if row.get("is_status") == "ok" and pd.notna(_safe_float(row.get(f"is_{ranking_metric}")))
        ]
        if not ok_is_rows:
            continue
        winner = max(ok_is_rows, key=lambda row: _safe_float(row.get(f"is_{ranking_metric}")))
        fold_winner_rows.append(
            {
                "asset": primary_market.replace("KRW-", ""),
                "fold": fold_index,
                "winner_label": winner["candidate_label"],
                "is_start": is_eval_start.isoformat(),
                "is_end": is_eval_end.isoformat(),
                "oos_start": oos_eval_start.isoformat(),
                "oos_end": oos_eval_end.isoformat(),
                f"is_{ranking_metric}": winner.get(f"is_{ranking_metric}"),
                "oos_Annualized Information Ratio": winner.get("oos_Annualized Information Ratio"),
                "oos_CAGR [%]": winner.get("oos_CAGR [%]"),
                "oos_Max Drawdown [%]": winner.get("oos_Max Drawdown [%]"),
                "oos_Longest Peak-to-Recovery Bars": winner.get("oos_Longest Peak-to-Recovery Bars"),
            }
        )

    fold_candidate_frame = pd.DataFrame(fold_candidate_rows)
    fold_winner_frame = pd.DataFrame(fold_winner_rows)
    fold_candidate_frame.to_csv(out_dir / "fold_candidates.csv", index=False)
    fold_winner_frame.to_csv(out_dir / "fold_winners.csv", index=False)
    _summarize_candidates(fold_candidate_frame, fold_winner_frame, ranking_metric).to_csv(
        out_dir / "candidate_summary.csv",
        index=False,
    )
    _summarize_winners(fold_winner_frame, ranking_metric).to_csv(
        out_dir / "walkforward_summary.csv",
        index=False,
    )
    print(f"{primary_market.replace('KRW-', '')}: {len(folds)} folds, {len(candidates)} grid candidates")


if __name__ == "__main__":
    main()
