#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.features_v2 import build_feature_frames_from_cache
from lib.universe_v2 import build_universe_mask_v2
from lib.weights_v2 import build_weight_frame_v2
from lib.vectorbt_adapter import VectorBTSpec, run_portfolio_from_target_weights
from scripts.build_features import load_feature_specs
from scripts.build_universe import load_universe_spec
from scripts.build_weights import load_weight_spec
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
    compute_rolling_information_ratio,
    infer_periods_per_year,
    infer_timeframe,
    load_price_frame,
    periods_per_day_for_timeframe,
    print_summary,
    summarize_rolling_information_ratio,
    timeframe_to_pandas_freq,
    trim_frames_to_first_weight,
    write_equity_csv,
    write_summary_csv,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run frame-native v2 cross-sectional backtest from wide source cache."
    )
    parser.add_argument("--candle-dir", required=True, help="Raw candle dir for fallback/timeframe metadata")
    parser.add_argument("--source-cache-dir", required=True, help="Wide source cache directory")
    parser.add_argument("--feature-spec-json", required=True, help="Feature spec JSON path")
    parser.add_argument("--universe-spec-json", required=True, help="Universe spec JSON path")
    parser.add_argument("--weight-spec-json", required=True, help="Weight spec JSON path")
    parser.add_argument("--portfolio-dir", required=True, help="Output directory for v2 weight files")
    parser.add_argument("--backtest-out-dir", required=True, help="Output directory for vectorbt backtest files")
    parser.add_argument("--fees", type=float, default=0.0, help="Per-order proportional fees")
    parser.add_argument("--slippage", type=float, default=0.0, help="Per-order proportional slippage")
    parser.add_argument("--benchmark-market", default="KRW-BTC", help="Benchmark market for vectorbt summary")
    parser.add_argument("--max-markets", type=int, default=None, help="Optional market limit for smoke runs")
    parser.add_argument("--tail-hours", type=int, default=None, help="Optional tail length for smoke runs")
    parser.add_argument(
        "--save-weights-parquet",
        action="store_true",
        help="Save weights_v2.parquet for inspection; disabled by default to reduce I/O",
    )
    parser.add_argument(
        "--save-target-weights-full",
        action="store_true",
        help="Write target_weights_full.csv; disabled by default to reduce output size",
    )
    parser.add_argument(
        "--save-excess-returns-csv",
        action="store_true",
        help="Write excess_returns.csv; disabled by default to reduce output size",
    )
    parser.add_argument(
        "--save-rolling-ir-csv",
        action="store_true",
        help="Write rolling_information_ratio.csv; disabled by default to reduce output size",
    )
    return parser


def _write_wide_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = frame.copy()
    output.index = pd.to_datetime(output.index, utc=False)
    output = output.sort_index().sort_index(axis=1)
    output = output.reset_index(names="date_utc")
    output.to_parquet(path, index=False)


def main() -> None:
    args = build_parser().parse_args()
    source_cache_dir = Path(args.source_cache_dir)
    portfolio_dir = Path(args.portfolio_dir)
    out_dir = Path(args.backtest_out_dir)
    portfolio_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    feature_specs = load_feature_specs(Path(args.feature_spec_json))
    universe_spec = load_universe_spec(Path(args.universe_spec_json))
    weight_spec = load_weight_spec(Path(args.weight_spec_json))

    feature_frames = build_feature_frames_from_cache(
        source_cache_dir,
        feature_specs,
        max_markets=args.max_markets,
        tail_rows=args.tail_hours,
    )
    warning_frame = pd.read_parquet(source_cache_dir / "market_warning.parquet")
    warning_frame.index = pd.to_datetime(warning_frame.index, utc=False)
    if args.max_markets is not None:
        warning_frame = warning_frame.reindex(columns=sorted(warning_frame.columns)[: args.max_markets])
    reference_index = next(iter(feature_frames.values())).index if feature_frames else warning_frame.index
    warning_frame = warning_frame.reindex(index=reference_index)

    universe_result = build_universe_mask_v2(feature_frames, warning_frame, universe_spec)
    weight_frame = build_weight_frame_v2(universe_result.selection_mask, weight_spec)

    if args.save_weights_parquet:
        weights_path = portfolio_dir / "weights_v2.parquet"
        _write_wide_frame(weights_path, weight_frame)

    price_frame = load_price_frame(
        Path(args.candle_dir),
        "trade_price",
        load_mode="wide",
        source_cache_dir=source_cache_dir,
    )
    price_frame, weight_frame, trimmed_start_timestamp = trim_frames_to_first_weight(
        price_frame,
        weight_frame,
    )

    timeframe = infer_timeframe(Path(args.candle_dir), None)
    periods_per_year = infer_periods_per_year(timeframe)
    periods_per_day = periods_per_day_for_timeframe(timeframe)
    pandas_freq = timeframe_to_pandas_freq(timeframe)

    portfolio = run_portfolio_from_target_weights(
        price_frame=price_frame,
        target_weight_frame=weight_frame,
        spec=VectorBTSpec(
            price_column="trade_price",
            init_cash=1_000_000.0,
            fees=args.fees,
            slippage=args.slippage,
            freq=pandas_freq,
        ),
    )

    summary = portfolio.stats(settings={"freq": pandas_freq})
    equity_curve = portfolio.value()
    benchmark_curve = build_benchmark_curve(price_frame, args.benchmark_market, 1_000_000.0)
    aligned_benchmark_curve = benchmark_curve.reindex(equity_curve.index).ffill()
    benchmark_stats = benchmark_summary(
        benchmark_curve,
        1_000_000.0,
        args.benchmark_market,
        annualization_factor=periods_per_year,
    )
    strategy_returns = compute_return_series(equity_curve)
    benchmark_returns = compute_return_series(aligned_benchmark_curve)
    ir_stats = compute_information_ratio(
        strategy_returns,
        benchmark_returns,
        annualization_factor=periods_per_year,
    )
    excess_returns, excess_equity_curve = compute_excess_curves(
        equity_curve,
        aligned_benchmark_curve,
        1_000_000.0,
    )
    rolling_ir = compute_rolling_information_ratio(
        excess_returns,
        periods_per_day=periods_per_day,
        annualization_factor=periods_per_year,
    )
    recent_1y_stats = compute_recent_1y_stats(
        equity_curve,
        aligned_benchmark_curve,
        annualization_factor=periods_per_year,
    )
    recent_2y_stats = compute_recent_2y_stats(
        equity_curve,
        aligned_benchmark_curve,
        annualization_factor=periods_per_year,
    )
    rolling_ir_summary = summarize_rolling_information_ratio(rolling_ir)
    summary.loc["CAGR [%]"] = compute_annualized_return(
        equity_curve,
        annualization_factor=periods_per_year,
    ) * 100.0
    summary.loc["Load Mode"] = "wide"
    summary.loc["Weights CSV Format"] = "wide"
    summary.loc["Trim Start Mode"] = "first_weight"
    if trimmed_start_timestamp is not None:
        summary.loc["Trimmed Start Timestamp"] = trimmed_start_timestamp.isoformat()
    summary.loc["Timeframe"] = timeframe
    summary.loc["Periods Per Year"] = periods_per_year
    summary = pd.concat(
        [
            summary,
            benchmark_stats,
            ir_stats,
            recent_1y_stats,
            recent_2y_stats,
            compute_drawdown_recovery_stats(equity_curve),
            rolling_ir_summary,
        ]
    )

    print_summary(summary)
    write_summary_csv(out_dir / "summary.csv", summary)
    write_equity_csv(out_dir / "equity_curve.csv", equity_curve)
    write_equity_csv(out_dir / "benchmark_curve.csv", aligned_benchmark_curve)
    write_equity_csv(out_dir / "excess_equity_curve.csv", excess_equity_curve)
    if args.save_excess_returns_csv:
        write_equity_csv(out_dir / "excess_returns.csv", excess_returns)
    if args.save_rolling_ir_csv:
        write_equity_csv(out_dir / "rolling_information_ratio.csv", rolling_ir)
    if args.save_target_weights_full:
        weight_frame.to_csv(out_dir / "target_weights_full.csv", encoding="utf-8-sig")

    print("Resolved load mode: wide")
    print(f"Wrote vectorbt results to {out_dir}")


if __name__ == "__main__":
    main()
