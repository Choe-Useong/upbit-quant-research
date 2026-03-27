#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.dataframes import (
    apply_by_market_column,
    build_wide_frames_from_candle_dir,
    compute_market_momentum_frame,
    compute_market_residual_momentum_frame,
    compute_market_rolling_sum_frame,
    compute_market_turnover_weighted_momentum_frame,
)
from lib.storage import write_table_csv
from lib.legacy.weights import weight_columns


DEFAULT_EXCLUDE_MARKETS = [
    "KRW-BTC",
    "KRW-ETH",
    "KRW-XRP",
    "KRW-ADA",
    "KRW-DOGE",
    "KRW-SOL",
    "KRW-AVAX",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build sparse target weights for a cross-sectional turnover + momentum strategy."
    )
    parser.add_argument(
        "--candle-dir",
        default="data/upbit_research/minutes/60",
        help="Directory containing per-market 60m candle CSV files",
    )
    parser.add_argument(
        "--output-csv",
        default="data/portfolio/upbit_turnover_momentum_60m/weights.csv",
        help="Output weights CSV path",
    )
    parser.add_argument(
        "--metadata-json",
        default="",
        help="Optional JSON metadata output path",
    )
    parser.add_argument("--turnover-hours", type=int, default=24)
    parser.add_argument("--lookback-hours", type=int, default=24 * 30)
    parser.add_argument("--momentum-hours", type=int, default=48)
    parser.add_argument(
        "--momentum-mode",
        choices=["price", "turnover_weighted", "residual_btc"],
        default="price",
        help="Momentum definition: plain price return, turnover-weighted mean return, or BTC-residual momentum",
    )
    parser.add_argument(
        "--benchmark-market",
        default="KRW-BTC",
        help="Benchmark market used for beta/residual calculations",
    )
    parser.add_argument(
        "--beta-lookback-hours",
        type=int,
        default=48,
        help="Lookback used for rolling beta estimation when momentum-mode=residual_btc",
    )
    parser.add_argument(
        "--signal-lag-hours",
        type=int,
        default=1,
        help="Lag applied to turnover/momentum eligibility before rebalancing to avoid same-bar lookahead",
    )
    parser.add_argument("--require-positive-momentum", action="store_true")
    parser.add_argument(
        "--min-momentum-cross-percentile",
        type=float,
        default=0.0,
        help="Optional cross-sectional momentum percentile floor in [0, 1], applied before selection",
    )
    parser.add_argument(
        "--max-momentum-cross-percentile",
        type=float,
        default=1.0,
        help="Optional cross-sectional momentum percentile ceiling in [0, 1], applied before selection",
    )
    parser.add_argument(
        "--max-positions",
        type=int,
        default=0,
        help="Optional cap on selected markets per rebalance, keeping highest-momentum names only; 0 means keep all eligible",
    )
    parser.add_argument("--turnover-quantiles", type=int, default=5)
    parser.add_argument(
        "--turnover-buckets",
        default="1,2,3",
        help="Comma-separated turnover buckets to keep, with Q1 as low turnover score",
    )
    parser.add_argument("--rebalance-hours", type=int, default=24)
    parser.add_argument("--gross-exposure", type=float, default=1.0)
    parser.add_argument(
        "--min-cross-section-size",
        type=int,
        default=0,
        help="Minimum number of valid markets at a timestamp for cross-sectional ranks to be trusted; 0 disables the filter",
    )
    parser.add_argument(
        "--min-age-bars",
        type=int,
        default=24 * 180,
        help="Minimum number of 60m bars required before a market becomes eligible",
    )
    parser.add_argument("--exclude-warnings", action="store_true")
    parser.add_argument(
        "--exclude-markets",
        default=",".join(DEFAULT_EXCLUDE_MARKETS),
        help="Comma-separated markets to exclude",
    )
    return parser


def _build_score_series(turnover_series: pd.Series, lookback_hours: int) -> pd.Series:
    return turnover_series.rolling(lookback_hours, min_periods=lookback_hours).rank(pct=True)


def _bucketize_cross_section(percentile_frame: pd.DataFrame, quantiles: int) -> pd.DataFrame:
    clipped = percentile_frame.clip(lower=0.0, upper=1.0)
    buckets = np.ceil(clipped * quantiles).astype("float64")
    buckets = buckets.where(clipped.notna(), np.nan)
    return buckets.clip(lower=1, upper=quantiles)


def _parse_bucket_values(raw: str) -> tuple[int, ...]:
    values = tuple(sorted({int(part.strip()) for part in raw.split(",") if part.strip()}))
    if not values:
        raise ValueError("turnover-buckets must contain at least one bucket")
    return values


def main() -> None:
    args = build_parser().parse_args()
    if not 0.0 <= args.min_momentum_cross_percentile <= 1.0:
        raise SystemExit("--min-momentum-cross-percentile must be within [0, 1]")
    if not 0.0 <= args.max_momentum_cross_percentile <= 1.0:
        raise SystemExit("--max-momentum-cross-percentile must be within [0, 1]")
    if args.min_momentum_cross_percentile > args.max_momentum_cross_percentile:
        raise SystemExit("--min-momentum-cross-percentile cannot exceed --max-momentum-cross-percentile")
    if args.max_positions < 0:
        raise SystemExit("--max-positions must be >= 0")
    if args.min_cross_section_size < 0:
        raise SystemExit("--min-cross-section-size must be >= 0")

    candle_dir = ROOT_DIR / args.candle_dir
    output_csv = ROOT_DIR / args.output_csv
    metadata_json = ROOT_DIR / args.metadata_json if args.metadata_json else None
    excluded_markets = {item.strip().upper() for item in args.exclude_markets.split(",") if item.strip()}
    turnover_buckets = _parse_bucket_values(args.turnover_buckets)

    raw_frames, meta_by_market = build_wide_frames_from_candle_dir(
        candle_dir,
        value_columns=["trade_price", "candle_acc_trade_price"],
        pattern="KRW-*.csv",
    )
    if raw_frames["trade_price"].empty:
        raise SystemExit(f"No candle files found in {candle_dir}")

    eligible_markets = [
        market
        for market in raw_frames["trade_price"].columns
        if market not in excluded_markets
        and (not args.exclude_warnings or meta_by_market[market]["market_warning"] == "NONE")
    ]
    if not eligible_markets:
        raise SystemExit("No eligible markets remained after filtering")

    benchmark_price = raw_frames["trade_price"].get(args.benchmark_market)
    if args.momentum_mode == "residual_btc" and benchmark_price is None:
        raise SystemExit(f"Benchmark market not found for residual momentum: {args.benchmark_market}")

    price_frame = raw_frames["trade_price"][eligible_markets].sort_index().sort_index(axis=1)
    trade_value_frame = raw_frames["candle_acc_trade_price"][eligible_markets].sort_index().sort_index(axis=1)
    valid_rows = price_frame.notna().any(axis=1)
    price_frame = price_frame.loc[valid_rows]
    trade_value_frame = trade_value_frame.loc[valid_rows]
    if benchmark_price is not None:
        benchmark_price = benchmark_price.reindex(price_frame.index)
    turnover_frame = compute_market_rolling_sum_frame(trade_value_frame, args.turnover_hours)
    score_frame = apply_by_market_column(
        turnover_frame,
        lambda series: _build_score_series(series, args.lookback_hours),
    )
    if args.momentum_mode == "price":
        momentum_frame = compute_market_momentum_frame(price_frame, args.momentum_hours)
    elif args.momentum_mode == "turnover_weighted":
        momentum_frame = compute_market_turnover_weighted_momentum_frame(
            price_frame,
            trade_value_frame,
            args.momentum_hours,
        )
    else:
        momentum_frame = compute_market_residual_momentum_frame(
            price_frame,
            benchmark_price,
            args.momentum_hours,
            args.beta_lookback_hours,
        )
    age_frame = price_frame.notna().cumsum().astype(float)

    cross_pct_frame = score_frame.rank(axis=1, pct=True, method="average")
    turnover_bucket_frame = _bucketize_cross_section(cross_pct_frame, args.turnover_quantiles)
    momentum_cross_pct_frame = momentum_frame.rank(axis=1, pct=True, method="average")
    cross_section_count = (score_frame.notna() & momentum_frame.notna() & (age_frame >= float(args.min_age_bars))).sum(
        axis=1
    )
    cross_section_ok = cross_section_count >= int(args.min_cross_section_size)

    eligible = turnover_bucket_frame.isin(turnover_buckets) & (age_frame >= float(args.min_age_bars))
    if args.require_positive_momentum:
        eligible &= momentum_frame > 0.0
    if args.min_momentum_cross_percentile > 0.0:
        eligible &= momentum_cross_pct_frame >= args.min_momentum_cross_percentile
    if args.max_momentum_cross_percentile < 1.0:
        eligible &= momentum_cross_pct_frame <= args.max_momentum_cross_percentile
    if args.signal_lag_hours > 0:
        eligible = eligible.shift(args.signal_lag_hours)
        momentum_frame = momentum_frame.shift(args.signal_lag_hours)
        momentum_cross_pct_frame = momentum_cross_pct_frame.shift(args.signal_lag_hours)
        cross_section_ok = cross_section_ok.shift(args.signal_lag_hours)
    cross_section_ok = cross_section_ok.reindex(eligible.index, fill_value=False)
    cross_section_ok = cross_section_ok.where(cross_section_ok.notna(), False).astype(bool)
    eligible = eligible & cross_section_ok.to_numpy()[:, None]
    eligible = eligible.infer_objects(copy=False).fillna(False).astype(bool)

    rebalance_index = price_frame.index[:: args.rebalance_hours]
    weight_rows: list[dict[str, str]] = []

    for timestamp in rebalance_index:
        row_mask = eligible.loc[timestamp]
        selected_markets = sorted(row_mask[row_mask].index.tolist())
        if not selected_markets:
            continue
        date_utc = timestamp.isoformat()
        date_kst = (pd.Timestamp(timestamp) + pd.Timedelta(hours=9)).isoformat()

        selected_with_mom = sorted(
            selected_markets,
            key=lambda market: float(momentum_frame.at[timestamp, market]),
            reverse=True,
        )
        if args.max_positions > 0:
            selected_with_mom = selected_with_mom[: args.max_positions]
        if not selected_with_mom:
            continue
        target_weight = args.gross_exposure / len(selected_with_mom)
        feature_column = f"{args.momentum_mode}_mom_{args.momentum_hours}"
        for local_rank, market in enumerate(selected_with_mom, start=1):
            meta = meta_by_market[market]
            feature_value = float(momentum_frame.at[timestamp, market])
            weight_rows.append(
                {
                    "date_utc": date_utc,
                    "date_kst": date_kst,
                    "market": market,
                    "korean_name": meta["korean_name"],
                    "english_name": meta["english_name"],
                    "market_warning": meta["market_warning"],
                    "feature_column": feature_column,
                    "feature_value": f"{feature_value:.12g}",
                    "rank": str(local_rank),
                    "selected_rank": str(local_rank),
                    "weight_rank": str(local_rank),
                    "target_weight": f"{target_weight:.12g}",
                    "gross_exposure": f"{args.gross_exposure:.12g}",
                    "weighting": "equal",
                    "rebalance_frequency": "sparse",
                    "weights_name": "upbit_turnover_momentum_sparse",
                    "universe_name": "upbit_turnover_momentum_sparse",
                }
            )

    write_table_csv(output_csv, weight_rows, weight_columns())
    print(f"Wrote {len(weight_rows)} weight rows to {output_csv}")

    if metadata_json is not None:
        metadata_json.parent.mkdir(parents=True, exist_ok=True)
        metadata = {
            "parameter_turnover_hours": args.turnover_hours,
            "parameter_lookback_hours": args.lookback_hours,
            "parameter_momentum_hours": args.momentum_hours,
            "parameter_momentum_mode": args.momentum_mode,
            "parameter_benchmark_market": args.benchmark_market,
            "parameter_beta_lookback_hours": args.beta_lookback_hours,
            "parameter_signal_lag_hours": args.signal_lag_hours,
            "parameter_require_positive_momentum": bool(args.require_positive_momentum),
            "parameter_min_momentum_cross_percentile": args.min_momentum_cross_percentile,
            "parameter_max_momentum_cross_percentile": args.max_momentum_cross_percentile,
            "parameter_max_positions": args.max_positions,
            "parameter_turnover_quantiles": args.turnover_quantiles,
            "parameter_turnover_buckets": ",".join(str(item) for item in turnover_buckets),
            "parameter_rebalance_hours": args.rebalance_hours,
            "parameter_gross_exposure": args.gross_exposure,
            "parameter_min_cross_section_size": args.min_cross_section_size,
            "parameter_min_age_bars": args.min_age_bars,
            "parameter_excluded_markets": ",".join(sorted(excluded_markets)),
            "selected_rebalance_rows": len(weight_rows),
        }
        metadata_json.write_text(json.dumps(metadata, indent=2), encoding="utf-8-sig")
        print(f"Wrote metadata to {metadata_json}")


if __name__ == "__main__":
    main()
