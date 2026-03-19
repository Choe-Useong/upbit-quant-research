#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
import sys

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.vectorbt_adapter import VectorBTSpec, run_portfolio_from_target_weights
from scripts.run_grid import _grid_combinations, _passes_constraints
from scripts.run_vectorbt import (
    compute_annualized_return,
    compute_max_drawdown_pct,
    infer_periods_per_year,
    load_all_candles,
    timeframe_to_pandas_freq,
)
from scripts.walkforward.run_fast_ma_cross_walkforward import _simulate_window_from_target_weights


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate fast exact mixed MA simulation against vectorbt.")
    parser.add_argument("--grid-config-json", required=True, help="Grid configuration JSON path")
    parser.add_argument("--label-a", required=True, help="First run_name label to mix")
    parser.add_argument("--label-b", required=True, help="Second run_name label to mix")
    parser.add_argument("--weight-a", type=float, default=0.5, help="Mix weight for label-a")
    parser.add_argument("--weight-b", type=float, default=0.5, help="Mix weight for label-b")
    parser.add_argument("--out-dir", required=True, help="Directory to write comparison outputs")
    return parser


def _build_signal_map(config: dict[str, Any], price: pd.Series) -> dict[str, pd.Series]:
    raw_combinations = _grid_combinations(config.get("grid", {}))
    constraints = list(config.get("constraints", []))
    combos = [combo for combo in raw_combinations if _passes_constraints(combo, constraints)]
    all_windows = sorted(
        {int(combo["short"]) for combo in combos}
        | {int(combo["long"]) for combo in combos}
        | {int(combo["mid"]) for combo in combos if "mid" in combo}
    )
    ma_map = {window: price.rolling(window).mean() for window in all_windows}
    signal_map: dict[str, pd.Series] = {}
    for combo in combos:
        short = int(combo["short"])
        long = int(combo["long"])
        run_name = config["run_name_template"].format(**combo)
        if "mid" in combo:
            mid = int(combo["mid"])
            signal_map[run_name] = (ma_map[short] > ma_map[mid]) & (ma_map[mid] > ma_map[long])
        else:
            signal_map[run_name] = ma_map[short] > ma_map[long]
    return signal_map


def _series_value(value: Any) -> float:
    if isinstance(value, pd.Series):
        return float(value.iloc[0])
    return float(value)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = json.loads(Path(args.grid_config_json).read_text(encoding="utf-8"))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    allowed_markets = tuple(config["universe_spec_template"].get("allowed_markets", []))
    if len(allowed_markets) != 1:
        raise ValueError("Validation expects exactly one allowed market")
    market = allowed_markets[0]
    timeframe = str(config.get("timeframe", "60m"))
    periods_per_year = infer_periods_per_year(timeframe)
    fee = float(config["vectorbt_spec_template"].get("fees", 0.0))
    slippage = float(config["vectorbt_spec_template"].get("slippage", 0.0))
    if slippage != 0.0:
        raise ValueError("This validator currently supports slippage=0 only")

    rows = load_all_candles(Path(config["candle_dir"]))
    price_rows = [row for row in rows if row.market == market]
    if not price_rows:
        raise ValueError(f"No rows for {market}")
    price = (
        pd.DataFrame(
            [{"date_utc": pd.Timestamp(r.date_utc), "price": float(r.trade_price)} for r in price_rows]
        )
        .sort_values("date_utc")
        .drop_duplicates("date_utc")
        .set_index("date_utc")["price"]
    )

    signal_map = _build_signal_map(config, price)
    if args.label_a not in signal_map:
        raise ValueError(f"Unknown label-a: {args.label_a}")
    if args.label_b not in signal_map:
        raise ValueError(f"Unknown label-b: {args.label_b}")

    total_weight = float(args.weight_a + args.weight_b)
    if total_weight <= 0:
        raise ValueError("Mix weights must sum to a positive value")
    weight_a = float(args.weight_a) / total_weight
    weight_b = float(args.weight_b) / total_weight

    target_weight = (
        signal_map[args.label_a].shift(1).fillna(False).astype(float) * weight_a
        + signal_map[args.label_b].shift(1).fillna(False).astype(float) * weight_b
    ).clip(lower=0.0, upper=1.0)

    fast_equity = _simulate_window_from_target_weights(price, target_weight, fee)

    price_frame = price.to_frame(name=market)
    target_weight_frame = target_weight.to_frame(name=market)
    portfolio = run_portfolio_from_target_weights(
        price_frame=price_frame,
        target_weight_frame=target_weight_frame,
        spec=VectorBTSpec(
            init_cash=1_000_000.0,
            fees=fee,
            slippage=0.0,
            cash_sharing=True,
            group_by=True,
            size_type="targetpercent",
            call_seq="auto",
            freq=timeframe_to_pandas_freq(timeframe),
        ),
    )
    vectorbt_equity = portfolio.value()
    if isinstance(vectorbt_equity, pd.DataFrame):
        vectorbt_equity = vectorbt_equity.iloc[:, 0]

    aligned = pd.concat(
        [
            fast_equity.rename("fast_equity"),
            pd.Series(vectorbt_equity, index=price.index, name="vectorbt_equity"),
        ],
        axis=1,
    ).dropna()
    aligned["abs_diff"] = (aligned["fast_equity"] - aligned["vectorbt_equity"]).abs()

    comparison = pd.Series(
        {
            "label_a": args.label_a,
            "label_b": args.label_b,
            "weight_a": weight_a,
            "weight_b": weight_b,
            "row_count": len(aligned),
            "max_abs_diff": float(aligned["abs_diff"].max()),
            "end_value_fast": float(aligned["fast_equity"].iloc[-1]),
            "end_value_vectorbt": float(aligned["vectorbt_equity"].iloc[-1]),
            "cagr_fast_pct": compute_annualized_return(aligned["fast_equity"], periods_per_year) * 100.0,
            "cagr_vectorbt_pct": compute_annualized_return(aligned["vectorbt_equity"], periods_per_year) * 100.0,
            "mdd_fast_pct": compute_max_drawdown_pct(aligned["fast_equity"]),
            "mdd_vectorbt_pct": compute_max_drawdown_pct(aligned["vectorbt_equity"]),
            "total_trades_vectorbt": _series_value(portfolio.stats().get("Total Trades")),
        }
    )

    aligned.to_csv(out_dir / "equity_comparison.csv", encoding="utf-8-sig")
    comparison.to_frame(name="value").to_csv(out_dir / "comparison_summary.csv", encoding="utf-8-sig")
    print(comparison.to_string())


if __name__ == "__main__":
    main()
