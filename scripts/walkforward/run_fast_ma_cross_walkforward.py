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

from scripts.run_grid import _grid_combinations, _passes_constraints
from scripts.run_vectorbt import (
    compute_annualized_return,
    compute_drawdown_recovery_stats,
    compute_information_ratio,
    compute_max_drawdown_pct,
    compute_return_series,
    infer_periods_per_year,
    load_all_candles,
)
from scripts.walkforward.run_walkforward_validation import _summarize_candidates, _summarize_winners


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fast walk-forward validation for simple MA cross grids.")
    parser.add_argument("--grid-config-json", required=True, help="Grid configuration JSON path")
    parser.add_argument("--out-dir", required=True, help="Output directory")
    parser.add_argument("--is-months", type=int, default=18)
    parser.add_argument("--oos-months", type=int, default=6)
    parser.add_argument("--step-months", type=int, default=3)
    parser.add_argument(
        "--window-mode",
        choices=("rolling", "expanding"),
        default="rolling",
        help="Use a fixed rolling IS window or an expanding IS window anchored at the first timestamp",
    )
    parser.add_argument("--ranking-metric", default="Annualized Information Ratio")
    return parser


def _safe_float(value: Any) -> float:
    if value in ("", None):
        return float("nan")
    return float(value)


def _build_folds(
    timestamps: pd.DatetimeIndex,
    is_months: int,
    oos_months: int,
    step_months: int,
    window_mode: str,
) -> list[dict[str, pd.Timestamp]]:
    folds: list[dict[str, pd.Timestamp]] = []
    initial_start = pd.Timestamp(timestamps.min())
    current_start = initial_start
    max_timestamp = pd.Timestamp(timestamps.max())
    while True:
        is_end = current_start + pd.DateOffset(months=is_months)
        oos_end = is_end + pd.DateOffset(months=oos_months)
        if oos_end > max_timestamp:
            break
        folds.append(
            {
                "is_start": initial_start if window_mode == "expanding" else current_start,
                "is_end": is_end,
                "oos_start": is_end,
                "oos_end": oos_end,
            }
        )
        current_start = current_start + pd.DateOffset(months=step_months)
    return folds


def _window_mask(index: pd.DatetimeIndex, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    return (index >= start) & (index < end)


def _simulate_window_from_target_weights(
    window_price: pd.Series,
    target_weight: pd.Series,
    fee: float,
    init_cash: float = 1_000_000.0,
) -> pd.Series:
    aligned_target_weight = (
        pd.to_numeric(target_weight.reindex(window_price.index), errors="coerce")
        .fillna(0.0)
        .clip(lower=0.0, upper=1.0)
    )

    cash = float(init_cash)
    units = 0.0
    equity_values: list[float] = []

    for timestamp, price_value in window_price.items():
        current_price = float(price_value)
        current_target_weight = float(aligned_target_weight.loc[timestamp])

        equity_before_trade = cash + (units * current_price)
        target_value = equity_before_trade * current_target_weight
        current_value = units * current_price
        value_diff = target_value - current_value

        if abs(value_diff) > 1e-12:
            if value_diff > 0.0:
                # Solve A' = w * E' under fees, where:
                # A' = A + b, E' = E - fee * b
                buy_notional = min(value_diff / (1.0 + (current_target_weight * fee)), cash)
                if buy_notional > 0.0:
                    fee_paid = buy_notional * fee
                    units += buy_notional / current_price
                    cash -= buy_notional + fee_paid
            else:
                # Solve A' = w * E' under fees, where:
                # A' = A - s, E' = E - fee * s
                sell_value = min(
                    (-value_diff) / max(1.0 - (current_target_weight * fee), 1e-12),
                    current_value,
                )
                if sell_value > 0.0:
                    units_to_sell = sell_value / current_price
                    proceeds = sell_value * (1.0 - fee)
                    units -= units_to_sell
                    cash += proceeds

        equity_values.append(cash + (units * current_price))

    return pd.Series(equity_values, index=window_price.index, dtype=float)


def _run_window(
    price: pd.Series,
    signal: pd.Series,
    start: pd.Timestamp,
    end: pd.Timestamp,
    fee: float,
    periods_per_year: int,
) -> dict[str, Any]:
    mask = _window_mask(price.index, start, end)
    window_price = price.loc[mask]
    if window_price.empty:
        return {"status": "empty_prices"}

    target_weight = signal.shift(1).reindex(window_price.index)
    target_weight = target_weight.where(pd.notna(target_weight), False).astype(float)
    equity = _simulate_window_from_target_weights(window_price, target_weight, fee)

    asset_returns = window_price.pct_change().fillna(0.0)
    benchmark = (1.0 + asset_returns).cumprod() * 1_000_000.0
    ir = compute_information_ratio(
        compute_return_series(equity),
        compute_return_series(benchmark),
        annualization_factor=periods_per_year,
    )
    dd = compute_drawdown_recovery_stats(equity)
    return {
        "status": "ok",
        "Annualized Information Ratio": float(ir["Annualized Information Ratio"]),
        "CAGR [%]": compute_annualized_return(equity, annualization_factor=periods_per_year) * 100.0,
        "Max Drawdown [%]": compute_max_drawdown_pct(equity),
        "Longest Peak-to-Recovery Bars": float(dd["Longest Peak-to-Recovery Bars"]),
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = json.loads(Path(args.grid_config_json).read_text(encoding="utf-8"))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    allowed_markets = tuple(config["universe_spec_template"].get("allowed_markets", []))
    if len(allowed_markets) != 1:
        raise ValueError("Fast runner expects exactly one allowed market")
    market = allowed_markets[0]
    timeframe = str(config.get("timeframe", "60m"))
    periods_per_year = infer_periods_per_year(timeframe)
    fee = float(config["vectorbt_spec_template"].get("fees", 0.0))

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

    raw_combinations = _grid_combinations(config.get("grid", {}))
    constraints = list(config.get("constraints", []))
    combos = [combo for combo in raw_combinations if _passes_constraints(combo, constraints)]
    folds = _build_folds(price.index, args.is_months, args.oos_months, args.step_months, args.window_mode)

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

    fold_candidate_rows: list[dict[str, Any]] = []
    fold_winner_rows: list[dict[str, Any]] = []
    asset_label = market.replace("KRW-", "")

    for fold_index, fold in enumerate(folds, start=1):
        fold_rows: list[dict[str, Any]] = []
        for combo in combos:
            run_name = config["run_name_template"].format(**combo)
            signal = signal_map[run_name]
            is_result = _run_window(
                price,
                signal,
                fold["is_start"],
                fold["is_end"],
                fee,
                periods_per_year,
            )
            oos_result = _run_window(
                price,
                signal,
                fold["oos_start"],
                fold["oos_end"],
                fee,
                periods_per_year,
            )
            row = {
                "asset": asset_label,
                "fold": fold_index,
                "candidate_label": run_name,
                "is_start": fold["is_start"].isoformat(),
                "is_end": fold["is_end"].isoformat(),
                "oos_start": fold["oos_start"].isoformat(),
                "oos_end": fold["oos_end"].isoformat(),
            }
            row.update({f"is_{key}": value for key, value in is_result.items()})
            row.update({f"oos_{key}": value for key, value in oos_result.items()})
            fold_rows.append(row)
        fold_candidate_rows.extend(fold_rows)

        ok_is_rows = [
            row
            for row in fold_rows
            if row.get("is_status") == "ok" and pd.notna(_safe_float(row.get(f"is_{args.ranking_metric}")))
        ]
        if not ok_is_rows:
            continue
        winner = max(ok_is_rows, key=lambda row: _safe_float(row.get(f"is_{args.ranking_metric}")))
        fold_winner_rows.append(
            {
                "asset": asset_label,
                "fold": fold_index,
                "winner_label": winner["candidate_label"],
                "is_start": fold["is_start"].isoformat(),
                "is_end": fold["is_end"].isoformat(),
                "oos_start": fold["oos_start"].isoformat(),
                "oos_end": fold["oos_end"].isoformat(),
                f"is_{args.ranking_metric}": winner.get(f"is_{args.ranking_metric}"),
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
    _summarize_candidates(fold_candidate_frame, fold_winner_frame, args.ranking_metric).to_csv(
        out_dir / "candidate_summary.csv",
        index=False,
    )
    _summarize_winners(fold_winner_frame, args.ranking_metric).to_csv(
        out_dir / "walkforward_summary.csv",
        index=False,
    )
    print(f"{asset_label}: {len(folds)} folds, {len(combos)} grid candidates, mode={args.window_mode}")


if __name__ == "__main__":
    main()
