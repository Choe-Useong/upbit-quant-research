#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import replace
from pathlib import Path
from typing import Any

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
import sys

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.vectorbt_adapter import build_price_frame, build_target_weight_frame, run_portfolio_from_target_weights
from lib.weights import build_weight_table
from lib.universe import build_universe_table
from lib.features import build_feature_table
from scripts.run_grid import (
    _load_feature_specs_from_payload,
    _load_universe_spec_from_payload,
    _load_vectorbt_spec_from_payload,
    _load_weight_spec_from_payload,
    _preferred_summary_fields,
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
    periods_per_day_for_timeframe,
    timeframe_to_pandas_freq,
    load_all_candles,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run walk-forward validation over a fixed candidate set.")
    parser.add_argument("--candidates-json", required=True, help="Walk-forward candidate definition JSON")
    parser.add_argument(
        "--out-dir",
        default="data/validation/60m_walkforward",
        help="Directory to write fold and summary CSVs",
    )
    parser.add_argument("--is-months", type=int, default=None, help="Override in-sample months")
    parser.add_argument("--oos-months", type=int, default=None, help="Override out-of-sample months")
    parser.add_argument("--step-months", type=int, default=None, help="Override step months")
    parser.add_argument(
        "--window-mode",
        choices=("rolling", "expanding"),
        default="rolling",
        help="Use a fixed rolling IS window or an expanding IS window anchored at the first timestamp",
    )
    parser.add_argument(
        "--ranking-metric",
        default="Annualized Information Ratio",
        help="IS metric used to choose each fold winner",
    )
    return parser


def _parse_timestamp(value: str) -> pd.Timestamp:
    return pd.Timestamp(value)


def _filter_rows_by_window(candle_rows: list, start: pd.Timestamp, end: pd.Timestamp) -> list:
    filtered = []
    for row in candle_rows:
        timestamp = _parse_timestamp(row.date_utc)
        if start <= timestamp < end:
            filtered.append(row)
    return filtered


def _filter_rows_before_end(candle_rows: list, end: pd.Timestamp) -> list:
    filtered = []
    for row in candle_rows:
        timestamp = _parse_timestamp(row.date_utc)
        if timestamp < end:
            filtered.append(row)
    return filtered


def _required_markets(candidate: dict[str, Any], benchmark_market: str) -> tuple[str, ...]:
    markets = set(candidate["universe_spec"].get("allowed_markets", []))
    if benchmark_market:
        markets.add(benchmark_market)
    return tuple(sorted(markets))


def _primary_market(asset_payload: dict[str, Any]) -> str:
    if asset_payload.get("primary_market"):
        return str(asset_payload["primary_market"])
    if asset_payload.get("benchmark_market"):
        return str(asset_payload["benchmark_market"])
    first_candidate = asset_payload["candidates"][0]
    allowed = first_candidate["universe_spec"].get("allowed_markets", [])
    if not allowed:
        raise ValueError(f"Missing primary market for asset {asset_payload.get('asset', '')}")
    return str(allowed[0])


def _available_timestamps(candle_rows: list, market: str) -> pd.DatetimeIndex:
    timestamps = sorted({_parse_timestamp(row.date_utc) for row in candle_rows if row.market == market})
    return pd.DatetimeIndex(timestamps)


def _build_folds(
    timestamps: pd.DatetimeIndex,
    is_months: int,
    oos_months: int,
    step_months: int,
    window_mode: str,
) -> list[dict[str, pd.Timestamp]]:
    if timestamps.empty:
        return []
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


def _run_candidate(
    candidate: dict[str, Any],
    candle_rows: list,
    benchmark_market: str,
    timeframe: str,
    evaluation_start: pd.Timestamp | None = None,
    evaluation_end: pd.Timestamp | None = None,
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
    if evaluation_start is not None and evaluation_end is not None:
        weight_rows = [
            row
            for row in weight_rows
            if evaluation_start <= pd.Timestamp(row["date_utc"]) < evaluation_end
        ]
    if not weight_rows:
        return {"status": "empty_weights"}

    required_markets = _required_markets(candidate, benchmark_market)
    run_candle_rows = [row for row in candle_rows if row.market in required_markets]
    if evaluation_start is not None and evaluation_end is not None:
        run_candle_rows = [
            row
            for row in run_candle_rows
            if evaluation_start <= _parse_timestamp(row.date_utc) < evaluation_end
        ]
    price_frame = build_price_frame(run_candle_rows, price_column=vectorbt_spec.price_column)
    if price_frame.empty:
        return {"status": "empty_prices"}

    target_weight_frame = build_target_weight_frame(weight_rows, price_frame)
    portfolio = run_portfolio_from_target_weights(
        price_frame=price_frame,
        target_weight_frame=target_weight_frame,
        spec=vectorbt_spec,
    )
    equity_curve = portfolio.value()
    benchmark_curve = build_benchmark_curve(price_frame, benchmark_market, vectorbt_spec.init_cash)
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


def _safe_float(value: Any) -> float:
    if value in ("", None):
        return float("nan")
    return float(value)


def _summarize_candidates(
    frame: pd.DataFrame,
    winners_frame: pd.DataFrame,
    ranking_metric: str,
) -> pd.DataFrame:
    summary_rows: list[dict[str, Any]] = []
    winner_counts_by_asset_candidate: dict[tuple[str, str], int] = defaultdict(int)
    winner_shares_by_asset_candidate: dict[tuple[str, str], float] = {}
    for asset, asset_winners in winners_frame.groupby("asset"):
        asset_fold_count = int(len(asset_winners))
        asset_counter = Counter(asset_winners["winner_label"])
        for candidate_label, count in asset_counter.items():
            key = (asset, candidate_label)
            winner_counts_by_asset_candidate[key] = count
            winner_shares_by_asset_candidate[key] = count / asset_fold_count if asset_fold_count else float("nan")
    for (asset, candidate_label), candidate_frame in frame.groupby(["asset", "candidate_label"]):
        oos_air = pd.to_numeric(candidate_frame["oos_Annualized Information Ratio"], errors="coerce")
        oos_cagr = pd.to_numeric(candidate_frame["oos_CAGR [%]"], errors="coerce")
        oos_mdd = pd.to_numeric(candidate_frame["oos_Max Drawdown [%]"], errors="coerce")
        oos_recovery = pd.to_numeric(candidate_frame["oos_Longest Peak-to-Recovery Bars"], errors="coerce")
        is_metric = pd.to_numeric(candidate_frame[f"is_{ranking_metric}"], errors="coerce")
        winner_key = (asset, candidate_label)
        row = {
            "asset": asset,
            "candidate_label": candidate_label,
            "fold_count": int(len(candidate_frame)),
            "winner_count": winner_counts_by_asset_candidate.get(winner_key, 0),
            "winner_share": winner_shares_by_asset_candidate.get(winner_key, 0.0),
            f"median_is_{ranking_metric}": is_metric.median(),
            f"mean_is_{ranking_metric}": is_metric.mean(),
            "median_oos_air": oos_air.median(),
            "mean_oos_air": oos_air.mean(),
            "worst_oos_air": oos_air.min(),
            "best_oos_air": oos_air.max(),
            "oos_air_positive_ratio": oos_air.gt(0).mean(),
            "median_oos_cagr_pct": oos_cagr.median(),
            "mean_oos_cagr_pct": oos_cagr.mean(),
            "best_oos_cagr_pct": oos_cagr.max(),
            "worst_oos_mdd_pct": oos_mdd.max(),
            "median_oos_longest_recovery_bars": oos_recovery.median(),
            "mean_oos_longest_recovery_bars": oos_recovery.mean(),
        }
        summary_rows.append(row)
    return pd.DataFrame(summary_rows)


def _summarize_winners(frame: pd.DataFrame, ranking_metric: str) -> pd.DataFrame:
    summary_rows: list[dict[str, Any]] = []
    for asset, asset_frame in frame.groupby("asset"):
        winner_counter = Counter(asset_frame["winner_label"])
        oos_air = pd.to_numeric(asset_frame["oos_Annualized Information Ratio"], errors="coerce")
        oos_cagr = pd.to_numeric(asset_frame["oos_CAGR [%]"], errors="coerce")
        oos_mdd = pd.to_numeric(asset_frame["oos_Max Drawdown [%]"], errors="coerce")
        oos_recovery = pd.to_numeric(asset_frame["oos_Longest Peak-to-Recovery Bars"], errors="coerce")
        fold_count = int(len(asset_frame))
        top_winner_label, top_winner_count = winner_counter.most_common(1)[0]
        winner_shares = {label: count / fold_count for label, count in winner_counter.items()}
        row = {
            "asset": asset,
            "fold_count": fold_count,
            "median_oos_air": oos_air.median(),
            "mean_oos_air": oos_air.mean(),
            "worst_oos_air": oos_air.min(),
            "best_oos_air": oos_air.max(),
            "oos_air_positive_ratio": oos_air.gt(0).mean(),
            "median_oos_cagr_pct": oos_cagr.median(),
            "mean_oos_cagr_pct": oos_cagr.mean(),
            "best_oos_cagr_pct": oos_cagr.max(),
            "worst_oos_mdd_pct": oos_mdd.max(),
            "median_oos_longest_recovery_bars": oos_recovery.median(),
            "mean_oos_longest_recovery_bars": oos_recovery.mean(),
            "ranking_metric": ranking_metric,
            "top_winner_label": top_winner_label,
            "top_winner_count": top_winner_count,
            "top_winner_share": top_winner_count / fold_count,
            "winner_counts": json.dumps(dict(winner_counter), ensure_ascii=False),
            "winner_shares": json.dumps(winner_shares, ensure_ascii=False),
        }
        summary_rows.append(row)
    return pd.DataFrame(summary_rows)


def main() -> None:
    args = build_parser().parse_args()
    payload = json.loads(Path(args.candidates_json).read_text(encoding="utf-8-sig"))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    root_timeframe = str(payload.get("timeframe", "60m"))
    root_is_months = int(args.is_months or payload.get("is_months", 24))
    root_oos_months = int(args.oos_months or payload.get("oos_months", 12))
    root_step_months = int(args.step_months or payload.get("step_months", 6))
    ranking_metric = args.ranking_metric
    window_mode = args.window_mode

    fold_candidate_rows: list[dict[str, Any]] = []
    fold_winner_rows: list[dict[str, Any]] = []

    for asset_payload in payload["assets"]:
        asset = str(asset_payload["asset"])
        timeframe = str(asset_payload.get("timeframe", root_timeframe))
        candle_dir = Path(asset_payload.get("candle_dir", payload.get("candle_dir", "data/upbit/minutes/60")))
        benchmark_market = str(asset_payload.get("benchmark_market", _primary_market(asset_payload)))
        is_months = int(asset_payload.get("is_months", root_is_months))
        oos_months = int(asset_payload.get("oos_months", root_oos_months))
        step_months = int(asset_payload.get("step_months", root_step_months))

        candle_rows = load_all_candles(candle_dir)
        primary_market = _primary_market(asset_payload)
        timestamps = _available_timestamps(candle_rows, primary_market)
        folds = _build_folds(timestamps, is_months, oos_months, step_months, window_mode)
        print(f"{asset}: {len(folds)} folds")

        asset_out_dir = out_dir / asset.lower()
        asset_out_dir.mkdir(parents=True, exist_ok=True)

        for fold_idx, fold in enumerate(folds, start=1):
            fold_label = f"fold_{fold_idx:02d}"
            is_rows = _filter_rows_before_end(candle_rows, fold["is_end"])
            oos_rows = _filter_rows_before_end(candle_rows, fold["oos_end"])
            if not is_rows or not oos_rows:
                continue

            candidate_is_rows: list[dict[str, Any]] = []
            for candidate in asset_payload["candidates"]:
                label = str(candidate["label"])
                is_result = _run_candidate(
                    candidate,
                    is_rows,
                    benchmark_market,
                    timeframe,
                    evaluation_start=fold["is_start"],
                    evaluation_end=fold["is_end"],
                )
                oos_result = _run_candidate(
                    candidate,
                    oos_rows,
                    benchmark_market,
                    timeframe,
                    evaluation_start=fold["oos_start"],
                    evaluation_end=fold["oos_end"],
                )
                row = {
                    "asset": asset,
                    "timeframe": timeframe,
                    "fold": fold_label,
                    "candidate_label": label,
                    "is_start": fold["is_start"].isoformat(),
                    "is_end": fold["is_end"].isoformat(),
                    "oos_start": fold["oos_start"].isoformat(),
                    "oos_end": fold["oos_end"].isoformat(),
                }
                for prefix, result in (("is", is_result), ("oos", oos_result)):
                    row[f"{prefix}_status"] = result.get("status", "")
                    for key, value in result.items():
                        if key == "status":
                            continue
                        row[f"{prefix}_{key}"] = value
                fold_candidate_rows.append(row)
                candidate_is_rows.append(row)

            ok_is_rows = [
                row
                for row in candidate_is_rows
                if row.get("is_status") == "ok" and pd.notna(_safe_float(row.get(f"is_{ranking_metric}")))
            ]
            if not ok_is_rows:
                continue
            winner = max(ok_is_rows, key=lambda row: _safe_float(row.get(f"is_{ranking_metric}")))
            fold_winner_rows.append(
                {
                    "asset": asset,
                    "timeframe": timeframe,
                    "fold": fold_label,
                    "winner_label": winner["candidate_label"],
                    "is_start": winner["is_start"],
                    "is_end": winner["is_end"],
                    "oos_start": winner["oos_start"],
                    "oos_end": winner["oos_end"],
                    f"is_{ranking_metric}": winner.get(f"is_{ranking_metric}"),
                    "is_CAGR [%]": winner.get("is_CAGR [%]"),
                    "is_Max Drawdown [%]": winner.get("is_Max Drawdown [%]"),
                    "oos_Annualized Information Ratio": winner.get("oos_Annualized Information Ratio"),
                    "oos_CAGR [%]": winner.get("oos_CAGR [%]"),
                    "oos_Max Drawdown [%]": winner.get("oos_Max Drawdown [%]"),
                    "oos_Longest Peak-to-Recovery Bars": winner.get("oos_Longest Peak-to-Recovery Bars"),
                }
            )

        pd.DataFrame(
            [row for row in fold_candidate_rows if row["asset"] == asset]
        ).to_csv(asset_out_dir / "fold_candidate_results.csv", index=False, encoding="utf-8-sig")
        pd.DataFrame(
            [row for row in fold_winner_rows if row["asset"] == asset]
        ).to_csv(asset_out_dir / "fold_winners.csv", index=False, encoding="utf-8-sig")

    fold_candidate_frame = pd.DataFrame(fold_candidate_rows)
    fold_winner_frame = pd.DataFrame(fold_winner_rows)
    fold_candidate_frame.to_csv(out_dir / "fold_candidate_results.csv", index=False, encoding="utf-8-sig")
    fold_winner_frame.to_csv(out_dir / "fold_winners.csv", index=False, encoding="utf-8-sig")
    _summarize_candidates(fold_candidate_frame, fold_winner_frame, ranking_metric).to_csv(
        out_dir / "candidate_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    _summarize_winners(fold_winner_frame, ranking_metric).to_csv(
        out_dir / "walkforward_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )


if __name__ == "__main__":
    main()
