#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import itertools
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.dataframes import build_long_frame_from_candle_dir, build_wide_frames_from_candle_dir
from lib.features_v2 import build_feature_frames_from_cache
from lib.spec_io import (
    load_feature_specs_from_payload,
    load_universe_spec_from_payload,
    load_weight_spec_from_payload,
)
from lib.universe_v2 import build_universe_mask_v2
from lib.vectorbt_adapter import VectorBTSpec, run_portfolio_from_target_weights
from lib.weights_v2 import build_weight_frame_v2
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


DEFAULT_CACHE_VALUE_COLUMNS = (
    "trade_price",
    "opening_price",
    "high_price",
    "low_price",
    "candle_acc_trade_volume",
    "candle_acc_trade_price",
    "timestamp",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run frame-native v2 cross-sectional grid from config JSON."
    )
    parser.add_argument("--config-json", required=True, help="Cross-sectional grid configuration JSON path")
    return parser


def _render_value(template: Any, context: dict[str, Any]) -> Any:
    if isinstance(template, dict):
        return {key: _render_value(value, context) for key, value in template.items()}
    if isinstance(template, list):
        return [_render_value(value, context) for value in template]
    if isinstance(template, str):
        if template.startswith("{") and template.endswith("}") and template.count("{") == 1 and template.count("}") == 1:
            key = template[1:-1]
            if key in context:
                return context[key]
        return template.format(**context)
    return template


def _grid_combinations(grid: dict[str, list[Any]]) -> list[dict[str, Any]]:
    if not grid:
        return [{}]
    keys = list(grid.keys())
    values = [grid[key] for key in keys]
    return [dict(zip(keys, combo)) for combo in itertools.product(*values)]


def _passes_constraints(combo: dict[str, Any], constraints: list[str]) -> bool:
    if not constraints:
        return True
    safe_globals = {"__builtins__": {}}
    safe_locals = dict(combo)
    for constraint in constraints:
        if not bool(eval(constraint, safe_globals, safe_locals)):
            return False
    return True


def _canonical_payload_key(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _selection_stats(weight_frame: pd.DataFrame) -> dict[str, float | int]:
    rebalance_mask = weight_frame.notna().any(axis=1)
    if not bool(rebalance_mask.any()):
        return {
            "rebalance_dates": 0,
            "avg_selected_count": 0.0,
            "min_selected_count": 0,
            "max_selected_count": 0,
            "weight_rows": 0,
        }
    counts = weight_frame.loc[rebalance_mask].fillna(0.0).gt(0.0).sum(axis=1)
    values = [int(value) for value in counts.tolist()]
    return {
        "rebalance_dates": len(values),
        "avg_selected_count": (sum(values) / len(values)) if values else 0.0,
        "min_selected_count": min(values) if values else 0,
        "max_selected_count": max(values) if values else 0,
        "weight_rows": int(sum(values)),
    }


def _write_numeric_cache(
    candle_dir: Path,
    cache_dir: Path,
    max_markets: int | None,
    tail_rows: int | None,
) -> None:
    frames, meta_by_market = build_wide_frames_from_candle_dir(
        candle_dir,
        DEFAULT_CACHE_VALUE_COLUMNS,
        max_markets=max_markets,
        tail_rows=tail_rows,
    )
    cache_dir.mkdir(parents=True, exist_ok=True)
    for column, frame in frames.items():
        frame.to_parquet(cache_dir / f"{column}.parquet")
    meta_frame = (
        pd.DataFrame.from_records(sorted(meta_by_market.values(), key=lambda item: item["market"]))
        if meta_by_market
        else pd.DataFrame(columns=["market", "korean_name", "english_name", "market_warning"])
    )
    meta_frame.to_parquet(cache_dir / "market_meta.parquet", index=False)


def _write_warning_cache(
    candle_dir: Path,
    cache_dir: Path,
    max_markets: int | None,
    tail_rows: int | None,
) -> None:
    long_frame = build_long_frame_from_candle_dir(
        candle_dir,
        usecols=["date_utc", "market", "market_warning"],
        max_markets=max_markets,
        tail_rows=tail_rows,
    )
    if long_frame.empty:
        warning_frame = pd.DataFrame()
    else:
        warning_frame = (
            long_frame.assign(
                date_utc=pd.to_datetime(long_frame["date_utc"], utc=False),
                market=long_frame["market"].astype(str).str.upper(),
                market_warning=long_frame["market_warning"].astype(str).str.upper(),
            )
            .pivot(index="date_utc", columns="market", values="market_warning")
            .sort_index()
            .sort_index(axis=1)
        )
    warning_frame.to_parquet(cache_dir / "market_warning.parquet")


def _resolve_source_cache_dir(
    config: dict[str, Any],
    candle_dir: Path,
    out_dir: Path,
) -> Path:
    configured = config.get("source_cache_dir")
    if configured:
        return Path(str(configured))

    parts = candle_dir.as_posix().split("/")
    if len(parts) >= 4 and parts[0] == "data":
        timeframe = parts[-1]
        if parts[1] == "upbit_research" and parts[2] == "minutes":
            candidate = ROOT_DIR / "data" / "upbit_research_cache" / timeframe
            if candidate.exists():
                return candidate
        if parts[1] == "upbit" and parts[2] == "minutes":
            candidate = ROOT_DIR / "data" / "upbit_cache" / timeframe
            if candidate.exists():
                return candidate

    cache_dir = out_dir / "_source_cache"
    if not cache_dir.exists():
        _write_numeric_cache(
            candle_dir,
            cache_dir,
            max_markets=config.get("max_markets"),
            tail_rows=config.get("tail_hours"),
        )
        _write_warning_cache(
            candle_dir,
            cache_dir,
            max_markets=config.get("max_markets"),
            tail_rows=config.get("tail_hours"),
        )
    return cache_dir


def _run_v2_backtest(
    candle_dir: Path,
    source_cache_dir: Path,
    feature_frames: dict[str, pd.DataFrame],
    universe_payload: dict[str, Any],
    weight_payload: dict[str, Any],
    vectorbt_payload: dict[str, Any],
    backtest_dir: Path,
    max_markets: int | None,
    tail_hours: int | None,
    compute_rolling_ir_enabled: bool,
    print_run_summary: bool,
) -> tuple[pd.Series, pd.DataFrame]:
    universe_spec = load_universe_spec_from_payload(universe_payload)
    weight_spec = load_weight_spec_from_payload(weight_payload)

    warning_frame = pd.read_parquet(source_cache_dir / "market_warning.parquet")
    warning_frame.index = pd.to_datetime(warning_frame.index, utc=False)
    if max_markets is not None:
        warning_frame = warning_frame.reindex(columns=sorted(warning_frame.columns)[:max_markets])
    reference_index = next(iter(feature_frames.values())).index
    warning_frame = warning_frame.reindex(index=reference_index)

    universe_result = build_universe_mask_v2(feature_frames, warning_frame, universe_spec)
    weight_frame = build_weight_frame_v2(universe_result.selection_mask, weight_spec)

    price_column = str(vectorbt_payload.get("price_column", "trade_price"))
    init_cash = float(vectorbt_payload.get("init_cash", 1_000_000.0))
    fees = float(vectorbt_payload.get("fees", 0.0))
    slippage = float(vectorbt_payload.get("slippage", 0.0))
    benchmark_market = str(vectorbt_payload.get("benchmark_market", "KRW-BTC"))

    price_frame = load_price_frame(
        candle_dir,
        price_column,
        load_mode="wide",
        source_cache_dir=source_cache_dir,
    )
    price_frame, weight_frame, trimmed_start_timestamp = trim_frames_to_first_weight(
        price_frame,
        weight_frame,
    )

    timeframe = infer_timeframe(candle_dir, None)
    periods_per_year = infer_periods_per_year(timeframe)
    periods_per_day = periods_per_day_for_timeframe(timeframe)
    pandas_freq = timeframe_to_pandas_freq(timeframe)

    portfolio = run_portfolio_from_target_weights(
        price_frame=price_frame,
        target_weight_frame=weight_frame,
        spec=VectorBTSpec(
            price_column=price_column,
            init_cash=init_cash,
            fees=fees,
            slippage=slippage,
            freq=pandas_freq,
        ),
    )

    summary = portfolio.stats(settings={"freq": pandas_freq})
    if "Benchmark Return [%]" in summary.index:
        summary = summary.rename(index={"Benchmark Return [%]": "VectorBT Benchmark Return [%]"})
    equity_curve = portfolio.value()
    benchmark_curve = build_benchmark_curve(price_frame, benchmark_market, init_cash)
    aligned_benchmark_curve = benchmark_curve.reindex(equity_curve.index).ffill()
    benchmark_stats = benchmark_summary(
        benchmark_curve,
        init_cash,
        benchmark_market,
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
        init_cash,
    )
    rolling_ir = pd.DataFrame(index=excess_returns.index)
    if compute_rolling_ir_enabled:
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

    backtest_dir.mkdir(parents=True, exist_ok=True)
    if print_run_summary:
        print_summary(summary)
    write_summary_csv(backtest_dir / "summary.csv", summary)
    write_equity_csv(backtest_dir / "equity_curve.csv", equity_curve)
    write_equity_csv(backtest_dir / "benchmark_curve.csv", aligned_benchmark_curve)
    write_equity_csv(backtest_dir / "excess_equity_curve.csv", excess_equity_curve)
    if compute_rolling_ir_enabled:
        write_equity_csv(backtest_dir / "rolling_information_ratio.csv", rolling_ir)
    return summary, weight_frame


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config_path = Path(args.config_json)
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))

    candle_dir_value = config.get("candle_dir") or config.get("daily_dir")
    if not candle_dir_value:
        raise SystemExit("config must define candle_dir or daily_dir")
    candle_dir = Path(str(candle_dir_value))

    out_dir = Path(config.get("out_dir", "data/grid/cross_section_v2"))
    out_dir.mkdir(parents=True, exist_ok=True)
    source_cache_dir = _resolve_source_cache_dir(config, candle_dir, out_dir)

    run_name_template = config["run_name_template"]
    compute_rolling_ir_enabled = bool(config.get("compute_rolling_ir", False))
    print_run_summaries = bool(config.get("print_run_summaries", False))
    max_markets = config.get("max_markets")
    tail_hours = config.get("tail_hours")

    raw_combinations = _grid_combinations(config.get("grid", {}))
    combinations = [combo for combo in raw_combinations if _passes_constraints(combo, config.get("constraints", []))]
    if not combinations:
        raise SystemExit("No valid grid combinations after applying constraints")

    shared_feature_payload = config.get("shared_feature_spec_template")
    shared_feature_frames: dict[str, pd.DataFrame] | None = None
    shared_feature_rows: int | None = None
    if shared_feature_payload:
        shared_specs = load_feature_specs_from_payload(shared_feature_payload)
        shared_feature_frames = build_feature_frames_from_cache(
            source_cache_dir,
            shared_specs,
            max_markets=max_markets,
            tail_rows=tail_hours,
        )
        primary_frame = next(iter(shared_feature_frames.values()))
        shared_feature_rows = int(primary_frame.notna().sum().sum())

    feature_cache: dict[str, dict[str, pd.DataFrame]] = {}
    results: list[dict[str, Any]] = []

    total_runs = len(combinations)

    for run_index, combo in enumerate(combinations, start=1):
        context = dict(combo)
        run_name = run_name_template.format(**context)
        context["run_name"] = run_name
        print(f"[{run_index}/{total_runs}] {run_name}")

        run_dir = out_dir / run_name
        specs_dir = run_dir / "specs"
        backtest_dir = run_dir / "backtest"
        specs_dir.mkdir(parents=True, exist_ok=True)
        backtest_dir.mkdir(parents=True, exist_ok=True)

        universe_payload = _render_value(config["universe_spec_template"], context)
        weight_payload = _render_value(config["weight_spec_template"], context)
        vectorbt_payload = _render_value(config.get("vectorbt_spec_template", {}), context)
        feature_payload = None if shared_feature_frames is not None else _render_value(config["feature_spec_template"], context)

        if feature_payload is not None:
            (specs_dir / "features.json").write_text(
                json.dumps(feature_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        (specs_dir / "universe.json").write_text(
            json.dumps(universe_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (specs_dir / "weights.json").write_text(
            json.dumps(weight_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        result_row: dict[str, Any] = {"run_name": run_name, **combo}
        try:
            if shared_feature_frames is not None:
                feature_frames = shared_feature_frames
                result_row["feature_rows"] = shared_feature_rows
                result_row["features_source"] = "shared"
            else:
                feature_key = _canonical_payload_key(feature_payload)
                feature_frames = feature_cache.get(feature_key)
                if feature_frames is None:
                    feature_specs = load_feature_specs_from_payload(feature_payload)
                    feature_frames = build_feature_frames_from_cache(
                        source_cache_dir,
                        feature_specs,
                        max_markets=max_markets,
                        tail_rows=tail_hours,
                    )
                    feature_cache[feature_key] = feature_frames
                primary_frame = next(iter(feature_frames.values()))
                result_row["feature_rows"] = int(primary_frame.notna().sum().sum())
                result_row["features_source"] = "per_run"

            summary, weight_frame = _run_v2_backtest(
                candle_dir,
                source_cache_dir,
                feature_frames,
                universe_payload,
                weight_payload,
                vectorbt_payload,
                backtest_dir,
                max_markets=max_markets,
                tail_hours=tail_hours,
                compute_rolling_ir_enabled=compute_rolling_ir_enabled,
                print_run_summary=print_run_summaries,
            )
            result_row.update(_selection_stats(weight_frame))
            result_row["status"] = "ok"
            result_row.update(summary.to_dict())
            print(f"[{run_index}/{total_runs}] ok: {run_name}")
        except Exception as exc:
            result_row["status"] = "error"
            result_row["error"] = str(exc)
            print(f"[{run_index}/{total_runs}] error: {run_name} :: {exc}")
        results.append(result_row)

        columns: list[str] = []
        for row in results:
            for key in row.keys():
                if key not in columns:
                    columns.append(key)
        with (out_dir / "summary_results.csv").open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows(results)

    print(f"Wrote {len(results)} grid result rows to {out_dir / 'summary_results.csv'}")


if __name__ == "__main__":
    main()
