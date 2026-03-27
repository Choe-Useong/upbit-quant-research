#!/usr/bin/env python3
from __future__ import annotations

import argparse
import itertools
import json
import re
import sys
import webbrowser
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.legacy.features import (
    build_feature_table,
)
from lib.legacy.universe import build_universe_table
from lib.specs import (
    CompareSpec,
    FeatureSpec,
    LogicalSpec,
    RankFilterSpec,
    ScoreComponentSpec,
    StateSpec,
    TransformSpec,
    UniverseSpec,
    ValueFilterSpec,
    WeightSpec,
)
from lib.vectorbt_adapter import (
    VectorBTSpec,
    build_price_frame,
    build_target_weight_frame,
    run_portfolio_from_target_weights,
)
from lib.legacy.weights import build_weight_table
from scripts.run_vectorbt import (
    benchmark_summary,
    build_benchmark_curve,
    compute_excess_curves,
    compute_drawdown_recovery_stats,
    compute_information_ratio,
    compute_annualized_return,
    infer_periods_per_year,
    infer_timeframe,
    periods_per_day_for_timeframe,
    timeframe_to_pandas_freq,
    compute_return_series,
    compute_rolling_information_ratio,
    compute_recent_1y_stats,
    compute_recent_2y_stats,
    summarize_rolling_information_ratio,
    load_all_candles,
)


PLACEHOLDER_PATTERN = re.compile(r"^\{([a-zA-Z_][a-zA-Z0-9_]*)\}$")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run feature/universe/weight/vectorbt parameter grid and aggregate results."
    )
    parser.add_argument("--config-json", required=True, help="Grid configuration JSON path")
    parser.add_argument(
        "--open-plot",
        action="store_true",
        help="Open the generated top curve comparison plot in the default browser",
    )
    return parser


def _render_value(template: Any, context: dict[str, Any]) -> Any:
    if isinstance(template, dict):
        return {key: _render_value(value, context) for key, value in template.items()}
    if isinstance(template, list):
        return [_render_value(value, context) for value in template]
    if isinstance(template, str):
        matched = PLACEHOLDER_PATTERN.match(template)
        if matched:
            return context[matched.group(1)]
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
        try:
            passed = bool(eval(constraint, safe_globals, safe_locals))
        except Exception as exc:
            raise ValueError(f"Invalid grid constraint '{constraint}': {exc}") from exc
        if not passed:
            return False
    return True


def _load_feature_specs_from_payload(payload: list[dict[str, Any]]) -> list[FeatureSpec]:
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


def _load_universe_spec_from_payload(payload: dict[str, Any]) -> UniverseSpec:
    return UniverseSpec(
        feature_column=payload["feature_column"],
        sort_column=payload.get("sort_column"),
        lag=int(payload.get("lag", 1)),
        mode=payload.get("mode", "top_n"),
        top_n=int(payload.get("top_n", 30)),
        quantiles=int(payload.get("quantiles", 5)),
        bucket_values=tuple(int(value) for value in payload.get("bucket_values", [1])),
        ascending=bool(payload.get("ascending", False)),
        exclude_warnings=bool(payload.get("exclude_warnings", False)),
        min_age_days=(None if payload.get("min_age_days") is None else int(payload["min_age_days"])),
        allowed_markets=tuple(payload.get("allowed_markets", [])),
        excluded_markets=tuple(payload.get("excluded_markets", [])),
        value_filters=tuple(
            ValueFilterSpec(
                feature_column=item["feature_column"],
                operator=item["operator"],
                value=float(item["value"]),
                lag=int(item.get("lag", 0)),
            )
            for item in payload.get("value_filters", [])
        ),
        rank_filters=tuple(
            RankFilterSpec(
                feature_column=item["feature_column"],
                mode=item.get("mode", "top_n"),
                lag=int(item.get("lag", 0)),
                top_n=int(item.get("top_n", 30)),
                quantiles=int(item.get("quantiles", 5)),
                bucket_values=tuple(int(value) for value in item.get("bucket_values", [1])),
                ascending=bool(item.get("ascending", False)),
            )
            for item in payload.get("rank_filters", [])
        ),
        name=payload.get("name"),
    )


def _load_weight_spec_from_payload(payload: dict[str, Any]) -> WeightSpec:
    return WeightSpec(
        weighting=payload.get("weighting", "equal"),
        gross_exposure=float(payload.get("gross_exposure", 1.0)),
        rank_power=float(payload.get("rank_power", 1.0)),
        max_positions=(None if payload.get("max_positions") is None else int(payload["max_positions"])),
        universe_name=payload.get("universe_name"),
        rebalance_frequency=payload.get("rebalance_frequency", "daily"),
        feature_value_scale=float(payload.get("feature_value_scale", 1.0)),
        feature_value_clip_min=float(payload.get("feature_value_clip_min", 0.0)),
        feature_value_clip_max=float(payload.get("feature_value_clip_max", 1.0)),
        incremental_step_size=float(payload.get("incremental_step_size", 0.25)),
        incremental_step_up=(
            None
            if payload.get("incremental_step_up") is None
            else float(payload.get("incremental_step_up"))
        ),
        incremental_step_down=(
            None
            if payload.get("incremental_step_down") is None
            else float(payload.get("incremental_step_down"))
        ),
        incremental_min_weight=float(payload.get("incremental_min_weight", 0.0)),
        incremental_max_weight=float(payload.get("incremental_max_weight", 1.0)),
    )


def _load_vectorbt_spec_from_payload(payload: dict[str, Any]) -> tuple[VectorBTSpec, str]:
    benchmark_market = payload.get("benchmark_market", "KRW-BTC")
    return (
        VectorBTSpec(
            price_column=payload.get("price_column", "trade_price"),
            init_cash=float(payload.get("init_cash", 1_000_000.0)),
            fees=float(payload.get("fees", 0.0)),
            slippage=float(payload.get("slippage", 0.0)),
            cash_sharing=bool(payload.get("cash_sharing", True)),
            group_by=bool(payload.get("group_by", True)),
            size_type=payload.get("size_type", "targetpercent"),
            call_seq=payload.get("call_seq", "auto"),
        ),
        benchmark_market,
    )


def _selection_stats(weight_rows: list[dict[str, str]]) -> dict[str, float | int]:
    if not weight_rows:
        return {
            "rebalance_dates": 0,
            "avg_selected_count": 0.0,
            "min_selected_count": 0,
            "max_selected_count": 0,
        }
    counts = Counter(row["date_utc"] for row in weight_rows)
    values = list(counts.values())
    return {
        "rebalance_dates": len(values),
        "avg_selected_count": sum(values) / len(values),
        "min_selected_count": min(values),
        "max_selected_count": max(values),
    }


def _preferred_summary_fields(summary: pd.Series) -> dict[str, Any]:
    keys = [
        "Start Value",
        "End Value",
        "Total Return [%]",
        "CAGR [%]",
        "Longest Peak-to-Recovery Bars",
        "Second Longest Peak-to-Recovery Bars",
        "Timeframe",
        "Periods Per Year",
        "Benchmark Market",
        "Benchmark Total Return [%]",
        "Benchmark CAGR [%]",
        "Benchmark Max Drawdown [%]",
        "Benchmark Sharpe Ratio",
        "Benchmark Sortino Ratio",
        "Benchmark Calmar Ratio",
        "Information Ratio",
        "Annualized Information Ratio",
        "Recent 1Y Return [%]",
        "Recent 1Y Benchmark Return [%]",
        "Recent 1Y Information Ratio",
        "Recent 1Y AIR",
        "Recent 1Y Max Drawdown [%]",
        "Recent 2Y Return [%]",
        "Recent 2Y Benchmark Return [%]",
        "Recent 2Y Information Ratio",
        "Recent 2Y AIR",
        "Recent 2Y Max Drawdown [%]",
        "Max Drawdown [%]",
        "Sharpe Ratio",
        "Calmar Ratio",
        "Sortino Ratio",
        "Total Trades",
        "Win Rate [%]",
    ]
    result: dict[str, Any] = {}
    for key in keys:
        if key in summary.index:
            result[key] = summary[key]
    for key in summary.index:
        if str(key).startswith("Rolling IR "):
            result[str(key)] = summary[key]
    return result


def _attach_plateau_air_mean(
    result_frame: pd.DataFrame,
    grid_config: dict[str, list[Any]],
) -> pd.DataFrame:
    if result_frame.empty:
        return result_frame
    if "Annualized Information Ratio" not in result_frame.columns:
        return result_frame
    if "short" not in result_frame.columns or "long" not in result_frame.columns:
        return result_frame

    short_values = list(grid_config.get("short", []))
    long_values = list(grid_config.get("long", []))
    if not short_values or not long_values:
        return result_frame

    short_index = {value: idx for idx, value in enumerate(short_values)}
    long_index = {value: idx for idx, value in enumerate(long_values)}

    frame = result_frame.copy()
    frame["Plateau AIR Mean"] = float("nan")

    group_cols = ["status"]
    if "rebalance_frequency" in frame.columns:
        group_cols.append("rebalance_frequency")

    for _, group in frame.groupby(group_cols, dropna=False):
        ok_group = group[group["status"] == "ok"].copy()
        if ok_group.empty:
            continue
        for idx, row in ok_group.iterrows():
            try:
                s_idx = short_index[row["short"]]
                l_idx = long_index[row["long"]]
            except Exception:
                continue
            neighbors = ok_group[
                (ok_group.index != idx)
                & (ok_group["short"].map(short_index).sub(s_idx).abs() <= 1)
                & (ok_group["long"].map(long_index).sub(l_idx).abs() <= 1)
            ]
            if neighbors.empty:
                continue
            frame.loc[idx, "Plateau AIR Mean"] = pd.to_numeric(
                neighbors["Annualized Information Ratio"],
                errors="coerce",
            ).mean()
    return frame


def _render_run_payloads(config: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    return {
        "feature_payload": _render_value(config["feature_spec_template"], context),
        "universe_payload": _render_value(config["universe_spec_template"], context),
        "weight_payload": _render_value(config["weight_spec_template"], context),
        "vectorbt_payload": _render_value(config.get("vectorbt_spec_template", {}), context),
        "metadata_payload": _render_value(config.get("strategy_metadata_template", {}), context),
        "parameter_metadata_payload": _render_value(config.get("parameter_metadata_template", {}), context),
    }


def _feature_reference_markets(feature_payload: list[dict[str, Any]]) -> set[str]:
    markets: set[str] = set()
    for item in feature_payload:
        source = item.get("source")
        if isinstance(source, str) and source.startswith("market:"):
            _, market_code, _ = source.split(":", 2)
            markets.add(market_code)
    return markets


def _required_markets_for_run(
    feature_payload: list[dict[str, Any]],
    universe_payload: dict[str, Any],
    benchmark_market: str,
) -> tuple[str, ...]:
    markets = set(universe_payload.get("allowed_markets", []))
    markets.update(_feature_reference_markets(feature_payload))
    if benchmark_market:
        markets.add(benchmark_market)
    return tuple(sorted(markets))


def _filter_candle_rows(candle_rows: list, required_markets: tuple[str, ...]) -> list:
    if not required_markets:
        return candle_rows
    allowed = set(required_markets)
    return [row for row in candle_rows if row.market in allowed]


def _build_top_curves_figure(curves: dict[str, pd.Series], benchmark_curve: pd.Series | None) -> go.Figure:
    fig = go.Figure()
    for name, curve in curves.items():
        fig.add_trace(
            go.Scatter(
                x=curve.index,
                y=curve.values,
                mode="lines",
                name=name,
            )
        )
    if benchmark_curve is not None:
        fig.add_trace(
            go.Scatter(
                x=benchmark_curve.index,
                y=benchmark_curve.values,
                mode="lines",
                name=str(benchmark_curve.name or "Benchmark"),
                line={"width": 3},
            )
        )
    fig.update_layout(
        title="Top Grid Strategies vs Benchmark",
        xaxis_title="Date",
        yaxis_title="Portfolio Value",
    )
    return fig


def _build_rolling_ir_figure(rolling_ir: pd.DataFrame, run_name: str) -> go.Figure:
    fig = go.Figure()
    for column in rolling_ir.columns:
        fig.add_trace(
            go.Scatter(
                x=rolling_ir.index,
                y=rolling_ir[column],
                mode="lines",
                name=column,
            )
        )
    fig.update_layout(
        title=f"Rolling IR: {run_name}",
        xaxis_title="Date",
        yaxis_title="Annualized Information Ratio",
    )
    return fig


def _build_top_rolling_ir_figure(
    rolling_ir_by_run: dict[str, pd.DataFrame],
    column: str,
) -> go.Figure:
    fig = go.Figure()
    for run_name, rolling_ir in rolling_ir_by_run.items():
        if column not in rolling_ir.columns:
            continue
        series = rolling_ir[column]
        fig.add_trace(
            go.Scatter(
                x=series.index,
                y=series.values,
                mode="lines",
                name=run_name,
            )
        )
    fig.update_layout(
        title=f"Top Strategies Comparison: {column}",
        xaxis_title="Date",
        yaxis_title="Annualized Information Ratio",
    )
    return fig


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config_path = Path(args.config_json)
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    candle_dir = Path(config.get("candle_dir", config.get("daily_dir", "data/upbit/daily")))
    timeframe = infer_timeframe(candle_dir, config.get("timeframe"))
    periods_per_year = int(config.get("periods_per_year", infer_periods_per_year(timeframe)))
    periods_per_day = periods_per_day_for_timeframe(timeframe)
    pandas_freq = timeframe_to_pandas_freq(timeframe)
    out_dir = Path(config.get("out_dir", "data/grid/default"))
    out_dir.mkdir(parents=True, exist_ok=True)
    top_curve_count = int(config.get("top_curve_count", 5))
    ranking_metric = str(config.get("ranking_metric", "Total Return [%]"))
    compute_rolling_ir_enabled = bool(config.get("compute_rolling_ir", False))
    rolling_ir_windows = tuple(int(value) for value in config.get("rolling_ir_windows", [126, 252]))
    save_summary_plots = bool(config.get("save_summary_plots", False))
    save_rolling_ir_plots = bool(config.get("save_rolling_ir_plots", False))
    rolling_ir_dir = out_dir / "rolling_ir"
    if compute_rolling_ir_enabled:
        rolling_ir_dir.mkdir(parents=True, exist_ok=True)

    candle_rows = load_all_candles(candle_dir)
    if not candle_rows:
        raise SystemExit(f"No candle rows found in {candle_dir}")

    run_name_template = config.get("run_name_template", "run_{run_id}")
    raw_combinations = _grid_combinations(config.get("grid", {}))
    constraints = list(config.get("constraints", []))
    combinations = [combo for combo in raw_combinations if _passes_constraints(combo, constraints)]
    feature_cache: dict[str, list[dict[str, str]]] = {}
    filtered_candle_cache: dict[tuple[str, ...], list] = {}
    price_frame_cache: dict[tuple[str, tuple[str, ...]], pd.DataFrame] = {}
    result_rows: list[dict[str, Any]] = []
    run_payloads_by_name: dict[str, dict[str, Any]] = {}

    for run_idx, combo in enumerate(combinations, start=1):
        context = dict(combo)
        context["run_id"] = run_idx
        context["run_name"] = run_name_template.format(**context)

        rendered = _render_run_payloads(config, context)
        feature_payload = rendered["feature_payload"]
        universe_payload = rendered["universe_payload"]
        weight_payload = rendered["weight_payload"]
        vectorbt_payload = rendered["vectorbt_payload"]
        metadata_payload = rendered.get("metadata_payload", {})
        parameter_metadata_payload = rendered.get("parameter_metadata_payload", {})
        run_payloads_by_name[context["run_name"]] = rendered

        universe_spec = _load_universe_spec_from_payload(universe_payload)
        weight_spec = _load_weight_spec_from_payload(weight_payload)
        vectorbt_spec, benchmark_market = _load_vectorbt_spec_from_payload(vectorbt_payload)
        vectorbt_spec = replace(vectorbt_spec, freq=pandas_freq)
        required_markets = _required_markets_for_run(
            feature_payload,
            universe_payload,
            benchmark_market,
        )
        if required_markets not in filtered_candle_cache:
            filtered_candle_cache[required_markets] = _filter_candle_rows(candle_rows, required_markets)
        run_candle_rows = filtered_candle_cache[required_markets]

        feature_specs = _load_feature_specs_from_payload(feature_payload)
        feature_cache_key = json.dumps(
            {
                "feature_payload": feature_payload,
                "required_markets": required_markets,
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        if feature_cache_key not in feature_cache:
            feature_cache[feature_cache_key] = build_feature_table(run_candle_rows, feature_specs)
        feature_rows = feature_cache[feature_cache_key]

        universe_rows = build_universe_table(feature_rows, universe_spec)
        weight_rows = build_weight_table(universe_rows, weight_spec)
        selection_stats = _selection_stats(weight_rows)

        result_row: dict[str, Any] = {"run_name": context["run_name"], **combo}
        for key in ("strategy_family", "strategy_label", "asset_scope"):
            value = metadata_payload.get(key, "")
            if value not in (None, ""):
                result_row[key] = value
        for key, value in parameter_metadata_payload.items():
            if str(key).startswith("parameter_") and value not in (None, ""):
                result_row[str(key)] = value
        result_row.update(selection_stats)
        result_row["universe_rows"] = len(universe_rows)
        result_row["weight_rows"] = len(weight_rows)

        if not weight_rows:
            result_row["status"] = "empty_weights"
            result_rows.append(result_row)
            print(f"[{run_idx}/{len(combinations)}] {context['run_name']}: empty weights")
            continue

        price_cache_key = (vectorbt_spec.price_column, required_markets)
        if price_cache_key not in price_frame_cache:
            price_frame_cache[price_cache_key] = build_price_frame(
                run_candle_rows,
                price_column=vectorbt_spec.price_column,
            )
        price_frame = price_frame_cache[price_cache_key]
        target_weight_frame = build_target_weight_frame(weight_rows, price_frame)
        portfolio = run_portfolio_from_target_weights(
            price_frame=price_frame,
            target_weight_frame=target_weight_frame,
            spec=vectorbt_spec,
        )
        summary = portfolio.stats(settings={"freq": pandas_freq})
        benchmark_curve = build_benchmark_curve(price_frame, benchmark_market, vectorbt_spec.init_cash)
        equity_curve = portfolio.value()
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
        rolling_ir_summary = pd.Series(dtype=float)
        rolling_ir = pd.DataFrame(index=excess_returns.index)
        if compute_rolling_ir_enabled:
            rolling_ir = compute_rolling_information_ratio(
                excess_returns,
                windows=rolling_ir_windows,
                periods_per_day=periods_per_day,
                annualization_factor=periods_per_year,
            )
            rolling_ir_summary = summarize_rolling_information_ratio(rolling_ir)
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
                recent_1y_stats,
                recent_2y_stats,
                compute_drawdown_recovery_stats(equity_curve),
                rolling_ir_summary,
            ]
        )
        summary.loc["CAGR [%]"] = compute_annualized_return(
            equity_curve,
            annualization_factor=periods_per_year,
        ) * 100.0
        summary.loc["Timeframe"] = timeframe
        summary.loc["Periods Per Year"] = periods_per_year
        for key, summary_key in (
            ("strategy_family", "Strategy Family"),
            ("strategy_label", "Strategy Label"),
            ("asset_scope", "Asset Scope"),
        ):
            value = metadata_payload.get(key, "")
            if value not in (None, ""):
                summary.loc[summary_key] = value
        for key, value in parameter_metadata_payload.items():
            if str(key).startswith("parameter_") and value not in (None, ""):
                summary.loc[str(key)] = value

        result_row["status"] = "ok"
        result_row.update(_preferred_summary_fields(summary))
        result_rows.append(result_row)
        if compute_rolling_ir_enabled:
            rolling_ir.to_csv(
                rolling_ir_dir / f"{context['run_name']}_rolling_ir.csv",
                encoding="utf-8-sig",
            )
        if compute_rolling_ir_enabled and save_rolling_ir_plots:
            rolling_ir_figure = _build_rolling_ir_figure(rolling_ir, context["run_name"])
            rolling_ir_figure.write_html(
                str(rolling_ir_dir / f"{context['run_name']}_rolling_ir.html"),
                auto_open=False,
            )
        print(
            f"[{run_idx}/{len(combinations)}] {context['run_name']}: "
            f"return={result_row.get('Total Return [%]')} "
            f"mdd={result_row.get('Max Drawdown [%]')} "
            f"ir={result_row.get('Annualized Information Ratio')}"
        )

    result_frame = pd.DataFrame(result_rows)
    result_frame = _attach_plateau_air_mean(result_frame, config.get("grid", {}))
    result_path = out_dir / "summary_results.csv"
    result_frame.to_csv(result_path, index=False, encoding="utf-8-sig")
    print(f"Wrote {len(result_rows)} grid result rows to {result_path}")

    if top_curve_count <= 0 or result_frame.empty or ranking_metric not in result_frame.columns:
        return

    ok_frame = result_frame[result_frame["status"] == "ok"].copy()
    if ok_frame.empty:
        return
    ok_frame = ok_frame.sort_values(by=ranking_metric, ascending=False).head(top_curve_count)

    top_curves: dict[str, pd.Series] = {}
    benchmark_curve: pd.Series | None = None
    benchmark_market: str | None = None
    for _, row in ok_frame.iterrows():
        run_name = str(row["run_name"])
        rendered = run_payloads_by_name[run_name]
        vectorbt_spec, run_benchmark_market = _load_vectorbt_spec_from_payload(rendered["vectorbt_payload"])
        vectorbt_spec = replace(vectorbt_spec, freq=pandas_freq)
        required_markets = _required_markets_for_run(
            rendered["feature_payload"],
            rendered["universe_payload"],
            run_benchmark_market,
        )
        feature_specs = _load_feature_specs_from_payload(rendered["feature_payload"])
        feature_cache_key = json.dumps(
            {
                "feature_payload": rendered["feature_payload"],
                "required_markets": required_markets,
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        feature_rows = feature_cache[feature_cache_key]
        universe_spec = _load_universe_spec_from_payload(rendered["universe_payload"])
        weight_spec = _load_weight_spec_from_payload(rendered["weight_payload"])
        universe_rows = build_universe_table(feature_rows, universe_spec)
        weight_rows = build_weight_table(universe_rows, weight_spec)
        if required_markets not in filtered_candle_cache:
            filtered_candle_cache[required_markets] = _filter_candle_rows(candle_rows, required_markets)
        run_candle_rows = filtered_candle_cache[required_markets]
        price_cache_key = (vectorbt_spec.price_column, required_markets)
        if price_cache_key not in price_frame_cache:
            price_frame_cache[price_cache_key] = build_price_frame(
                run_candle_rows,
                price_column=vectorbt_spec.price_column,
            )
        price_frame = price_frame_cache[price_cache_key]
        target_weight_frame = build_target_weight_frame(weight_rows, price_frame)
        portfolio = run_portfolio_from_target_weights(
            price_frame=price_frame,
            target_weight_frame=target_weight_frame,
            spec=vectorbt_spec,
        )
        top_curves[run_name] = portfolio.value()
        if benchmark_curve is None:
            benchmark_curve = build_benchmark_curve(price_frame, run_benchmark_market, vectorbt_spec.init_cash)
            benchmark_market = run_benchmark_market

    if top_curves:
        curves_frame = pd.concat(
            [series.rename(name) for name, series in top_curves.items()],
            axis=1,
        )
        if benchmark_curve is not None:
            curves_frame[benchmark_curve.name or "benchmark"] = benchmark_curve
        curves_path = out_dir / "top_curves.csv"
        curves_frame.to_csv(curves_path, encoding="utf-8-sig")
        if compute_rolling_ir_enabled and save_summary_plots:
            top_rolling_ir: dict[str, pd.DataFrame] = {}
            for _, row in ok_frame.iterrows():
                run_name = str(row["run_name"])
                rolling_ir_path = rolling_ir_dir / f"{run_name}_rolling_ir.csv"
                if rolling_ir_path.exists():
                    top_rolling_ir[run_name] = pd.read_csv(
                        rolling_ir_path,
                        encoding="utf-8-sig",
                        index_col=0,
                        parse_dates=True,
                    )

            figure = _build_top_curves_figure(top_curves, benchmark_curve)
            plot_path = out_dir / "top_curves_plot.html"
            figure.write_html(str(plot_path), auto_open=False)
            if args.open_plot:
                webbrowser.open(plot_path.resolve().as_uri())
            suffix = f" with benchmark {benchmark_market}" if benchmark_market else ""
            print(
                f"Wrote top {len(top_curves)} curve comparison to {plot_path}{suffix}"
            )
            for window in rolling_ir_windows:
                column = f"rolling_ir_{window}d"
                figure = _build_top_rolling_ir_figure(top_rolling_ir, column)
                rolling_plot_path = out_dir / f"top_{column}_plot.html"
                figure.write_html(str(rolling_plot_path), auto_open=False)
                if args.open_plot:
                    webbrowser.open(rolling_plot_path.resolve().as_uri())
                print(f"Wrote top rolling IR comparison to {rolling_plot_path}")
        elif save_summary_plots:
            figure = _build_top_curves_figure(top_curves, benchmark_curve)
            plot_path = out_dir / "top_curves_plot.html"
            figure.write_html(str(plot_path), auto_open=False)
            if args.open_plot:
                webbrowser.open(plot_path.resolve().as_uri())
            suffix = f" with benchmark {benchmark_market}" if benchmark_market else ""
            print(
                f"Wrote top {len(top_curves)} curve comparison to {plot_path}{suffix}"
            )


if __name__ == "__main__":
    main()
