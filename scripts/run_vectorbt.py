#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
import sys
import webbrowser
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.dataframes import build_wide_frame_from_candle_dir
from lib.storage import read_candles_csv, read_table_csv
from lib.vectorbt_adapter import (
    VectorBTSpec,
    build_price_frame,
    build_target_weight_frame,
    build_target_weight_frame_from_wide_csv,
    run_portfolio_from_target_weights,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run vectorbt portfolio simulation from candle CSVs and target weight CSV."
    )
    parser.add_argument(
        "--candle-dir",
        "--daily-dir",
        dest="candle_dir",
        default="data/upbit/daily",
        help="Directory containing per-market candle CSV files",
    )
    parser.add_argument(
        "--load-mode",
        choices=["auto", "candles", "wide"],
        default="auto",
        help="auto/wide: dataframe-first loading, candles: legacy CandleRow loading",
    )
    parser.add_argument(
        "--weights-csv",
        required=True,
        help="CSV file containing sparse target weights by date and market",
    )
    parser.add_argument(
        "--price-column",
        default="trade_price",
        help="CandleRow field to use as the close price input",
    )
    parser.add_argument(
        "--init-cash",
        type=float,
        default=1_000_000.0,
        help="Initial cash for the portfolio",
    )
    parser.add_argument(
        "--fees",
        type=float,
        default=0.0,
        help="Per-order proportional fees passed to vectorbt",
    )
    parser.add_argument(
        "--slippage",
        type=float,
        default=0.0,
        help="Per-order proportional slippage passed to vectorbt",
    )
    parser.add_argument(
        "--out-dir",
        default="data/backtest/vectorbt",
        help="Directory to write result CSVs",
    )
    parser.add_argument(
        "--show-plot",
        action="store_true",
        help="Show equity curve plot in a browser window",
    )
    parser.add_argument(
        "--plot-html",
        default="comparison_plot.html",
        help="HTML filename for the comparison plot inside out-dir",
    )
    parser.add_argument(
        "--save-rolling-ir-csv",
        action="store_true",
        help="Write rolling_information_ratio.csv to out-dir; disabled by default to reduce output size",
    )
    parser.add_argument(
        "--benchmark-market",
        default="KRW-BTC",
        help="Market to use for buy-and-hold benchmark",
    )
    parser.add_argument(
        "--timeframe",
        default=None,
        help="Timeframe label such as daily, 240m, or 60m; omitted means infer from candle-dir",
    )
    parser.add_argument(
        "--periods-per-year",
        type=int,
        default=None,
        help="Annualization periods per year; omitted means infer from timeframe",
    )
    parser.add_argument("--strategy-family", default="", help="Optional strategy family metadata")
    parser.add_argument("--strategy-label", default="", help="Optional strategy label metadata")
    parser.add_argument("--asset-scope", default="", help="Optional asset scope metadata")
    parser.add_argument(
        "--parameter-metadata-json",
        default="",
        help="Optional JSON file containing parameter_* metadata fields to write into summary.csv",
    )
    parser.add_argument(
        "--trim-start-mode",
        choices=["first_weight", "none"],
        default="first_weight",
        help="Trim the simulation start to the first timestamp with non-zero target weights; default is first_weight",
    )
    return parser


def load_all_candles(candle_dir: Path) -> list:
    rows = []
    for csv_path in sorted(candle_dir.glob("*.csv")):
        rows.extend(read_candles_csv(csv_path))
    return rows


def resolve_load_mode(load_mode: str) -> str:
    if load_mode == "auto":
        return "wide"
    return load_mode


def detect_weight_csv_format(weights_csv: Path) -> str:
    header = pd.read_csv(weights_csv, nrows=0, encoding="utf-8-sig")
    columns = set(header.columns)
    if {"date_utc", "market", "target_weight"}.issubset(columns):
        return "sparse"
    if "date_utc" in columns:
        return "wide"
    raise ValueError(f"Unsupported weights CSV format: {weights_csv}")


def trim_frames_to_first_weight(
    price_frame: pd.DataFrame,
    target_weight_frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp | None]:
    active_mask = target_weight_frame.fillna(0.0).abs().sum(axis=1) > 0.0
    if not bool(active_mask.any()):
        return price_frame, target_weight_frame, None
    first_active = pd.Timestamp(active_mask[active_mask].index[0])
    return (
        price_frame.loc[price_frame.index >= first_active],
        target_weight_frame.loc[target_weight_frame.index >= first_active],
        first_active,
    )


def write_summary_csv(path: Path, summary: pd.Series) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_frame(name="value").to_csv(path, encoding="utf-8-sig")


def write_equity_csv(path: Path, portfolio) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(portfolio, pd.Series):
        portfolio.to_frame(name=portfolio.name or "value").to_csv(path, encoding="utf-8-sig")
    else:
        portfolio.to_csv(path, encoding="utf-8-sig")


def print_summary(summary: pd.Series) -> None:
    preferred_keys = [
        "Start Value",
        "End Value",
        "Total Return [%]",
        "CAGR [%]",
        "Longest Peak-to-Recovery Bars",
        "Second Longest Peak-to-Recovery Bars",
        "Benchmark Return [%]",
        "Benchmark Market",
        "Benchmark Start Value",
        "Benchmark End Value",
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
    print("VectorBT Summary")
    for key in preferred_keys:
        if key in summary.index:
            print(f"{key}: {summary[key]}")


def infer_timeframe(candle_dir: Path, timeframe: str | None = None) -> str:
    if timeframe:
        return timeframe.lower()

    parts = [part.lower() for part in candle_dir.parts]
    if "daily" in parts:
        return "daily"

    for idx, part in enumerate(parts):
        if part == "minutes" and idx + 1 < len(parts) and parts[idx + 1].isdigit():
            return f"{parts[idx + 1]}m"

    return "daily"


def periods_per_day_for_timeframe(timeframe: str) -> int:
    normalized = timeframe.lower()
    if normalized == "daily":
        return 1
    matched = re.fullmatch(r"(\d+)m", normalized)
    if not matched:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    minutes = int(matched.group(1))
    if minutes <= 0 or 1440 % minutes != 0:
        raise ValueError(f"Unsupported minute timeframe: {timeframe}")
    return 1440 // minutes


def infer_periods_per_year(timeframe: str) -> int:
    return 252 * periods_per_day_for_timeframe(timeframe)


def timeframe_to_pandas_freq(timeframe: str) -> str:
    normalized = timeframe.lower()
    if normalized == "daily":
        return "1D"
    matched = re.fullmatch(r"(\d+)m", normalized)
    if not matched:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    minutes = int(matched.group(1))
    return f"{minutes}min"


def build_benchmark_curve(
    price_frame: pd.DataFrame,
    benchmark_market: str,
    init_cash: float,
) -> pd.Series:
    if benchmark_market not in price_frame.columns:
        raise ValueError(f"Benchmark market not found in price frame: {benchmark_market}")
    benchmark_prices = price_frame[benchmark_market].dropna()
    if benchmark_prices.empty:
        raise ValueError(f"No valid benchmark prices for market: {benchmark_market}")
    first_price = float(benchmark_prices.iloc[0])
    curve = (benchmark_prices / first_price) * init_cash
    curve.name = f"{benchmark_market}_benchmark_value"
    return curve


def benchmark_summary(
    benchmark_curve: pd.Series,
    init_cash: float,
    benchmark_market: str,
    annualization_factor: int = 252,
) -> pd.Series:
    end_value = float(benchmark_curve.iloc[-1])
    total_return = ((end_value / init_cash) - 1.0) * 100.0
    returns = compute_return_series(benchmark_curve)
    max_drawdown_pct = compute_max_drawdown_pct(benchmark_curve)
    sharpe_ratio = compute_sharpe_ratio(returns, annualization_factor=annualization_factor)
    sortino_ratio = compute_sortino_ratio(returns, annualization_factor=annualization_factor)
    annualized_return = compute_annualized_return(benchmark_curve, annualization_factor=annualization_factor)
    calmar_ratio = float("nan")
    if max_drawdown_pct != 0:
        calmar_ratio = annualized_return / (abs(max_drawdown_pct) / 100.0)
    return pd.Series(
        {
            "Benchmark Market": benchmark_market,
            "Benchmark Start Value": init_cash,
            "Benchmark End Value": end_value,
            "Benchmark Total Return [%]": total_return,
            "Benchmark CAGR [%]": annualized_return * 100.0,
            "Benchmark Max Drawdown [%]": max_drawdown_pct,
            "Benchmark Sharpe Ratio": sharpe_ratio,
            "Benchmark Sortino Ratio": sortino_ratio,
            "Benchmark Calmar Ratio": calmar_ratio,
        }
    )


def compute_return_series(curve: pd.Series) -> pd.Series:
    returns = curve.pct_change()
    returns.iloc[0] = 0.0
    return returns.fillna(0.0)


def compute_max_drawdown_pct(curve: pd.Series) -> float:
    running_max = curve.cummax()
    drawdown = (curve / running_max) - 1.0
    return float(drawdown.min() * 100.0)


def compute_drawdown_recovery_stats(curve: pd.Series) -> pd.Series:
    series = curve.dropna()
    if series.empty:
        return pd.Series(
            {
                "Longest Peak-to-Recovery Bars": float("nan"),
                "Second Longest Peak-to-Recovery Bars": float("nan"),
            }
        )

    running_max = float(series.iloc[0])
    running_max_time = pd.Timestamp(series.index[0])
    in_drawdown = False
    trough_value = float(series.iloc[0])
    peak_to_recovery_bars_list: list[int] = []

    for timestamp, value in series.items():
        current_time = pd.Timestamp(timestamp)
        current_value = float(value)

        if current_value >= running_max:
            if in_drawdown:
                peak_to_recovery_bars = int(series.loc[running_max_time:current_time].shape[0] - 1)
                if peak_to_recovery_bars > 0:
                    peak_to_recovery_bars_list.append(peak_to_recovery_bars)
                in_drawdown = False

            running_max = current_value
            running_max_time = current_time
            trough_value = current_value
            continue

        if not in_drawdown:
            in_drawdown = True
            trough_value = current_value
            continue

        if current_value < trough_value:
            trough_value = current_value

    peak_to_recovery_bars_list.sort(reverse=True)
    longest = float("nan")
    second_longest = float("nan")
    if peak_to_recovery_bars_list:
        longest = float(peak_to_recovery_bars_list[0])
    if len(peak_to_recovery_bars_list) >= 2:
        second_longest = float(peak_to_recovery_bars_list[1])

    return pd.Series(
        {
            "Longest Peak-to-Recovery Bars": longest,
            "Second Longest Peak-to-Recovery Bars": second_longest,
        }
    )


def compute_sharpe_ratio(returns: pd.Series, annualization_factor: int = 252) -> float:
    std = float(returns.std(ddof=0))
    if std == 0.0:
        return float("nan")
    return float((returns.mean() / std) * (annualization_factor ** 0.5))


def compute_sortino_ratio(returns: pd.Series, annualization_factor: int = 252) -> float:
    downside = returns[returns < 0]
    downside_std = float(downside.std(ddof=0)) if not downside.empty else 0.0
    if downside_std == 0.0:
        return float("nan")
    return float((returns.mean() / downside_std) * (annualization_factor ** 0.5))


def compute_annualized_return(curve: pd.Series, annualization_factor: int = 252) -> float:
    if len(curve) < 2:
        return float("nan")
    total_return = float(curve.iloc[-1] / curve.iloc[0])
    periods = len(curve) - 1
    if total_return <= 0 or periods <= 0:
        return float("nan")
    return float(total_return ** (annualization_factor / periods) - 1.0)


def compute_information_ratio(
    strategy_returns: pd.Series,
    benchmark_returns: pd.Series,
    annualization_factor: int = 252,
) -> pd.Series:
    aligned = pd.concat(
        [
            strategy_returns.rename("strategy"),
            benchmark_returns.rename("benchmark"),
        ],
        axis=1,
        join="inner",
    ).dropna()
    excess_returns = aligned["strategy"] - aligned["benchmark"]
    tracking_error = float(excess_returns.std(ddof=0))
    if tracking_error == 0.0:
        info_ratio = float("nan")
        annualized_info_ratio = float("nan")
    else:
        info_ratio = float(excess_returns.mean() / tracking_error)
        annualized_info_ratio = info_ratio * (annualization_factor ** 0.5)
    return pd.Series(
        {
            "Information Ratio": info_ratio,
            "Annualized Information Ratio": annualized_info_ratio,
        }
    )


def compute_excess_curves(
    equity_curve: pd.Series,
    benchmark_curve: pd.Series,
    init_cash: float,
) -> tuple[pd.Series, pd.Series]:
    strategy_returns = compute_return_series(equity_curve)
    benchmark_returns = compute_return_series(benchmark_curve.reindex(equity_curve.index).ffill())
    aligned = pd.concat(
        [
            strategy_returns.rename("strategy"),
            benchmark_returns.rename("benchmark"),
        ],
        axis=1,
        join="inner",
    ).dropna()
    excess_returns = aligned["strategy"] - aligned["benchmark"]
    relative_curve = ((1.0 + excess_returns).cumprod()) * init_cash
    relative_curve.name = "excess_equity_curve"
    return excess_returns.rename("excess_return"), relative_curve


def compute_rolling_information_ratio(
    excess_returns: pd.Series,
    windows: tuple[int, ...] = (126, 252),
    periods_per_day: int = 1,
    annualization_factor: int = 252,
) -> pd.DataFrame:
    frame = pd.DataFrame(index=excess_returns.index)
    for window_days in windows:
        window_periods = max(1, int(math.ceil(window_days * periods_per_day)))
        rolling_mean = excess_returns.rolling(window_periods).mean()
        rolling_std = excess_returns.rolling(window_periods).std(ddof=0)
        rolling_ir = rolling_mean / rolling_std
        frame[f"rolling_ir_{window_days}d"] = rolling_ir * (annualization_factor ** 0.5)
    return frame


def compute_recent_1y_stats(
    equity_curve: pd.Series,
    benchmark_curve: pd.Series,
    annualization_factor: int = 252,
) -> pd.Series:
    empty_result = pd.Series(
        {
            "Recent 1Y Return [%]": float("nan"),
            "Recent 1Y Benchmark Return [%]": float("nan"),
            "Recent 1Y Information Ratio": float("nan"),
            "Recent 1Y AIR": float("nan"),
            "Recent 1Y Max Drawdown [%]": float("nan"),
        }
    )
    if equity_curve.empty or benchmark_curve.empty:
        return empty_result

    last_timestamp = pd.Timestamp(equity_curve.index[-1])
    start_timestamp = last_timestamp - pd.DateOffset(years=1)
    aligned = pd.concat(
        [
            equity_curve[equity_curve.index >= start_timestamp].rename("strategy"),
            benchmark_curve[benchmark_curve.index >= start_timestamp].rename("benchmark"),
        ],
        axis=1,
        join="inner",
    ).dropna()
    if len(aligned) < 2:
        return empty_result

    recent_equity = aligned["strategy"]
    recent_benchmark = aligned["benchmark"]
    recent_return = ((float(recent_equity.iloc[-1] / recent_equity.iloc[0])) - 1.0) * 100.0
    recent_benchmark_return = (
        (float(recent_benchmark.iloc[-1] / recent_benchmark.iloc[0])) - 1.0
    ) * 100.0
    recent_ir = compute_information_ratio(
        compute_return_series(recent_equity),
        compute_return_series(recent_benchmark),
        annualization_factor=annualization_factor,
    )
    return pd.Series(
        {
            "Recent 1Y Return [%]": recent_return,
            "Recent 1Y Benchmark Return [%]": recent_benchmark_return,
            "Recent 1Y Information Ratio": recent_ir["Information Ratio"],
            "Recent 1Y AIR": recent_ir["Annualized Information Ratio"],
            "Recent 1Y Max Drawdown [%]": compute_max_drawdown_pct(recent_equity),
        }
    )


def compute_recent_2y_stats(
    equity_curve: pd.Series,
    benchmark_curve: pd.Series,
    annualization_factor: int = 252,
) -> pd.Series:
    empty_result = pd.Series(
        {
            "Recent 2Y Return [%]": float("nan"),
            "Recent 2Y Benchmark Return [%]": float("nan"),
            "Recent 2Y Information Ratio": float("nan"),
            "Recent 2Y AIR": float("nan"),
            "Recent 2Y Max Drawdown [%]": float("nan"),
        }
    )
    if equity_curve.empty or benchmark_curve.empty:
        return empty_result

    last_timestamp = pd.Timestamp(equity_curve.index[-1])
    start_timestamp = last_timestamp - pd.DateOffset(years=2)
    aligned = pd.concat(
        [
            equity_curve[equity_curve.index >= start_timestamp].rename("strategy"),
            benchmark_curve[benchmark_curve.index >= start_timestamp].rename("benchmark"),
        ],
        axis=1,
        join="inner",
    ).dropna()
    if len(aligned) < 2:
        return empty_result

    recent_equity = aligned["strategy"]
    recent_benchmark = aligned["benchmark"]
    recent_return = ((float(recent_equity.iloc[-1] / recent_equity.iloc[0])) - 1.0) * 100.0
    recent_benchmark_return = (
        (float(recent_benchmark.iloc[-1] / recent_benchmark.iloc[0])) - 1.0
    ) * 100.0
    recent_ir = compute_information_ratio(
        compute_return_series(recent_equity),
        compute_return_series(recent_benchmark),
        annualization_factor=annualization_factor,
    )
    return pd.Series(
        {
            "Recent 2Y Return [%]": recent_return,
            "Recent 2Y Benchmark Return [%]": recent_benchmark_return,
            "Recent 2Y Information Ratio": recent_ir["Information Ratio"],
            "Recent 2Y AIR": recent_ir["Annualized Information Ratio"],
            "Recent 2Y Max Drawdown [%]": compute_max_drawdown_pct(recent_equity),
        }
    )


def summarize_rolling_information_ratio(rolling_ir: pd.DataFrame) -> pd.Series:
    summary: dict[str, float] = {}
    for column in rolling_ir.columns:
        series = (
            pd.to_numeric(rolling_ir[column], errors="coerce")
            .replace([float("inf"), float("-inf")], float("nan"))
            .dropna()
        )
        label = column.replace("rolling_ir_", "Rolling IR ").replace("d", "d")
        if series.empty:
            summary[f"{label} Mean"] = float("nan")
            summary[f"{label} Std"] = float("nan")
            summary[f"{label} Median"] = float("nan")
            summary[f"{label} Q25"] = float("nan")
            summary[f"{label} Positive Ratio"] = float("nan")
            summary[f"{label} Min"] = float("nan")
            summary[f"{label} Max"] = float("nan")
            continue
        summary[f"{label} Mean"] = float(series.mean())
        summary[f"{label} Std"] = float(series.std(ddof=0))
        summary[f"{label} Median"] = float(series.median())
        summary[f"{label} Q25"] = float(series.quantile(0.25))
        summary[f"{label} Positive Ratio"] = float((series > 0).mean())
        summary[f"{label} Min"] = float(series.min())
        summary[f"{label} Max"] = float(series.max())
    return pd.Series(summary)


def _as_single_series(curve, label: str) -> pd.Series:
    if isinstance(curve, pd.DataFrame):
        if curve.shape[1] != 1:
            raise ValueError(f"{label} must contain exactly one curve")
        return curve.iloc[:, 0]
    return curve


def build_comparison_figure(equity_curve, benchmark_curve: pd.Series | None = None) -> go.Figure:
    series = _as_single_series(equity_curve, "equity_curve")
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=series.index,
            y=series.values,
            mode="lines",
            name="Strategy",
        )
    )
    if benchmark_curve is not None:
        benchmark_series = _as_single_series(benchmark_curve, "benchmark_curve")
        fig.add_trace(
            go.Scatter(
                x=benchmark_series.index,
                y=benchmark_series.values,
                mode="lines",
                name=str(benchmark_series.name or "Benchmark"),
            )
        )
    fig.update_layout(
        title="Strategy vs Benchmark",
        xaxis_title="Date",
        yaxis_title="Portfolio Value",
    )
    return fig


def show_equity_plot(
    equity_curve,
    benchmark_curve: pd.Series | None = None,
    html_path: Path | None = None,
) -> None:
    fig = build_comparison_figure(equity_curve, benchmark_curve)
    if html_path is not None:
        html_path.parent.mkdir(parents=True, exist_ok=True)
        fig.write_html(str(html_path), auto_open=False)
        webbrowser.open(html_path.resolve().as_uri())
        return
    fig.show()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    timeframe = infer_timeframe(Path(args.candle_dir), args.timeframe)
    periods_per_year = args.periods_per_year or infer_periods_per_year(timeframe)
    periods_per_day = periods_per_day_for_timeframe(timeframe)
    pandas_freq = timeframe_to_pandas_freq(timeframe)
    parameter_metadata: dict[str, str | int | float] = {}
    if args.parameter_metadata_json:
        parameter_metadata = json.loads(Path(args.parameter_metadata_json).read_text(encoding="utf-8-sig"))

    weight_rows = read_table_csv(Path(args.weights_csv))
    if not weight_rows:
        raise SystemExit(f"No weight rows found in {args.weights_csv}")

    resolved_load_mode = resolve_load_mode(args.load_mode)
    candle_rows = None
    if resolved_load_mode == "wide":
        price_frame = build_wide_frame_from_candle_dir(
            Path(args.candle_dir),
            value_column=args.price_column,
        )
    else:
        candle_rows = load_all_candles(Path(args.candle_dir))
        if not candle_rows:
            raise SystemExit(f"No candle rows found in {args.candle_dir}")
        price_frame = build_price_frame(candle_rows, price_column=args.price_column)
    weight_csv_format = detect_weight_csv_format(Path(args.weights_csv))
    if weight_csv_format == "wide":
        target_weight_frame = build_target_weight_frame_from_wide_csv(
            args.weights_csv,
            price_frame,
        )
    else:
        target_weight_frame = build_target_weight_frame(weight_rows, price_frame)
    trimmed_start_timestamp = None
    if args.trim_start_mode == "first_weight":
        price_frame, target_weight_frame, trimmed_start_timestamp = trim_frames_to_first_weight(
            price_frame,
            target_weight_frame,
        )
    portfolio = run_portfolio_from_target_weights(
        price_frame=price_frame,
        target_weight_frame=target_weight_frame,
        spec=VectorBTSpec(
            price_column=args.price_column,
            init_cash=args.init_cash,
            fees=args.fees,
            slippage=args.slippage,
            freq=pandas_freq,
        ),
    )

    out_dir = Path(args.out_dir)
    summary = portfolio.stats(settings={"freq": pandas_freq})
    equity_curve = portfolio.value()
    benchmark_curve = build_benchmark_curve(price_frame, args.benchmark_market, args.init_cash)
    benchmark_stats = benchmark_summary(
        benchmark_curve,
        args.init_cash,
        args.benchmark_market,
        annualization_factor=periods_per_year,
    )
    aligned_benchmark_curve = benchmark_curve.reindex(equity_curve.index).ffill()
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
        args.init_cash,
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
    summary.loc["Load Mode"] = resolved_load_mode
    summary.loc["Weights CSV Format"] = weight_csv_format
    summary.loc["Trim Start Mode"] = args.trim_start_mode
    if trimmed_start_timestamp is not None:
        summary.loc["Trimmed Start Timestamp"] = trimmed_start_timestamp.isoformat()
    summary.loc["Timeframe"] = timeframe
    summary.loc["Periods Per Year"] = periods_per_year
    if args.strategy_family:
        summary.loc["Strategy Family"] = args.strategy_family
    if args.strategy_label:
        summary.loc["Strategy Label"] = args.strategy_label
    if args.asset_scope:
        summary.loc["Asset Scope"] = args.asset_scope
    for key, value in parameter_metadata.items():
        if str(key).startswith("parameter_"):
            summary.loc[str(key)] = value
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
    write_equity_csv(out_dir / "excess_returns.csv", excess_returns)
    if args.save_rolling_ir_csv:
        write_equity_csv(out_dir / "rolling_information_ratio.csv", rolling_ir)
    target_weight_frame.to_csv(out_dir / "target_weights_full.csv", encoding="utf-8-sig")
    if args.show_plot:
        show_equity_plot(equity_curve, aligned_benchmark_curve, out_dir / args.plot_html)
    print(f"Resolved load mode: {resolved_load_mode}")
    print(f"Wrote vectorbt results to {out_dir}")


if __name__ == "__main__":
    main()
