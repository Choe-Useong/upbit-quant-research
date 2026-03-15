#!/usr/bin/env python3
from __future__ import annotations

import argparse
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

from lib.storage import read_candles_csv, read_table_csv
from lib.vectorbt_adapter import (
    VectorBTSpec,
    build_price_frame,
    build_target_weight_frame,
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
    return parser


def load_all_candles(candle_dir: Path) -> list:
    rows = []
    for csv_path in sorted(candle_dir.glob("*.csv")):
        rows.extend(read_candles_csv(csv_path))
    return rows


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
        "Benchmark Return [%]",
        "Benchmark Market",
        "Benchmark Start Value",
        "Benchmark End Value",
        "Benchmark Total Return [%]",
        "Benchmark Max Drawdown [%]",
        "Benchmark Sharpe Ratio",
        "Benchmark Sortino Ratio",
        "Benchmark Calmar Ratio",
        "Information Ratio",
        "Annualized Information Ratio",
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


def summarize_rolling_information_ratio(rolling_ir: pd.DataFrame) -> pd.Series:
    summary: dict[str, float] = {}
    for column in rolling_ir.columns:
        series = pd.to_numeric(rolling_ir[column], errors="coerce").dropna()
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

    candle_rows = load_all_candles(Path(args.candle_dir))
    if not candle_rows:
        raise SystemExit(f"No candle rows found in {args.candle_dir}")

    timeframe = infer_timeframe(Path(args.candle_dir), args.timeframe)
    periods_per_year = args.periods_per_year or infer_periods_per_year(timeframe)
    periods_per_day = periods_per_day_for_timeframe(timeframe)
    pandas_freq = timeframe_to_pandas_freq(timeframe)

    weight_rows = read_table_csv(Path(args.weights_csv))
    if not weight_rows:
        raise SystemExit(f"No weight rows found in {args.weights_csv}")

    price_frame = build_price_frame(candle_rows, price_column=args.price_column)
    target_weight_frame = build_target_weight_frame(weight_rows, price_frame)
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
    rolling_ir_summary = summarize_rolling_information_ratio(rolling_ir)
    summary.loc["Timeframe"] = timeframe
    summary.loc["Periods Per Year"] = periods_per_year
    summary = pd.concat([summary, benchmark_stats, ir_stats, rolling_ir_summary])
    print_summary(summary)
    write_summary_csv(out_dir / "summary.csv", summary)
    write_equity_csv(out_dir / "equity_curve.csv", equity_curve)
    write_equity_csv(out_dir / "benchmark_curve.csv", aligned_benchmark_curve)
    write_equity_csv(out_dir / "excess_equity_curve.csv", excess_equity_curve)
    write_equity_csv(out_dir / "excess_returns.csv", excess_returns)
    write_equity_csv(out_dir / "rolling_information_ratio.csv", rolling_ir)
    target_weight_frame.to_csv(out_dir / "target_weights_full.csv", encoding="utf-8-sig")
    if args.show_plot:
        show_equity_plot(equity_curve, aligned_benchmark_curve, out_dir / args.plot_html)
    print(f"Wrote vectorbt results to {out_dir}")


if __name__ == "__main__":
    main()
