# Upbit Quant Research

Quant research and live-execution framework for Upbit KRW crypto data, features, universes, weights, vectorbt backtests, parameter grids, and weights-driven live jobs.

Current scope:
- collect Upbit candles
- build chained features
- build universe filters and weights
- run vectorbt backtests
- run parameter grids with summary, equity comparison, and rolling IR plots
- build live portfolio weights
- run weights-driven preview/live execution

## Purpose

This repository is for systematic crypto strategy research on Upbit KRW markets.

The current workflow is:
- collect candles from Upbit
- build reusable features
- construct universe rules
- generate target weights
- run benchmark-relative backtests
- materialize `data/portfolio/<name>/weights.csv`
- optionally refresh latest portfolio weights for live use
- run preview/live execution from the latest weights
- compare parameter grids with equity and rolling IR diagnostics

The repository is optimized for research iteration and simple weights-first live execution.

## Structure

- `lib/upbit_collector.py`: Upbit candle collection
- `lib/features.py`: feature engine
- `lib/universe.py`: universe selection
- `lib/weights.py`: target weight generation
- `lib/vectorbt_adapter.py`: vectorbt bridge
- `lib/allocation.py`: portfolio weight allocation / sleeve merge
- `lib/dataframes.py`: wide-frame utilities and factor calculations
- `scripts/build_features.py`: build feature CSV
- `scripts/build_universe.py`: build universe CSV
- `scripts/build_weights.py`: build weights CSV
- `scripts/build_portfolio_weights.py`: build one portfolio's weights end-to-end
- `scripts/build_allocated_weights.py`: merge strategy weights into one allocated portfolio
- `scripts/research_turnover_cross_section.py`: cross-sectional factor research
- `scripts/run_vectorbt.py`: run one backtest
- `scripts/run_grid.py`: run parameter grid
- `live/execute_portfolio.py`: execute latest weights in preview/live mode
- `live/run_live_job.py`: refresh weights then execute

## Current Capabilities

- Upbit KRW candle collection and wide-frame utilities
- Chained feature generation
  - momentum
  - median momentum
  - win rate
  - trend quality (`slope * R^2`)
  - rolling mean / sum
  - volatility
  - cross-sectional rank / percentile
  - calendar aggregations
  - reference-market comparisons
- Comparison and logical features
  - examples: `sma_20 > sma_60`, boolean feature combinations
- Universe construction
  - top-n
  - quantile
  - lagged selection
  - value filters
  - rank filters
  - allowed / excluded markets
- Weight generation
  - equal weight
  - rank weight
  - daily / weekly / monthly rebalance frequencies
- Portfolio materialization
  - `data/portfolio/<name>/weights.csv`
  - latest weight rebuild with local + refreshed candles
- Allocation
  - combine multiple strategy `weights.csv`
  - capital weights
  - keep-cash / redistribute
  - market caps and cap overflow handling
- Vectorbt backtesting
  - target weight based execution
  - benchmark-relative excess return tracking
  - information ratio and rolling information ratio
- Live execution
  - weights-first preview/live execution
  - scheduled live jobs via Windows Task Scheduler
  - preview/live logs under `live/logs/`
- Grid search
  - parameter sweeps across features, universe rules, and rebalancing
  - constraint support such as `short < long` or `short < mid < long`
  - automatic comparison plots for top runs

## Install

```powershell
py -m pip install -r requirements.txt
```

## Data Collection

Download all currently available Upbit KRW daily candles:

```powershell
py scripts/upbit_daily_collector.py
```

Limit scope for testing:

```powershell
py scripts/upbit_daily_collector.py --markets KRW-BTC,KRW-ETH --days 365
```

## Single Pipeline

Build features:

```powershell
py scripts/build_features.py --spec-json configs/features_momentum_10_liquidity.json --output-csv data/upbit/features/features_mom10_liq.csv
```

Build universe:

```powershell
py scripts/build_universe.py --features-csv data/upbit/features/features_mom10_liq.csv --spec-json configs/universe_btc_eth_mom10_positive.json --output-csv data/upbit/universe/universe_btc_eth_mom10_positive.csv
```

Build weights:

```powershell
py scripts/build_weights.py --universe-csv data/upbit/universe/universe_btc_eth_mom10_positive.csv --spec-json configs/weights_equal.json --output-csv data/upbit/weights/weights_btc_eth_mom10_positive_daily_equal.csv
```

Run backtest:

```powershell
py scripts/run_vectorbt.py --weights-csv data/upbit/weights/weights_btc_eth_mom10_positive_daily_equal.csv --out-dir data/backtest/btc_eth_mom10_positive_daily_equal_fee5bp --fees 0.0005 --show-plot
```

## Portfolio Build

Build one materialized portfolio weights artifact:

```powershell
py scripts/build_portfolio_weights.py --preset main_strategies_60m_equal_weight_4core
```

This updates:

- `data/portfolio/main_strategies_60m_equal_weight_4core/weights.csv`
- `data/portfolio/main_strategies_60m_equal_weight_4core/universe.csv`
- `data/portfolio/main_strategies_60m_equal_weight_4core/build_metadata.json`

Combine multiple strategy weight artifacts into one allocated portfolio:

```powershell
py scripts/build_allocated_weights.py --config-json configs/portfolio/allocated_upbit_turnover_examples_8_2.json
```

## Live Execution

Preview one latest weights artifact:

```powershell
py live/execute_portfolio.py --mode preview --execution-config-json configs/live/main_strategies_60m_equal_weight_4core_execution.json
```

Refresh weights then execute in one step:

```powershell
py live/run_live_job.py --mode preview
```

For unattended runs on Windows, register `live/run_live_job.cmd` with Task Scheduler.

## Grid Examples

BTC momentum grid:

```powershell
py scripts/run_grid.py --config-json configs/grid_btc_momentum_fee5bp.json --open-plot
```

ETH 2-line moving average crossover grid:

```powershell
py scripts/run_grid.py --config-json configs/grid_eth_ma_cross_fee5bp.json --open-plot
```

XRP 2-line moving average crossover grid:

```powershell
py scripts/run_grid.py --config-json configs/grid_xrp_ma_cross_fee5bp.json --open-plot
```

## Notes

- `data/` is ignored from Git by default.
- `live/logs/` is ignored from Git by default.
- `configs/` is the main reproducibility layer.
- Benchmark-relative metrics use the configured `benchmark_market`.
- Grid runner supports simple constraints such as `short < long` and `short < mid < long`.
- Current research includes both daily-style pipelines and 60m intraday cross-sectional studies.
- Upbit official API does not provide market cap data in this project.
- Many reported results are exploratory research results and should not be treated as production-ready signals without further validation.
