# Coin Project

Upbit KRW market quant research pipeline.

Current scope:
- collect Upbit daily candles
- build chained features
- build universe filters and weights
- run vectorbt backtests
- run parameter grids with summary, equity comparison, and rolling IR plots

## Structure

- `lib/upbit_collector.py`: Upbit daily candle collection
- `lib/features.py`: feature engine
- `lib/universe.py`: universe selection
- `lib/weights.py`: target weight generation
- `lib/vectorbt_adapter.py`: vectorbt bridge
- `scripts/build_features.py`: build feature CSV
- `scripts/build_universe.py`: build universe CSV
- `scripts/build_weights.py`: build weights CSV
- `scripts/run_vectorbt.py`: run one backtest
- `scripts/run_grid.py`: run parameter grid

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
- `configs/` is the main reproducibility layer.
- Benchmark-relative metrics use the configured `benchmark_market`.
- Grid runner supports simple constraints such as `short < long` and `short < mid < long`.
