# 2026-03-23 Upbit Cross-Section Turnover and Momentum Notes

This note records the first full research pass on `all KRW markets` using `60m` Upbit candles.
The work in this round had two goals:

- finish the raw data layer for all KRW pairs,
- test whether `turnover` and `momentum` can define a useful long-only alt universe before any per-coin strategy search.

## 1. Data Layer

All KRW markets were collected into:

- [markets.csv](/c:/Users/working/Desktop/Coin%20Project/data/upbit_research/markets.csv)
- [data/upbit_research/minutes/60](/c:/Users/working/Desktop/Coin%20Project/data/upbit_research/minutes/60)

Status:

- KRW markets found: `243`
- 60m CSV files saved: `243`

Important implementation fixes:

- [upbit_collector.py](/c:/Users/working/Desktop/Coin%20Project/lib/upbit_collector.py)
  - added retry logic for `RemoteDisconnected`, timeout, and transient HTTP errors
- [upbit_minute_collector.py](/c:/Users/working/Desktop/Coin%20Project/scripts/upbit_minute_collector.py)
  - added `--skip-existing`
  - added `--start-market`
  - one failed market no longer kills the whole overnight run

## 2. Research Script

Cross-sectional research now runs through:

- [research_turnover_cross_section.py](/c:/Users/working/Desktop/Coin%20Project/scripts/research_turnover_cross_section.py)

Current capabilities:

- per-market `turnover` aggregation from `candle_acc_trade_price`
- per-market time-series standardization:
  - `percentile`
  - `ratio`
  - `zscore`
- optional `log1p(turnover)` before standardization
- cross-sectional turnover percentile ranking
- optional momentum calculation with configurable lookback
- optional absolute momentum filter: `momentum > 0`
- optional cross-sectional momentum filter
- 1D bucket summary output
- 2D `turnover bucket x momentum bucket` matrix output

Outputs now include:

- `bucket_summary.csv`
- `latest_snapshot.csv`
- `matrix_mean_forward_return_pct.csv`
- `matrix_median_forward_return_pct.csv`
- `matrix_positive_ratio_pct.csv`
- `matrix_observations.csv`

## 3. Logic Checked

The script was validated on synthetic data.

Reference:

- [data/_tmp_turnover_test2/out_matrix](/c:/Users/working/Desktop/Coin%20Project/data/_tmp_turnover_test2/out_matrix)

What was verified:

- forward return is computed at each timestamp, not once globally
- bucket direction is correct
- `Q1` is low percentile, `Q5` is high percentile
- `min_cross_section_size` works

So the current outputs are structurally trustworthy.

## 4. Baseline Interpretation

The basic full-universe turnover-only result was:

- [bucket_summary.csv](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_full/bucket_summary.csv)

Key pattern:

- `Q5` was clearly bad
- `Q1` to `Q3` were positive and relatively similar

Meaning:

- this is not a simple `higher turnover is better` effect
- it is closer to `avoid extreme turnover overheating`

## 5. Turnover x Momentum Matrix

The more useful view is the 2D matrix:

- [matrix_mean_forward_return_pct.csv](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_matrix/matrix_mean_forward_return_pct.csv)
- [matrix_observations.csv](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_matrix/matrix_observations.csv)

At this stage:

- rows are `turnover_Q1` to `turnover_Q5`
- columns are `momentum_Q1` to `momentum_Q5`
- forward horizon is still conditional future return, not a portfolio backtest

Important caution:

- these values are not account CAGR
- they are average forward returns of coin-time observations in each cell

## 6. Momentum Filter vs Raw

Two different readings were checked.

### 6.1 Raw matrix

Examples:

- [fwd48 raw](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_matrix_fwd48/matrix_mean_forward_return_pct.csv)
- [mom72 raw](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_matrix_fwd48_mom72_raw/matrix_mean_forward_return_pct.csv)

In the raw matrix:

- `momentum_Q1` really is low and often negative momentum
- `turnover_Q5 x momentum_Q5` is among the worst regions
- low-to-mid turnover generally dominates high-turnover overheating

This means the raw data still looks more like:

- `avoid crowded momentum-chase`
- rather than `buy the strongest breakouts`

### 6.2 Positive-momentum filter

Example:

- [fwd48 posmom](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_matrix_fwd48_posmom/matrix_mean_forward_return_pct.csv)

With `momentum > 0` enforced:

- `turnover_Q5` still remained poor
- `Q1` to `Q3` remained better
- the main lesson did not change

Meaning:

- absolute positive momentum is a useful practical filter for long-only deployment
- but it does not rescue the high-turnover overheating bucket

## 7. Momentum Lookback Comparison

Compared under:

- raw
- `forward = 48h`
- turnover aggregation fixed at `24h`

References:

- [mom6](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_matrix_fwd48_posmom_mom6/bucket_summary.csv)
- [mom24](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_matrix_fwd48_posmom_mom24/bucket_summary.csv)
- [mom48](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_matrix_fwd48_posmom_mom48/bucket_summary.csv)
- [mom72](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_matrix_fwd48_posmom_mom72/bucket_summary.csv)

Observed ranking:

- `48h` and `72h` were stronger than `24h`
- `6h` looked weaker and noisier

Current reading:

- `momentum 48h` is the best short candidate
- `momentum 72h` is the slower alternative

## 8. Turnover Aggregation Comparison

Compared under:

- raw
- `momentum = 48h`
- `forward = 48h`

References:

- [turnover 6h](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_turn6_mom48_fwd48_raw/bucket_summary.csv)
- [turnover 24h](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_turn24_mom48_fwd48_raw/bucket_summary.csv)
- [turnover 72h](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_turn72_mom48_fwd48_raw/bucket_summary.csv)

Observed ranking:

- `24h` gave the cleanest separation
- `6h` was noisier
- `72h` was too dull

Current reading:

- `turnover 24h` is the most useful aggregation window

## 9. Turnover Time-Series Lookback Comparison

Compared under:

- raw
- `turnover 24h`
- `momentum 48h`
- `forward 48h`

References:

- [lookback 14d](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_turn24_lb14d_mom48_fwd48_raw/bucket_summary.csv)
- [lookback 30d](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_turn24_lb30d_mom48_fwd48_raw/bucket_summary.csv)
- [lookback 60d](/c:/Users/working/Desktop/Coin%20Project/data/research/turnover_cross_section_60m_turn24_lb60d_mom48_fwd48_raw/bucket_summary.csv)

Observed ranking:

- `14d` was weaker
- `30d` and `60d` were both credible
- `30d` gave slightly sharper overheating penalty
- `60d` gave slightly higher upside in the better buckets

Current reading:

- `30d` is the practical default
- `60d` is the main robustness alternative

## 10. Current Working Hypothesis

The first usable hypothesis is now:

- avoid `turnover_Q5`
- prefer `turnover_Q1` to `Q3`
- combine that with `momentum 48h`
- likely keep a simple long-only bias such as `momentum > 0` when moved into actual portfolio testing

More plainly:

- do not chase the most crowded, turnover-exploded alt moves
- look for names that are moving, but are not yet in the most overheated turnover regime

## 11. What This Is Not Yet

This is still not a true portfolio backtest.

It is:

- cross-sectional conditional forward-return research

It is not yet:

- equal-weight portfolio simulation
- rebalance model
- fee/slippage account curve
- walk-forward selection system

So the current result supports:

- `universe design`
- `factor direction`

but not yet:

- `deployable realized return claims`

## 12. Next Step

The next correct step is:

- convert the current factor reading into an actual simple portfolio rule

Most natural first candidate:

- `momentum 48h > 0`
- `turnover not in Q5`
- choose from `turnover Q1~Q3`
- equal-weight
- fixed rebalance schedule
- then test fee/slippage-aware portfolio performance
