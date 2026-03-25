# 2026-03-18 Portfolio Structure Review

This note records the portfolio-level follow-up after the per-asset `60m-first` strategy shortlist was built.
The main purpose was not to claim a final production portfolio, but to verify:

- whether a mixed-asset integrated portfolio can be reproduced through the common pipeline,
- whether `every_bar` target weights were causing real drift-correction trades,
- whether `change-only` execution changes the result materially,
- how much `common start`, `listed normalization`, and `slippage` matter,
- and whether `2`, `4`, or `7` assets make more sense under a realistic cost assumption.

## 1. Core Structural Findings

### 1.1 The separate sleeve runner was not necessary

The earlier `7`-asset portfolio was first built with a dedicated sleeve script.
That was later reproduced through the standard pipeline:

- common features
- market-specific score construction
- universe
- weights
- final `run_vectorbt`

Result:
- the common-pipeline version reproduced the old sleeve equity exactly
- `equity curve max_abs_diff = 0.0`

Conclusion:
- portfolio research should stay on the common pipeline
- the separate sleeve runner was only a temporary shortcut

### 1.2 `every_bar` really does create drift-correction orders

This was verified directly from actual orders.

Example:
- timestamp `2022-02-08 12:00:00`
- portfolio target weights were unchanged versus the prior bar
- but `every_bar targetpercent` still generated small rebalancing buys

This means:
- `every_bar` is not just "signal-based"
- it is truly re-targeting the portfolio every bar

### 1.3 The first `change-only` result was invalid

The original `change-only` implementation was wrong.

Problem:
- sparse weights were generated
- but `rebalance_frequency` remained `every_bar`
- so non-change bars were being zeroed out by the weight-frame builder

Fix:
- added `sparse` rebalance handling in [lib/vectorbt_adapter.py](../lib/vectorbt_adapter.py)
- updated [scripts/experiments/portfolio/build_change_only_weights.py](../scripts/experiments/portfolio/build_change_only_weights.py) to emit `rebalance_frequency = sparse`

After the fix:
- `change-only` means exactly what was intended:
  - rebalance only when portfolio signal state changes
  - no drift-correction trades on unchanged bars

This was verified directly:
- on bars with `all-NaN` target rows in `change-only`, actual order count was `0`

## 2. 7-Asset Portfolio Interpretation

### 2.1 Survivor bias remains the biggest caveat

The `7`-asset universe is still:

- ex-post selected
- composed of successful survivors
- combined with strategies chosen after extensive research

So the portfolio should not be read as:
- "this is a clean ex-ante investable historical portfolio"

It is better read as:
- "a structure test on a curated set of successful assets"

### 2.2 `listed_norm` and `common start` matter a lot

Two adjustments were tested:

- `listed_norm`
  - rescale weights across listed assets only
- `common start`
  - start only after all chosen assets are listed

Interpretation:
- `listed_norm` fixes early underinvestment
- `common start` removes the strongest early-period distortion

For honest portfolio reading, `common start` is the cleaner standard.

### 2.3 True benchmark should not be BTC only

Two better portfolio-level benchmarks were checked:

- `7`-asset equal-weight buy-and-hold
- `7`-asset equal-weight rebalanced benchmark

The strategy portfolio still beat them clearly, but this comparison is much more valid than using `KRW-BTC` alone.

## 3. 7-Asset 60m Portfolio

### 3.1 Strategy set

`60m` main rules used:

- BTC: `10/120`
- ETH: `5/60`
- SOL: `5/60/200`
- XRP: `10/20/120`
- ADA: `10/20/120`
- DOGE: `5/400`
- AVAX: `5/400`

### 3.2 Most realistic 60m version tested

Preferred interpretation:

- `common start`
- `change-only`
- `fees 5bp`
- `slippage 5bp`

Result:
- file: [summary.csv](../data/backtest/main_strategies_60m_equal_weight_change_only_common_start_true_slip5bp/summary.csv)
- CAGR `36.96%`
- MDD `22.99%`
- Total Trades `9,138`

Important comparison:

- `every_bar + common start + 5bp`
  - CAGR `36.99%`
  - MDD `23.07%`
  - Trades `47,149`
- `change-only + common start + 5bp`
  - CAGR `36.96%`
  - MDD `22.99%`
  - Trades `9,138`

Conclusion:
- `change-only` dominates `every_bar` on implementation realism
- performance stayed almost identical after the bug fix
- unnecessary drift-correction trading can be removed without meaningful loss

## 4. 7-Asset 240m Portfolio

### 4.1 Strategy set

`240m` rules selected from prior full-period strength plus practical robustness:

- BTC: `10/200`
- ETH: `5/20/120`
- SOL: `5/60`
- XRP: `10/60`
- ADA: `1/20/120`
- DOGE: `1/20/200`
- AVAX: `5/200`

### 4.2 Preferred 240m version tested

- `common start`
- `change-only`
- `fees 5bp`
- `slippage 5bp`

Result:
- file: [summary.csv](../data/backtest/main_strategies_240m_equal_weight_change_only_common_start_true_slip5bp/summary.csv)
- CAGR `36.29%`
- MDD `22.38%`
- Total Trades `3,212`

Interpretation:
- `240m` produced a very similar return profile to the preferred `60m` portfolio
- but with much fewer trades

This makes the `240m` version attractive from a practical execution standpoint.

## 5. 4-Asset 60m Portfolio

Universe:

- BTC
- ETH
- SOL
- XRP

Common start:
- `2021-10-15 06:00 UTC`

Preferred version:
- `change-only`
- `fees 5bp`
- `slippage 5bp`

Result:
- file: [summary.csv](../data/backtest/main_strategies_60m_equal_weight_4core_change_only_common_start_true_slip5bp/summary.csv)
- CAGR `32.88%`
- MDD `26.64%`
- Trades `4,526`

At `2.5bp` slippage:
- file: [summary.csv](../data/backtest/main_strategies_60m_equal_weight_4core_change_only_common_start_true_slip2p5bp/summary.csv)
- CAGR `37.64%`
- MDD `24.68%`

Interpretation:
- `4` assets gave a cleaner, more liquid-feeling universe
- but did not improve the return/drawdown balance versus the preferred `7`-asset setup

## 6. 2-Asset 60m Portfolio

Universe:

- BTC
- ETH

Preferred tested version:
- `change-only`
- `fees 5bp`
- `slippage 5bp`

Result:
- file: [summary.csv](../data/backtest/main_strategies_60m_equal_weight_2core_change_only_true_slip5bp/summary.csv)
- CAGR `68.98%`
- MDD `45.61%`
- Trades `2,709`

At `2.5bp` slippage:
- file: [summary.csv](../data/backtest/main_strategies_60m_equal_weight_2core_change_only_true_slip2p5bp/summary.csv)
- CAGR `74.40%`
- MDD `44.10%`

Interpretation:
- `2` assets concentrate the return engine strongly
- but cost sensitivity and drawdown are both much worse
- this is a return-first sleeve, not the best balanced portfolio

## 7. Current Take

### Best balanced `60m` portfolio candidate

- `7` assets
- `common start`
- `change-only`
- `fees 5bp`
- `slippage 5bp`

Metrics:
- CAGR `36.96%`
- MDD `22.99%`

### Best balanced `240m` portfolio candidate

- `7` assets
- `common start`
- `change-only`
- `fees 5bp`
- `slippage 5bp`

Metrics:
- CAGR `36.29%`
- MDD `22.38%`

### Practical interpretation

The `60m` and `240m` preferred versions are now surprisingly close.

Main trade-off:
- `60m`: slightly higher return
- `240m`: slightly lower drawdown and far fewer trades

## 8. Final Judgment For Now

If the goal is honest portfolio research under realistic execution:

- keep `change-only`
- keep `common start`
- compare against real multi-asset benchmarks
- treat the whole result as survivor-biased and therefore provisional

If the goal is practical live deployment:

- the `240m 7`-asset change-only portfolio looks especially appealing
- the `60m 7`-asset change-only portfolio remains the higher-return alternative

The portfolio work is now much more trustworthy than the original `every_bar` interpretation, but it is still not a true ex-ante historical top-coin portfolio.
