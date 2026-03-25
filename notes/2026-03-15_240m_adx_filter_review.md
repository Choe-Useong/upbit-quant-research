# 2026-03-15 240m ADX Filter Review

## Scope

- Data frequency: `240m`
- Lag assumption: `lag=1`
- Rebalance: `every_bar`
- Cost assumption: `fees = 0.05%`
- Purpose:
  - test whether `ADX` can improve core 2-line SMA strategies
  - compare filtered variants against existing unfiltered baseline candidates

## ADX Setup

- `ADX` was added as a feature transform.
- Tested filter form:
  - `SMA cross AND ADX(window) > threshold`
- Windows tested:
  - `10`
  - `14`
  - `20`
- Thresholds tested:
  - `15`
  - `20`
  - `25`
  - `30`

Interpretation:

- lower threshold = looser filter
- higher threshold = stricter filter
- larger window = slower trend-strength estimate

## BTC: `10/200 every_bar` + ADX Filter

### Baseline

- `BTC 10/200 every_bar`
  - AIR `0.3899`
  - MDD `49.30%`
  - Sharpe `1.7588`
  - Rolling IR 252d Median `0.3144`
  - Return `+30,929.14%`

### Best ADX Variants

- `ADX 10 > 15`
  - AIR `0.3218`
  - Recent 1Y AIR `0.3287`
  - MDD `51.93%`
  - Sharpe `1.6949`
  - Rolling IR 252d Median `0.2294`
  - Return `+23,387.57%`

- `ADX 14 > 15`
  - AIR `0.2904`
  - MDD `52.52%`
  - Sharpe `1.6760`
  - Return `+20,855.75%`

- `ADX 20 > 15`
  - AIR `0.2483`
  - MDD `52.21%`
  - Sharpe `1.6557`
  - Return `+17,958.71%`

### BTC Interpretation

- No tested ADX variant improved the baseline.
- Even the best ADX version was weaker on:
  - AIR
  - Sharpe
  - rolling IR median
  - total return
- MDD also did not improve meaningfully.
- Stricter thresholds (`20`, `25`, `30`) consistently degraded the strategy further.

Conclusion:

- `BTC 10/200` already behaves like a slow, selective trend filter.
- Adding `ADX` mostly removed profitable trend participation instead of removing enough bad trades.
- For BTC, `ADX` is not a useful add-on to the core `10/200` strategy in the tested range.

Files:

- [grid_btc_ma_10_200_adx_grid_240m_fee5bp.json](../configs/grid_btc_ma_10_200_adx_grid_240m_fee5bp.json)
- [summary_results.csv](../data/grid/btc_ma_10_200_adx_grid_240m_fee5bp/summary_results.csv)

## ETH: `5/20 every_bar` + ADX Filter

### Baseline

- `ETH 5/20 every_bar`
  - AIR `0.5213`
  - MDD `53.11%`
  - Sharpe `1.7875`
  - Rolling IR 252d Median `0.1772`
  - Return `+85,456.66%`

### Best ADX Variants

- `ADX 10 > 15`
  - AIR `0.4990`
  - Recent 1Y AIR `0.8704`
  - MDD `51.58%`
  - Sharpe `1.7855`
  - Rolling IR 252d Median `0.3142`
  - Return `+79,235.82%`

- `ADX 14 > 15`
  - AIR `0.4416`
  - Recent 1Y AIR `0.6524`
  - MDD `56.72%`
  - Sharpe `1.7471`
  - Rolling IR 252d Median `0.2806`
  - Return `+60,741.08%`

- `ADX 10 > 20`
  - AIR `0.3500`
  - Recent 1Y AIR `0.3112`
  - MDD `47.01%`
  - Sharpe `1.6848`
  - Return `+39,353.66%`

### ETH Interpretation

- ETH did not show a true performance upgrade either.
- The best variant, `ADX 10 > 15`, was close enough to matter:
  - AIR slightly lower than baseline
  - Sharpe almost unchanged
  - MDD slightly lower
  - rolling IR median much higher
- This makes `ADX 10 > 15` an interesting robustness-oriented alternative, but not a replacement.
- As with BTC, stricter thresholds caused the strategy to weaken quickly.

Conclusion:

- For ETH, `ADX` may have some value as a smoothing or robustness filter.
- But it still did not beat the plain `5/20` baseline on headline AIR or total return.
- ETH baseline remains the main candidate.
- `ADX 10 > 15` is the only ADX variant worth keeping as a secondary note.

Files:

- [grid_eth_ma_5_20_adx_grid_240m_fee5bp.json](../configs/grid_eth_ma_5_20_adx_grid_240m_fee5bp.json)
- [summary_results.csv](../data/grid/eth_ma_5_20_adx_grid_240m_fee5bp/summary_results.csv)

## Overall Interpretation

- ADX is a widely used indicator, but that does not mean it improves every MA strategy.
- In this project, the tested MA baselines were already strong and selective:
  - `BTC 10/200`
  - `ETH 5/20`
- In that context, `ADX` behaved more like a redundant confirmation layer than a useful improvement.
- The best ADX configurations were always the loosest ones:
  - `window 10`
  - `threshold 15`
- That is a sign that stronger ADX gating was too restrictive.

## Final Conclusion

- `BTC`: ADX filter should be rejected for now.
- `ETH`: ADX filter is not a main improvement, but `ADX 10 > 15` can be remembered as a softer, more robust-looking variant.
- The core research direction should remain:
  - base SMA
  - score exposure
  - asset-specific condition combinations
rather than ADX filtering.
