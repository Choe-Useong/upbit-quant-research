# 2026-03-15 240m VWMA Cross Asset Review

## Scope

- Assets: `KRW-BTC`, `KRW-ETH`, `KRW-SOL`, `KRW-XRP`
- Data frequency: `240m`
- Strategy family: `2-line VWMA crossover`
- Cost assumption: `fees = 0.05%`
- Comparison axes:
  - `every_bar` vs `daily` vs `weekly` rebalance
  - best and worst parameter combinations per asset
  - recent 1Y performance
  - comparison against prior `SMA` crossover results

## Method

- For each asset, ran a grid over:
  - `short = [5, 10, 20, 30]`
  - `long = [10, 20, 60, 120, 200]`
  - constraint: `short < long`
- Signal:
  - build `vwma_short`
  - build `vwma_long`
  - hold only when `vwma_short > vwma_long`
- Rebalance frequencies tested:
  - `every_bar`
  - `daily`
  - `weekly`

## BTC

### Overall Best 3

- `10/200 every_bar`: AIR `0.3286`, Sharpe `1.6999`, MDD `45.61%`, Return `+23,999.54%`
- `5/200 every_bar`: AIR `0.2973`, Sharpe `1.6710`, MDD `52.94%`, Return `+21,111.37%`
- `5/60 every_bar`: AIR `0.2751`, Sharpe `1.6773`, MDD `44.26%`, Return `+20,001.15%`

### Overall Worst 3

- `5/60 weekly`: AIR `-0.3351`, Sharpe `0.8885`, MDD `72.75%`, Return `+1,106.88%`
- `20/60 weekly`: AIR `-0.3291`, Sharpe `0.8923`, MDD `73.23%`, Return `+1,137.99%`
- `10/20 weekly`: AIR `-0.3147`, Sharpe `0.8838`, MDD `82.31%`, Return `+1,209.05%`

### Recent 1Y Review

- `10/200 every_bar`
  - Recent 1Y Return `+9.12%`, Recent 1Y AIR `0.5425`
- `5/200 every_bar`
  - Recent 1Y Return `+16.60%`, Recent 1Y AIR `0.7164`
- `5/60 every_bar`
  - Recent 1Y Return `+20.85%`, Recent 1Y AIR `0.8965`

### Interpretation

- BTC VWMA works, but full-sample AIR is weaker than BTC SMA.
- The recent 1Y picture is better than the full-sample headline, especially for `5/60` and `5/200`.
- `weekly` remains poor here as well.

## ETH

### Overall Best 3

- `5/20 every_bar`: AIR `0.5684`, Sharpe `1.8363`, MDD `51.63%`, Return `+109,663.60%`
- `5/120 every_bar`: AIR `0.5309`, Sharpe `1.7599`, MDD `42.94%`, Return `+82,889.97%`
- `5/200 every_bar`: AIR `0.4310`, Sharpe `1.5933`, MDD `58.02%`, Return `+42,564.43%`

### Overall Worst 3

- `10/20 weekly`: AIR `-0.2924`, Sharpe `0.6574`, MDD `90.84%`, Return `+511.72%`
- `20/60 weekly`: AIR `-0.1515`, Sharpe `0.8334`, MDD `77.04%`, Return `+1,292.35%`
- `5/10 weekly`: AIR `-0.1239`, Sharpe `0.9175`, MDD `75.13%`, Return `+1,756.53%`

### Recent 1Y Review

- `5/20 every_bar`
  - Recent 1Y Return `+106.21%`, Recent 1Y AIR `0.9741`
- `5/120 every_bar`
  - Recent 1Y Return `+54.12%`, Recent 1Y AIR `0.4034`
- `5/200 every_bar`
  - Recent 1Y Return `+57.17%`, Recent 1Y AIR `0.4328`

### Interpretation

- ETH is the clearest VWMA winner.
- `5/20 every_bar` improved over the prior ETH SMA best on both full-sample AIR and recent 1Y AIR.
- VWMA seems genuinely useful for ETH rather than just a cosmetic variation.

## SOL

### Overall Best 3

- `10/60 every_bar`: AIR `0.5538`, Sharpe `1.2758`, MDD `43.56%`, Return `+1,181.44%`
- `30/200 every_bar`: AIR `0.4781`, Sharpe `1.2400`, MDD `53.01%`, Return `+989.21%`
- `10/20 every_bar`: AIR `0.4670`, Sharpe `1.0885`, MDD `60.94%`, Return `+733.78%`

### Overall Worst 3

- `10/20 weekly`: AIR `-0.6045`, Sharpe `-0.1963`, MDD `88.21%`, Return `-78.83%`
- `5/10 weekly`: AIR `-0.5678`, Sharpe `-0.2286`, MDD `88.34%`, Return `-76.53%`
- `30/60 weekly`: AIR `-0.4619`, Sharpe `-0.0901`, MDD `87.29%`, Return `-66.39%`

### Recent 1Y Review

- `10/60 every_bar`
  - Recent 1Y Return `-21.31%`, Recent 1Y AIR `0.0506`
- `30/200 every_bar`
  - Recent 1Y Return `+11.33%`, Recent 1Y AIR `0.5323`
- `10/20 every_bar`
  - Recent 1Y Return `-41.57%`, Recent 1Y AIR `-0.4100`

### Interpretation

- SOL VWMA looks stronger than SOL SMA on full-sample AIR.
- But the headline winner `10/60 every_bar` is not convincing on the recent 1Y horizon.
- The more practical SOL VWMA candidate is `30/200 every_bar`, not the top full-sample AIR run.

## XRP

### Overall Best 3

- `10/60 every_bar`: AIR `0.3020`, Sharpe `1.4222`, MDD `51.18%`, Return `+64,721.77%`
- `5/20 every_bar`: AIR `0.2616`, Sharpe `1.3788`, MDD `57.91%`, Return `+49,375.90%`
- `5/60 every_bar`: AIR `0.2329`, Sharpe `1.3629`, MDD `60.58%`, Return `+42,890.43%`

### Overall Worst 3

- `5/10 weekly`: AIR `-0.6925`, Sharpe `0.1927`, MDD `91.28%`, Return `-49.30%`
- `5/20 weekly`: AIR `-0.3805`, Sharpe `0.6266`, MDD `86.60%`, Return `+423.31%`
- `10/200 weekly`: AIR `-0.3491`, Sharpe `0.6587`, MDD `78.37%`, Return `+530.77%`

### Recent 1Y Review

- `10/60 every_bar`
  - Recent 1Y Return `-4.41%`, Recent 1Y AIR `0.6131`
- `5/20 every_bar`
  - Recent 1Y Return `-5.40%`, Recent 1Y AIR `0.6130`
- `5/60 every_bar`
  - Recent 1Y Return `-5.94%`, Recent 1Y AIR `0.5906`

### Interpretation

- XRP VWMA is decent, but it does not beat XRP SMA on full-sample AIR.
- Recent 1Y AIR is fine, but recent absolute returns are still negative.
- That makes XRP VWMA harder to prioritize despite acceptable benchmark-relative behavior.

## SMA vs VWMA Summary

### Best-by-Asset Comparison

- BTC
  - SMA best: `10/200 every_bar`, AIR `0.3899`
  - VWMA best: `10/200 every_bar`, AIR `0.3286`
  - Review: SMA still better.

- ETH
  - SMA best: `5/20 every_bar`, AIR `0.5213`
  - VWMA best: `5/20 every_bar`, AIR `0.5684`
  - Review: VWMA improved the strategy.

- SOL
  - SMA best: `5/60 every_bar`, AIR `0.4354`
  - VWMA best: `10/60 every_bar`, AIR `0.5538`
  - Review: VWMA improved full-sample AIR, but recent 1Y behavior still needs caution.

- XRP
  - SMA best: `10/60 every_bar`, AIR `0.3276`
  - VWMA best: `10/60 every_bar`, AIR `0.3020`
  - Review: SMA still better.

## Final Interpretation

- VWMA is not a blanket improvement.
- It looks clearly useful for `ETH`, where the best VWMA setup outperformed the prior best SMA setup.
- It also looks interesting for `SOL`, but the best full-sample VWMA setup is not the best recent-1Y setup, so the signal needs more care.
- For `BTC` and `XRP`, VWMA did not beat the existing SMA baseline on full-sample AIR.
- Across all four assets, the same structural warning remains:
  - `weekly` is poor
  - `every_bar` dominates

## Current VWMA Candidates

- Primary
  - `ETH 5/20 every_bar`
  - `SOL 30/200 every_bar`

- Secondary
  - `BTC 5/60 every_bar`
  - `BTC 5/200 every_bar`
  - `XRP 10/60 every_bar`
