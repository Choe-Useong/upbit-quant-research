# 2026-03-16 60m vs 240m MA Cross Review

## Scope

- Assets:
  - `BTC`
  - `ETH`
  - `SOL`
  - `XRP`
  - `ADA`
  - `DOGE`
  - `AVAX`
- Strategy family: `2-line SMA crossover`
- Cost: `fees = 0.05%`
- Lag: `lag = 1`
- Purpose:
  - compare the best `60m` 2-line result for each asset
  - against the previously best `240m` 2-line result for the same asset

## Best-by-Asset Comparison

- `BTC`
  - `60m`: `10/120 every_bar`
    - AIR `0.4200`
    - CAGR `68.16%`
    - Recent 1Y AIR `0.4135`
    - MDD `36.71%`
  - `240m`: `10/200 every_bar`
    - AIR `0.3899`
    - MDD `49.30%`
  - Interpretation:
    - `60m` improved both AIR and MDD.
    - BTC appears to benefit from a faster timeframe while still preferring a long trend filter.

- `ETH`
  - `60m`: `5/60 every_bar`
    - AIR `0.5448`
    - CAGR `84.64%`
    - Recent 1Y AIR `0.1818`
    - MDD `52.05%`
  - `240m`: `5/20 every_bar`
    - AIR `0.5213`
    - MDD `53.11%`
  - Interpretation:
    - headline AIR improved on `60m`.
    - but recent 1Y strength was not especially convincing.
    - ETH still looks strong on both timeframes, but `60m` needs more robustness work than the raw AIR suggests.

- `SOL`
  - `60m`: `20/120 daily`
    - AIR `0.6290`
    - CAGR `59.19%`
    - Recent 1Y AIR `0.4148`
    - MDD `44.75%`
  - `240m`: `5/60 every_bar`
    - AIR `0.4354`
    - MDD `47.03%`
  - Interpretation:
    - `60m` was clearly better than `240m` for plain 2-line SMA.
    - unlike most other assets, `daily` rebalance won on `60m`.
    - this is a meaningful change in structure, not just a small parameter shift.

- `XRP`
  - `60m`: `10/20 every_bar`
    - AIR `0.3967`
    - CAGR `91.93%`
    - Recent 1Y AIR `1.1001`
    - MDD `66.28%`
  - `240m`: `10/60 every_bar`
    - AIR `0.3276`
    - MDD `55.52%`
  - Interpretation:
    - `60m` improved AIR and recent 1Y AIR materially.
    - the trade-off was a larger drawdown.
    - XRP on `60m` looks stronger, but still rough.

- `ADA`
  - `60m`: `20/60 every_bar`
    - AIR `0.4145`
    - CAGR `109.99%`
    - Recent 1Y AIR `0.9764`
    - MDD `57.33%`
  - `240m`: `10/200 daily`
    - AIR `0.4258`
    - Recent 1Y AIR `1.2446`
    - MDD `71.68%`
  - Interpretation:
    - AIR was slightly lower on `60m`.
    - MDD improved a lot.
    - for ADA 2-line only, `60m` looks more practical, but the true ADA main candidate is still the `240m 1/20/120` 3-line structure.

- `DOGE`
  - `60m`: `20/200 every_bar`
    - AIR `0.4847`
    - CAGR `84.46%`
    - Recent 1Y AIR `0.6384`
    - MDD `53.37%`
  - `240m`: `5/20 every_bar`
    - AIR `0.5286`
    - Recent 1Y AIR `0.5673`
    - MDD `67.57%`
  - Interpretation:
    - `60m` had lower AIR than `240m`.
    - but MDD was much better and recent 1Y AIR was slightly better.
    - DOGE on `60m` looks more balanced than on `240m`.

- `AVAX`
  - `60m`: `5/120 every_bar`
    - AIR `0.8296`
    - CAGR `36.31%`
    - Recent 1Y AIR `0.2853`
    - MDD `54.68%`
  - `240m`: `5/200 every_bar`
    - AIR `0.8724`
    - CAGR `35.12%`
    - Recent 1Y AIR `0.7222`
    - MDD `49.35%`
  - Interpretation:
    - `240m` remained better overall.
    - `60m` still worked, but AVAX did not improve by moving faster.

## Cross-Asset Interpretation

- `60m` did not simply dominate `240m`.
- The improvement pattern was asset-specific.
- `BTC`, `SOL`, and `XRP` benefited the most from moving down to `60m`.
- `AVAX` clearly remained better on `240m`.
- `ETH` and `DOGE` became more ambiguous:
  - `60m` improved some top-line metrics
  - but did not clearly settle the robustness question.
- `weekly` remained weak even on `60m`.
- `every_bar` still dominated most assets.
- The only strong `daily` exception on `60m` was `SOL`.

## Current View

- `BTC`
  - `60m 10/120 every_bar` is now a serious candidate against `240m 10/200`.
- `ETH`
  - `60m` 2-line looks good numerically, but `240m` score-exposure remains the more established result.
- `SOL`
  - `60m 20/120 daily` is the strongest plain 2-line SOL result so far.
- `XRP`
  - `60m 10/20 every_bar` improved the plain 2-line case, but MDD is still the main issue.
- `ADA`
  - `60m` 2-line got cleaner, but `240m 3-line` still looks like the real main candidate.
- `DOGE`
  - `60m` looks more balanced than `240m` in 2-line form.
- `AVAX`
  - stick with `240m` for now.

## Next Step

- The next meaningful comparison is not another wide 2-line scan.
- It is:
  - `60m` candidate extensions on the assets that improved
  - especially `BTC`, `SOL`, and `XRP`
- For `ETH`, `ADA`, and `DOGE`, the real question is whether their `240m` best structures
  - `score exposure`
  - or `3-line`
  still hold up after moving to `60m`.
