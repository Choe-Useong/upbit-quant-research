# 2026-03-16 60m MA Cross Asset Summary

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
- Timeframe: `60m`
- Cost: `fees = 0.05%`
- Lag: `lag = 1`
- Focus:
  - identify the best `60m` plain 2-line SMA candidates
  - compare them against the previously established `240m` structures
  - decide which assets really improved by moving down to `60m`

## Best 60m 2-Line by Asset

- `BTC`
  - Main candidate: `10/120 every_bar`
  - AIR `0.4200`
  - CAGR `68.16%`
  - Recent 1Y AIR `0.4135`
  - MDD `36.71%`
  - Interpretation:
    - BTC is the cleanest `60m` case.
    - The best result is still a plain 2-line structure.
    - Extra overlays such as score exposure, 3-line, and incremental sizing did not materially improve it.

- `ETH`
  - Main candidate: `5/60 every_bar`
  - AIR `0.5448`
  - CAGR `84.64%`
  - Recent 1Y AIR `0.1818`
  - MDD `52.05%`
  - Interpretation:
    - ETH improved on `60m` in headline AIR.
    - The structure is not a single isolated point.
    - `60`-long and `120~400`-long families both show meaningful plateau support.
    - Still, `240m` score-exposure remains the more established overall ETH result.

- `SOL`
  - Practical candidate: `10/60 every_bar`
  - AIR `0.5372`
  - CAGR `50.89%`
  - Recent 1Y AIR `0.3623`
  - MDD `47.76%`
  - Interpretation:
    - `5/60` is the headline top by AIR, but `10/60` is more balanced.
    - `60/200` is also notable because recent 1Y AIR is strong and MDD is lower.
    - SOL improved on `60m`, but the candidate set is more complex than BTC.

- `XRP`
  - Balanced candidate: `5/200 every_bar`
  - AIR `0.3634`
  - CAGR `85.70%`
  - Recent 1Y AIR `0.5258`
  - MDD `52.32%`
  - Interpretation:
    - `10/20` is the headline top result, but plateau is almost nonexistent.
    - XRP on `60m` is strong numerically but still very sensitive.
    - For research quality, robust candidates such as `5/200` or `10/120` are more convincing than the raw `10/20` winner.

- `ADA`
  - Best 60m 2-line: `20/60 every_bar`
  - AIR `0.4145`
  - CAGR `109.99%`
  - Recent 1Y AIR `0.9764`
  - MDD `57.33%`
  - Interpretation:
    - ADA `60m` 2-line is strong and more practical than the best `240m` 2-line.
    - However, the real ADA main candidate is still `240m 1/20/120` 3-line.
    - So `60m` is useful, but it does not overturn the current ADA core view.

- `DOGE`
  - Best 60m 2-line: `20/200 every_bar`
  - AIR `0.4847`
  - CAGR `84.46%`
  - Recent 1Y AIR `0.6384`
  - MDD `53.37%`
  - Interpretation:
    - DOGE `60m` 2-line is unusually strong and also reasonably robust.
    - Still, `240m 1/20/200` 3-line remains better as the full DOGE main structure.
    - `60m 20/200` is therefore a strong secondary candidate, not the final replacement.

- `AVAX`
  - Best 60m 2-line family:
    - `1/400`
    - `5/120`
    - `5/400`
  - Best headline result: `1/400 every_bar`
    - AIR `0.8327`
    - CAGR `38.07%`
    - Recent 1Y AIR `0.5320`
    - MDD `49.03%`
  - Interpretation:
    - AVAX `60m` works well, but `240m` still looks stronger.
    - Plateau support is good, yet the `240m 5/200` family remains superior overall.

## Cross-Asset Pattern

- `BTC`
  - `60m` plain 2-line is the clearest upgrade.
- `ETH`
  - `60m` plain 2-line improved, but the overall ETH thesis is still mixed with `240m` score-exposure.
- `SOL`
  - `60m` improved, though candidate interpretation is less clean than BTC.
- `XRP`
  - `60m` improved, but the best raw result is too sharp to trust blindly.
- `ADA`
  - `60m` 2-line improved the plain 2-line case, but not the true asset-level best structure.
- `DOGE`
  - `60m` 2-line is strong, yet the best full DOGE structure still comes from `240m` 3-line.
- `AVAX`
  - `240m` remains the preferred timeframe.

## Current Conclusion

- Assets where `60m` plain 2-line meaningfully matters:
  - `BTC`
  - `ETH`
  - `SOL`
  - `XRP`
- Assets where `60m` 2-line is good but does not replace the current best framework:
  - `ADA`
  - `DOGE`
- Asset where `240m` still clearly dominates:
  - `AVAX`

## Working View

- `BTC`
  - use `60m 10/120 every_bar` as the current main 2-line candidate.
- `ETH`
  - keep `60m 5/60 every_bar` as the main plain 2-line candidate.
  - but do not discard the stronger `240m` structured variants yet.
- `SOL`
  - track `10/60` as the balanced `60m` candidate.
- `XRP`
  - treat `10/20` as an aggressive result and `5/200` / `10/120` as more robust candidates.
- `ADA`
  - keep `240m 1/20/120` as the core candidate.
- `DOGE`
  - keep `240m 1/20/200` as the core candidate.
- `AVAX`
  - keep `240m 5/200` as the core candidate.
