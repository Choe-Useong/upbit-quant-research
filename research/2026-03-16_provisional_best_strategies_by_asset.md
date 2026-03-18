# 2026-03-16 Provisional Best Strategies By Asset

Current shortlist rewritten with a `60m-first` bias.
The idea is simple: prefer the best usable `60m` strategy for each asset, while still noting older `240m` references where they were historically stronger on full-period comparison.

## Current Picks

| Asset | Provisional Best | Timeframe | Family | AIR | CAGR | Recent 1Y AIR | Recent 2Y AIR | MDD | Longest Recovery Bars | Confidence | Note |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| BTC | `10/120 every_bar` | `60m` | `ma_cross` | 0.4200 | 68.16% | 0.4135 | 0.5362 | 36.71% | 11736 | High | Return-first main pick. |
| ETH | `5/60 every_bar` | `60m` | `ma_cross` | 0.5448 | 84.64% | 0.1818 | 0.4600 | 52.05% | 19444 | High | Walk-forward OOS main pick. |
| SOL | `5/60/200 every_bar` | `60m` | `ma_stack` | 0.5686 | 68.30% | 0.6502 | 0.2505 | 45.98% | 5670 | High | Best 60m three-line profile. |
| XRP | `10/20/120 every_bar` | `60m` | `ma_stack` | 0.3728 | 107.90% | 0.8661 | 0.5879 | 51.81% | 24140 | Medium | Main 60m pick despite XRP's naturally long recovery profile. |
| ADA | `10/20/120 every_bar` | `60m` | `ma_stack` | 0.2617 | 109.00% | 0.6808 | 0.6076 | 52.65% | 19998 | Medium | 60m-first main. Older 240m `1/20/120` remained stronger overall. |
| DOGE | `5/400 every_bar` | `60m` | `ma_cross` | 0.4695 | 78.27% | 0.7874 |  | 61.44% | 22568 | Medium | 60m-first robust pick. `20/200` stayed stronger on headline full-period return. |
| AVAX | `5/400 every_bar` | `60m` | `ma_cross` | 0.8148 | 36.68% | 0.6883 |  | 44.48% | 7296 | Medium | 60m-first robust pick. Older 240m `5/200` remained stronger in the old full-period comparison. |

## ETH Sub Pick

- Main: `60m 5/60 every_bar`
- Sub: `60m 3/400 every_bar`

Interpretation:
- `5/60` won most walk-forward folds.
- `3/400` had the cleaner conservative OOS profile.
- `5/60 + 3/400 score exposure` was strong on full backtest but weak in walk-forward OOS, so it stays out of the main slot.

## XRP Sub Pick

- Main: `60m 10/20/120 every_bar`
- Sub: `60m 10/20/120 + 10/400 score exposure`

Sub mix metrics:
- AIR `0.3201`
- CAGR `91.99%`
- Recent 1Y AIR `0.4246`
- Recent 2Y AIR `0.1950`
- MDD `50.26%`
- Longest Recovery `12209`

Interpretation:
- The mix gives up return versus the main.
- But it cuts recovery roughly in half versus `10/20/120`.
- So XRP keeps `10/20/120` as main and the mix as the practical compromise.

## ADA Sub Pick

- Main: `60m 10/20/120 every_bar`
- Sub: `60m 5/120/400 every_bar`

Interpretation:
- `10/20/120` looked cleaner on candidate OOS robustness.
- `5/120/400` won more folds in walk-forward, so it stays as the main 60m alternative.
- The older `240m 1/20/120` is still worth remembering as the stronger non-60m reference.

## DOGE Sub Pick

- Main: `60m 5/400 every_bar`
- Sub: `60m 20/200 + 5/400 score exposure`

Sub mix metrics:
- AIR `0.4798`
- CAGR `82.37%`
- Recent 1Y AIR `0.7331`
- Recent 2Y AIR `0.5560`
- MDD `51.04%`
- Longest Recovery `12932`

Interpretation:
- `5/400` looked better on 60m robustness.
- `20/200` still carried the stronger return-first profile.
- The mix stays as a useful 60m compromise because it keeps more upside than `5/400` while softening the `20/200` profile.

## AVAX Sub Pick

- Main: `60m 5/400 every_bar`
- Sub: `60m 1/400 every_bar`

Interpretation:
- `5/400` is the cleaner 60m robust pick.
- `1/400` won more folds, but only in a 3-fold sample.
- The older `240m 5/200` remains the stronger non-60m reference.

## Notes

- `Recent 2Y AIR` is blank for rows where that column was not refreshed in the source summary.
- `BTC` remains the cleanest 60m main among all assets.
- `ETH` is now a clear 60m asset after walk-forward validation.
- `SOL` keeps the strong `5/60/200` 60m three-line main even though walk-forward sample size was short.
- `XRP` remains usable only with explicit awareness of its long recovery behavior.
- `ADA`, `DOGE`, and `AVAX` are now written in a 60m-first style, but their older 240m references are still worth remembering.
