# 2026-03-16 Provisional Best Strategies By Asset

Current shortlist rewritten with a `60m-first` bias.
The idea is simple: prefer the best usable `60m` strategy for each asset, while still noting older `240m` references where they were historically stronger on full-period comparison.

## Current Picks

| Asset | Provisional Best | Timeframe | Family | AIR | CAGR | Recent 1Y AIR | Recent 2Y AIR | MDD | Longest Recovery Bars | Confidence | Note |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| BTC | `10/120 every_bar` | `60m` | `ma_cross` | 0.4200 | 68.16% | 0.4135 | 0.5362 | 36.71% | 11736 | High | Return-first main pick. |
| ETH | `5/60 every_bar` | `60m` | `ma_cross` | 0.5448 | 84.64% | 0.1818 | 0.4600 | 52.05% | 19444 | High | Walk-forward OOS main pick. `log-price+log-volume VWMA 5/400` looked interesting on full sample but is still a secondary experiment. |
| SOL | `logprice_vwma 5/60 every_bar` | `60m` | `logprice_vwma` | 0.6714 | 62.17% | 0.3834 | 0.5832 | 50.66% | 6870 | Medium | Promoted on candidate OOS robustness. Old `5/60/200` still had higher fold wins, but only in a 3-fold sample. |
| XRP | `10/20/120 every_bar` | `60m` | `ma_stack` | 0.3728 | 107.90% | 0.8661 | 0.5879 | 51.81% | 24140 | Medium | Main 60m pick despite XRP's naturally long recovery profile. |
| ADA | `10/20/120 every_bar` | `60m` | `ma_stack` | 0.2617 | 109.00% | 0.6808 | 0.6076 | 52.65% | 19998 | Medium | 60m-first main. Older 240m `1/20/120` remained stronger overall. |
| DOGE | `5/400 every_bar` | `60m` | `ma_cross` | 0.4695 | 78.27% | 0.7874 |  | 61.44% | 22568 | Medium | 60m-first robust pick. `20/200` stayed stronger on headline full-period return. |
| AVAX | `vwgm_logvol 5/400 every_bar` | `60m` | `vwgm_logvol` | 0.8646 | 40.35% | 0.6981 | 0.9171 | 44.11% | 7297 | Medium | Experimental log-price plus log-volume geometric-mean variant now leads the 60m shortlist, but confidence remains capped by the short 3-fold AVAX walk-forward sample. |

## ETH Sub Pick

- Main: `60m 5/60 every_bar`
- Sub: `60m 3/400 every_bar`

Interpretation:
- `5/60` won most walk-forward folds.
- `3/400` had the cleaner conservative OOS profile.
- `5/60 + 3/400 score exposure` was strong on full backtest but weak in walk-forward OOS, so it stays out of the main slot.
- `log-price+log-volume VWMA 5/400` is worth remembering as an experimental transform candidate, but it has not earned promotion over `3/400` yet.

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

- Main: `60m vwgm_logvol 5/400 every_bar`
- Sub: `60m 5/400 every_bar`

Interpretation:
- `vwgm_logvol 5/400` beat the prior `SMA 5/400` on both full-sample headline metrics and the short AVAX OOS candidate check.
- Confidence stays medium because AVAX still only has a 3-fold walk-forward sample.
- The older `240m 5/200` remains the stronger non-60m reference, and plain `SMA 5/400` stays as the conservative 60m fallback.

## Notes

- `Recent 2Y AIR` is blank for rows where that column was not refreshed in the source summary.
- `BTC` remains the cleanest 60m main among all assets.
- `ETH` is now a clear 60m asset after walk-forward validation.
- `SOL` now leans toward `logprice_vwma 5/60` because candidate OOS robustness looked much stronger than `5/60/200`, even though the walk-forward sample is still only 3 folds.
- `XRP` remains usable only with explicit awareness of its long recovery behavior.
- `ADA`, `DOGE`, and `AVAX` are now written in a 60m-first style, but their older 240m references are still worth remembering.
