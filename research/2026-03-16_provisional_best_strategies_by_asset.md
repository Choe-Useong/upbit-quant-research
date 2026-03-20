# 2026-03-16 Provisional Best Strategies By Asset

Current shortlist rewritten with a `60m-first` bias.
The idea is simple: prefer the best usable `60m` strategy for each asset, while still noting older `240m` references where they were historically stronger on full-period comparison.

## Current Picks

| Asset | Provisional Best | Timeframe | Family | AIR | CAGR | Recent 1Y AIR | Recent 2Y AIR | MDD | Longest Recovery Bars | Confidence | Note |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| BTC | `10/120 exit-persist3` | `60m` | `ma_cross_exit_persist` | 0.4420 | 69.17% | 0.5360 | 0.6229 | 39.27% | 11757 | High | Entry stays immediate, but exit requires 3 consecutive off-bars. This beat raw `10/120` on full sample and candidate OOS in `18/6/3` walk-forward. |
| ETH | `5/60 persist3 every_bar` | `60m` | `ma_cross_persist` | 0.5502 | 86.99% | 0.1487 | 0.4304 | 52.51% | 8869 | High | `3-bar` symmetric persistence improved full-sample AIR/CAGR and candidate OOS in `18/6/3` walk-forward. Rolling still often chose raw `5/60`, but expanding and fixed-candidate OOS both leaned toward `persist3`. |
| SOL | `logprice_vwma 5/60 every_bar` | `60m` | `logprice_vwma` | 0.6714 | 62.17% | 0.3834 | 0.5832 | 50.66% | 6870 | Medium | Promoted on candidate OOS robustness. Old `5/60/200` still had higher fold wins, but only in a 3-fold sample. |
| XRP | `10/20/200 every_bar` | `60m` | `ma_stack` | 0.3233 | 101.94% | 0.7456 | 0.2650 | 57.32% | 24254 | Medium | Promoted on true full-grid `3-line expanding` walk-forward. Short-window checks also kept the `3-line` family alive and expanding continued to favor `10/20/200`. |
| ADA | `10/20/120 every_bar` | `60m` | `ma_stack` | 0.2617 | 109.00% | 0.6808 | 0.6076 | 52.65% | 19998 | Medium | 60m-first main. Older 240m `1/20/120` remained stronger overall. |
| DOGE | `5/400 every_bar` | `60m` | `ma_cross` | 0.4695 | 78.27% | 0.7874 |  | 61.44% | 22568 | Medium | 60m-first robust pick. `20/200` stayed stronger on headline full-period return. |
| AVAX | `vwgm_logvol 5/400 every_bar` | `60m` | `vwgm_logvol` | 0.8646 | 40.35% | 0.6981 | 0.9171 | 44.11% | 7297 | Medium | Experimental log-price plus log-volume geometric-mean variant now leads the 60m shortlist, but confidence remains capped by the short 3-fold AVAX walk-forward sample. |

## ETH Sub Pick

- Main: `60m 5/60 persist3 every_bar`
- Sub: `60m 5/60 every_bar`

Interpretation:
- `5/60` was the original clean ETH champion.
- Adding `3-bar` persistence on both entry and exit improved full-sample AIR/CAGR and raised fixed-candidate OOS median AIR in `18/6/3` walk-forward.
- Rolling still selected raw `5/60` more often, so the raw version remains the simpler fallback.
- `3/400` and transformed VWMA variants stay as older experiments, but they are no longer the main ETH sub slot.

## BTC Sub Pick

- Main: `60m 10/120 exit-persist3`
- Sub: `60m 10/120 every_bar`

Interpretation:
- `10/120` was already the clean BTC baseline.
- The new stateful variant keeps entry immediate but requires `3` consecutive off-bars before exit.
- That asymmetric rule improved full-sample AIR/CAGR and also beat the baseline on candidate OOS in `18/6/3` walk-forward.
- `exit-persist6` was also viable, but `persist3` looked better on candidate OOS and stayed less delayed.

## XRP Sub Pick

- Main: `60m 10/20/200 every_bar`
- Sub: `60m 10/20/120 every_bar`

Interpretation:
- `10/20/120` was the prior main and remains the more conservative rolling-style candidate.
- But true full-grid `3-line expanding` walk-forward strongly favored `10/20/200`.
- Shorter `3/1/1` and `6/3/1` tests did not break the `3-line` conclusion.
- So XRP now keeps `10/20/200` as the selection-style main and `10/20/120` as the conservative fallback.

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
- `BTC` remains the cleanest 60m main among all assets, but the raw baseline is now replaced by a stateful `10/120 exit-persist3` variant.
- `ETH` is now a clear 60m asset after walk-forward validation, and the first ETH-specific rule extension that survived both full-sample and walk-forward review is `persist3`.
- `SOL` now leans toward `logprice_vwma 5/60` because candidate OOS robustness looked much stronger than `5/60/200`, even though the walk-forward sample is still only 3 folds.
- `XRP` remains usable only with explicit awareness of its long recovery behavior, and now leans toward `10/20/200` when viewed in the same expanding-style way the current research process naturally selects strategies.
- `ADA`, `DOGE`, and `AVAX` are now written in a 60m-first style, but their older 240m references are still worth remembering.
