# 2026-03-16 240m Hierarchical Alt Review

## Objective

Review an alternative hierarchical score-exposure design on 240m candles.

This version used:

- 1st stage: `short > long`
- 2nd stage: `mid > long`

Score definition:

- `0.0` if `short <= long`
- `0.5` if `short > long` but `mid <= long`
- `1.0` if `short > long` and `mid > long`

In feature terms:

- `weak_signal = sma_short > sma_long`
- `full_signal = weak_signal AND (sma_mid > sma_long)`
- `exposure_score = 0.5 * weak_signal + 0.5 * full_signal`

This differs from the earlier hierarchy:

- earlier: `weak = mid > long`, `strong = short > mid`
- alternative: `weak = short > long`, `strong = mid > long`

The alternative version is more like:

- fast entry first
- slower confirmation later

## BTC

Candidates:

- `5/20/200`
- `10/20/200`
- `10/60/200`

Best alternative hierarchical candidate:

- `10/20/200 every_bar`
  - AIR `0.3455`
  - Recent 1Y AIR `0.3872`
  - Sharpe `1.7182`
  - Rolling IR 252d Median `0.2050`

Earlier hierarchy best:

- `10/20/200 every_bar`
  - AIR `0.2297`
  - Recent 1Y AIR `0.6433`
  - Sharpe `1.8641`
  - Rolling IR 252d Median `-0.0044`

Main BTC baseline:

- `10/200 every_bar`
  - AIR `0.3899`

Review:

- The alternative hierarchy is clearly better than the earlier hierarchy for BTC.
- The earlier version was too restrictive and unstable on rolling IR.
- But the plain BTC baseline still remains stronger overall.

## ETH

Candidates:

- `5/20/120`
- `5/20/200`
- `10/20/200`

Best alternative hierarchical candidate:

- `10/20/200 every_bar`
  - AIR `0.4374`
  - Recent 1Y AIR `0.2470`
  - Sharpe `1.5757`
  - Rolling IR 252d Median `0.1760`

Earlier hierarchy best:

- `5/20/200 every_bar`
  - AIR `0.3996`
  - Recent 1Y AIR `0.6374`
  - Sharpe `1.8268`
  - Rolling IR 252d Median `0.1916`

Main ETH baselines:

- Equal-mix score exposure `1/120 + 5/20`
  - AIR `0.5485`
- Plain `5/20 every_bar`
  - AIR `0.5213`

Review:

- The alternative hierarchy slightly improves full-sample AIR over the earlier hierarchy.
- But recent 1Y behavior becomes much worse.
- ETH still clearly prefers the previously identified top candidates.

## ADA

Candidates:

- `1/20/120`
- `5/20/120`
- `10/20/120`

Best alternative hierarchical candidate:

- `1/20/120 every_bar`
  - AIR `0.4545`
  - Recent 1Y AIR `0.7417`
  - Sharpe `1.6838`
  - Rolling IR 252d Median `0.6554`

Earlier hierarchy best:

- `1/20/120 every_bar`
  - AIR `0.4207`
  - Recent 1Y AIR `0.8815`
  - Sharpe `1.8253`
  - Rolling IR 252d Median `0.4923`

Main ADA baseline:

- Plain `1/20/120 every_bar`
  - AIR `0.4356`
  - Recent 1Y AIR `0.9934`
  - Sharpe `2.0132`

Review:

- ADA is the strongest case for this alternative hierarchy.
- AIR and rolling IR median improved versus the earlier hierarchy.
- But Sharpe and recent 1Y AIR remain weaker than the plain 3-line stack.
- So even here, the alternative hierarchy is a serious secondary candidate, not the new main candidate.

## DOGE

Candidates:

- `1/20/200`
- `5/20/200`
- `10/20/200`

Best alternative hierarchical candidate:

- `1/20/200 every_bar`
  - AIR `0.3763`
  - Recent 1Y AIR `0.4041`
  - Sharpe `1.2474`
  - Rolling IR 252d Median `0.4448`

Earlier hierarchy best:

- `1/20/200 every_bar`
  - AIR `0.4101`
  - Recent 1Y AIR `0.3802`
  - Sharpe `1.4827`
  - Rolling IR 252d Median `0.4165`

Main DOGE baseline:

- Plain `1/20/200 every_bar`
  - AIR `0.4945`
  - Recent 1Y AIR `0.7514`
  - Sharpe `1.7782`

Review:

- DOGE does not benefit from the alternative hierarchy.
- It weakens AIR and Sharpe versus the earlier hierarchy.
- The plain 3-line stack still dominates.

## Cross-Asset Interpretation

Compared with the earlier hierarchy:

- Better on BTC
- Better on ADA
- Worse on DOGE
- Mixed on ETH, but not useful enough

This suggests the alternative hierarchy is more natural when:

- the asset benefits from faster initial entry
- while still needing slower confirmation later

That pattern seems more relevant to BTC and ADA than to ETH or DOGE.

## Final Conclusion

- The alternative hierarchy is not a dead idea.
- It is a better hierarchy definition than the earlier version for BTC and ADA.
- But it still does not replace the current best plain candidates.

Current preferred strategies remain:

- BTC: `10/200`
- ETH: `1/120 + 5/20` or plain `5/20`
- ADA: `1/20/120`
- DOGE: `1/20/200`

## Next Actions

- Keep the alternative hierarchy as a secondary template for BTC and ADA only.
- Do not expand it broadly across all assets.
- If revisited, compare it only against already strong plain strategies, not against weak baselines.
