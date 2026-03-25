# 2026-03-16 240m Hierarchical Stack Review

## Objective

Review a hierarchical score-exposure variant of MA strategies on 240m candles.

The intended structure was:

- `weak`: `mid > long`
- `strong`: `short > mid`
- score:
  - `0.0` if `weak == 0`
  - `0.5` if `weak == 1` and `strong == 0`
  - `1.0` if `weak == 1` and `strong == 1`

In feature terms, this was implemented as:

- `weak_signal = sma_mid > sma_long`
- `full_signal = weak_signal AND (sma_short > sma_mid)`
- `exposure_score = 0.5 * weak_signal + 0.5 * full_signal`

This is different from the earlier equal-mix score exposure.
The weak regime must be on first, and the strong condition only upgrades exposure inside that regime.

## Tested Candidates

### BTC

Candidates:

- `5/20/200`
- `10/20/200`
- `10/60/200`

Best hierarchical candidate:

- `10/20/200 every_bar`
  - AIR `0.2297`
  - Recent 1Y AIR `0.6433`
  - Sharpe `1.8641`
  - Rolling IR 252d Median `-0.0044`
  - Total Return `+19,910.43%`

Baseline to compare:

- `10/200 every_bar`
  - AIR `0.3899`
  - Sharpe `1.7588`
  - Rolling IR 252d Median `0.3144`

Review:

- BTC does not benefit from this structure.
- Recent 1Y AIR is decent, but full-sample AIR drops too much.
- The negative rolling IR median is the clearest warning.
- BTC still prefers the plain `10/200` baseline.

## ETH

Candidates:

- `5/20/120`
- `5/20/200`
- `10/20/200`

Best hierarchical candidate:

- `5/20/200 every_bar`
  - AIR `0.3996`
  - Recent 1Y AIR `0.6374`
  - Sharpe `1.8268`
  - Rolling IR 252d Median `0.1916`
  - Total Return `+52,274.10%`

Baselines to compare:

- Equal-mix score exposure `1/120 + 5/20`
  - AIR `0.5485`
  - Recent 1Y AIR `0.8123`
  - Sharpe `1.9019`
  - Rolling IR 252d Median `0.3270`
- Plain `5/20 every_bar`
  - AIR `0.5213`

Review:

- ETH hierarchical candidates are valid, but none beat the existing ETH leaders.
- The structure is coherent, but it is too restrictive relative to the stronger equal-mix score exposure.
- ETH still prefers either:
  - plain `5/20`
  - or equal-mix `1/120 + 5/20`

## ADA

Candidates:

- `1/20/120`
- `5/20/120`
- `10/20/120`

Best hierarchical candidate:

- `1/20/120 every_bar`
  - AIR `0.4207`
  - Recent 1Y AIR `0.8815`
  - Sharpe `1.8253`
  - Rolling IR 252d Median `0.4923`
  - Total Return `+768,396.39%`

Baseline to compare:

- Plain 3-line `1/20/120 every_bar`
  - AIR `0.4356`
  - Recent 1Y AIR `0.9934`
  - Sharpe `2.0132`
  - Rolling IR 252d Median `0.4344`
  - Total Return `+1,600,013.01%`

Review:

- ADA is the most interesting asset in this test.
- Hierarchical scoring is not bad here.
- It preserved strong rolling IR behavior and still produced a good AIR.
- But the plain `1/20/120` 3-line stack remains stronger overall.

## DOGE

Candidates:

- `1/20/200`
- `5/20/200`
- `10/20/200`

Best hierarchical candidate:

- `1/20/200 every_bar`
  - AIR `0.4101`
  - Recent 1Y AIR `0.3802`
  - Sharpe `1.4827`
  - Rolling IR 252d Median `0.4165`
  - Total Return `+5,957.14%`

Baseline to compare:

- Plain 3-line `1/20/200 every_bar`
  - AIR `0.4945`
  - Recent 1Y AIR `0.7514`
  - Sharpe `1.7782`
  - Rolling IR 252d Median `0.5008`
  - Total Return `+13,783.86%`

Review:

- DOGE hierarchical scoring works in principle, but weakens the best 3-line structure.
- The plain stack is clearly better on AIR, recent 1Y AIR, Sharpe, and total return.
- DOGE should keep the plain `1/20/200` stack as the main candidate.

## Summary

- The hierarchical stack idea is structurally valid.
- It makes more sense than the earlier `1 > 120 -> 5 > 20` variant because the roles are clearer:
  - `mid > long` defines regime
  - `short > mid` upgrades exposure inside that regime
- But in the tested candidate set, it did not replace any current best strategy.

Asset-level conclusion:

- BTC: reject hierarchical stack
- ETH: secondary only, behind existing top candidates
- ADA: meaningful, but still behind plain `1/20/120`
- DOGE: meaningful, but still behind plain `1/20/200`

## Final Interpretation

This experiment supports a broader pattern:

- When a good 3-line stack already exists, partially relaxing it into hierarchical score exposure usually smooths the structure, but often gives up too much alpha.
- That tradeoff was not attractive enough in BTC, ETH, or DOGE.
- ADA came closest to justifying it, but still did not overtake the plain 3-line leader.

So the hierarchy idea is worth keeping as a secondary design pattern, but not as the current main path.

## Next Actions

- Keep hierarchical stacks as a secondary template only.
- Continue using:
  - BTC: `10/200`
  - ETH: `1/120 + 5/20` or plain `5/20`
  - ADA: `1/20/120`
  - DOGE: `1/20/200`
- If this idea is revisited later, test it only where a plain 3-line stack is already strong.
