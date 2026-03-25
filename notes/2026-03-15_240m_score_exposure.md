# 2026-03-15 240m Score Exposure Review

## Scope

- Data frequency: `240m`
- Rebalance: `every_bar`
- Lag assumption: `lag=1`
- Cost assumption: `fees = 0.05%`
- Strategy family:
  - build multiple boolean MA conditions
  - convert them into a score
  - map the score into target exposure

## Weighting Logic

- For each asset, build a score in `[0, 1]`
- Example with two conditions:
  - both false -> `0.0`
  - one true -> `0.5`
  - both true -> `1.0`
- `feature_value` weighting then interprets that score as target exposure
- In multi-asset cases:
  - weights are score-proportional
  - total portfolio exposure remains below or equal to `100%`
  - remaining capital stays in cash

## ETH Single-Asset Score Exposure

### 2-Condition Version

- Conditions:
  - `1 > 120`
  - `5 > 20`
- Score:
  - `0 / 0.5 / 1.0`

Results:

- AIR `0.5485`
- Sharpe `1.9019`
- MDD `42.71%`
- Recent 1Y AIR `0.8123`
- Rolling IR 252d Median `0.3270`
- Rolling IR 252d Positive Ratio `0.6486`
- Return `+94,156.78%`

Review:

- This is one of the strongest ETH 240m variants tested so far.
- It improved on plain `5/20` and also looked more balanced than relying on one fast crossover alone.

### 3-Condition Version

- Conditions:
  - `1 > 120`
  - `5 > 20`
  - `10 > 200`
- Score:
  - `0 / 1/3 / 2/3 / 1`

Results:

- AIR `0.5515`
- Sharpe `1.8819`
- MDD `43.08%`
- Recent 1Y AIR `0.6151`
- Rolling IR 252d Median `0.3020`

Review:

- Long-run AIR rose only slightly versus the 2-condition version.
- Recent 1Y AIR, Sharpe, and rolling median all weakened.
- Conclusion: adding `10 > 200` diluted the cleaner 2-condition structure.

### 5-Condition Version

- Conditions:
  - `5 > 20`
  - `10 > 200`
  - `5 > 120`
  - `20 > 200`
  - `5 > 200`
- Score:
  - each condition weighted `0.2`

Results:

- AIR `0.4781`
- Sharpe `1.7341`
- MDD `46.07%`
- Recent 1Y AIR `0.4675`
- Rolling IR 252d Median `0.2307`

Review:

- Clear dilution.
- Too many overlapping ETH trend conditions made the exposure smoother but less effective.

### ETH Interpretation

- Best ETH score-exposure candidate remains:
  - `1 > 120` + `5 > 20`
- The 2-condition version is better than the 3-condition and 5-condition expansions.
- ETH appears to benefit from combining:
  - one very fast condition
  - one strong medium-trend condition

## BTC Single-Asset Score Exposure

### Base 2-Condition Version

- Conditions:
  - `1 > 200`
  - `10 > 200`
- Score:
  - `0 / 0.5 / 1.0`

Results:

- AIR `0.3534`
- Sharpe `1.7220`
- MDD `55.27%`
- Recent 1Y AIR `0.4251`
- Rolling IR 252d Median `0.2861`
- Return `+26,147.88%`

Review:

- Better than `1/200` alone.
- Better than some weaker BTC short/long variants.
- Still did not beat the BTC baseline `10/200 every_bar`.

### BTC Candidate Grid

Tested combinations:

- `1/120 + 5/200`
- `1/120 + 10/200`
- `1/200 + 5/200`
- `1/200 + 10/200`

Top two:

- `1/200 + 10/200`
  - AIR `0.3534`
  - Sharpe `1.7220`
  - MDD `55.27%`
  - Recent 1Y AIR `0.4251`

- `1/120 + 10/200`
  - AIR `0.3428`
  - Sharpe `1.7660`
  - MDD `44.44%`
  - Recent 1Y AIR `0.6354`

Review:

- BTC still prefers plain `10/200` as the main long-run candidate.
- `1/120 + 10/200` is more interesting as a recent-year and lower-drawdown variant.
- BTC score exposure is useful, but not yet a true upgrade over the core BTC 2-line SMA.

## BTC + ETH Custom Multi-Asset Score Exposure

### Custom Per-Asset Conditions

- BTC:
  - `1 > 120`
  - `10 > 200`
- ETH:
  - `1 > 120`
  - `5 > 20`

Score per asset:

- both false -> `0.0`
- one true -> `0.5`
- both true -> `1.0`

### Results

- AIR `0.4438`
- Sharpe `1.7887`
- MDD `45.91%`
- Recent 1Y AIR `1.2354`
- Rolling IR 252d Mean `0.3375`
- Rolling IR 252d Median `0.5040`
- Rolling IR 252d Positive Ratio `0.5836`
- Return `+37,570.33%`

Files:

- [features_btc_eth_custom_score_240m.csv](../data/upbit/features/features_btc_eth_custom_score_240m.csv)
- [universe_btc_eth_custom_score_240m.csv](../data/upbit/universe/universe_btc_eth_custom_score_240m.csv)
- [weights_btc_eth_custom_score_240m.csv](../data/upbit/weights/weights_btc_eth_custom_score_240m.csv)
- [summary.csv](../data/backtest/btc_eth_custom_score_240m_fee5bp/summary.csv)

Review:

- This is better than the earlier common-condition BTC+ETH score portfolio.
- The custom per-asset conditions matter.
- Recent 1Y AIR is especially strong, which suggests this structure adapts better to the current BTC/ETH regime than a single shared rule.

## Final Interpretation

- Score exposure is a real extension worth keeping.
- It worked best when:
  - the number of conditions stayed small
  - the conditions were not overly redundant
  - conditions were customized by asset
- ETH benefited the most.
- BTC improved somewhat, but the baseline `10/200` still remains the core BTC candidate.
- The strongest score-exposure result in this round is not the 5-condition expansion, but the simpler:
  - `ETH 1 > 120 + 5 > 20`
- The most useful multi-asset outcome is:
  - custom BTC+ETH score exposure with different conditions per asset

## Next Actions

- Keep `lag=1` as the main assumption.
- Add this score-exposure family to future candidate review tables.
- Consider the next expansion only if it preserves simplicity:
  - `BTC + ETH + SOL` custom score portfolio
  - or `ETH` score-exposure robustness review against recent-only windows
