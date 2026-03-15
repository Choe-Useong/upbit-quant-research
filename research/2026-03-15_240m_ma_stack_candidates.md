# 2026-03-15 240m 3-Line MA Stack Candidates

## Scope

- Objective: test whether a narrow 3-line moving-average stack can improve robustness over the existing 240m 2-line SMA crossover candidates.
- Universe: single-asset long-only on `KRW-BTC`, `KRW-ETH`, `KRW-SOL`, `KRW-XRP`
- Data: Upbit 240m candles
- Rebalance: `every_bar`
- Fees: `0.05%`
- Benchmark: same asset buy-and-hold

## Candidate Grids

### BTC

- `short`: `5`, `10`
- `mid`: `10`, `20`
- `long`: `120`, `200`

### ETH / SOL / XRP

- `short`: `5`, `10`
- `mid`: `20`, `60`
- `long`: `120`, `200`

### Common Rule

- Entry when `short > mid > long`
- Otherwise flat

## BTC Review

Top 3:

1. `10/20/200 every_bar`
   - AIR: `0.1988`
   - Recent 1Y AIR: `0.7439`
   - Sharpe: `2.0018`
   - Rolling IR 252d Median: `0.0161`
   - Plateau AIR Mean: `0.0945`

2. `5/20/200 every_bar`
   - AIR: `0.1582`
   - Recent 1Y AIR: `0.6544`
   - Sharpe: `1.9520`
   - Rolling IR 252d Median: `0.0441`
   - Plateau AIR Mean: `0.1026`

3. `10/20/120 every_bar`
   - AIR: `0.1373`
   - Recent 1Y AIR: `0.8092`
   - Sharpe: `1.8683`
   - Rolling IR 252d Median: `0.0466`
   - Plateau AIR Mean: `0.1068`

Comparison with 2-line baseline:

- Best 2-line BTC candidate remains `10/200 every_bar`
  - AIR: `0.3899`
  - Rolling IR 252d Median: `0.3144`
  - Plateau AIR Mean: `0.2475`

Interpretation:

- 3-line BTC variants reduced aggressiveness and improved the feel of risk-adjusted return, but they did not beat the core 2-line BTC candidate on benchmark-relative alpha.
- The main weakness is that rolling excess quality collapsed too much versus the baseline. The 3-line BTC candidates look like defensive alternatives, not replacements.

## ETH Review

Top 3:

1. `5/20/120 every_bar`
   - AIR: `0.3831`
   - Recent 1Y AIR: `0.9474`
   - Sharpe: `2.0265`
   - Rolling IR 252d Median: `0.1973`
   - Plateau AIR Mean: `0.2012`

2. `5/20/200 every_bar`
   - AIR: `0.3568`
   - Recent 1Y AIR: `0.9134`
   - Sharpe: `1.9854`
   - Rolling IR 252d Median: `0.1772`
   - Plateau AIR Mean: `0.2049`

3. `5/60/120 every_bar`
   - AIR: `0.2421`
   - Recent 1Y AIR: `0.4420`
   - Sharpe: `1.6408`
   - Rolling IR 252d Median: `0.1279`
   - Plateau AIR Mean: `0.2213`

Comparison with 2-line baselines:

- Best performance 2-line candidate: `5/20 every_bar`
  - AIR: `0.5213`
  - Rolling IR 252d Median: `0.1772`
  - Plateau AIR Mean: `0.1781`

- Best balanced 2-line candidate: `10/200 every_bar`
  - AIR: `0.4650`
  - Rolling IR 252d Median: `0.1523`
  - Plateau AIR Mean: `0.3906`

Interpretation:

- ETH is the only asset where 3-line candidates look meaningfully interesting.
- `5/20/120` and `5/20/200` gave up some AIR versus the best 2-line ETH setup, but kept strong recent performance and strong Sharpe.
- The trade-off looks reasonable enough to justify keeping ETH 3-line candidates on the board as defensive ETH variants.

## SOL Review

Top 3:

1. `10/20/200 every_bar`
   - AIR: `0.3349`
   - Recent 1Y AIR: `0.2389`
   - Sharpe: `1.3416`
   - Rolling IR 252d Median: `0.1013`

2. `10/60/200 every_bar`
   - AIR: `0.3312`
   - Recent 1Y AIR: `0.3780`
   - Sharpe: `1.2894`
   - Rolling IR 252d Median: `-0.0198`

3. `5/60/200 every_bar`
   - AIR: `0.3218`
   - Recent 1Y AIR: `0.4315`
   - Sharpe: `1.2898`
   - Rolling IR 252d Median: `-0.0118`

Comparison with 2-line baselines:

- Best 2-line SOL candidate: `5/60 every_bar`
  - AIR: `0.4354`
  - Rolling IR 252d Median: `-0.0110`

- Secondary 2-line SOL candidate: `10/60 every_bar`
  - AIR: `0.3662`
  - Rolling IR 252d Median: `-0.0404`

Interpretation:

- SOL 3-line candidates look cleaner than expected, but still failed to beat the 2-line SOL leader on AIR.
- `10/20/200` stands out as the most interpretable 3-line SOL candidate because its rolling median is actually positive, unlike most SOL alternatives.
- Even so, this still reads as a robustness-oriented variant rather than a clearly better strategy.

## XRP Review

Top 3:

1. `5/60/120 every_bar`
   - AIR: `0.1189`
   - Recent 1Y AIR: `0.3951`
   - Sharpe: `1.4485`
   - Rolling IR 252d Median: `-0.0137`
   - Plateau AIR Mean: `0.0425`

2. `10/60/120 every_bar`
   - AIR: `0.1149`
   - Recent 1Y AIR: `0.4606`
   - Sharpe: `1.4213`
   - Rolling IR 252d Median: `-0.0009`
   - Plateau AIR Mean: `0.0431`

3. `5/60/200 every_bar`
   - AIR: `0.1082`
   - Recent 1Y AIR: `0.5842`
   - Sharpe: `1.4566`
   - Rolling IR 252d Median: `0.0025`
   - Plateau AIR Mean: `0.0440`

Comparison with 2-line baselines:

- Best 2-line XRP candidate: `10/60 every_bar`
  - AIR: `0.3276`
  - Rolling IR 252d Median: `0.3910`
  - Plateau AIR Mean: `0.1689`

- Balanced 2-line XRP candidate: `5/20 every_bar`
  - AIR: `0.3119`
  - Rolling IR 252d Median: `0.1950`
  - Plateau AIR Mean: `0.1499`

Interpretation:

- XRP 3-line candidates are not competitive.
- They preserved some recent 1Y behavior, but long-run AIR and rolling robustness both fell too much versus the 2-line XRP setups.
- XRP remains a clear case where 2-line works better than 3-line.

## Cross-Asset Summary

- BTC: 3-line did not beat 2-line. Defensive variant only.
- ETH: 3-line is worth keeping as a secondary path, especially `5/20/120` and `5/20/200`.
- SOL: 3-line is acceptable but still not superior. `10/20/200` is the only clearly coherent candidate.
- XRP: 3-line should be dropped for now.

## Final Interpretation

The 3-line candidate study did not overturn the main 2-line conclusion. Across four assets, only ETH showed a meaningful case for retaining 3-line structures as viable alternatives. BTC and SOL can justify a 3-line note as defensive variants, but not as new core strategies. XRP should stay with 2-line only.

At this point:

- Keep core focus on 2-line SMA/VWMA research.
- Keep ETH `5/20/120` and `5/20/200` as 3-line follow-up candidates.
- Treat BTC `10/20/200` and SOL `10/20/200` as optional robustness variants, not primary signals.
