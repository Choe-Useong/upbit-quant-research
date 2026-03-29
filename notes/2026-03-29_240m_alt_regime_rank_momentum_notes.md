# 2026-03-29 240m Alt Regime and Rank-Momentum Notes

This note records the 4h (`240m`) research pass focused on three questions:

- whether a simple BTC regime filter improves long-only alt selection,
- whether `rank momentum` should use long momentum windows or short momentum windows,
- and whether `turnover` / `impact` add real value beyond momentum itself.

## 1. Infrastructure Changes Used Today

Today the following research-layer changes were used:

- staged universe filters:
  - `sequential`
  - `and`
- explicit scope naming:
  - `global`
  - `filtered`
- feature presets:
  - `rank_momentum`
  - `gaussian_momentum`
  - `impact_illiquidity`
  - `turnover_rank_persist`
- `Plateau AIR Mean` now appears in V2 grid summaries
- `market:KRW-BTC:*` references no longer collapse the full cross-section to BTC-only

Main code touched:

- [universe_v2.py](../lib/universe_v2.py)
- [spec_io.py](../lib/spec_io.py)
- [specs.py](../lib/specs.py)
- [run_cross_section_grid_v2.py](../scripts/run_cross_section_grid_v2.py)
- [run_cross_section_backtest_v2.py](../scripts/run_cross_section_backtest_v2.py)
- [features_v2_presets.json](../configs/presets/features_v2_presets.json)

## 2. BTC 4h Baseline

### 2.1 BTC MA grid

Reference:

- [grid config](../configs/grid_btc_ma_cross_240m_fee5bp.json)
- [summary](../data/grid/btc_ma_cross_240m_fee5bp/summary_results.csv)

Best result:

- `2 / 30`
- Total Return: `41078.42%`
- CAGR: `63.82%`
- MDD: `41.36%`
- Sharpe: `1.93`
- AIR: `0.519`
- Plateau AIR Mean: `0.429`
- Recent 1Y: `+8.47%`
- Recent 2Y: `+92.63%`

Interpretation:

- BTC 4h still likes a simple MA regime.
- `2/30` is a strong anchor regime candidate.

### 2.2 BTC pure momentum grid

Reference:

- [grid config](../configs/grid_btc_momentum_240m_fee5bp.json)
- [summary](../data/grid/btc_momentum_240m_fee5bp/summary_results.csv)

Best result:

- `momentum_window = 30`
- Total Return: `15015.90%`
- CAGR: `50.91%`
- MDD: `53.69%`
- Sharpe: `1.61`

Interpretation:

- pure BTC momentum is weaker than BTC MA on 4h
- for regime use, `BTC 2/30` remains more convincing than `BTC momentum > 0`

## 3. Alt Selection With BTC 2/30 Regime

### 3.1 Turnover + Impact + Rank Momentum

Reference:

- [grid config](../configs/grid_upbit_turnrank180d_cutq5_turnq2_impactq2_rankmomq2_btcregime230_240m_fee5bp.json)
- [summary](../data/grid/upbit_turnrank180d_cutq5_turnq2_impactq2_rankmomq2_btcregime230_240m_fee5bp/summary_results.csv)

Best result:

- `turnq2 / impactq2 / rankmomq2 / momw168 / avgw18`
- Total Return: `45518.76%`
- CAGR: `93.09%`
- MDD: `55.68%`
- Sharpe: `1.959`
- AIR: `0.796`
- Plateau AIR Mean: `0.355`
- Recent 1Y: `-9.09%`
- Recent 2Y: `-14.15%`

Interpretation:

- BTC regime filter clearly helped relative to the same family without regime gating.
- The winning profile was still not large-cap / low-impact / strongest-momentum.
- It stayed closer to:
  - lower persistent turnover,
  - higher impact,
  - weaker rank momentum.

This still looks like a regime-gated small-cap reversal family.

### 3.2 Rank momentum only, long-window version

Reference:

- [grid config](../configs/grid_upbit_turnrank180d_cutq5_rankmomq4_btcregime230_240m_fee5bp.json)
- [summary](../data/grid/upbit_turnrank180d_cutq5_rankmomq4_btcregime230_240m_fee5bp/summary_results.csv)

Best result:

- `rankmomq4 / momw168 / avgw18`
- Total Return: `11766.20%`
- CAGR: `61.95%`
- MDD: `51.83%`
- Sharpe: `1.658`

Interpretation:

- when `momentum_window` was long (`18/42/84/168`), weaker rank momentum buckets dominated
- this originally suggested a weak-momentum / pullback effect

But this turned out to be partly a parameter-definition issue.

## 4. Important Correction: The Intended Rank-Momentum Was Short-Momentum Rank Averaged Over Mid-Term

The intended definition was:

- short momentum,
- cross-sectional rank at each timestamp,
- then a mid-term rolling average of that rank.

That is different from:

- medium/long momentum,
- then ranking,
- then averaging.

### 4.1 Short rank momentum only

Reference:

- [grid config](../configs/grid_upbit_turnrank180d_cutq5_rankmomq4_shortmom_btcregime230_240m_fee5bp.json)
- [summary](../data/grid/upbit_turnrank180d_cutq5_rankmomq4_shortmom_btcregime230_240m_fee5bp/summary_results.csv)

Parameters:

- `momentum_window = 1, 2, 3, 4, 7`
- `rank_avg_window = 18, 42, 84`

Best result:

- `rankmomq1 / momw2 / avgw84`
- Total Return: `14641.09%`
- CAGR: `65.49%`
- MDD: `58.27%`
- Sharpe: `1.643`
- AIR: `0.529`

Bucket pattern:

- `q1` mean return was best
- then `q2`
- then `q3`
- then `q4`

Interpretation:

- once rank momentum was defined the intended way, strong short-term relative strength became the better side again
- so the earlier weak-momentum dominance was not a stable truth; it was tied to using long momentum windows

## 5. Short Rank Momentum + Turnover 3x3

Reference:

- [grid config](../configs/grid_upbit_turnrank180d_cutq5_turnq3_rankmomq3_shortmom_btcregime230_240m_fee5bp.json)
- [summary](../data/grid/upbit_turnrank180d_cutq5_turnq3_rankmomq3_shortmom_btcregime230_240m_fee5bp/summary_results.csv)

Structure:

- cut bottom `20%` of `turn_rank_1bar_180d_avg`
- within the survivors:
  - turnover `3-quantile`
  - short rank momentum `3-quantile`
- apply both as an `and` stage
- keep `BTC 2/30` regime on top

Best result:

- `turnq3 / rankmomq2 / momw7 / avgw42`
- Total Return: `100087.90%`
- CAGR: `110.12%`
- MDD: `60.87%`
- Sharpe: `1.959`
- AIR: `0.902`
- Plateau AIR Mean: `0.289`
- Recent 1Y: `-19.17%`
- Recent 2Y: `-26.07%`
- average selected count: `4.74`

Pair means:

- `turnq3 / rankmomq2`: `16541.69%`
- `turnq3 / rankmomq3`: `7441.06%`
- `turnq1 / rankmomq1`: `7117.62%`

Interpretation:

- after bottom-tail removal, the strongest area was:
  - lower turnover bucket (`turnq3`)
  - but not strongest momentum (`q1`)
  - rather the next tier (`rankmomq2`)
- this looks more like:
  - smaller, still-active coins,
  - with strong but not fully overheated short-term relative strength

This is not clean large-cap trend following.
It still leans toward smaller-coin rotation.

## 6. What Did Not Work

### 6.1 Self-MA trend filter on alts

Reference:

- [grid config](../configs/grid_upbit_turnrank180d_cutq5_turnq3_selfma_240m_fee5bp.json)
- [summary](../data/grid/upbit_turnrank180d_cutq5_turnq3_selfma_240m_fee5bp/summary_results.csv)

Result:

- `90` runs
- `0` positive runs

Best:

- `turnq1 / selfma 4 / 40`
- Total Return: `-58.68%`

Interpretation:

- simple self-trend filtering by per-coin MA cross did not work here
- this universe behaves much more like rotation/reversal under a higher-level regime than simple per-coin trend following

## 7. Current Working Interpretation

At this point the most defensible interpretation is:

- BTC regime matters a lot
- `BTC 2/30` is currently a useful simple regime gate
- raw large-cap style turnover preference is not the winning direction
- short-term relative strength does matter, but the very strongest bucket is not always best
- a lot of the edge still looks like smaller-coin rotation inside favorable crypto regime

This is not yet a clean “buy large liquid strong coins” result.

It is closer to:

- cut the dead tail,
- wait for BTC risk-on,
- then trade smaller active names that are strong but not the most crowded.

## 8. Main Cautions

- survivorship bias remains a real concern, especially on smaller-coin buckets
- many top full-sample results still have weak recent 1Y / 2Y behavior
- high full-sample CAGR should not be over-trusted without recent-period confirmation

## 9. Next Likely Tests

Good next tests from here:

- drawdown-from-high / proximity-to-high as a direct “pullback in uptrend” measure
- stricter recent-period evaluation rather than full-sample top selection
- compare `turnq3 / rankmomq2` against a more conservative `turnq1 / rankmomq1` sleeve under the same BTC regime

Current practical anchor:

- keep [btc_ma_cross_240m_fee5bp](../data/grid/btc_ma_cross_240m_fee5bp/summary_results.csv) as the clean single-asset benchmark
- treat the alt rotation family as exploratory but promising, not production-ready
