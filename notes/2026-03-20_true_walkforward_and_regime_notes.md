# 2026-03-20 True Walk-Forward And Regime Notes

This note records the follow-up after the earlier candidate-restricted validation work.
The focus was:

- building a true full-grid walk-forward path for simple MA families,
- separating `selection-style` interpretation from `full-sample best` interpretation,
- checking whether expanding behavior matches the actual decision style better than rolling,
- and testing whether a very simple BTC volatility regime filter adds value.

## 1. Method Clarification

The earlier walk-forward work was not fully ex-ante strategy discovery.
It was candidate-restricted validation.

The new `fast_exact` path is closer to a true walk-forward for simple single-asset MA families:

- full candidate grid per fold,
- exact cash/units simulation for `0/1` and fractional target weights,
- explicit rolling and expanding window modes,
- and direct validation against vectorbt for mixed-signal cases.

Reference files:

- [run_fast_ma_cross_walkforward.py](../scripts/walkforward/run_fast_ma_cross_walkforward.py)
- [validate_fast_exact_mix_against_vectorbt.py](../scripts/walkforward/validate_fast_exact_mix_against_vectorbt.py)

## 2. BTC

### 2.1 BTC 2-line

Reference:

- [rolling summary](../data/validation/btc_60m_true_walkforward_2line_18m_6m_3m_fast_exact/walkforward_summary.csv)
- [expanding summary](../data/validation/btc_60m_true_walkforward_2line_18m_6m_3m_expanding_fast_exact/walkforward_summary.csv)
- [expanding fold candidates](../data/validation/btc_60m_true_walkforward_2line_18m_6m_3m_expanding_fast_exact/fold_candidates.csv)

Key result:

- rolling median OOS AIR: `0.0866`
- expanding median OOS AIR: `0.0918`

Interpretation:

- expanding was slightly better than rolling,
- `10/120` dominated the expanding selection path,
- and its mean IS rank was clearly best.

Average IS ranks in expanding:

- `10/120`: `1.35`
- `10/200`: `4.23`
- `20/200`: `10.27`

Meaning:

- for BTC, the "long-run winner stays the winner" interpretation is reasonable,
- and this matches the actual decision style better than a short rolling selector.

### 2.2 BTC 3-line

Reference:

- [expanding summary](../data/validation/btc_60m_true_walkforward_3line_18m_6m_3m_expanding_fast_exact/walkforward_summary.csv)

Result:

- expanding median OOS AIR: `-0.1062`

Interpretation:

- BTC `3-line` is weaker than BTC `2-line`.
- BTC remains a `2-line` asset.

### 2.3 BTC top-2 mix

Reference:

- [rolling top2 mix summary](../data/validation/btc_60m_true_walkforward_2line_18m_6m_3m_top2mix_fast_exact/walkforward_summary.csv)
- [expanding top2 mix summary](../data/validation/btc_60m_true_walkforward_2line_18m_6m_3m_expanding_top2mix_fast_exact/walkforward_summary.csv)

Result:

- rolling top2 mix median OOS AIR: `-0.1894`
- expanding top2 mix median OOS AIR: `-0.0326`

Interpretation:

- BTC `top2` equal mixing hurt performance.
- A clear BTC winner should not be diluted just because mixing feels safer.

## 3. ETH

Reference:

- [rolling summary](../data/validation/eth_60m_true_walkforward_2line_18m_6m_3m_fast_exact/walkforward_summary.csv)
- [expanding summary](../data/validation/eth_60m_true_walkforward_2line_18m_6m_3m_expanding_fast_exact/walkforward_summary.csv)

Result:

- rolling median OOS AIR: `-0.2163`
- expanding median OOS AIR: `0.1613`

Interpretation:

- ETH behaved even more cleanly than BTC.
- `5/60` kept winning on the expanding path.
- ETH remains the strongest case for a simple expanding-style champion.

## 4. XRP

### 4.1 XRP 2-line

Reference:

- [rolling summary](../data/validation/xrp_60m_true_walkforward_2line_18m_6m_3m_fast_exact/walkforward_summary.csv)
- [expanding summary](../data/validation/xrp_60m_true_walkforward_2line_18m_6m_3m_expanding_fast_exact/walkforward_summary.csv)

Result:

- both rolling and expanding were weak
- this was not a good `2-line` asset

### 4.2 XRP 3-line

Reference:

- [rolling summary](../data/validation/xrp_60m_true_walkforward_3line_18m_6m_3m_fast_exact/walkforward_summary.csv)
- [expanding summary](../data/validation/xrp_60m_true_walkforward_3line_18m_6m_3m_expanding_fast_exact/walkforward_summary.csv)
- [expanding fold candidates](../data/validation/xrp_60m_true_walkforward_3line_18m_6m_3m_expanding_fast_exact/fold_candidates.csv)

Result:

- rolling median OOS AIR: `0.2225`
- expanding median OOS AIR: `0.2280`
- expanding winner: `10/20/200` in `23/26` folds

Average IS rank in expanding:

- `10/20/200`: `1.12`
- `10/20/120`: `3.73`

Interpretation:

- XRP is a `3-line` asset.
- Under an expanding-style interpretation, `10/20/200` is the cleaner champion than the older `10/20/120`.

## 5. ADA

### 5.1 ADA 2-line

Reference:

- [expanding summary](../data/validation/ada_60m_true_walkforward_2line_18m_6m_3m_expanding_fast_exact/walkforward_summary.csv)

Result:

- expanding median OOS AIR: `-0.1288`
- dynamic selection was weak

### 5.2 ADA 3-line

Reference:

- [rolling summary](../data/validation/ada_60m_true_walkforward_3line_18m_6m_3m_fast_exact/walkforward_summary.csv)
- [expanding summary](../data/validation/ada_60m_true_walkforward_3line_18m_6m_3m_expanding_fast_exact/walkforward_summary.csv)
- [expanding fold candidates](../data/validation/ada_60m_true_walkforward_3line_18m_6m_3m_expanding_fast_exact/fold_candidates.csv)
- [expanding fold winners](../data/validation/ada_60m_true_walkforward_3line_18m_6m_3m_expanding_fast_exact/fold_winners.csv)

Result:

- rolling median OOS AIR: `0.0015`
- expanding median OOS AIR: `-0.0835`

Average IS rank in expanding:

- `10/20/120`: `1.96`
- `5/120/400`: `8.88`
- `3/120/400`: `13.85`

Important nuance:

- `10/20/120` itself was not the main problem.
- The expanding selector lost performance partly because it temporarily switched into weak alternatives such as `5/20/120` and `5/60/120`.

Interpretation:

- ADA still points to `10/20/120`,
- but only as a low-confidence provisional champion,
- not as a clean expanding winner like BTC or ETH.

## 6. Selection Logic Interpretation

The main conceptual point from this round:

- `full-sample best`,
- `fixed candidate OOS robustness`,
- and `expanding selection`

are not the same thing.

But the actual human decision style is often closest to:

- "today, look at all accumulated history and keep one champion."

That means:

- expanding is a better behavioral proxy than rolling,
- but it is still not free from hindsight,
- and it should be treated as a practical decision proxy rather than a clean ex-ante proof.

Current practical interpretation:

- BTC: expanding-style choice is credible
- ETH: expanding-style choice is very credible
- XRP: only after moving to `3-line`
- ADA: expanding-style interpretation is not clean enough to justify confidence

## 7. BTC Regime Exploration

The first attempt was deliberately low-degree-of-freedom.

### 7.1 MA-momentum long/cash

Reference:

- [config](../configs/grid_btc_sma_slope_longcash_60m_fee5bp.json)
- [results](../data/grid/btc_sma_slope_longcash_60m_fee5bp/summary_results.csv)

Important naming note:

- this is not a true regression slope,
- it is effectively long-MA momentum:
  - `SMA_t / SMA_t-k - 1`

Top result:

- `ma800, slope6`
- AIR `0.2689`
- CAGR `54.75%`
- MDD `68.45%`

Interpretation:

- this is useful as a regime diagnostic,
- but not strong enough as a standalone BTC main strategy.

### 7.2 Volatility-only long/cash

Reference:

- [low-vol results](../data/grid/btc_vol_cross_longcash_60m_fee5bp/summary_results.csv)
- [high-vol results](../data/grid/btc_vol_cross_highvol_longcash_60m_fee5bp/summary_results.csv)

Result:

- low-vol long was weak
- high-vol long was less bad, but still inferior to BTC buy-and-hold on AIR

Interpretation:

- volatility alone does not seem strong enough as the full BTC rule.

### 7.3 BTC `10/120` plus volatility filter

Reference:

- [low-vol filter on 10/120](../data/grid/btc_ma_10_120_with_lowvol_filter_60m_fee5bp/summary_results.csv)
- [high-vol filter on 10/120](../data/grid/btc_ma_10_120_with_highvol_filter_60m_fee5bp/summary_results.csv)

Baseline:

- [BTC 10/120 baseline](../data/grid/btc_ma_cross_60m_fee5bp/summary_results.csv)
- CAGR `68.16%`
- MDD `36.71%`
- AIR `0.4200`

Best filtered variants:

- low-vol filter best:
  - CAGR `33.47%`
  - MDD `50.16%`
  - AIR `-0.1889`
- high-vol filter best:
  - CAGR `33.26%`
  - MDD `37.41%`
  - AIR `-0.1935`

Interpretation:

- volatility overlay did not improve `10/120`.
- It cut return heavily and did not improve risk enough to justify inclusion.

## 8. Current Take

- BTC remains `10/120`
- ETH remains `5/60`
- XRP should now be treated as a `3-line` asset, with `10/20/200` the strongest expanding-style candidate
- ADA remains `10/20/120`, but only with low confidence
- simple BTC volatility filters are worth remembering for diagnosis, but they have not yet earned promotion into the core strategy layer
