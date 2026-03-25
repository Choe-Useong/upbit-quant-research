# 2026-03-21 Persistence Follow-Ups

This note records the follow-up after the true walk-forward and provisional-champion update round.
The focus here was narrow:

- test whether simple `n-bar persistence` can improve already-selected champions,
- separate `symmetric persistence` from `exit-only persistence`,
- and decide which changes are strong enough to enter the provisional-best list.

## 1. Definitions

Two different persistence rules were tested.

### Symmetric persistence

- entry requires `n` consecutive on-bars,
- exit also requires `n` consecutive off-bars.

This is the usual `persist n` form.

### Exit-only persistence

- entry remains immediate,
- exit requires `n` consecutive off-bars.

This is a stateful asymmetric rule.
It is materially different from the usual symmetric persistence check.

## 2. BTC

### 2.1 Raw `10/120` vs exit-only persistence

Reference:

- [full-sample summary](../data/grid/btc_ma_10_120_exit_persistence_stateful_60m_fee5bp_summary.csv)
- [rolling 18/6/3](../data/validation/btc_60m_10_120_exit_persistence_18m_6m_3m_rolling_exact/walkforward_summary.csv)
- [expanding 18/6/3](../data/validation/btc_60m_10_120_exit_persistence_18m_6m_3m_expanding_exact/walkforward_summary.csv)

Full-sample AIR:

- raw `10/120`: `0.4200`
- `exit_persist_2`: `0.4312`
- `exit_persist_3`: `0.4420`
- `exit_persist_6`: `0.4297`
- `exit_persist_8`: `0.4276`

Key interpretation:

- BTC improved when persistence was applied only to exit.
- The effect was not a single-point fluke.
- `2`, `3`, `6`, and `8` all stayed above the raw baseline on full-sample AIR.
- The local plateau is real, but `3` remained the cleanest compromise.

Candidate OOS median AIR in `18/6/3`:

- raw `10/120`: `0.1927`
- `exit_persist_3`: `0.3093`
- `exit_persist_6`: `0.2452`

Decision:

- provisional BTC main was promoted to `10/120 exit-persist3`
- raw `10/120` remains the fallback

### 2.2 Entry-only persistence

Reference:

- [full-sample summary](../data/grid/btc_ma_10_120_entry_persistence_stateful_60m_fee5bp_summary.csv)

Result:

- entry-only persistence did not beat the raw BTC baseline.
- `entry_persist_3` came closest but still remained below raw `10/120`.

Meaning:

- BTC did not want slower entry.
- BTC only benefited from slower exit.

## 3. ETH

### 3.1 Symmetric persistence on `5/60`

Reference:

- [rolling 18/6/3](../data/validation/eth_60m_persist_check_18m_6m_3m_rolling/walkforward_summary.csv)
- [rolling candidates](../data/validation/eth_60m_persist_check_18m_6m_3m_rolling/candidate_summary.csv)
- [expanding 18/6/3](../data/validation/eth_60m_persist_check_18m_6m_3m_expanding/walkforward_summary.csv)

Fixed-candidate OOS median AIR:

- raw `5/60`: `0.1896`
- `5/60 persist3`: `0.3036`

Selection behavior:

- rolling still chose raw `5/60` more often
- expanding slightly preferred `persist3`

Meaning:

- ETH did benefit from symmetric persistence.
- The raw baseline stayed defensible as the simpler rolling-style choice.
- But under the current research style, `persist3` became strong enough to promote.

Decision:

- provisional ETH main was promoted to `5/60 persist3`
- raw `5/60` became the sub pick

### 3.2 Exit-only persistence on `5/60`

Reference:

- [full-sample summary](../data/grid/eth_ma_5_60_exit_persistence_stateful_60m_fee5bp_summary.csv)

Result:

- exit-only persistence did not improve ETH.
- raw `5/60` remained better or cleaner than all exit-only variants.

Meaning:

- ETH was the opposite of BTC.
- ETH liked symmetric confirmation.
- ETH did not like delayed exit by itself.

## 4. XRP

### 4.1 Symmetric persistence on `10/20/200`

Reference:

- [rolling 18/6/3](../data/validation/xrp_60m_persist_check_18m_6m_3m_rolling_exact/walkforward_summary.csv)
- [expanding 18/6/3](../data/validation/xrp_60m_persist_check_18m_6m_3m_expanding_exact/walkforward_summary.csv)

Result:

- symmetric persistence was not helpful for XRP.
- rolling still preferred the raw `10/20/200`.
- expanding over-selected `persist6`, but fixed-candidate OOS did not justify promotion.

Meaning:

- plain `persist n` was rejected for XRP.

### 4.2 Exit-only persistence on `10/20/200`

Reference:

- [full-sample summary](../data/grid/xrp_ma_10_20_200_exit_persistence_stateful_60m_fee5bp_summary.csv)
- [rolling 18/6/3](../data/validation/xrp_60m_exit_persist_check_18m_6m_3m_rolling_exact/walkforward_summary.csv)
- [rolling candidates](../data/validation/xrp_60m_exit_persist_check_18m_6m_3m_rolling_exact/candidate_summary.csv)
- [expanding 18/6/3](../data/validation/xrp_60m_exit_persist_check_18m_6m_3m_expanding_exact/walkforward_summary.csv)

Full-sample AIR:

- raw `10/20/200`: `0.3233`
- `exit_persist_2`: `0.3683`
- `exit_persist_6`: `0.3882`

Fixed-candidate OOS median AIR:

- raw baseline: `0.2280`
- `exit_persist_6`: `0.2315`
- `exit_persist_8`: `0.2737`
- `exit_persist_2`: `0.2248`

Selection behavior:

- rolling top winner: `exit_persist_6`
- expanding top winner: `exit_persist_2`

Meaning:

- XRP reacted very differently to exit-only persistence than to symmetric persistence.
- There is now a live XRP extension candidate, especially around `exit_persist_6`.
- But this result was kept as a candidate note rather than an immediate provisional-best promotion.

Current status:

- provisional XRP main remains raw `10/20/200`
- `exit-only persistence` is the next XRP refinement candidate if another promotion round is opened

## 5. Practical Summary

The main lesson from this round:

- `persistence` is not one thing,
- and the direction matters.

Observed pattern:

- BTC: `exit-only` worked, `entry-only` did not
- ETH: `symmetric` worked, `exit-only` did not
- XRP: `symmetric` did not work, `exit-only` did

That makes the process feel more asset-specific, but it is still defensible for now because:

- the same added rule was not forced across all assets,
- several variants were explicitly rejected,
- and promotions were made only when the follow-up walk-forward stayed supportive.

Current provisional-best implications:

- BTC: upgraded to `10/120 exit-persist3`
- ETH: upgraded to `5/60 persist3`
- XRP: raw `10/20/200` kept, but `exit-only persistence` is now the leading XRP extension candidate
