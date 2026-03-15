# 2026-03-15 240m MA Cross Asset Review

## Scope

- Assets: `KRW-BTC`, `KRW-ETH`, `KRW-SOL`, `KRW-XRP`
- Data frequency: `240m`
- Strategy family: `2-line SMA crossover`
- Cost assumption: `fees = 0.05%`
- Comparison axes:
  - `every_bar` vs `daily` vs `weekly` rebalance
  - best and worst parameter combinations per asset
  - benchmark-relative quality via `Annualized Information Ratio`

## Method

- For each asset, ran a grid over:
  - `short = [5, 10, 20, 30]`
  - `long = [10, 20, 60, 120, 200]`
  - constraint: `short < long`
- Signal:
  - build `sma_short`
  - build `sma_long`
  - hold only when `sma_short > sma_long`
- Rebalance frequencies tested:
  - `every_bar`
  - `daily`
  - `weekly`

## BTC

### Overall Best 3

- `10/200 every_bar`: AIR `0.3899`, Sharpe `1.7588`, MDD `49.30%`, Return `+30,929.14%`
- `5/200 daily`: AIR `0.3611`, Sharpe `1.7188`, MDD `52.23%`, Return `+26,951.38%`
- `10/200 daily`: AIR `0.3331`, Sharpe `1.6875`, MDD `46.49%`, Return `+23,852.34%`

### Overall Worst 3

- `20/60 weekly`: AIR `-0.3145`, Sharpe `0.9001`, MDD `73.42%`, Return `+1,213.65%`
- `10/20 weekly`: AIR `-0.3175`, Sharpe `0.8743`, MDD `87.38%`, Return `+1,190.04%`
- `30/60 weekly`: AIR `-0.4725`, Sharpe `0.7193`, MDD `74.91%`, Return `+564.85%`

### Every-Bar Best 3

- `10/200 every_bar`: AIR `0.3899`, Sharpe `1.7588`, MDD `49.30%`, Return `+30,929.14%`
- `5/200 every_bar`: AIR `0.3329`, Sharpe `1.6822`, MDD `53.41%`, Return `+23,642.47%`
- `5/120 every_bar`: AIR `0.2808`, Sharpe `1.6548`, MDD `50.25%`, Return `+19,694.24%`

### Every-Bar Worst 3

- `30/120 every_bar`: AIR `-0.0093`, Sharpe `1.2755`, MDD `63.83%`, Return `+5,064.99%`
- `30/60 every_bar`: AIR `-0.0269`, Sharpe `1.2583`, MDD `63.30%`, Return `+4,698.33%`
- `5/10 every_bar`: AIR `-0.1196`, Sharpe `1.1910`, MDD `64.34%`, Return `+3,228.82%`

### Best

- Run: `btc_ma_240m_10_200_every_bar_fee5bp`
- AIR: `0.3899`
- Sharpe: `1.7588`
- MDD: `49.30%`
- Total Return: `+30,929.14%`

### Worst

- Run: `btc_ma_240m_30_60_weekly_fee5bp`
- AIR: `-0.4725`
- Sharpe: `0.7193`
- MDD: `74.91%`
- Total Return: `+564.85%`

### Frequency Comparison

- Best `every_bar`: `10/200`, AIR `0.3899`
- Best `daily`: `5/200`, AIR `0.3611`
- Best `weekly`: `20/200`, AIR `0.1421`

### Top 3 Robustness Review

- `10/200 every_bar`
  - Plateau `0.2475`, Med252 `0.3144`, Q25 `-0.2132`, Pos252 `0.6545`
  - Review: Best BTC candidate. Strong AIR plus the cleanest time-series robustness among the BTC top 3.

- `5/200 daily`
  - Plateau `0.2130`, Med252 `0.2484`, Q25 `-0.1657`, Pos252 `0.6510`
  - Review: Slightly weaker than `10/200 every_bar`, but still strong. `daily` rebalancing preserves most of the quality, which is a practical positive.

- `10/200 daily`
  - Plateau `0.1527`, Med252 `0.1924`, Q25 `-0.2924`, Pos252 `0.6058`
  - Review: Still viable, but clearly behind the top two on both plateau and rolling IR quality.

### Interpretation

- BTC in 240m favors slower long windows, especially `200`.
- `every_bar` is best, but `daily` is still competitive.
- `weekly` degrades materially.

## ETH

### Overall Best 3

- `5/20 every_bar`: AIR `0.5213`, Sharpe `1.7875`, MDD `53.11%`, Return `+85,456.66%`
- `10/200 every_bar`: AIR `0.4650`, Sharpe `1.5953`, MDD `54.76%`, Return `+47,097.47%`
- `5/120 every_bar`: AIR `0.4194`, Sharpe `1.6032`, MDD `54.08%`, Return `+42,085.58%`

### Overall Worst 3

- `10/20 daily`: AIR `-0.1947`, Sharpe `0.8260`, MDD `61.16%`, Return `+1,126.54%`
- `5/10 weekly`: AIR `-0.2001`, Sharpe `0.8146`, MDD `77.49%`, Return `+1,078.72%`
- `10/20 weekly`: AIR `-0.2550`, Sharpe `0.7032`, MDD `86.48%`, Return `+662.29%`

### Every-Bar Best 3

- `5/20 every_bar`: AIR `0.5213`, Sharpe `1.7875`, MDD `53.11%`, Return `+85,456.66%`
- `10/200 every_bar`: AIR `0.4650`, Sharpe `1.5953`, MDD `54.76%`, Return `+47,097.47%`
- `5/120 every_bar`: AIR `0.4194`, Sharpe `1.6032`, MDD `54.08%`, Return `+42,085.58%`

### Every-Bar Worst 3

- `30/60 every_bar`: AIR `0.1287`, Sharpe `1.1905`, MDD `61.27%`, Return `+7,022.79%`
- `20/60 every_bar`: AIR `0.0338`, Sharpe `1.0980`, MDD `60.72%`, Return `+4,292.95%`
- `5/10 every_bar`: AIR `-0.0079`, Sharpe `1.0767`, MDD `67.77%`, Return `+3,625.71%`

### Best

- Run: `eth_ma_240m_5_20_every_bar_fee5bp`
- AIR: `0.5213`
- Sharpe: `1.7875`
- MDD: `53.11%`
- Total Return: `+85,456.66%`

### Worst

- Run: `eth_ma_240m_10_20_weekly_fee5bp`
- AIR: `-0.2550`
- Sharpe: `0.7032`
- MDD: `86.48%`
- Total Return: `+662.29%`

### Frequency Comparison

- Best `every_bar`: `5/20`, AIR `0.5213`
- Best `daily`: `10/120`, AIR `0.3954`
- Best `weekly`: `10/200`, AIR `0.2557`

### Top 3 Robustness Review

- `5/20 every_bar`
  - Plateau `0.1781`, Med252 `0.1772`, Q25 `-0.2690`, Pos252 `0.5891`
  - Review: Best performer in the whole study. However, plateau is not high, so this looks like a strong but narrower peak rather than a broad stable region.

- `10/200 every_bar`
  - Plateau `0.3906`, Med252 `0.1523`, Q25 `-0.2847`, Pos252 `0.5979`
  - Review: More balanced than `5/20`. AIR is lower, but plateau is much stronger, so this is the best ETH candidate from a robustness perspective.

- `5/120 every_bar`
  - Plateau `0.3551`, Med252 `0.2887`, Q25 `-0.5425`, Pos252 `0.5900`
  - Review: Strong median and decent plateau, but lower-tail behavior is rough. Attractive, but less balanced than `10/200`.

### Interpretation

- ETH is the strongest asset in this study.
- Short/fast crossovers work especially well on 240m ETH.
- `every_bar` dominates, but even `daily` remains strong.
- This is the clearest candidate for follow-up validation.

## SOL

### Overall Best 3

- `5/60 every_bar`: AIR `0.4354`, Sharpe `1.1167`, MDD `47.03%`, Return `+745.73%`
- `10/60 every_bar`: AIR `0.3662`, Sharpe `1.0227`, MDD `50.81%`, Return `+562.08%`
- `10/200 every_bar`: AIR `0.3661`, Sharpe `1.0642`, MDD `68.46%`, Return `+610.68%`

### Overall Worst 3

- `30/60 weekly`: AIR `-0.5629`, Sharpe `-0.2295`, MDD `91.23%`, Return `-76.20%`
- `5/20 weekly`: AIR `-0.5982`, Sharpe `-0.1980`, MDD `89.00%`, Return `-78.49%`
- `5/10 weekly`: AIR `-0.7300`, Sharpe `-0.4409`, MDD `91.47%`, Return `-86.60%`

### Every-Bar Best 3

- `5/60 every_bar`: AIR `0.4354`, Sharpe `1.1167`, MDD `47.03%`, Return `+745.73%`
- `10/60 every_bar`: AIR `0.3662`, Sharpe `1.0227`, MDD `50.81%`, Return `+562.08%`
- `10/200 every_bar`: AIR `0.3661`, Sharpe `1.0642`, MDD `68.46%`, Return `+610.68%`

### Every-Bar Worst 3

- `20/120 every_bar`: AIR `0.0410`, Sharpe `0.5786`, MDD `81.61%`, Return `+110.27%`
- `30/120 every_bar`: AIR `-0.0903`, Sharpe `0.3872`, MDD `88.31%`, Return `+27.25%`
- `5/10 every_bar`: AIR `-0.1549`, Sharpe `0.2944`, MDD `81.75%`, Return `-2.23%`

### Best

- Run: `sol_ma_240m_5_60_every_bar_fee5bp`
- AIR: `0.4354`
- Sharpe: `1.1167`
- MDD: `47.03%`
- Total Return: `+745.73%`

### Worst

- Run: `sol_ma_240m_5_10_weekly_fee5bp`
- AIR: `-0.7300`
- Sharpe: `-0.4409`
- MDD: `91.47%`
- Total Return: `-86.60%`

### Frequency Comparison

- Best `every_bar`: `5/60`, AIR `0.4354`
- Best `daily`: `5/60`, AIR `0.3190`
- Best `weekly`: `10/200`, AIR `0.0034`

### Top 3 Robustness Review

- `5/60 every_bar`
  - Plateau `0.2557`, Med252 `-0.0110`, Q25 `-0.3064`, Pos252 `0.4933`
  - Review: High headline AIR, but the slightly negative median suggests unstable persistence. Good on paper, but not convincing enough.

- `10/60 every_bar`
  - Plateau `0.2239`, Med252 `-0.0404`, Q25 `-0.3296`, Pos252 `0.4643`
  - Review: Similar profile to `5/60`, but weaker. Looks like another unstable SOL candidate.

- `10/200 every_bar`
  - Plateau `0.2135`, Med252 `0.2280`, Q25 `0.0260`, Pos252 `0.7715`
  - Review: This is the most robust SOL setup. AIR is a bit lower than `5/60`, but time-series quality is clearly better.

### Interpretation

- SOL shows decent benchmark-relative quality, but absolute returns are much smaller than ETH/BTC/XRP.
- `weekly` is almost unusable here.
- SOL seems to prefer medium-speed trend filters rather than very short crosses.

## XRP

### Overall Best 3

- `10/60 every_bar`: AIR `0.3276`, Sharpe `1.4509`, MDD `55.52%`, Return `+77,254.50%`
- `5/20 every_bar`: AIR `0.3119`, Sharpe `1.4362`, MDD `58.41%`, Return `+70,239.16%`
- `5/60 every_bar`: AIR `0.2757`, Sharpe `1.3991`, MDD `52.95%`, Return `+55,364.04%`

### Overall Worst 3

- `5/200 weekly`: AIR `-0.3019`, Sharpe `0.7106`, MDD `82.00%`, Return `+762.39%`
- `5/20 weekly`: AIR `-0.5093`, Sharpe `0.4595`, MDD `89.11%`, Return `+107.69%`
- `5/10 weekly`: AIR `-0.6670`, Sharpe `0.2308`, MDD `93.77%`, Return `-37.96%`

### Every-Bar Best 3

- `10/60 every_bar`: AIR `0.3276`, Sharpe `1.4509`, MDD `55.52%`, Return `+77,254.50%`
- `5/20 every_bar`: AIR `0.3119`, Sharpe `1.4362`, MDD `58.41%`, Return `+70,239.16%`
- `5/60 every_bar`: AIR `0.2757`, Sharpe `1.3991`, MDD `52.95%`, Return `+55,364.04%`

### Every-Bar Worst 3

- `30/200 every_bar`: AIR `-0.0119`, Sharpe `1.0464`, MDD `81.08%`, Return `+6,656.01%`
- `5/10 every_bar`: AIR `-0.0714`, Sharpe `0.9921`, MDD `80.71%`, Return `+4,616.59%`
- `30/60 every_bar`: AIR `-0.0757`, Sharpe `0.9874`, MDD `80.56%`, Return `+4,471.49%`

### Best

- Run: `xrp_ma_240m_10_60_every_bar_fee5bp`
- AIR: `0.3276`
- Sharpe: `1.4509`
- MDD: `55.52%`
- Total Return: `+77,254.50%`

### Worst

- Run: `xrp_ma_240m_5_10_weekly_fee5bp`
- AIR: `-0.6670`
- Sharpe: `0.2308`
- MDD: `93.77%`
- Total Return: `-37.96%`

### Frequency Comparison

- Best `every_bar`: `10/60`, AIR `0.3276`
- Best `daily`: `5/60`, AIR `0.2638`
- Best `weekly`: `30/120`, AIR `-0.0218`

### Top 3 Robustness Review

- `10/60 every_bar`
  - Plateau `0.1689`, Med252 `0.3910`, Q25 `-0.7347`, Pos252 `0.5866`
  - Review: Highest AIR for XRP and very strong median, but the lower tail is severe. Attractive, though not especially smooth.

- `5/20 every_bar`
  - Plateau `0.1499`, Med252 `0.1950`, Q25 `-0.3341`, Pos252 `0.5798`
  - Review: Slightly weaker on AIR than `10/60`, but lower-tail behavior is much better. This may be the more balanced XRP choice.

- `5/60 every_bar`
  - Plateau `0.2176`, Med252 `0.1954`, Q25 `-0.5868`, Pos252 `0.5568`
  - Review: Best plateau of the XRP top 3, but weaker tail behavior than `5/20`. A stable alternative, not the strongest conviction pick.

### Interpretation

- XRP can generate very large absolute returns, but benchmark-relative quality is weaker than ETH and weaker than BTC/SOL in AIR terms.
- `every_bar` and `daily` can work; `weekly` is poor.
- XRP is promising but less convincing than ETH.

## Cross-Asset Summary

### Best-by-Asset

- BTC: `10/200 every_bar`, AIR `0.3899`
- ETH: `5/20 every_bar`, AIR `0.5213`
- SOL: `5/60 every_bar`, AIR `0.4354`
- XRP: `10/60 every_bar`, AIR `0.3276`

### Worst-by-Asset

- BTC: `30/60 weekly`, AIR `-0.4725`
- ETH: `10/20 weekly`, AIR `-0.2550`
- SOL: `5/10 weekly`, AIR `-0.7300`
- XRP: `5/10 weekly`, AIR `-0.6670`

## Final Interpretation

- The dominant pattern today is that `every_bar` outperforms `weekly` across all four assets.
- `weekly` is consistently the weakest choice and often collapses in AIR.
- `daily` can still work, but in most assets it lags `every_bar`.
- ETH is the standout asset. Among everything tested today, `ETH 240m 5/20 every_bar` is the strongest candidate.
- BTC is stable and credible, but less explosive than ETH. `BTC 10/200 every_bar` is the cleanest BTC setup, and `BTC 5/200 daily` is a practical alternative.
- SOL has respectable AIR, but the most robust SOL setup is likely `10/200 every_bar`, not the headline AIR winner `5/60 every_bar`.
- XRP can produce very large returns, but its lower-tail rolling IR behavior is rough. `10/60 every_bar` is the strongest XRP setup, while `5/20 every_bar` is arguably the more balanced one.

## Next Actions

- Prioritize validation on:
  - `ETH 5/20 every_bar`
  - `ETH 10/200 every_bar`
  - `BTC 10/200 every_bar`
- Secondary candidates:
  - `BTC 5/200 daily`
  - `SOL 10/200 every_bar`
  - `XRP 5/20 every_bar`
