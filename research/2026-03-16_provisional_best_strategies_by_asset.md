# 2026-03-16 Provisional Best Strategies By Asset

자산별 잠정 최고 전략만 기록한다.
기준은 `전체 AIR`, `CAGR`, `Recent 1Y/2Y AIR`, `MDD`, `Longest Recovery`를 같이 본 종합 판단이다.

## Current Picks

| Asset | Provisional Best | Timeframe | Family | AIR | CAGR | Recent 1Y AIR | Recent 2Y AIR | MDD | Longest Recovery Bars | Confidence | Note |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| BTC | `10/120 every_bar` | `60m` | `ma_cross` | 0.4200 | 68.16% | 0.4135 | 0.5362 | 36.71% | 11736 | High | 수익 최적. 3선과 혼합은 recovery 개선은 있으나 종합 1위는 유지 |
| ETH | `5/60 + 3/400 score exposure` | `60m` | `score_exposure` | 0.5702 | 84.83% | 0.3393 | 0.6494 | 51.14% | 7028 | High | `5/60` 단독과 `3/400` 단독을 모두 상회 |
| SOL | `5/60/200 every_bar` | `60m` | `ma_stack` | 0.5686 | 68.30% | 0.6502 | 0.2505 | 45.98% | 5670 | High | `10/60` 2선보다 종합 우위. 60m 3선 메인 |
| XRP | `10/20/120 every_bar` | `60m` | `ma_stack` | 0.3728 | 107.90% | 0.8661 | 0.5879 | 51.81% | 24140 | Medium | 메인 후보. 다만 XRP 자체 recovery가 매우 길어 신뢰도는 중간 |
| ADA | `1/20/120 every_bar` | `240m` | `ma_stack` | 0.4356 | 120.96% | 0.9934 |  | 43.26% | 4748 | High | 2선보다 3선 우위가 더 명확 |
| DOGE | `1/20/200 every_bar` | `240m` | `ma_stack` | 0.4945 | 96.21% | 0.7514 |  | 40.60% | 3254 | High | 2선보다 recovery/MDD 포함 종합 우위 |
| AVAX | `5/200 every_bar` | `240m` | `ma_cross` | 0.8724 | 35.12% | 0.7222 |  | 49.35% | 1696 | Medium | 60m도 강하지만 현재까지는 240m 우위 |

## XRP Sub Pick

- Main: `60m 10/20/120 every_bar`
- Sub: `60m 10/20/120 + 10/400 score exposure`

서브 혼합 성과:
- AIR `0.3201`
- CAGR `91.99%`
- Recent 1Y AIR `0.4246`
- Recent 2Y AIR `0.1950`
- MDD `50.26%`
- Longest Recovery `12209`

해석:
- 메인 대비 수익은 내려간다
- 대신 recovery가 `24140 -> 12209`로 크게 줄어든다
- 따라서 XRP에선 메인은 `10/20/120`, 서브 절충안은 `10/20/120 + 10/400 혼합`으로 둔다

## Notes

- `Recent 2Y AIR`가 비어 있는 자산은 아직 2Y 컬럼으로 재실행하지 않았다.
- `BTC`의 방어형 대안으로 `240m 5/20/200`은 계속 보관한다.
- `ETH`는 `60m score exposure`가 현재 가장 강한 업그레이드다.
- `SOL`은 `60m 3선 5/60/200`에서 메인이 바뀌었다.
- `XRP`는 메인을 기록하되, 자산 특성상 recovery가 길다는 점을 항상 같이 본다.
