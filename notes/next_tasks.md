# Next Tasks

## Source Cache

- Add incremental update mode to [build_upbit_research_cache.py](/c:/Users/working/Desktop/Coin%20Project/scripts/build_upbit_research_cache.py) so cache files do not rebuild from scratch after every raw source refresh.
- Define cache invalidation rules:
  - rebuild all when raw schema changes
  - append/update tail when only latest source candles changed
- Wire [research_turnover_cross_section.py](/c:/Users/working/Desktop/Coin%20Project/scripts/research_turnover_cross_section.py) to prefer `data/upbit_research_cache/60`.
- Wire [build_portfolio_weights.py](/c:/Users/working/Desktop/Coin%20Project/scripts/build_portfolio_weights.py) to prefer `data/upbit_research_cache/60`.
- Decide whether live jobs should rebuild the source cache automatically after raw candle updates or run it as a separate step.
