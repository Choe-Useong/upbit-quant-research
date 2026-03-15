#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.storage import write_candles_csv, write_market_manifest
from lib.upbit_collector import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_PAUSE_SECONDS,
    collect_minute_candles,
    list_markets,
)


VALID_UNITS = {1, 3, 5, 10, 15, 30, 60, 240}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download Upbit minute candles and save them as per-market CSV files."
    )
    parser.add_argument(
        "--unit",
        type=int,
        default=240,
        help="Minute candle unit. Supported values: 1,3,5,10,15,30,60,240",
    )
    parser.add_argument(
        "--quote",
        default="KRW",
        help="Quote currency prefix used to filter markets",
    )
    parser.add_argument(
        "--candles",
        type=int,
        default=None,
        help="Maximum number of minute candles per market; omit to collect as far back as possible",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Maximum candles requested per API call",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=DEFAULT_PAUSE_SECONDS,
        help="Delay between API calls for one market",
    )
    parser.add_argument(
        "--exclude-warnings",
        action="store_true",
        help="Exclude markets currently marked with investment warnings",
    )
    parser.add_argument(
        "--markets",
        default="",
        help="Comma-separated subset of markets to download, for example KRW-BTC,KRW-ETH",
    )
    parser.add_argument(
        "--max-markets",
        type=int,
        default=None,
        help="Limit the number of markets after filtering",
    )
    parser.add_argument(
        "--out-dir",
        default="data/upbit",
        help="Base output directory",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.unit not in VALID_UNITS:
        raise SystemExit(f"Unsupported --unit {args.unit}. Valid values: {sorted(VALID_UNITS)}")

    out_dir = Path(args.out_dir)
    minute_dir = out_dir / "minutes" / str(args.unit)
    markets = list_markets(
        quote=args.quote,
        include_warnings=not args.exclude_warnings,
    )

    if args.markets:
        allowed = {market.strip().upper() for market in args.markets.split(",") if market.strip()}
        markets = [market for market in markets if market.market in allowed]
    if args.max_markets is not None:
        markets = markets[: args.max_markets]

    write_market_manifest(out_dir / "markets.csv", markets)
    print(f"Found {len(markets)} {args.quote.upper()} markets for {args.unit}-minute candles")

    for idx, market in enumerate(markets, start=1):
        candles = collect_minute_candles(
            market=market,
            unit=args.unit,
            candles=args.candles,
            batch_size=args.batch_size,
            pause_seconds=args.pause_seconds,
        )
        row_count = write_candles_csv(minute_dir / f"{market.market}.csv", candles)
        print(f"[{idx}/{len(markets)}] {market.market}: saved {row_count} rows")


if __name__ == "__main__":
    main()
