#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.storage import read_table_csv, write_table_csv
from lib.universe import (
    RankFilterSpec,
    UniverseSpec,
    ValueFilterSpec,
    build_universe_table,
    universe_columns,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build universe CSV from feature CSV and a JSON universe spec."
    )
    parser.add_argument("--features-csv", required=True, help="Input feature CSV path")
    parser.add_argument("--spec-json", required=True, help="JSON file containing one universe spec")
    parser.add_argument("--output-csv", default="data/upbit/universe/universe.csv", help="Output universe CSV path")
    return parser


def load_universe_spec(path: Path) -> UniverseSpec:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return UniverseSpec(
        feature_column=payload["feature_column"],
        sort_column=payload.get("sort_column"),
        lag=payload.get("lag", 1),
        signal_lag=payload.get("signal_lag", 0),
        mode=payload.get("mode", "top_n"),
        top_n=payload.get("top_n", 30),
        quantiles=payload.get("quantiles", 5),
        bucket_values=tuple(payload.get("bucket_values", [1])),
        ascending=payload.get("ascending", False),
        exclude_warnings=payload.get("exclude_warnings", False),
        min_age_days=payload.get("min_age_days"),
        min_cross_section_size=payload.get("min_cross_section_size", 0),
        allowed_markets=tuple(payload.get("allowed_markets", [])),
        excluded_markets=tuple(payload.get("excluded_markets", [])),
        value_filters=tuple(
            ValueFilterSpec(
                feature_column=item["feature_column"],
                operator=item["operator"],
                value=float(item["value"]),
                lag=item.get("lag", 0),
            )
            for item in payload.get("value_filters", [])
        ),
        rank_filters=tuple(
            RankFilterSpec(
                feature_column=item["feature_column"],
                mode=item.get("mode", "top_n"),
                lag=item.get("lag", 0),
                top_n=item.get("top_n", 30),
                quantiles=item.get("quantiles", 5),
                bucket_values=tuple(item.get("bucket_values", [1])),
                ascending=item.get("ascending", False),
            )
            for item in payload.get("rank_filters", [])
        ),
        name=payload.get("name"),
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    feature_rows = read_table_csv(Path(args.features_csv))
    if not feature_rows:
        raise SystemExit(f"No feature rows found in {args.features_csv}")

    universe_spec = load_universe_spec(Path(args.spec_json))
    universe_rows = build_universe_table(feature_rows, universe_spec)
    write_table_csv(Path(args.output_csv), universe_rows, universe_columns())
    print(f"Wrote {len(universe_rows)} universe rows to {args.output_csv}")


if __name__ == "__main__":
    main()
