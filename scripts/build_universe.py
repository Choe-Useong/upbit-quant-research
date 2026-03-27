#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.spec_io import load_universe_spec
from lib.storage import read_table, write_table
from lib.universe import build_universe_table, universe_columns, write_universe_table_from_feature_csv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build universe CSV from feature CSV and a JSON universe spec."
    )
    parser.add_argument("--features-csv", required=True, help="Input feature CSV path")
    parser.add_argument("--spec-json", required=True, help="JSON file containing one universe spec")
    parser.add_argument("--output-csv", default="data/upbit/universe/universe.csv", help="Output universe CSV path")
    parser.add_argument(
        "--engine",
        choices=["auto", "legacy", "stream"],
        default="auto",
        help="Universe build engine. auto uses stream.",
    )
    return parser
def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    universe_spec = load_universe_spec(Path(args.spec_json))
    engine = "stream" if args.engine == "auto" else args.engine
    if engine == "legacy":
        feature_rows = read_table(Path(args.features_csv))
        if not feature_rows:
            raise SystemExit(f"No feature rows found in {args.features_csv}")
        universe_rows = build_universe_table(feature_rows, universe_spec)
        write_table(Path(args.output_csv), universe_rows, universe_columns())
        row_count = len(universe_rows)
    else:
        row_count = write_universe_table_from_feature_csv(
            args.features_csv,
            args.output_csv,
            universe_spec,
        )
    print(f"Wrote {row_count} universe rows to {args.output_csv}")


if __name__ == "__main__":
    main()
