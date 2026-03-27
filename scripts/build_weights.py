#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.spec_io import load_weight_spec
from lib.storage import read_table, write_table
from lib.weights import build_weight_table, weight_columns


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build weight table from universe table and a JSON weight spec."
    )
    parser.add_argument(
        "--universe-csv",
        "--universe-path",
        dest="universe_csv",
        required=True,
        help="Input universe table path (.csv or .parquet)",
    )
    parser.add_argument("--spec-json", required=True, help="JSON file containing one weight spec")
    parser.add_argument(
        "--output-csv",
        "--output-path",
        dest="output_csv",
        default="data/upbit/weights/weights.csv",
        help="Output weight table path (.csv or .parquet)",
    )
    return parser
def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    universe_rows = read_table(Path(args.universe_csv))
    if not universe_rows:
        raise SystemExit(f"No universe rows found in {args.universe_csv}")

    weight_spec = load_weight_spec(Path(args.spec_json))
    weight_rows = build_weight_table(universe_rows, weight_spec)
    write_table(Path(args.output_csv), weight_rows, weight_columns())
    print(f"Wrote {len(weight_rows)} weight rows to {args.output_csv}")


if __name__ == "__main__":
    main()
