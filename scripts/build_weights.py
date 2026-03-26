#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.storage import read_table, write_table
from lib.weights import WeightSpec, build_weight_table, weight_columns


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


def load_weight_spec(path: Path) -> WeightSpec:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return WeightSpec(
        weighting=payload.get("weighting", "equal"),
        gross_exposure=payload.get("gross_exposure", 1.0),
        fixed_weight=payload.get("fixed_weight"),
        rank_power=payload.get("rank_power", 1.0),
        max_positions=payload.get("max_positions"),
        universe_name=payload.get("universe_name"),
        rebalance_frequency=payload.get("rebalance_frequency", "daily"),
        feature_value_scale=payload.get("feature_value_scale", 1.0),
        feature_value_clip_min=payload.get("feature_value_clip_min", 0.0),
        feature_value_clip_max=payload.get("feature_value_clip_max", 1.0),
        incremental_step_size=payload.get("incremental_step_size", 0.25),
        incremental_step_up=payload.get("incremental_step_up"),
        incremental_step_down=payload.get("incremental_step_down"),
        incremental_min_weight=payload.get("incremental_min_weight", 0.0),
        incremental_max_weight=payload.get("incremental_max_weight", 1.0),
    )


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
