#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.features import (
    CompareSpec,
    FeatureSpec,
    LogicalSpec,
    ScoreComponentSpec,
    TransformSpec,
    build_feature_table,
    feature_columns,
)
from lib.storage import read_candles_csv, write_table_csv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build feature table CSV from candle CSV files and a JSON feature spec."
    )
    parser.add_argument(
        "--candle-dir",
        "--daily-dir",
        dest="candle_dir",
        default="data/upbit/daily",
        help="Directory containing per-market candle CSV files",
    )
    parser.add_argument("--spec-json", required=True, help="JSON file containing feature spec list")
    parser.add_argument("--output-csv", default="data/upbit/features/features.csv", help="Output feature CSV path")
    return parser


def load_feature_specs(path: Path) -> list[FeatureSpec]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    specs: list[FeatureSpec] = []
    for item in payload:
        steps = tuple(
            TransformSpec(kind=step["kind"], params=step.get("params", {}))
            for step in item.get("steps", [])
        )
        components = tuple(
            ScoreComponentSpec(
                feature_column=component["feature_column"],
                weight=float(component.get("weight", 1.0)),
            )
            for component in item.get("components", [])
        )
        compare = None
        if "compare" in item:
            compare_payload = item["compare"]
            compare = CompareSpec(
                left_feature=compare_payload["left_feature"],
                operator=compare_payload["operator"],
                right_feature=compare_payload.get("right_feature"),
                right_value=(
                    None
                    if compare_payload.get("right_value") is None
                    else float(compare_payload["right_value"])
                ),
            )
        logical = None
        if "logical" in item:
            logical_payload = item["logical"]
            logical = LogicalSpec(
                operator=logical_payload["operator"],
                features=tuple(logical_payload["features"]),
            )
        specs.append(
            FeatureSpec(
                source=item.get("source"),
                steps=steps,
                components=components,
                combine=item.get("combine"),
                compare=compare,
                logical=logical,
                column_name=item.get("column_name"),
            )
        )
    return specs


def load_all_candles(candle_dir: Path) -> list:
    rows = []
    for csv_path in sorted(candle_dir.glob("*.csv")):
        rows.extend(read_candles_csv(csv_path))
    return rows


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    candle_rows = load_all_candles(Path(args.candle_dir))
    if not candle_rows:
        raise SystemExit(f"No candle rows found in {args.candle_dir}")

    feature_specs = load_feature_specs(Path(args.spec_json))
    feature_rows = build_feature_table(candle_rows, feature_specs)
    write_table_csv(Path(args.output_csv), feature_rows, feature_columns(feature_specs))
    print(f"Wrote {len(feature_rows)} feature rows to {args.output_csv}")


if __name__ == "__main__":
    main()
