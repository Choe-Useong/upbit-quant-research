#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.allocation import (
    build_allocated_weight_frame,
    compress_weight_frame_to_rows,
    load_allocation_config,
)
from lib.storage import write_table_csv
from lib.legacy.weights import weight_columns


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build allocated portfolio weights from multiple strategy weights.csv sources."
    )
    parser.add_argument("--config-json", required=True, help="Allocation config JSON path")
    parser.add_argument(
        "--output-dir",
        default="",
        help="Output portfolio directory; defaults to data/portfolio/<portfolio_name>",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config_path = ROOT_DIR / args.config_json if not Path(args.config_json).is_absolute() else Path(args.config_json)
    allocation_config = load_allocation_config(config_path, ROOT_DIR)
    portfolio_name = str(allocation_config["portfolio_name"])
    output_dir = (
        (ROOT_DIR / args.output_dir) if args.output_dir else (ROOT_DIR / "data" / "portfolio" / portfolio_name)
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    combined_frame, metadata_by_market, source_names = build_allocated_weight_frame(
        sleeves=allocation_config["sleeves"],
        portfolio_inactive_mode=str(allocation_config["portfolio_inactive_mode"]),
        market_caps=dict(allocation_config["market_caps"]),
        cap_overflow_mode=str(allocation_config["cap_overflow_mode"]),
    )
    weight_rows = compress_weight_frame_to_rows(
        combined_frame,
        metadata_by_market,
        weights_name=portfolio_name,
        universe_name=portfolio_name,
        source_names=source_names,
    )

    weights_csv = output_dir / "weights.csv"
    metadata_json = output_dir / "allocation_metadata.json"
    write_table_csv(weights_csv, weight_rows, weight_columns())
    metadata_payload = {
        "portfolio_name": portfolio_name,
        "config_json": str(config_path.relative_to(ROOT_DIR)) if config_path.is_relative_to(ROOT_DIR) else str(config_path),
        "portfolio_inactive_mode": allocation_config["portfolio_inactive_mode"],
        "market_caps": allocation_config["market_caps"],
        "cap_overflow_mode": allocation_config["cap_overflow_mode"],
        "source_count": len(allocation_config["sleeves"]),
        "source_names": source_names,
        "weights_row_count": len(weight_rows),
        "timestamps": int(len(combined_frame.index)),
        "markets": int(len(combined_frame.columns)),
        "sources": [
            {
                "name": sleeve.name,
                "weights_csv": str(sleeve.weights_csv.relative_to(ROOT_DIR)) if sleeve.weights_csv.is_relative_to(ROOT_DIR) else str(sleeve.weights_csv),
                "capital_weight": sleeve.capital_weight,
                "weight_scale_mode": sleeve.weight_scale_mode,
            }
            for sleeve in allocation_config["sleeves"]
        ],
    }
    metadata_json.write_text(json.dumps(metadata_payload, indent=2), encoding="utf-8-sig")

    print(f"Wrote {len(weight_rows)} allocated weight rows to {weights_csv}")
    print(f"Wrote allocation metadata to {metadata_json}")


if __name__ == "__main__":
    main()
