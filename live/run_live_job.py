#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from live.execute_portfolio import main as execute_portfolio_main
from scripts.build_portfolio_weights import main as build_portfolio_weights_main


DEFAULT_BUILD_PRESET = "main_strategies_60m_equal_weight_4core"
DEFAULT_EXECUTION_CONFIG = "configs/live/main_strategies_60m_equal_weight_4core_execution.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build latest portfolio weights and immediately execute preview/live orders."
    )
    parser.add_argument("--mode", choices=["preview", "live"], default="preview")
    parser.add_argument("--build-preset", default=DEFAULT_BUILD_PRESET)
    parser.add_argument("--candle-dir", default="", help="Optional candle dir override for weight build step")
    parser.add_argument("--execution-config-json", default=DEFAULT_EXECUTION_CONFIG)
    parser.add_argument("--min-order-krw", type=float, default=None)
    parser.add_argument("--ignore-unmanaged", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not args.skip_build:
        build_args = ["--preset", args.build_preset]
        if args.candle_dir:
            build_args.extend(["--candle-dir", args.candle_dir])
        build_rc = build_portfolio_weights_main(build_args)
        if build_rc != 0:
            return int(build_rc)

    execute_args = [
        "--mode",
        args.mode,
        "--execution-config-json",
        args.execution_config_json,
    ]
    if args.min_order_krw is not None:
        execute_args.extend(["--min-order-krw", str(args.min_order_krw)])
    if args.ignore_unmanaged:
        execute_args.append("--ignore-unmanaged")
    return int(execute_portfolio_main(execute_args))


if __name__ == "__main__":
    raise SystemExit(main())
