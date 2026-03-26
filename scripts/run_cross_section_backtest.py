#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a standard-tree cross-sectional backtest via build_features/build_universe/build_weights/run_vectorbt."
    )
    parser.add_argument(
        "--candle-dir",
        "--daily-dir",
        dest="candle_dir",
        required=True,
        help="Directory containing per-market candle CSV files",
    )
    parser.add_argument("--feature-spec-json", required=True, help="Feature spec JSON path")
    parser.add_argument("--universe-spec-json", required=True, help="Universe spec JSON path")
    parser.add_argument("--weight-spec-json", required=True, help="Weight spec JSON path")
    parser.add_argument(
        "--portfolio-dir",
        required=True,
        help="Output directory for generated features/universe/weights CSVs",
    )
    parser.add_argument(
        "--backtest-out-dir",
        required=True,
        help="Output directory for vectorbt backtest files",
    )
    parser.add_argument("--fees", type=float, default=0.0, help="Per-order proportional fees")
    parser.add_argument("--slippage", type=float, default=0.0, help="Per-order proportional slippage")
    parser.add_argument(
        "--benchmark-market",
        default="KRW-BTC",
        help="Benchmark market for vectorbt summary",
    )
    parser.add_argument(
        "--max-markets",
        type=int,
        default=None,
        help="Optional feature-build market limit for smoke runs",
    )
    parser.add_argument(
        "--tail-hours",
        type=int,
        default=None,
        help="Optional feature-build tail length for smoke runs",
    )
    parser.add_argument(
        "--source-cache-dir",
        default="",
        help="Optional wide-parquet source cache directory, e.g. data/upbit_research_cache/60",
    )
    parser.add_argument(
        "--engine",
        choices=["auto", "legacy", "market_stream"],
        default="auto",
        help="Feature build engine",
    )
    return parser


def _run(command: list[str]) -> None:
    completed = subprocess.run(
        command,
        cwd=ROOT_DIR,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    portfolio_dir = Path(args.portfolio_dir)
    build_dir = portfolio_dir / "_build"
    build_dir.mkdir(parents=True, exist_ok=True)
    portfolio_dir.mkdir(parents=True, exist_ok=True)

    features_csv = build_dir / "features.parquet"
    universe_csv = portfolio_dir / "universe.parquet"
    weights_csv = portfolio_dir / "weights.parquet"

    feature_command = [
        sys.executable,
        "scripts/build_features.py",
        "--candle-dir",
        args.candle_dir,
        "--spec-json",
        args.feature_spec_json,
        "--output-csv",
        str(features_csv),
        "--engine",
        args.engine,
    ]
    if args.max_markets is not None:
        feature_command.extend(["--max-markets", str(args.max_markets)])
    if args.tail_hours is not None:
        feature_command.extend(["--tail-hours", str(args.tail_hours)])
    if args.source_cache_dir:
        feature_command.extend(["--source-cache-dir", args.source_cache_dir])
    _run(feature_command)

    universe_command = [
        sys.executable,
        "scripts/build_universe.py",
        "--features-csv",
        str(features_csv),
        "--spec-json",
        args.universe_spec_json,
        "--output-csv",
        str(universe_csv),
    ]
    _run(universe_command)

    weights_command = [
        sys.executable,
        "scripts/build_weights.py",
        "--universe-csv",
        str(universe_csv),
        "--spec-json",
        args.weight_spec_json,
        "--output-csv",
        str(weights_csv),
    ]
    _run(weights_command)

    backtest_command = [
        sys.executable,
        "scripts/run_vectorbt.py",
        "--candle-dir",
        args.candle_dir,
        "--weights-csv",
        str(weights_csv),
        "--out-dir",
        args.backtest_out_dir,
        "--fees",
        str(args.fees),
        "--slippage",
        str(args.slippage),
        "--benchmark-market",
        args.benchmark_market,
    ]
    if args.source_cache_dir:
        backtest_command.extend(["--source-cache-dir", args.source_cache_dir])
    _run(backtest_command)


if __name__ == "__main__":
    main()
