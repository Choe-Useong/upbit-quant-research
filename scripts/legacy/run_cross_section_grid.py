#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import itertools
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a standard-tree cross-sectional grid via build_features/build_universe/build_weights/run_vectorbt."
    )
    parser.add_argument("--config-json", required=True, help="Cross-sectional grid configuration JSON path")
    return parser


def _render_value(template: Any, context: dict[str, Any]) -> Any:
    if isinstance(template, dict):
        return {key: _render_value(value, context) for key, value in template.items()}
    if isinstance(template, list):
        return [_render_value(value, context) for value in template]
    if isinstance(template, str):
        if template.startswith("{") and template.endswith("}") and template.count("{") == 1 and template.count("}") == 1:
            key = template[1:-1]
            if key in context:
                return context[key]
        return template.format(**context)
    return template


def _grid_combinations(grid: dict[str, list[Any]]) -> list[dict[str, Any]]:
    if not grid:
        return [{}]
    keys = list(grid.keys())
    values = [grid[key] for key in keys]
    return [dict(zip(keys, combo)) for combo in itertools.product(*values)]


def _passes_constraints(combo: dict[str, Any], constraints: list[str]) -> bool:
    if not constraints:
        return True
    safe_globals = {"__builtins__": {}}
    safe_locals = dict(combo)
    for constraint in constraints:
        if not bool(eval(constraint, safe_globals, safe_locals)):
            return False
    return True


def _cli_flag(name: str) -> str:
    return f"--{name.replace('_', '-')}"


def _extend_command_with_options(command: list[str], options: dict[str, Any]) -> None:
    for key, value in options.items():
        if value is None:
            continue
        flag = _cli_flag(key)
        if isinstance(value, bool):
            if value:
                command.append(flag)
            continue
        if isinstance(value, list):
            command.extend([flag, ",".join(str(item) for item in value)])
            continue
        command.extend([flag, str(value)])


def _run_command(command: list[str], log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as handle:
        completed = subprocess.run(
            command,
            cwd=ROOT_DIR,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {completed.returncode}: {' '.join(command)}")


def _read_summary_csv(path: Path) -> dict[str, str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return {row[""]: row["value"] for row in reader}


def _selection_stats(weight_csv: Path) -> dict[str, float | int]:
    with weight_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        return {
            "rebalance_dates": 0,
            "avg_selected_count": 0.0,
            "min_selected_count": 0,
            "max_selected_count": 0,
            "weight_rows": 0,
        }
    counts = Counter(row["date_utc"] for row in rows)
    values = list(counts.values())
    return {
        "rebalance_dates": len(values),
        "avg_selected_count": sum(values) / len(values),
        "min_selected_count": min(values),
        "max_selected_count": max(values),
        "weight_rows": len(rows),
    }


def _row_count(csv_path: Path) -> int:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def _prepare_shared_features(
    config: dict[str, Any],
    out_dir: Path,
    candle_dir: str,
) -> tuple[Path | None, int | None]:
    prebuilt_features_csv = config.get("prebuilt_features_csv")
    if prebuilt_features_csv:
        features_csv = Path(str(prebuilt_features_csv))
        if not features_csv.exists():
            raise SystemExit(f"prebuilt_features_csv not found: {features_csv}")
        return features_csv, _row_count(features_csv)

    shared_feature_spec_template = config.get("shared_feature_spec_template")
    if not shared_feature_spec_template:
        return None, None

    shared_context = dict(config.get("shared_feature_context", {}))
    shared_features_dir = out_dir / "_shared_features"
    shared_specs_dir = shared_features_dir / "specs"
    shared_logs_dir = shared_features_dir / "logs"
    shared_build_dir = shared_features_dir / "_build"
    shared_specs_dir.mkdir(parents=True, exist_ok=True)
    shared_logs_dir.mkdir(parents=True, exist_ok=True)
    shared_build_dir.mkdir(parents=True, exist_ok=True)

    feature_spec = _render_value(shared_feature_spec_template, shared_context)
    feature_spec_path = shared_specs_dir / "features.json"
    feature_spec_path.write_text(json.dumps(feature_spec, ensure_ascii=False, indent=2), encoding="utf-8")
    features_csv = shared_build_dir / "features.csv"

    feature_command = [
        sys.executable,
        "scripts/legacy/build_features.py",
        "--candle-dir",
        str(candle_dir),
        "--spec-json",
        str(feature_spec_path),
        "--output-csv",
        str(features_csv),
    ]
    shared_feature_build_options = config.get("shared_feature_build_options", config.get("feature_build_options", {}))
    _extend_command_with_options(feature_command, _render_value(shared_feature_build_options, shared_context))
    _run_command(feature_command, shared_logs_dir / "build_features.log")
    return features_csv, _row_count(features_csv)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config_path = Path(args.config_json)
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))

    candle_dir = config.get("candle_dir") or config.get("daily_dir")
    if not candle_dir:
        raise SystemExit("config must define candle_dir or daily_dir")

    out_dir = Path(config.get("out_dir", "data/grid/cross_section"))
    out_dir.mkdir(parents=True, exist_ok=True)
    run_name_template = config["run_name_template"]
    shared_features_csv, shared_feature_rows = _prepare_shared_features(config, out_dir, str(candle_dir))

    raw_combinations = _grid_combinations(config.get("grid", {}))
    combinations = [combo for combo in raw_combinations if _passes_constraints(combo, config.get("constraints", []))]
    if not combinations:
        raise SystemExit("No valid grid combinations after applying constraints")

    results: list[dict[str, Any]] = []

    for combo in combinations:
        context = dict(combo)
        run_name = run_name_template.format(**context)
        context["run_name"] = run_name

        run_dir = out_dir / run_name
        specs_dir = run_dir / "specs"
        build_dir = run_dir / "_build"
        backtest_dir = run_dir / "backtest"
        logs_dir = run_dir / "logs"
        specs_dir.mkdir(parents=True, exist_ok=True)
        build_dir.mkdir(parents=True, exist_ok=True)
        backtest_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        feature_spec = None
        if shared_features_csv is None:
            feature_spec = _render_value(config["feature_spec_template"], context)
        universe_spec = _render_value(config["universe_spec_template"], context)
        weight_spec = _render_value(config["weight_spec_template"], context)
        vectorbt_spec = _render_value(config.get("vectorbt_spec_template", {}), context)

        feature_spec_path = specs_dir / "features.json"
        universe_spec_path = specs_dir / "universe.json"
        weight_spec_path = specs_dir / "weights.json"
        if feature_spec is not None:
            feature_spec_path.write_text(json.dumps(feature_spec, ensure_ascii=False, indent=2), encoding="utf-8")
        universe_spec_path.write_text(json.dumps(universe_spec, ensure_ascii=False, indent=2), encoding="utf-8")
        weight_spec_path.write_text(json.dumps(weight_spec, ensure_ascii=False, indent=2), encoding="utf-8")

        features_csv = build_dir / "features.csv"
        universe_csv = run_dir / "universe.csv"
        weights_csv = run_dir / "weights.csv"

        result_row: dict[str, Any] = {"run_name": run_name, **combo}
        try:
            if shared_features_csv is None:
                feature_command = [
                    sys.executable,
                    "scripts/legacy/build_features.py",
                    "--candle-dir",
                    str(candle_dir),
                    "--spec-json",
                    str(feature_spec_path),
                    "--output-csv",
                    str(features_csv),
                ]
                _extend_command_with_options(feature_command, _render_value(config.get("feature_build_options", {}), context))
                _run_command(feature_command, logs_dir / "build_features.log")
            else:
                features_csv = shared_features_csv

            universe_command = [
                sys.executable,
                "scripts/build_universe.py",
                "--features-csv",
                str(features_csv),
                "--spec-json",
                str(universe_spec_path),
                "--output-csv",
                str(universe_csv),
            ]
            _extend_command_with_options(universe_command, _render_value(config.get("universe_build_options", {}), context))
            _run_command(universe_command, logs_dir / "build_universe.log")

            weight_command = [
                sys.executable,
                "scripts/build_weights.py",
                "--universe-csv",
                str(universe_csv),
                "--spec-json",
                str(weight_spec_path),
                "--output-csv",
                str(weights_csv),
            ]
            _extend_command_with_options(weight_command, _render_value(config.get("weight_build_options", {}), context))
            _run_command(weight_command, logs_dir / "build_weights.log")

            vectorbt_command = [
                sys.executable,
                "scripts/run_vectorbt.py",
                "--candle-dir",
                str(candle_dir),
                "--weights-csv",
                str(weights_csv),
                "--out-dir",
                str(backtest_dir),
            ]
            _extend_command_with_options(vectorbt_command, vectorbt_spec)
            _extend_command_with_options(vectorbt_command, _render_value(config.get("vectorbt_options", {}), context))
            _run_command(vectorbt_command, logs_dir / "run_vectorbt.log")

            result_row["feature_rows"] = shared_feature_rows if shared_features_csv is not None else _row_count(features_csv)
            result_row["features_source"] = "shared" if shared_features_csv is not None else "per_run"
            result_row["universe_rows"] = _row_count(universe_csv)
            result_row.update(_selection_stats(weights_csv))
            result_row["status"] = "ok"
            result_row.update(_read_summary_csv(backtest_dir / "summary.csv"))
        except Exception as exc:
            result_row["status"] = "error"
            result_row["error"] = str(exc)
        results.append(result_row)

        result_frame = results
        result_path = out_dir / "summary_results.csv"
        if result_frame:
            columns: list[str] = []
            for row in result_frame:
                for key in row.keys():
                    if key not in columns:
                        columns.append(key)
            with result_path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=columns)
                writer.writeheader()
                writer.writerows(result_frame)

    print(f"Wrote {len(results)} grid result rows to {out_dir / 'summary_results.csv'}")


if __name__ == "__main__":
    main()
