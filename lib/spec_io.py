from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path

from lib.specs import (
    CompareSpec,
    FeatureSpec,
    FilterStageSpec,
    LogicalSpec,
    MarketScoreComponentSpec,
    MarketScoreRuleSpec,
    MarketScoreSpec,
    RankFilterSpec,
    ScoreComponentSpec,
    StateSpec,
    TransformSpec,
    UniverseSpec,
    ValueFilterSpec,
    WeightSpec,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_FEATURE_PRESET_PATH = ROOT_DIR / "configs" / "presets" / "features_v2_presets.json"


def _render_template_value(template, context: dict[str, object]):
    if isinstance(template, dict):
        return {key: _render_template_value(value, context) for key, value in template.items()}
    if isinstance(template, list):
        return [_render_template_value(value, context) for value in template]
    if isinstance(template, str):
        if template.startswith("{") and template.endswith("}") and template.count("{") == 1 and template.count("}") == 1:
            key = template[1:-1]
            if key in context:
                return context[key]
        return template.format(**context)
    return template


@lru_cache(maxsize=1)
def _load_feature_preset_catalog() -> dict[str, dict]:
    if not DEFAULT_FEATURE_PRESET_PATH.exists():
        return {}
    payload = json.loads(DEFAULT_FEATURE_PRESET_PATH.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError("Feature preset catalog must be a JSON object")
    return payload


def _expand_feature_preset_item(item: dict) -> dict:
    preset_name = item.get("preset")
    if not preset_name:
        return item
    catalog = _load_feature_preset_catalog()
    if preset_name not in catalog:
        raise ValueError(f"Unknown feature preset: {preset_name}")
    preset_template = copy.deepcopy(catalog[preset_name])
    params = item.get("params", {})
    if not isinstance(params, dict):
        raise ValueError(f"Feature preset params must be an object: {preset_name}")
    expanded = _render_template_value(preset_template, params)
    if not isinstance(expanded, dict):
        raise ValueError(f"Expanded feature preset must be an object: {preset_name}")
    result = dict(expanded)
    if "default_column_name" in result and "column_name" not in item:
        result["column_name"] = result.pop("default_column_name")
    else:
        result.pop("default_column_name", None)
    for key, value in item.items():
        if key in {"preset", "params"}:
            continue
        result[key] = value
    return result


def load_feature_specs_from_payload(payload: list[dict]) -> list[FeatureSpec]:
    specs: list[FeatureSpec] = []
    for raw_item in payload:
        item = _expand_feature_preset_item(raw_item)
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
        state = None
        if "state" in item:
            state_payload = item["state"]
            state = StateSpec(
                entry_feature=state_payload["entry_feature"],
                exit_feature=state_payload["exit_feature"],
            )
        specs.append(
            FeatureSpec(
                source=item.get("source"),
                steps=steps,
                components=components,
                combine=item.get("combine"),
                compare=compare,
                logical=logical,
                state=state,
                column_name=item.get("column_name"),
            )
        )
    return specs


def load_feature_specs(path: Path) -> list[FeatureSpec]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return load_feature_specs_from_payload(payload)


def load_universe_spec_from_payload(payload: dict) -> UniverseSpec:
    def _load_rank_filter_spec(item: dict) -> RankFilterSpec:
        return RankFilterSpec(
            feature_column=item["feature_column"],
            mode=item.get("mode", "top_n"),
            lag=item.get("lag", 0),
            top_n=item.get("top_n", 30),
            quantiles=item.get("quantiles", 5),
            bucket_values=tuple(item.get("bucket_values", [1])),
            ascending=item.get("ascending", False),
            scope=item.get("scope", "filtered"),
        )

    return UniverseSpec(
        feature_column=payload["feature_column"],
        sort_column=payload.get("sort_column"),
        lag=payload.get("lag", 1),
        signal_lag=payload.get("signal_lag", 0),
        start_min_cross_section_size=payload.get("start_min_cross_section_size", 0),
        mode=payload.get("mode", "top_n"),
        top_n=payload.get("top_n", 30),
        quantiles=payload.get("quantiles", 5),
        bucket_values=tuple(payload.get("bucket_values", [1])),
        ascending=payload.get("ascending", False),
        scope=payload.get("scope", "filtered"),
        exclude_warnings=payload.get("exclude_warnings", False),
        min_age_days=payload.get("min_age_days"),
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
        rank_filters=tuple(_load_rank_filter_spec(item) for item in payload.get("rank_filters", [])),
        filter_stages=tuple(
            FilterStageSpec(
                mode=item.get("mode", "sequential"),
                filters=tuple(_load_rank_filter_spec(filter_item) for filter_item in item.get("filters", [])),
            )
            for item in payload.get("filter_stages", [])
        ),
        name=payload.get("name"),
    )


def load_universe_spec(path: Path) -> UniverseSpec:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return load_universe_spec_from_payload(payload)


def load_weight_spec_from_payload(payload: dict) -> WeightSpec:
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


def load_weight_spec(path: Path) -> WeightSpec:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return load_weight_spec_from_payload(payload)


def load_market_score_spec_from_payload(payload: dict) -> MarketScoreSpec:
    return MarketScoreSpec(
        output_column=str(payload.get("output_column", "custom_score")),
        rules=tuple(
            MarketScoreRuleSpec(
                market=str(item["market"]).upper(),
                mode=str(item.get("mode", "weighted_sum")),
                components=tuple(
                    MarketScoreComponentSpec(
                        feature_column=str(component["feature_column"]),
                        weight=float(component.get("weight", 1.0)),
                    )
                    for component in item.get("components", [])
                ),
            )
            for item in payload.get("rules", [])
        ),
    )


def load_market_score_spec(path: Path) -> MarketScoreSpec:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return load_market_score_spec_from_payload(payload)
