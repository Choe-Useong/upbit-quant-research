from __future__ import annotations

import json
from pathlib import Path

from lib.specs import (
    CompareSpec,
    FeatureSpec,
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


def load_feature_specs_from_payload(payload: list[dict]) -> list[FeatureSpec]:
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
