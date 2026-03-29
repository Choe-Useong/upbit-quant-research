from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TransformSpec:
    kind: str
    params: dict[str, int | float | str] = field(default_factory=dict)

    def resolved_name(self) -> str:
        if not self.params:
            return self.kind
        suffix = "_".join(f"{key}{value}" for key, value in sorted(self.params.items()))
        return f"{self.kind}_{suffix}"


@dataclass(frozen=True)
class ScoreComponentSpec:
    feature_column: str
    weight: float = 1.0


@dataclass(frozen=True)
class MarketScoreComponentSpec:
    feature_column: str
    weight: float = 1.0


@dataclass(frozen=True)
class MarketScoreRuleSpec:
    market: str
    mode: str = "weighted_sum"
    components: tuple[MarketScoreComponentSpec, ...] = ()


@dataclass(frozen=True)
class MarketScoreSpec:
    output_column: str = "custom_score"
    rules: tuple[MarketScoreRuleSpec, ...] = ()


@dataclass(frozen=True)
class CompareSpec:
    left_feature: str
    operator: str
    right_feature: str | None = None
    right_value: float | None = None


@dataclass(frozen=True)
class LogicalSpec:
    operator: str
    features: tuple[str, ...]


@dataclass(frozen=True)
class StateSpec:
    entry_feature: str
    exit_feature: str


@dataclass(frozen=True)
class BreadthSpec:
    driver_feature: str
    signal_feature: str
    mode: str = "top_n"
    top_n: int = 4
    quantiles: int = 4
    bucket_values: tuple[int, ...] = (1,)
    ascending: bool = False


@dataclass(frozen=True)
class FeatureSpec:
    source: str | None = None
    steps: tuple[TransformSpec, ...] = ()
    components: tuple[ScoreComponentSpec, ...] = ()
    combine: str | None = None
    compare: CompareSpec | None = None
    logical: LogicalSpec | None = None
    state: StateSpec | None = None
    breadth: BreadthSpec | None = None
    column_name: str | None = None

    def resolved_column_name(self) -> str:
        if self.column_name:
            return self.column_name
        if self.compare is not None:
            right = (
                self.compare.right_feature
                if self.compare.right_feature is not None
                else f"value{self.compare.right_value:g}"
            )
            return f"{self.compare.left_feature}_{self.compare.operator}_{right}"
        if self.logical is not None:
            joined = "__".join(self.logical.features)
            return f"{self.logical.operator}__{joined}"
        if self.state is not None:
            return f"hold__{self.state.entry_feature}__until__{self.state.exit_feature}"
        if self.breadth is not None:
            if self.breadth.mode == "top_n":
                order = "asc" if self.breadth.ascending else "desc"
                return (
                    f"breadth__{self.breadth.driver_feature}__top{self.breadth.top_n}_{order}"
                    f"__{self.breadth.signal_feature}"
                )
            buckets = "-".join(str(value) for value in self.breadth.bucket_values)
            order = "asc" if self.breadth.ascending else "desc"
            return (
                f"breadth__{self.breadth.driver_feature}__q{self.breadth.quantiles}_b{buckets}_{order}"
                f"__{self.breadth.signal_feature}"
            )
        if self.components:
            suffix = "__".join(
                f"{component.feature_column}_w{component.weight:g}"
                for component in self.components
            )
            prefix = self.combine or "weighted_sum"
            return f"{prefix}__{suffix}"
        if not self.steps:
            if self.source is None:
                raise ValueError("FeatureSpec must define source, components, compare, logical, state, or breadth")
            return self.source
        suffix = "__".join(step.resolved_name() for step in self.steps)
        if self.source is None:
            raise ValueError("FeatureSpec with steps must define source")
        return f"{self.source}__{suffix}"


@dataclass(frozen=True)
class ValueFilterSpec:
    feature_column: str
    operator: str
    value: float
    lag: int = 0


@dataclass(frozen=True)
class RankFilterSpec:
    feature_column: str
    mode: str = "top_n"
    lag: int = 0
    top_n: int = 30
    quantiles: int = 5
    bucket_values: tuple[int, ...] = (1,)
    ascending: bool = False
    scope: str = "filtered"


@dataclass(frozen=True)
class FilterStageSpec:
    mode: str = "sequential"
    filters: tuple[RankFilterSpec, ...] = ()


@dataclass(frozen=True)
class UniverseSpec:
    feature_column: str
    sort_column: str | None = None
    lag: int = 1
    signal_lag: int = 0
    start_min_cross_section_size: int = 0
    mode: str = "top_n"
    top_n: int = 30
    quantiles: int = 5
    bucket_values: tuple[int, ...] = (1,)
    ascending: bool = False
    scope: str = "filtered"
    exclude_warnings: bool = False
    min_age_days: int | None = None
    allowed_markets: tuple[str, ...] = ()
    excluded_markets: tuple[str, ...] = ()
    value_filters: tuple[ValueFilterSpec, ...] = ()
    rank_filters: tuple[RankFilterSpec, ...] = ()
    filter_stages: tuple[FilterStageSpec, ...] = ()
    name: str | None = None

    def resolved_name(self) -> str:
        sort_column = self.sort_column or self.feature_column
        lag_part = f"lag{self.lag}"
        if self.signal_lag > 0:
            lag_part += f"_siglag{self.signal_lag}"
        if self.start_min_cross_section_size > 0:
            lag_part += f"_startcs{self.start_min_cross_section_size}"
        if self.name:
            return self.name
        if self.mode == "top_n":
            order = "asc" if self.ascending else "desc"
            return f"{sort_column}_{lag_part}_{order}_top{self.top_n}"
        if self.mode == "all":
            return f"{sort_column}_{lag_part}_all"
        order = "asc" if self.ascending else "desc"
        buckets = "-".join(str(value) for value in self.bucket_values)
        return f"{sort_column}_{lag_part}_{order}_q{self.quantiles}_b{buckets}"


@dataclass(frozen=True)
class WeightSpec:
    weighting: str = "equal"
    gross_exposure: float = 1.0
    gross_exposure_feature: str | None = None
    gross_exposure_lag: int = 0
    gross_exposure_clip_min: float = 0.0
    gross_exposure_clip_max: float = 1.0
    fixed_weight: float | None = None
    rank_power: float = 1.0
    max_positions: int | None = None
    universe_name: str | None = None
    rebalance_frequency: str = "daily"
    feature_value_scale: float = 1.0
    feature_value_clip_min: float = 0.0
    feature_value_clip_max: float = 1.0
    incremental_step_size: float = 0.25
    incremental_step_up: float | None = None
    incremental_step_down: float | None = None
    incremental_min_weight: float = 0.0
    incremental_max_weight: float = 1.0

    def resolved_name(self) -> str:
        prefix = self.universe_name or "universe"
        exposure_suffix = ""
        if self.gross_exposure_feature:
            feature_token = (
                str(self.gross_exposure_feature)
                .replace(":", "_")
                .replace("/", "_")
                .replace("\\", "_")
                .replace("{", "")
                .replace("}", "")
            )
            exposure_suffix = (
                f"_gfeat{feature_token}"
                f"_lag{self.gross_exposure_lag}"
                f"_clip{self.gross_exposure_clip_min:g}_{self.gross_exposure_clip_max:g}"
            )
        if self.weighting == "equal":
            return (
                f"{prefix}__equal_{self.rebalance_frequency}"
                f"_gross{self.gross_exposure:g}"
                f"{exposure_suffix}"
            )
        if self.weighting == "feature_value":
            return (
                f"{prefix}__feature_value_{self.rebalance_frequency}"
                f"_gross{self.gross_exposure:g}"
                f"{exposure_suffix}"
            )
        if self.weighting == "fixed":
            if self.fixed_weight is None:
                raise ValueError("fixed_weight must be provided for fixed weighting")
            return (
                f"{prefix}__fixed_{self.rebalance_frequency}"
                f"_w{self.fixed_weight:g}"
                f"{exposure_suffix}"
            )
        if self.weighting == "incremental_signal":
            step_up = self.incremental_step_up if self.incremental_step_up is not None else self.incremental_step_size
            step_down = self.incremental_step_down if self.incremental_step_down is not None else self.incremental_step_size
            return (
                f"{prefix}__incremental_signal_{self.rebalance_frequency}"
                f"_up{step_up:g}_down{step_down:g}"
                f"_gross{self.gross_exposure:g}"
                f"{exposure_suffix}"
            )
        return (
            f"{prefix}__rank_p{self.rank_power:g}_{self.rebalance_frequency}"
            f"_gross{self.gross_exposure:g}"
            f"{exposure_suffix}"
        )
