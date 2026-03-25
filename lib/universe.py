from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable


UNIVERSE_MODES = {"top_n", "quantile"}
FILTER_OPERATORS = {"gt", "ge", "lt", "le", "eq", "ne"}


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


@dataclass(frozen=True)
class UniverseSpec:
    feature_column: str
    sort_column: str | None = None
    lag: int = 1
    signal_lag: int = 0
    mode: str = "top_n"
    top_n: int = 30
    quantiles: int = 5
    bucket_values: tuple[int, ...] = (1,)
    ascending: bool = False
    exclude_warnings: bool = False
    min_age_days: int | None = None
    min_cross_section_size: int = 0
    allowed_markets: tuple[str, ...] = ()
    excluded_markets: tuple[str, ...] = ()
    value_filters: tuple[ValueFilterSpec, ...] = ()
    rank_filters: tuple[RankFilterSpec, ...] = ()
    name: str | None = None

    def resolved_name(self) -> str:
        sort_column = self.sort_column or self.feature_column
        lag_part = f"lag{self.lag}"
        if self.signal_lag > 0:
            lag_part += f"_siglag{self.signal_lag}"
        if self.min_cross_section_size > 0:
            lag_part += f"_mincs{self.min_cross_section_size}"
        if self.name:
            return self.name
        if self.mode == "top_n":
            order = "asc" if self.ascending else "desc"
            return f"{sort_column}_{lag_part}_{order}_top{self.top_n}"
        order = "asc" if self.ascending else "desc"
        buckets = "-".join(str(value) for value in self.bucket_values)
        return f"{sort_column}_{lag_part}_{order}_q{self.quantiles}_b{buckets}"


def _parse_float(value: str) -> float | None:
    if value == "":
        return None
    parsed = float(value)
    if math.isnan(parsed):
        return None
    return parsed


def _parse_int(value: str) -> int | None:
    if value == "":
        return None
    return int(float(value))


def _market_groups(rows: list[dict[str, str]]) -> list[list[int]]:
    grouped: dict[str, list[int]] = defaultdict(list)
    for idx, row in enumerate(rows):
        grouped[row["market"]].append(idx)
    return [
        sorted(indexes, key=lambda idx: rows[idx]["date_utc"])
        for _, indexes in sorted(grouped.items())
    ]


def _date_groups(rows: list[dict[str, str]]) -> list[list[int]]:
    grouped: dict[str, list[int]] = defaultdict(list)
    for idx, row in enumerate(rows):
        grouped[row["date_utc"]].append(idx)
    return [
        sorted(indexes, key=lambda idx: rows[idx]["market"])
        for _, indexes in sorted(grouped.items())
    ]


def _lag_feature_values(
    rows: list[dict[str, str]],
    feature_column: str,
    lag: int,
) -> list[float | None]:
    result: list[float | None] = [None] * len(rows)
    for indexes in _market_groups(rows):
        market_values = [_parse_float(rows[idx].get(feature_column, "")) for idx in indexes]
        for offset, idx in enumerate(indexes):
            ref_idx = offset - lag
            if lag <= 0:
                result[idx] = market_values[offset]
            elif ref_idx >= 0:
                result[idx] = market_values[ref_idx]
            else:
                result[idx] = None
    return result


def _row_passes_filters(row: dict[str, str], spec: UniverseSpec) -> bool:
    if spec.exclude_warnings and row.get("market_warning", "") != "NONE":
        return False
    if spec.allowed_markets and row["market"] not in spec.allowed_markets:
        return False
    if spec.excluded_markets and row["market"] in spec.excluded_markets:
        return False
    if spec.min_age_days is not None:
        age_days = _parse_int(row.get("age_days", ""))
        if age_days is None or age_days < spec.min_age_days:
            return False
    return True


def _compare(operator: str, left: float, right: float) -> bool:
    if operator == "gt":
        return left > right
    if operator == "ge":
        return left >= right
    if operator == "lt":
        return left < right
    if operator == "le":
        return left <= right
    if operator == "eq":
        return left == right
    if operator == "ne":
        return left != right
    raise ValueError(f"Unsupported filter operator: {operator}")


def _bucket_for_rank(rank: int, size: int, quantiles: int) -> int:
    if size <= 0:
        raise ValueError("Cross-sectional size must be positive")
    return 1 + (((rank - 1) * quantiles) // size)


def _selected_rows_for_date(
    rows: list[dict[str, str]],
    lagged_value_map: dict[tuple[str, int], list[float | None]],
    indexes: list[int],
    spec: UniverseSpec,
) -> list[dict[str, str]]:
    sort_column = spec.sort_column or spec.feature_column
    sort_values = lagged_value_map[(sort_column, spec.lag)]
    candidates: list[int] = []
    for idx in indexes:
        if not _row_passes_filters(rows[idx], spec):
            continue
        final_value = sort_values[idx]
        if final_value is None:
            continue
        blocked = False
        for filter_spec in spec.value_filters:
            filter_values = lagged_value_map[(filter_spec.feature_column, filter_spec.lag)]
            value = filter_values[idx]
            if value is None or not _compare(filter_spec.operator, value, filter_spec.value):
                blocked = True
                break
        if blocked:
            continue
        candidates.append(idx)

    if spec.min_cross_section_size > 0 and len(candidates) < spec.min_cross_section_size:
        return []

    for rank_filter in spec.rank_filters:
        rank_values = lagged_value_map[(rank_filter.feature_column, rank_filter.lag)]
        scoped = [(idx, rank_values[idx]) for idx in candidates if rank_values[idx] is not None]
        ordered = sorted(scoped, key=lambda item: item[1], reverse=not rank_filter.ascending)
        total = len(ordered)
        filtered: list[int] = []
        for rank, (idx, _) in enumerate(ordered, start=1):
            bucket = _bucket_for_rank(rank, total, rank_filter.quantiles)
            keep = False
            if rank_filter.mode == "top_n":
                keep = rank <= rank_filter.top_n
            elif rank_filter.mode == "quantile":
                keep = bucket in rank_filter.bucket_values
            else:
                raise ValueError(f"Unsupported rank filter mode: {rank_filter.mode}")
            if keep:
                filtered.append(idx)
        candidates = filtered

    ordered = sorted(
        [(idx, sort_values[idx]) for idx in candidates],
        key=lambda item: item[1],
        reverse=not spec.ascending,
    )
    total = len(ordered)
    selected: list[dict[str, str]] = []
    for rank, (idx, value) in enumerate(ordered, start=1):
        bucket = _bucket_for_rank(rank, total, spec.quantiles)
        keep = False
        if spec.mode == "top_n":
            keep = rank <= spec.top_n
        elif spec.mode == "quantile":
            keep = bucket in spec.bucket_values
        else:
            raise ValueError(f"Unsupported universe mode: {spec.mode}")
        if not keep:
            continue

        row = rows[idx]
        selected.append(
            {
                "date_utc": row["date_utc"],
                "date_kst": row["date_kst"],
                "market": row["market"],
                "korean_name": row["korean_name"],
                "english_name": row["english_name"],
                "market_warning": row["market_warning"],
                "feature_column": sort_column,
                "feature_value": f"{value:.12g}",
                "rank": str(rank),
                "bucket": str(bucket),
                "cross_section_size": str(total),
            }
        )
    for selected_rank, row in enumerate(selected, start=1):
        row["selected_rank"] = str(selected_rank)
        row["universe_name"] = spec.resolved_name()
    return selected


def build_universe_table(
    feature_rows: Iterable[dict[str, str]],
    spec: UniverseSpec,
) -> list[dict[str, str]]:
    if spec.mode not in UNIVERSE_MODES:
        raise ValueError(f"Unsupported universe mode: {spec.mode}")
    if spec.signal_lag < 0:
        raise ValueError("signal_lag must be >= 0")
    if spec.min_cross_section_size < 0:
        raise ValueError("min_cross_section_size must be >= 0")
    for filter_spec in spec.value_filters:
        if filter_spec.operator not in FILTER_OPERATORS:
            raise ValueError(f"Unsupported filter operator: {filter_spec.operator}")
    for rank_filter in spec.rank_filters:
        if rank_filter.mode not in UNIVERSE_MODES:
            raise ValueError(f"Unsupported rank filter mode: {rank_filter.mode}")

    rows = sorted(feature_rows, key=lambda row: (row["date_utc"], row["market"]))
    if not rows:
        return []
    effective_sort_lag = spec.lag + spec.signal_lag
    effective_value_filters = tuple(
        ValueFilterSpec(
            feature_column=filter_spec.feature_column,
            operator=filter_spec.operator,
            value=filter_spec.value,
            lag=filter_spec.lag + spec.signal_lag,
        )
        for filter_spec in spec.value_filters
    )
    effective_rank_filters = tuple(
        RankFilterSpec(
            feature_column=rank_filter.feature_column,
            mode=rank_filter.mode,
            lag=rank_filter.lag + spec.signal_lag,
            top_n=rank_filter.top_n,
            quantiles=rank_filter.quantiles,
            bucket_values=rank_filter.bucket_values,
            ascending=rank_filter.ascending,
        )
        for rank_filter in spec.rank_filters
    )
    effective_spec = UniverseSpec(
        feature_column=spec.feature_column,
        sort_column=spec.sort_column,
        lag=effective_sort_lag,
        signal_lag=spec.signal_lag,
        mode=spec.mode,
        top_n=spec.top_n,
        quantiles=spec.quantiles,
        bucket_values=spec.bucket_values,
        ascending=spec.ascending,
        exclude_warnings=spec.exclude_warnings,
        min_age_days=spec.min_age_days,
        min_cross_section_size=spec.min_cross_section_size,
        allowed_markets=spec.allowed_markets,
        excluded_markets=spec.excluded_markets,
        value_filters=effective_value_filters,
        rank_filters=effective_rank_filters,
        name=spec.name,
    )

    required_pairs = {
        (effective_spec.sort_column or effective_spec.feature_column, effective_spec.lag),
        *[(filter_spec.feature_column, filter_spec.lag) for filter_spec in effective_spec.value_filters],
        *[(rank_filter.feature_column, rank_filter.lag) for rank_filter in effective_spec.rank_filters],
    }
    for feature_column, _ in required_pairs:
        if feature_column not in rows[0]:
            raise ValueError(f"Unknown feature column: {feature_column}")

    lagged_value_map = {
        (feature_column, lag): _lag_feature_values(rows, feature_column, lag)
        for feature_column, lag in required_pairs
    }
    selected_rows: list[dict[str, str]] = []
    for indexes in _date_groups(rows):
        selected_rows.extend(_selected_rows_for_date(rows, lagged_value_map, indexes, effective_spec))
    return selected_rows


def universe_columns() -> list[str]:
    return [
        "date_utc",
        "date_kst",
        "market",
        "korean_name",
        "english_name",
        "market_warning",
        "feature_column",
        "feature_value",
        "rank",
        "selected_rank",
        "bucket",
        "cross_section_size",
        "universe_name",
    ]
