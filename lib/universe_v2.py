from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

from lib.specs import RankFilterSpec, UniverseSpec, ValueFilterSpec


@dataclass(frozen=True)
class UniverseV2Result:
    selection_mask: pd.DataFrame
    base_eligible_mask: pd.DataFrame


def _bucket_for_rank(rank: int, size: int, quantiles: int) -> int:
    if size <= 0:
        raise ValueError("Cross-sectional size must be positive")
    return 1 + (((rank - 1) * quantiles) // size)


def _compare_frame(operator: str, left: pd.DataFrame, right: float) -> pd.DataFrame:
    if operator == "gt":
        return left.gt(right)
    if operator == "ge":
        return left.ge(right)
    if operator == "lt":
        return left.lt(right)
    if operator == "le":
        return left.le(right)
    if operator == "eq":
        return left.eq(right)
    if operator == "ne":
        return left.ne(right)
    raise ValueError(f"Unsupported filter operator: {operator}")


def _effective_universe_spec(spec: UniverseSpec) -> UniverseSpec:
    return UniverseSpec(
        feature_column=spec.feature_column,
        sort_column=spec.sort_column,
        lag=int(spec.lag or 0) + int(spec.signal_lag or 0),
        signal_lag=spec.signal_lag,
        start_min_cross_section_size=spec.start_min_cross_section_size,
        mode=spec.mode,
        top_n=spec.top_n,
        quantiles=spec.quantiles,
        bucket_values=spec.bucket_values,
        ascending=spec.ascending,
        exclude_warnings=spec.exclude_warnings,
        min_age_days=spec.min_age_days,
        allowed_markets=spec.allowed_markets,
        excluded_markets=spec.excluded_markets,
        value_filters=tuple(
            ValueFilterSpec(
                feature_column=item.feature_column,
                operator=item.operator,
                value=item.value,
                lag=int(item.lag or 0) + int(spec.signal_lag or 0),
            )
            for item in spec.value_filters
        ),
        rank_filters=tuple(
            RankFilterSpec(
                feature_column=item.feature_column,
                mode=item.mode,
                lag=int(item.lag or 0) + int(spec.signal_lag or 0),
                top_n=item.top_n,
                quantiles=item.quantiles,
                bucket_values=item.bucket_values,
                ascending=item.ascending,
            )
            for item in spec.rank_filters
        ),
        name=spec.name,
    )


def _shift_frame(frame: pd.DataFrame, lag: int) -> pd.DataFrame:
    if lag <= 0:
        return frame.copy()
    result: dict[str, pd.Series] = {}
    for column in frame.columns:
        valid = frame[column].dropna()
        result[column] = valid.shift(lag).reindex(frame.index)
    return pd.DataFrame(result, index=frame.index).sort_index(axis=1)


def _allowed_market_mask(columns: pd.Index, spec: UniverseSpec) -> pd.Series:
    mask = pd.Series(True, index=columns)
    if spec.allowed_markets:
        mask &= columns.to_series().isin(set(spec.allowed_markets))
    if spec.excluded_markets:
        mask &= ~columns.to_series().isin(set(spec.excluded_markets))
    return mask


def _apply_rank_filter(
    candidates: pd.DataFrame,
    feature_frame: pd.DataFrame,
    rank_filter: RankFilterSpec,
) -> pd.DataFrame:
    values = _shift_frame(feature_frame, rank_filter.lag)
    scoped = values.where(candidates)
    ranks = scoped.rank(axis=1, method="first", ascending=rank_filter.ascending)
    if rank_filter.mode == "top_n":
        return ranks.le(float(rank_filter.top_n)).fillna(False)
    if rank_filter.mode == "quantile":
        counts = scoped.notna().sum(axis=1).astype(float)
        bucket = 1.0 + np.floor((ranks.sub(1.0)).mul(float(rank_filter.quantiles)).div(counts, axis=0))
        return bucket.isin([float(value) for value in rank_filter.bucket_values]).fillna(False)
    raise ValueError(f"Unsupported rank filter mode: {rank_filter.mode}")


def _apply_final_selection(
    candidates: pd.DataFrame,
    sort_frame: pd.DataFrame,
    spec: UniverseSpec,
) -> pd.DataFrame:
    scoped = sort_frame.where(candidates)
    ranks = scoped.rank(axis=1, method="first", ascending=spec.ascending)
    if spec.mode == "top_n":
        return ranks.le(float(spec.top_n)).fillna(False)
    if spec.mode == "quantile":
        counts = scoped.notna().sum(axis=1).astype(float)
        bucket = 1.0 + np.floor((ranks.sub(1.0)).mul(float(spec.quantiles)).div(counts, axis=0))
        return bucket.isin([float(value) for value in spec.bucket_values]).fillna(False)
    raise ValueError(f"Unsupported universe mode: {spec.mode}")


def build_universe_mask_v2(
    feature_frames: dict[str, pd.DataFrame],
    market_warning_frame: pd.DataFrame,
    spec: UniverseSpec,
) -> UniverseV2Result:
    effective_spec = _effective_universe_spec(spec)
    sort_column = effective_spec.sort_column or effective_spec.feature_column
    if sort_column not in feature_frames:
        raise ValueError(f"Missing sort feature for frame_v2 universe: {sort_column}")
    sort_frame = _shift_frame(feature_frames[sort_column], effective_spec.lag)
    columns = sort_frame.columns
    index = sort_frame.index

    market_mask = _allowed_market_mask(columns, effective_spec)
    market_mask_frame = pd.DataFrame([market_mask.to_numpy(dtype=bool)] * len(index), index=index, columns=columns)
    base = sort_frame.notna() & market_mask_frame

    if effective_spec.exclude_warnings and not market_warning_frame.empty:
        warning_frame = market_warning_frame.reindex(index=index, columns=columns)
        base &= warning_frame.fillna("NONE").eq("NONE")

    if effective_spec.min_age_days is not None:
        if "trade_price" not in feature_frames:
            raise ValueError("trade_price frame is required for min_age_days in frame_v2")
        age_frame = feature_frames["trade_price"].notna().cumsum().astype(float).where(feature_frames["trade_price"].notna())
        base &= age_frame.ge(float(effective_spec.min_age_days)).fillna(False)

    started = effective_spec.start_min_cross_section_size <= 0
    if not started:
        counts = base.sum(axis=1)
        if (counts >= effective_spec.start_min_cross_section_size).any():
            first_start = counts[counts >= effective_spec.start_min_cross_section_size].index[0]
            base.loc[base.index < first_start, :] = False
            started = True
        else:
            base.loc[:, :] = False

    candidates = base.copy()
    for value_filter in effective_spec.value_filters:
        if value_filter.feature_column not in feature_frames:
            raise ValueError(f"Missing value filter feature for frame_v2 universe: {value_filter.feature_column}")
        value_frame = _shift_frame(feature_frames[value_filter.feature_column], value_filter.lag)
        candidates &= _compare_frame(value_filter.operator, value_frame, float(value_filter.value)).fillna(False)

    for rank_filter in effective_spec.rank_filters:
        if rank_filter.feature_column not in feature_frames:
            raise ValueError(f"Missing rank filter feature for frame_v2 universe: {rank_filter.feature_column}")
        candidates = _apply_rank_filter(candidates, feature_frames[rank_filter.feature_column], rank_filter)

    selection = _apply_final_selection(candidates, sort_frame, effective_spec)
    return UniverseV2Result(selection_mask=selection, base_eligible_mask=base)
