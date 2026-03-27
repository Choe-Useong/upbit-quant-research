from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from lib.dataframes import read_wide_frames_from_cache
from lib.specs import FeatureSpec


SUPPORTED_SOURCE_COLUMNS = {
    "trade_price",
    "opening_price",
    "high_price",
    "low_price",
    "candle_acc_trade_volume",
    "candle_acc_trade_price",
    "timestamp",
}

SUPPORTED_TRANSFORMS = {
    "rolling_mean",
    "rolling_sum",
    "momentum",
    "simple_return",
    "delta",
    "ewma",
    "age_days",
    "cross_rank",
    "cross_percentile",
}


def _apply_per_market_tail(
    frames: dict[str, pd.DataFrame],
    tail_rows: int | None,
) -> dict[str, pd.DataFrame]:
    if tail_rows is None or tail_rows <= 0 or not frames:
        return frames
    primary_name = "trade_price" if "trade_price" in frames else next(iter(frames.keys()))
    primary = frames[primary_name].copy()
    for column in primary.columns:
        valid_index = primary[column].dropna().index
        if len(valid_index) <= tail_rows:
            continue
        trimmed_index = valid_index[:-tail_rows]
        primary.loc[trimmed_index, column] = np.nan
    keep_index = primary.notna().any(axis=1)
    updated: dict[str, pd.DataFrame] = {}
    for name, frame in frames.items():
        next_frame = frame.copy()
        if not frame.columns.equals(primary.columns):
            next_frame = next_frame.reindex(columns=primary.columns)
        for column in primary.columns:
            valid_index = frame[column].dropna().index
            if len(valid_index) <= tail_rows:
                continue
            trimmed_index = valid_index[:-tail_rows]
            next_frame.loc[trimmed_index, column] = np.nan
        updated[name] = next_frame.loc[keep_index].copy()
    return updated


def _ewma_series(series: pd.Series, window: int) -> pd.Series:
    if window <= 0:
        raise ValueError("window must be positive")
    result = pd.Series(np.nan, index=series.index, dtype=float)
    valid = series.dropna()
    if len(valid) < window:
        return result

    alpha = 2.0 / (window + 1.0)
    seed_index = valid.index[:window]
    current = float(valid.iloc[:window].mean())
    result.loc[seed_index[-1]] = current

    for timestamp in valid.index[window:]:
        current = (alpha * float(valid.at[timestamp])) + ((1.0 - alpha) * current)
        result.loc[timestamp] = current
    return result


def _age_days_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.notna().cumsum().astype(float)
    return result.where(frame.notna())


def _apply_by_market_column(
    frame: pd.DataFrame,
    transform,
) -> pd.DataFrame:
    result: dict[str, pd.Series] = {}
    for column in frame.columns:
        valid = frame[column].dropna().astype(float)
        transformed = transform(valid)
        result[column] = transformed.reindex(frame.index)
    return pd.DataFrame(result, index=frame.index).sort_index(axis=1)


def _delta_frame(frame: pd.DataFrame, periods: int) -> pd.DataFrame:
    return _apply_by_market_column(frame, lambda series: series - series.shift(periods))


def _simple_return_frame(frame: pd.DataFrame, periods: int) -> pd.DataFrame:
    return _apply_by_market_column(frame, lambda series: (series / series.shift(periods)) - 1.0)


def _momentum_frame(frame: pd.DataFrame, periods: int) -> pd.DataFrame:
    def _transform(series: pd.Series) -> pd.Series:
        positive = series.where(series > 0.0)
        return np.log(positive) - np.log(positive.shift(periods))

    return _apply_by_market_column(frame, _transform)


def _rolling_mean_frame(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return _apply_by_market_column(frame, lambda series: _rolling_mean_series_fast(series, window))


def _rolling_sum_frame(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return _apply_by_market_column(frame, lambda series: _rolling_sum_series_fast(series, window))


def _rolling_sum_series_fast(series: pd.Series, window: int) -> pd.Series:
    if window <= 0:
        raise ValueError("window must be positive")
    result = pd.Series(np.nan, index=series.index, dtype=float)
    if len(series) < window:
        return result
    values = series.to_numpy(dtype=float, copy=False)
    cumsum = np.cumsum(np.insert(values, 0, 0.0))
    sums = cumsum[window:] - cumsum[:-window]
    result.iloc[window - 1 :] = sums
    return result


def _rolling_mean_series_fast(series: pd.Series, window: int) -> pd.Series:
    result = _rolling_sum_series_fast(series, window)
    if result.notna().any():
        result = result / float(window)
    return result


def _ewma_frame(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return _apply_by_market_column(frame, lambda series: _ewma_series(series.astype(float), window))


def _cross_rank_base(frame: pd.DataFrame, descending: bool) -> pd.DataFrame:
    return frame.rank(axis=1, method="first", ascending=not descending).astype(float)


def _cross_rank_frame(frame: pd.DataFrame, descending: bool) -> pd.DataFrame:
    return _cross_rank_base(frame, descending)


def _cross_percentile_frame(frame: pd.DataFrame, descending: bool) -> pd.DataFrame:
    ranks = _cross_rank_base(frame, descending)
    counts = frame.notna().sum(axis=1).astype(float)
    percentile = (ranks.sub(1.0)).div(counts.sub(1.0), axis=0)
    single_mask = counts.eq(1.0)
    if bool(single_mask.any()):
        percentile.loc[single_mask, :] = ranks.loc[single_mask, :].where(ranks.loc[single_mask, :].isna(), 1.0)
    return percentile.where(frame.notna())


def _bucket_frame(ranks: pd.DataFrame, counts: pd.Series, quantiles: int) -> pd.DataFrame:
    bucket = 1.0 + np.floor((ranks.sub(1.0)).mul(float(quantiles)).div(counts, axis=0))
    return bucket.where(ranks.notna())


def _apply_transform(frame: pd.DataFrame, kind: str, params: dict[str, int | float | str]) -> pd.DataFrame:
    if kind not in SUPPORTED_TRANSFORMS:
        raise ValueError(f"Unsupported frame_v2 transform: {kind}")
    if kind == "rolling_mean":
        return _rolling_mean_frame(frame, int(params["window"]))
    if kind == "rolling_sum":
        return _rolling_sum_frame(frame, int(params["window"]))
    if kind == "momentum":
        return _momentum_frame(frame, int(params["window"]))
    if kind == "simple_return":
        return _simple_return_frame(frame, int(params["window"]))
    if kind == "delta":
        return _delta_frame(frame, int(params.get("periods", 1)))
    if kind == "ewma":
        return _ewma_frame(frame, int(params["window"]))
    if kind == "age_days":
        return _age_days_frame(frame)
    if kind == "cross_rank":
        return _cross_rank_frame(frame, bool(params.get("descending", True)))
    if kind == "cross_percentile":
        return _cross_percentile_frame(frame, bool(params.get("descending", True)))
    raise ValueError(f"Unsupported frame_v2 transform: {kind}")


def _compare_frames(
    left: pd.DataFrame,
    operator: str,
    right: pd.DataFrame | float,
) -> pd.DataFrame:
    left = left.astype(float)
    if isinstance(right, pd.DataFrame):
        right_frame = right.astype(float)
        mask = left.notna() & right_frame.notna()
        if operator == "gt":
            result = left.gt(right_frame)
        elif operator == "ge":
            result = left.ge(right_frame)
        elif operator == "lt":
            result = left.lt(right_frame)
        elif operator == "le":
            result = left.le(right_frame)
        elif operator == "eq":
            result = left.eq(right_frame)
        elif operator == "ne":
            result = left.ne(right_frame)
        else:
            raise ValueError(f"Unsupported compare operator: {operator}")
        return result.astype(float).where(mask)

    mask = left.notna()
    scalar = float(right)
    if operator == "gt":
        result = left.gt(scalar)
    elif operator == "ge":
        result = left.ge(scalar)
    elif operator == "lt":
        result = left.lt(scalar)
    elif operator == "le":
        result = left.le(scalar)
    elif operator == "eq":
        result = left.eq(scalar)
    elif operator == "ne":
        result = left.ne(scalar)
    else:
        raise ValueError(f"Unsupported compare operator: {operator}")
    return result.astype(float).where(mask)


def build_feature_frames_from_cache(
    cache_dir: Path,
    feature_specs: Sequence[FeatureSpec],
    *,
    market_columns: Sequence[str] | None = None,
    max_markets: int | None = None,
    tail_rows: int | None = None,
    source_frames: dict[str, pd.DataFrame] | None = None,
    frame_cache: dict[tuple[Any, ...], pd.DataFrame] | None = None,
    frame_cache_namespace: tuple[Any, ...] | None = None,
) -> dict[str, pd.DataFrame]:
    from lib.feature_graph_v2 import build_feature_frames_from_cache_graph

    return build_feature_frames_from_cache_graph(
        cache_dir,
        feature_specs,
        market_columns=market_columns,
        max_markets=max_markets,
        tail_rows=tail_rows,
        source_frames=source_frames,
        frame_cache=frame_cache,
        frame_cache_namespace=frame_cache_namespace,
    )
