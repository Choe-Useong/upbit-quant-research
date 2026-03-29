from __future__ import annotations

import numpy as np
import pandas as pd

from lib.specs import WeightSpec


def _rebalance_mask(index: pd.Index, frequency: str) -> pd.Series:
    timestamps = pd.to_datetime(index)
    if frequency == "every_bar":
        return pd.Series(True, index=index)
    if frequency == "daily":
        keys = pd.Index([ts.strftime("%Y-%m-%d") for ts in timestamps], dtype="object")
    elif frequency == "weekly":
        keys = pd.Index([f"{ts.isocalendar().year}-{ts.isocalendar().week}" for ts in timestamps], dtype="object")
    elif frequency == "monthly":
        keys = pd.Index([f"{ts.year}-{ts.month:02d}" for ts in timestamps], dtype="object")
    else:
        raise ValueError(f"Unsupported rebalance frequency for frame_v2: {frequency}")
    return pd.Series(~keys.duplicated(), index=index)


def _exposure_scale_series(
    selection_mask: pd.DataFrame,
    spec: WeightSpec,
    feature_frames: dict[str, pd.DataFrame] | None,
) -> pd.Series | None:
    feature_name = spec.gross_exposure_feature
    if not feature_name:
        return None
    if feature_frames is None or feature_name not in feature_frames:
        raise ValueError(f"Missing gross exposure feature for weights_v2: {feature_name}")

    frame = feature_frames[feature_name].copy()
    frame = frame.reindex(selection_mask.index)

    if frame.shape[1] == 0:
        raise ValueError(f"Gross exposure feature has no columns for weights_v2: {feature_name}")

    if frame.shape[1] == 1:
        scalar = frame.iloc[:, 0].astype(float)
    else:
        values = frame.astype(float)
        row_min = values.min(axis=1, skipna=True)
        row_max = values.max(axis=1, skipna=True)
        inconsistent = row_min.notna() & row_max.notna() & ~np.isclose(row_min, row_max, atol=1e-12, rtol=0.0)
        if bool(inconsistent.any()):
            first_bad = str(inconsistent[inconsistent].index[0])
            raise ValueError(
                f"gross_exposure_feature must be scalar/broadcast across markets for weights_v2: "
                f"{feature_name} at {first_bad}"
            )
        scalar = values.bfill(axis=1).iloc[:, 0]

    lag = int(spec.gross_exposure_lag)
    if lag > 0:
        scalar = scalar.shift(lag)
    elif lag < 0:
        raise ValueError("gross_exposure_lag must be non-negative for weights_v2")

    clip_min = float(spec.gross_exposure_clip_min)
    clip_max = float(spec.gross_exposure_clip_max)
    if clip_min > clip_max:
        raise ValueError("gross_exposure_clip_min must be <= gross_exposure_clip_max")

    return scalar.astype(float).clip(lower=clip_min, upper=clip_max).fillna(0.0)


def build_weight_frame_v2(
    selection_mask: pd.DataFrame,
    spec: WeightSpec,
    feature_frames: dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    rebalance_dates = _rebalance_mask(selection_mask.index, spec.rebalance_frequency)
    selected = selection_mask.fillna(False).astype(bool)
    exposure_scale = _exposure_scale_series(selection_mask, spec, feature_frames)

    if spec.weighting == "equal":
        gross_exposure = float(spec.gross_exposure)
        if spec.rebalance_frequency == "every_bar":
            counts = selected.sum(axis=1).replace(0, np.nan)
            target = selected.astype(float).mul(gross_exposure).div(counts, axis=0)
            if exposure_scale is not None:
                target = target.mul(exposure_scale, axis=0)
            return target.fillna(0.0)

        rebalance_index = selection_mask.index[rebalance_dates]
        target = pd.DataFrame(float("nan"), index=selection_mask.index, columns=selection_mask.columns)
        if len(rebalance_index) == 0:
            return target

        rebalance_selected = selected.loc[rebalance_index]
        counts = rebalance_selected.sum(axis=1).replace(0, np.nan)
        rebalance_target = rebalance_selected.astype(float).mul(gross_exposure).div(counts, axis=0).fillna(0.0)
        if exposure_scale is not None:
            rebalance_target = rebalance_target.mul(exposure_scale.loc[rebalance_index], axis=0)
        target.loc[rebalance_index, :] = rebalance_target
        return target

    if spec.weighting == "fixed":
        if spec.fixed_weight is None:
            raise ValueError("fixed_weight must be provided for fixed weighting")
        fixed_weight = float(spec.fixed_weight)
        if spec.rebalance_frequency == "every_bar":
            target = selected.astype(float).mul(fixed_weight)
            if exposure_scale is not None:
                target = target.mul(exposure_scale, axis=0)
            return target

        rebalance_index = selection_mask.index[rebalance_dates]
        target = pd.DataFrame(float("nan"), index=selection_mask.index, columns=selection_mask.columns)
        if len(rebalance_index) == 0:
            return target

        rebalance_selected = selected.loc[rebalance_index]
        rebalance_target = rebalance_selected.astype(float).mul(fixed_weight)
        if exposure_scale is not None:
            rebalance_target = rebalance_target.mul(exposure_scale.loc[rebalance_index], axis=0)
        target.loc[rebalance_index, :] = rebalance_target
        return target

    raise ValueError("frame_v2 currently supports equal and fixed weighting only")
