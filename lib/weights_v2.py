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


def build_weight_frame_v2(
    selection_mask: pd.DataFrame,
    spec: WeightSpec,
) -> pd.DataFrame:
    rebalance_dates = _rebalance_mask(selection_mask.index, spec.rebalance_frequency)
    selected = selection_mask.fillna(False).astype(bool)

    if spec.weighting == "equal":
        gross_exposure = float(spec.gross_exposure)
        if spec.rebalance_frequency == "every_bar":
            counts = selected.sum(axis=1).replace(0, np.nan)
            target = selected.astype(float).mul(gross_exposure).div(counts, axis=0)
            return target.fillna(0.0)

        rebalance_index = selection_mask.index[rebalance_dates]
        target = pd.DataFrame(float("nan"), index=selection_mask.index, columns=selection_mask.columns)
        if len(rebalance_index) == 0:
            return target

        rebalance_selected = selected.loc[rebalance_index]
        counts = rebalance_selected.sum(axis=1).replace(0, np.nan)
        rebalance_target = rebalance_selected.astype(float).mul(gross_exposure).div(counts, axis=0).fillna(0.0)
        target.loc[rebalance_index, :] = rebalance_target
        return target

    if spec.weighting == "fixed":
        if spec.fixed_weight is None:
            raise ValueError("fixed_weight must be provided for fixed weighting")
        fixed_weight = float(spec.fixed_weight)
        if spec.rebalance_frequency == "every_bar":
            return selected.astype(float).mul(fixed_weight)

        rebalance_index = selection_mask.index[rebalance_dates]
        target = pd.DataFrame(float("nan"), index=selection_mask.index, columns=selection_mask.columns)
        if len(rebalance_index) == 0:
            return target

        rebalance_selected = selected.loc[rebalance_index]
        rebalance_target = rebalance_selected.astype(float).mul(fixed_weight)
        target.loc[rebalance_index, :] = rebalance_target
        return target

    raise ValueError("frame_v2 currently supports equal and fixed weighting only")
