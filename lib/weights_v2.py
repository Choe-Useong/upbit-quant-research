from __future__ import annotations

import pandas as pd

from lib.weights import WeightSpec


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
    if spec.weighting != "equal":
        raise ValueError("frame_v2 currently supports equal weighting only")

    target = pd.DataFrame(float("nan"), index=selection_mask.index, columns=selection_mask.columns)
    rebalance_dates = _rebalance_mask(selection_mask.index, spec.rebalance_frequency)
    gross_exposure = float(spec.gross_exposure)

    for timestamp in selection_mask.index[rebalance_dates]:
        chosen = selection_mask.loc[timestamp].fillna(False)
        target.loc[timestamp, :] = 0.0
        active_columns = list(chosen.index[chosen])
        if not active_columns:
            continue
        weight = gross_exposure / len(active_columns)
        target.loc[timestamp, active_columns] = weight

    return target
