from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from lib.upbit_collector import CandleRow


@dataclass(frozen=True)
class VectorBTSpec:
    price_column: str = "trade_price"
    init_cash: float = 1_000_000.0
    fees: float = 0.0
    slippage: float = 0.0
    cash_sharing: bool = True
    group_by: bool = True
    size_type: str = "targetpercent"
    call_seq: str = "auto"


def _price_value(row: CandleRow, price_column: str) -> float:
    if price_column not in CandleRow.__dataclass_fields__:
        raise ValueError(f"Unsupported CandleRow price column: {price_column}")
    return float(getattr(row, price_column))


def build_price_frame(
    rows: Iterable[CandleRow],
    price_column: str = "trade_price",
) -> pd.DataFrame:
    records = [
        {
            "date_utc": row.date_utc,
            "market": row.market,
            "price": _price_value(row, price_column),
        }
        for row in rows
    ]
    if not records:
        return pd.DataFrame()

    frame = pd.DataFrame.from_records(records)
    price_frame = (
        frame.pivot(index="date_utc", columns="market", values="price")
        .sort_index()
        .sort_index(axis=1)
    )
    price_frame.index = pd.to_datetime(price_frame.index, utc=False)
    return price_frame


def build_target_weight_frame(
    weight_rows: Iterable[dict[str, str]],
    price_frame: pd.DataFrame,
) -> pd.DataFrame:
    if price_frame.empty:
        return pd.DataFrame()

    target_frame = pd.DataFrame(float("nan"), index=price_frame.index, columns=price_frame.columns)
    rows = list(weight_rows)
    if not rows:
        return target_frame

    rebalance_frequency = rows[0].get("rebalance_frequency", "daily")
    rebalance_dates = _scheduled_rebalance_dates(price_frame.index, rebalance_frequency)
    grouped_rows: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        grouped_rows.setdefault(row["date_utc"], []).append(row)

    for date_utc in rebalance_dates:
        timestamp = pd.Timestamp(date_utc)
        if timestamp not in target_frame.index:
            continue
        # On rebalance dates, explicit zeros close positions not selected that day.
        target_frame.loc[timestamp, :] = 0.0
        for row in grouped_rows.get(date_utc, []):
            market = row["market"]
            if market not in target_frame.columns:
                continue
            target_frame.loc[timestamp, market] = float(row["target_weight"])

    return target_frame


def _scheduled_rebalance_dates(index: pd.Index, rebalance_frequency: str) -> list[str]:
    timestamps = [pd.Timestamp(value) for value in index]
    if rebalance_frequency == "daily":
        return [timestamp.isoformat() for timestamp in timestamps]

    keys: list[tuple[int, ...]] = []
    if rebalance_frequency == "weekly":
        keys = [timestamp.isocalendar()[:2] for timestamp in timestamps]
    elif rebalance_frequency == "monthly":
        keys = [(timestamp.year, timestamp.month) for timestamp in timestamps]
    else:
        raise ValueError(f"Unsupported rebalance frequency: {rebalance_frequency}")

    chosen: list[str] = []
    seen: set[tuple[int, ...]] = set()
    for timestamp, key in zip(timestamps, keys):
        if key in seen:
            continue
        seen.add(key)
        chosen.append(timestamp.isoformat())
    return chosen


def run_portfolio_from_target_weights(
    price_frame: pd.DataFrame,
    target_weight_frame: pd.DataFrame,
    spec: VectorBTSpec | None = None,
):
    if spec is None:
        spec = VectorBTSpec()

    try:
        import vectorbt as vbt
    except ModuleNotFoundError as exc:
        raise RuntimeError("vectorbt is not installed") from exc

    if price_frame.empty:
        raise ValueError("price_frame is empty")
    if target_weight_frame.empty:
        raise ValueError("target_weight_frame is empty")

    aligned_weights = target_weight_frame.reindex(index=price_frame.index, columns=price_frame.columns)
    return vbt.Portfolio.from_orders(
        close=price_frame,
        size=aligned_weights,
        size_type=spec.size_type,
        init_cash=spec.init_cash,
        fees=spec.fees,
        slippage=spec.slippage,
        cash_sharing=spec.cash_sharing,
        group_by=True if spec.group_by else False,
        call_seq=spec.call_seq,
    )
