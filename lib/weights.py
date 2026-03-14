from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable


WEIGHTING_METHODS = {"equal", "rank"}
REBALANCE_FREQUENCIES = {"daily", "weekly", "monthly"}


@dataclass(frozen=True)
class WeightSpec:
    weighting: str = "equal"
    gross_exposure: float = 1.0
    rank_power: float = 1.0
    max_positions: int | None = None
    universe_name: str | None = None
    rebalance_frequency: str = "daily"

    def resolved_name(self) -> str:
        prefix = self.universe_name or "universe"
        if self.weighting == "equal":
            return (
                f"{prefix}__equal_{self.rebalance_frequency}"
                f"_gross{self.gross_exposure:g}"
            )
        return (
            f"{prefix}__rank_p{self.rank_power:g}_{self.rebalance_frequency}"
            f"_gross{self.gross_exposure:g}"
        )


def _date_groups(rows: list[dict[str, str]]) -> list[list[int]]:
    grouped: dict[str, list[int]] = defaultdict(list)
    for idx, row in enumerate(rows):
        grouped[row["date_utc"]].append(idx)
    return [
        sorted(indexes, key=lambda idx: int(rows[idx]["selected_rank"]))
        for _, indexes in sorted(grouped.items())
    ]


def _equal_weights(count: int, gross_exposure: float) -> list[float]:
    if count <= 0:
        return []
    value = gross_exposure / count
    return [value] * count


def _rank_weights(count: int, gross_exposure: float, rank_power: float) -> list[float]:
    if count <= 0:
        return []
    raw = [1.0 / (rank ** rank_power) for rank in range(1, count + 1)]
    total = sum(raw)
    return [(value / total) * gross_exposure for value in raw]


def _parse_date(date_utc: str) -> date:
    return datetime.fromisoformat(date_utc).date()


def _period_key(date_utc: str, frequency: str) -> tuple[int, ...]:
    parsed = _parse_date(date_utc)
    if frequency == "daily":
        return (parsed.year, parsed.month, parsed.day)
    if frequency == "weekly":
        iso_year, iso_week, _ = parsed.isocalendar()
        return (iso_year, iso_week)
    if frequency == "monthly":
        return (parsed.year, parsed.month)
    raise ValueError(f"Unsupported rebalance frequency: {frequency}")


def _selected_rebalance_dates(rows: list[dict[str, str]], frequency: str) -> set[str]:
    chosen: dict[tuple[int, ...], str] = {}
    for row in rows:
        date_utc = row["date_utc"]
        key = _period_key(date_utc, frequency)
        if key not in chosen:
            chosen[key] = date_utc
    return set(chosen.values())


def build_weight_table(
    universe_rows: Iterable[dict[str, str]],
    spec: WeightSpec,
) -> list[dict[str, str]]:
    if spec.weighting not in WEIGHTING_METHODS:
        raise ValueError(f"Unsupported weighting method: {spec.weighting}")
    if spec.rebalance_frequency not in REBALANCE_FREQUENCIES:
        raise ValueError(f"Unsupported rebalance frequency: {spec.rebalance_frequency}")

    rows = sorted(universe_rows, key=lambda row: (row["date_utc"], int(row["selected_rank"])))
    if not rows:
        return []

    active_dates = _selected_rebalance_dates(rows, spec.rebalance_frequency)
    weighted_rows: list[dict[str, str]] = []
    for indexes in _date_groups(rows):
        if rows[indexes[0]]["date_utc"] not in active_dates:
            continue
        scoped = indexes
        if spec.max_positions is not None:
            scoped = scoped[: spec.max_positions]
        count = len(scoped)
        if spec.weighting == "equal":
            weights = _equal_weights(count, spec.gross_exposure)
        else:
            weights = _rank_weights(count, spec.gross_exposure, spec.rank_power)

        for local_rank, (idx, weight) in enumerate(zip(scoped, weights), start=1):
            row = rows[idx]
            weighted_rows.append(
                {
                    "date_utc": row["date_utc"],
                    "date_kst": row["date_kst"],
                    "market": row["market"],
                    "korean_name": row["korean_name"],
                    "english_name": row["english_name"],
                    "market_warning": row["market_warning"],
                    "feature_column": row["feature_column"],
                    "feature_value": row["feature_value"],
                    "rank": row["rank"],
                    "selected_rank": row["selected_rank"],
                    "weight_rank": str(local_rank),
                    "target_weight": f"{weight:.12g}",
                    "gross_exposure": f"{spec.gross_exposure:.12g}",
                    "weighting": spec.weighting,
                    "rebalance_frequency": spec.rebalance_frequency,
                    "weights_name": spec.resolved_name(),
                    "universe_name": row["universe_name"],
                }
            )
    return weighted_rows


def weight_columns() -> list[str]:
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
        "weight_rank",
        "target_weight",
        "gross_exposure",
        "weighting",
        "rebalance_frequency",
        "weights_name",
        "universe_name",
    ]
