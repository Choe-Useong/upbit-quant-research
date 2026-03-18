from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable


WEIGHTING_METHODS = {"equal", "rank", "feature_value", "incremental_signal", "fixed"}
REBALANCE_FREQUENCIES = {"every_bar", "daily", "weekly", "monthly"}


@dataclass(frozen=True)
class WeightSpec:
    weighting: str = "equal"
    gross_exposure: float = 1.0
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
        if self.weighting == "equal":
            return (
                f"{prefix}__equal_{self.rebalance_frequency}"
                f"_gross{self.gross_exposure:g}"
            )
        if self.weighting == "feature_value":
            return (
                f"{prefix}__feature_value_{self.rebalance_frequency}"
                f"_gross{self.gross_exposure:g}"
            )
        if self.weighting == "fixed":
            if self.fixed_weight is None:
                raise ValueError("fixed_weight must be provided for fixed weighting")
            return (
                f"{prefix}__fixed_{self.rebalance_frequency}"
                f"_w{self.fixed_weight:g}"
            )
        if self.weighting == "incremental_signal":
            step_up = self.incremental_step_up if self.incremental_step_up is not None else self.incremental_step_size
            step_down = self.incremental_step_down if self.incremental_step_down is not None else self.incremental_step_size
            return (
                f"{prefix}__incremental_signal_{self.rebalance_frequency}"
                f"_up{step_up:g}_down{step_down:g}"
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


def _fixed_weights(count: int, fixed_weight: float | None) -> list[float]:
    if count <= 0:
        return []
    if fixed_weight is None:
        raise ValueError("fixed_weight must be provided for fixed weighting")
    return [fixed_weight] * count


def _rank_weights(count: int, gross_exposure: float, rank_power: float) -> list[float]:
    if count <= 0:
        return []
    raw = [1.0 / (rank ** rank_power) for rank in range(1, count + 1)]
    total = sum(raw)
    return [(value / total) * gross_exposure for value in raw]


def _clip(value: float, minimum: float, maximum: float) -> float:
    return min(max(value, minimum), maximum)


def _feature_value_weights(
    scoped_rows: list[dict[str, str]],
    gross_exposure: float,
    scale: float,
    clip_min: float,
    clip_max: float,
) -> list[tuple[int, float]]:
    if not scoped_rows:
        return []
    if scale <= 0:
        raise ValueError("feature_value_scale must be positive")

    all_scores: list[tuple[int, float]] = []
    for idx, row in enumerate(scoped_rows):
        raw_value = float(row["feature_value"]) / scale
        score = _clip(raw_value, clip_min, clip_max)
        all_scores.append((idx, score))

    scored_rows = [(idx, score) for idx, score in all_scores if score > 0]
    active_count = len(scored_rows)
    if active_count <= 0:
        return [(idx, 0.0) for idx, _ in all_scores]

    active_weights = {
        idx: gross_exposure * (score / active_count)
        for idx, score in scored_rows
    }
    return [
        (idx, active_weights.get(idx, 0.0))
        for idx, _ in all_scores
    ]


def _scaled_feature_score(
    row: dict[str, str],
    scale: float,
    clip_min: float,
    clip_max: float,
) -> float:
    raw_value = float(row["feature_value"]) / scale
    return _clip(raw_value, clip_min, clip_max)


def _incremental_signal_weight_rows(
    rows: list[dict[str, str]],
    spec: WeightSpec,
) -> list[dict[str, str]]:
    step_up = spec.incremental_step_up if spec.incremental_step_up is not None else spec.incremental_step_size
    step_down = spec.incremental_step_down if spec.incremental_step_down is not None else spec.incremental_step_size
    if step_up <= 0:
        raise ValueError("incremental_step_up must be positive")
    if step_down <= 0:
        raise ValueError("incremental_step_down must be positive")
    if spec.incremental_min_weight > spec.incremental_max_weight:
        raise ValueError("incremental_min_weight must be <= incremental_max_weight")
    if spec.feature_value_scale <= 0:
        raise ValueError("feature_value_scale must be positive")

    active_dates = _selected_rebalance_dates(rows, spec.rebalance_frequency)
    grouped_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped_rows[row["date_utc"]].append(row)

    market_reference: dict[str, dict[str, str]] = {}
    current_scores: dict[str, float] = {}
    weighted_rows: list[dict[str, str]] = []

    for date_utc in sorted(active_dates):
        date_rows = sorted(grouped_rows.get(date_utc, []), key=lambda row: int(row["selected_rank"]))
        if spec.max_positions is not None:
            date_rows = date_rows[: spec.max_positions]

        row_by_market = {row["market"]: row for row in date_rows}
        for row in date_rows:
            market_reference[row["market"]] = row

        tracked_markets = sorted(set(market_reference.keys()) | set(current_scores.keys()) | set(row_by_market.keys()))
        next_weights: dict[str, float] = {}
        feature_values_by_market: dict[str, str] = {}
        feature_columns_by_market: dict[str, str] = {}

        for market in tracked_markets:
            current_score = current_scores.get(market, 0.0)
            row = row_by_market.get(market)
            signal_on = False
            if row is not None:
                signal_on = _scaled_feature_score(
                    row,
                    spec.feature_value_scale,
                    spec.feature_value_clip_min,
                    spec.feature_value_clip_max,
                ) > 0.0
                feature_values_by_market[market] = row["feature_value"]
                feature_columns_by_market[market] = row["feature_column"]
            else:
                feature_values_by_market[market] = "0"
                feature_columns_by_market[market] = market_reference[market].get("feature_column", "")

            if signal_on:
                next_score = min(current_score + step_up, spec.incremental_max_weight)
            else:
                next_score = max(current_score - step_down, spec.incremental_min_weight)
            next_weights[market] = next_score

        active_markets = [market for market, score in next_weights.items() if score > 0.0]
        active_count = len(active_markets)
        if active_count <= 0:
            current_scores = {market: score for market, score in next_weights.items() if score > 0.0}
            continue

        for market in tracked_markets:
            current_scores[market] = next_weights[market]
            scaled_weight = (spec.gross_exposure * next_weights[market]) / active_count
            if scaled_weight <= 0.0:
                continue

            reference_row = row_by_market.get(market) or market_reference[market]
            is_current_row = market in row_by_market
            weighted_rows.append(
                {
                    "date_utc": date_utc,
                    "date_kst": row_by_market[market]["date_kst"] if is_current_row else _utc_to_kst(date_utc),
                    "market": market,
                    "korean_name": reference_row["korean_name"],
                    "english_name": reference_row["english_name"],
                    "market_warning": reference_row["market_warning"],
                    "feature_column": feature_columns_by_market[market],
                    "feature_value": feature_values_by_market[market],
                    "rank": reference_row["rank"] if is_current_row else "",
                    "selected_rank": reference_row["selected_rank"] if is_current_row else "",
                    "weight_rank": "",
                    "target_weight": f"{scaled_weight:.12g}",
                    "gross_exposure": f"{spec.gross_exposure:.12g}",
                    "weighting": spec.weighting,
                    "rebalance_frequency": spec.rebalance_frequency,
                    "weights_name": spec.resolved_name(),
                    "universe_name": reference_row["universe_name"],
                }
            )

    return weighted_rows


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


def _utc_to_kst(date_utc: str) -> str:
    return (datetime.fromisoformat(date_utc) + timedelta(hours=9)).isoformat()


def _selected_rebalance_dates(rows: list[dict[str, str]], frequency: str) -> set[str]:
    if frequency == "every_bar":
        return {row["date_utc"] for row in rows}

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
    if spec.weighting == "incremental_signal":
        return _incremental_signal_weight_rows(rows, spec)

    active_dates = _selected_rebalance_dates(rows, spec.rebalance_frequency)
    weighted_rows: list[dict[str, str]] = []
    for indexes in _date_groups(rows):
        if rows[indexes[0]]["date_utc"] not in active_dates:
            continue
        scoped = indexes
        if spec.max_positions is not None:
            scoped = scoped[: spec.max_positions]
        scoped_rows = [rows[idx] for idx in scoped]
        count = len(scoped_rows)
        if spec.weighting == "equal":
            weights = _equal_weights(count, spec.gross_exposure)
            weighted_pairs = list(zip(range(count), weights))
        elif spec.weighting == "fixed":
            weights = _fixed_weights(count, spec.fixed_weight)
            weighted_pairs = list(zip(range(count), weights))
        elif spec.weighting == "rank":
            weights = _rank_weights(count, spec.gross_exposure, spec.rank_power)
            weighted_pairs = list(zip(range(count), weights))
        else:
            weighted_pairs = _feature_value_weights(
                scoped_rows,
                spec.gross_exposure,
                spec.feature_value_scale,
                spec.feature_value_clip_min,
                spec.feature_value_clip_max,
            )

        for local_rank, (scoped_idx, weight) in enumerate(weighted_pairs, start=1):
            row = scoped_rows[scoped_idx]
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
