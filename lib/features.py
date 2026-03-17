from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Iterable, Sequence

from lib.upbit_collector import CandleRow


TransformFn = Callable[[list[dict[str, str]], list[float | None], dict[str, int | float | str]], list[float | None]]
COMPARE_OPERATORS = {"gt", "ge", "lt", "le", "eq", "ne"}
LOGICAL_OPERATORS = {"and", "or", "not"}


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
class FeatureSpec:
    source: str | None = None
    steps: tuple[TransformSpec, ...] = ()
    components: tuple[ScoreComponentSpec, ...] = ()
    combine: str | None = None
    compare: CompareSpec | None = None
    logical: LogicalSpec | None = None
    state: StateSpec | None = None
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
        if self.components:
            suffix = "__".join(
                f"{component.feature_column}_w{component.weight:g}"
                for component in self.components
            )
            prefix = self.combine or "weighted_sum"
            return f"{prefix}__{suffix}"
        if not self.steps:
            if self.source is None:
                raise ValueError("FeatureSpec must define source, components, compare, logical, or state")
            return self.source
        suffix = "__".join(step.resolved_name() for step in self.steps)
        if self.source is None:
            raise ValueError("FeatureSpec with steps must define source")
        return f"{self.source}__{suffix}"


BASE_COLUMNS = [
    "date_utc",
    "date_kst",
    "market",
    "korean_name",
    "english_name",
    "market_warning",
    "opening_price",
    "high_price",
    "low_price",
    "trade_price",
    "candle_acc_trade_volume",
    "candle_acc_trade_price",
    "timestamp",
]


def _parse_float(value: str) -> float | None:
    if value == "":
        return None
    parsed = float(value)
    if math.isnan(parsed):
        return None
    return parsed


def _format_value(value: float | int | None) -> str:
    if value is None:
        return ""
    if isinstance(value, int):
        return str(value)
    if math.isnan(value):
        return ""
    return f"{value:.12g}"


def _date_key(date_utc: str) -> datetime:
    return datetime.strptime(date_utc, "%Y-%m-%dT%H:%M:%S")


def _values_from_column(rows: list[dict[str, str]], column: str) -> list[float | None]:
    return [_parse_float(row.get(column, "")) for row in rows]


def _weighted_sum(values_by_component: list[list[float | None]], weights: list[float]) -> list[float | None]:
    if not values_by_component:
        return []
    length = len(values_by_component[0])
    result: list[float | None] = [None] * length
    for idx in range(length):
        total = 0.0
        for component_values, weight in zip(values_by_component, weights):
            value = component_values[idx]
            if value is None:
                result[idx] = None
                break
            total += value * weight
        else:
            result[idx] = total
    return result


def _compare_value(operator: str, left: float, right: float) -> bool:
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
    raise ValueError(f"Unsupported compare operator: {operator}")


def _resolve_source_values(rows: list[dict[str, str]], source: str) -> list[float | None]:
    if source.startswith("market:"):
        _, market_code, column = source.split(":", 2)
        reference_by_date = {
            row["date_utc"]: _parse_float(row.get(column, ""))
            for row in rows
            if row["market"] == market_code
        }
        return [reference_by_date.get(row["date_utc"]) for row in rows]
    return _values_from_column(rows, source)


def _source_exists(rows: list[dict[str, str]], source: str) -> bool:
    if source.startswith("market:"):
        _, market_code, column = source.split(":", 2)
        return any(row["market"] == market_code and column in row for row in rows)
    return source in rows[0]


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


def _period_value(date_utc: str, freq: str) -> tuple[int, int]:
    dt = _date_key(date_utc)
    normalized = freq.upper()
    if normalized == "M":
        return (dt.year, dt.month)
    if normalized == "W":
        iso = dt.isocalendar()
        return (iso.year, iso.week)
    raise ValueError(f"Unsupported frequency: {freq}")


def _market_period_groups(rows: list[dict[str, str]], freq: str) -> list[list[int]]:
    grouped: dict[tuple[str, tuple[int, int]], list[int]] = defaultdict(list)
    for idx, row in enumerate(rows):
        grouped[(row["market"], _period_value(row["date_utc"], freq))].append(idx)
    return [
        sorted(indexes, key=lambda idx: rows[idx]["date_utc"])
        for _, indexes in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1]))
    ]


def _rolling_mean(values: Sequence[float | None], window: int) -> list[float | None]:
    result: list[float | None] = []
    acc = 0.0
    missing = 0
    for idx, value in enumerate(values):
        if value is None:
            missing += 1
        else:
            acc += value
        if idx >= window:
            old = values[idx - window]
            if old is None:
                missing -= 1
            else:
                acc -= old
        if idx + 1 < window or missing > 0:
            result.append(None)
        else:
            result.append(acc / window)
    return result


def _ewma(values: Sequence[float | None], window: int) -> list[float | None]:
    if window <= 0:
        raise ValueError("window must be positive")

    result: list[float | None] = [None] * len(values)
    if len(values) < window:
        return result

    seed = values[:window]
    if any(value is None for value in seed):
        return result

    alpha = 2.0 / (window + 1.0)
    current = sum(float(value) for value in seed if value is not None) / window
    result[window - 1] = current

    for idx in range(window, len(values)):
        value = values[idx]
        if value is None:
            result[idx] = None
            continue
        current = (alpha * float(value)) + ((1.0 - alpha) * current)
        result[idx] = current
    return result


def _rolling_sum(values: Sequence[float | None], window: int) -> list[float | None]:
    result: list[float | None] = []
    acc = 0.0
    missing = 0
    for idx, value in enumerate(values):
        if value is None:
            missing += 1
        else:
            acc += value
        if idx >= window:
            old = values[idx - window]
            if old is None:
                missing -= 1
            else:
                acc -= old
        if idx + 1 < window or missing > 0:
            result.append(None)
        else:
            result.append(acc)
    return result


def _rolling_vwma(
    prices: Sequence[float | None],
    volumes: Sequence[float | None],
    window: int,
) -> list[float | None]:
    if len(prices) != len(volumes):
        raise ValueError("VWMA price and volume lengths must match")

    result: list[float | None] = []
    weighted_sum = 0.0
    volume_sum = 0.0
    missing = 0

    for idx, (price, volume) in enumerate(zip(prices, volumes)):
        if price is None or volume is None:
            missing += 1
        else:
            weighted_sum += price * volume
            volume_sum += volume

        if idx >= window:
            old_price = prices[idx - window]
            old_volume = volumes[idx - window]
            if old_price is None or old_volume is None:
                missing -= 1
            else:
                weighted_sum -= old_price * old_volume
                volume_sum -= old_volume

        if idx + 1 < window or missing > 0 or volume_sum == 0:
            result.append(None)
        else:
            result.append(weighted_sum / volume_sum)

    return result


def _rolling_std(values: Sequence[float | None], window: int) -> list[float | None]:
    result: list[float | None] = []
    for idx in range(len(values)):
        start = idx - window + 1
        if start < 0:
            result.append(None)
            continue
        sample = values[start : idx + 1]
        if any(value is None for value in sample):
            result.append(None)
            continue
        valid = [float(value) for value in sample if value is not None]
        mean = sum(valid) / window
        variance = sum((value - mean) ** 2 for value in valid) / window
        result.append(math.sqrt(variance))
    return result


def _pct_change(values: Sequence[float | None], periods: int) -> list[float | None]:
    result: list[float | None] = []
    for idx, value in enumerate(values):
        prev_idx = idx - periods
        if prev_idx < 0:
            result.append(None)
            continue
        prev = values[prev_idx]
        if value is None or prev is None or prev == 0:
            result.append(None)
            continue
        result.append((value / prev) - 1.0)
    return result


def _difference(values: Sequence[float | None], periods: int) -> list[float | None]:
    result: list[float | None] = []
    for idx, value in enumerate(values):
        prev_idx = idx - periods
        if prev_idx < 0:
            result.append(None)
            continue
        prev = values[prev_idx]
        if value is None or prev is None:
            result.append(None)
            continue
        result.append(value - prev)
    return result


def _log_return(values: Sequence[float | None], periods: int) -> list[float | None]:
    result: list[float | None] = []
    for idx, value in enumerate(values):
        prev_idx = idx - periods
        if prev_idx < 0:
            result.append(None)
            continue
        prev = values[prev_idx]
        if value is None or prev is None or value <= 0 or prev <= 0:
            result.append(None)
            continue
        result.append(math.log(value) - math.log(prev))
    return result


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _wilder_smooth(values: Sequence[float | None], window: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if window <= 0:
        raise ValueError("window must be positive")
    if len(values) < window:
        return result

    initial = values[:window]
    if any(value is None for value in initial):
        return result

    smoothed = sum(float(value) for value in initial if value is not None)
    result[window - 1] = smoothed
    for idx in range(window, len(values)):
        value = values[idx]
        if value is None:
            result[idx] = None
            continue
        smoothed = smoothed - (smoothed / window) + float(value)
        result[idx] = smoothed
    return result


def _adx(
    highs: Sequence[float | None],
    lows: Sequence[float | None],
    closes: Sequence[float | None],
    window: int,
) -> list[float | None]:
    length = len(closes)
    if len(highs) != length or len(lows) != length:
        raise ValueError("ADX input lengths must match")

    true_ranges: list[float | None] = [None] * length
    plus_dm: list[float | None] = [None] * length
    minus_dm: list[float | None] = [None] * length

    for idx in range(length):
        high = highs[idx]
        low = lows[idx]
        close = closes[idx]
        if high is None or low is None or close is None:
            continue
        if idx == 0:
            true_ranges[idx] = high - low
            plus_dm[idx] = 0.0
            minus_dm[idx] = 0.0
            continue

        prev_high = highs[idx - 1]
        prev_low = lows[idx - 1]
        prev_close = closes[idx - 1]
        if prev_high is None or prev_low is None or prev_close is None:
            continue

        up_move = high - prev_high
        down_move = prev_low - low
        plus_dm[idx] = up_move if up_move > down_move and up_move > 0 else 0.0
        minus_dm[idx] = down_move if down_move > up_move and down_move > 0 else 0.0
        true_ranges[idx] = max(high - low, abs(high - prev_close), abs(low - prev_close))

    smoothed_tr = _wilder_smooth(true_ranges, window)
    smoothed_plus_dm = _wilder_smooth(plus_dm, window)
    smoothed_minus_dm = _wilder_smooth(minus_dm, window)

    plus_di: list[float | None] = [None] * length
    minus_di: list[float | None] = [None] * length
    dx: list[float | None] = [None] * length
    for idx in range(length):
        tr = smoothed_tr[idx]
        pdm = smoothed_plus_dm[idx]
        mdm = smoothed_minus_dm[idx]
        if tr is None or pdm is None or mdm is None or tr == 0:
            continue
        plus_di[idx] = 100.0 * (pdm / tr)
        minus_di[idx] = 100.0 * (mdm / tr)
        denom = plus_di[idx] + minus_di[idx]
        if denom == 0:
            dx[idx] = 0.0
        else:
            dx[idx] = 100.0 * abs(plus_di[idx] - minus_di[idx]) / denom

    adx: list[float | None] = [None] * length
    first_adx_idx = (window * 2) - 2
    if first_adx_idx >= length:
        return adx

    initial_dx = dx[window - 1 : first_adx_idx + 1]
    if any(value is None for value in initial_dx):
        return adx

    current_adx = sum(float(value) for value in initial_dx if value is not None) / window
    adx[first_adx_idx] = current_adx
    for idx in range(first_adx_idx + 1, length):
        value = dx[idx]
        if value is None:
            continue
        current_adx = ((current_adx * (window - 1)) + float(value)) / window
        adx[idx] = current_adx
    return adx


def _atr(
    highs: Sequence[float | None],
    lows: Sequence[float | None],
    closes: Sequence[float | None],
    window: int,
) -> list[float | None]:
    length = len(closes)
    if len(highs) != length or len(lows) != length:
        raise ValueError("ATR input lengths must match")

    true_ranges: list[float | None] = [None] * length
    for idx in range(length):
        high = highs[idx]
        low = lows[idx]
        close = closes[idx]
        if high is None or low is None or close is None:
            continue
        if idx == 0:
            true_ranges[idx] = high - low
            continue

        prev_close = closes[idx - 1]
        if prev_close is None:
            continue

        true_ranges[idx] = max(high - low, abs(high - prev_close), abs(low - prev_close))

    smoothed_tr = _wilder_smooth(true_ranges, window)
    atr: list[float | None] = [None] * length
    for idx, value in enumerate(smoothed_tr):
        if value is None:
            continue
        atr[idx] = value / window
    return atr


def _apply_by_market(
    rows: list[dict[str, str]],
    values: list[float | None],
    fn: Callable[[list[float | None], dict[str, int | float | str]], list[float | None]],
    params: dict[str, int | float | str],
) -> list[float | None]:
    result: list[float | None] = [None] * len(rows)
    for indexes in _market_groups(rows):
        market_values = [values[idx] for idx in indexes]
        transformed = fn(market_values, params)
        if len(transformed) != len(indexes):
            raise ValueError("Transform length mismatch in market scope")
        for idx, value in zip(indexes, transformed):
            result[idx] = value
    return result


def _holding_state(
    rows: list[dict[str, str]],
    entry_values: list[float | None],
    exit_values: list[float | None],
) -> list[float | None]:
    if len(entry_values) != len(rows) or len(exit_values) != len(rows):
        raise ValueError("Holding state input lengths must match rows")

    result: list[float | None] = [None] * len(rows)
    for indexes in _market_groups(rows):
        holding = False
        for idx in indexes:
            entry_value = entry_values[idx]
            exit_value = exit_values[idx]
            if entry_value is None or exit_value is None:
                result[idx] = None
                continue
            entry_flag = entry_value != 0.0
            exit_flag = exit_value != 0.0
            if holding:
                if exit_flag:
                    holding = False
            elif entry_flag:
                holding = True
            result[idx] = 1.0 if holding else 0.0
    return result


def _apply_by_date(
    rows: list[dict[str, str]],
    values: list[float | None],
    fn: Callable[[list[float | None], dict[str, int | float | str]], list[float | None]],
    params: dict[str, int | float | str],
) -> list[float | None]:
    result: list[float | None] = [None] * len(rows)
    for indexes in _date_groups(rows):
        date_values = [values[idx] for idx in indexes]
        transformed = fn(date_values, params)
        if len(transformed) != len(indexes):
            raise ValueError("Transform length mismatch in date scope")
        for idx, value in zip(indexes, transformed):
            result[idx] = value
    return result


def _apply_by_market_period(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
    reducer: Callable[[list[float], dict[str, int | float | str]], float | None],
) -> list[float | None]:
    freq = str(params.get("freq", "M"))
    result: list[float | None] = [None] * len(rows)
    for indexes in _market_period_groups(rows, freq=freq):
        valid = [values[idx] for idx in indexes if values[idx] is not None]
        if not valid:
            aggregate = None
        else:
            aggregate = reducer([float(value) for value in valid], params)
        for idx in indexes:
            result[idx] = aggregate
    return result


def _apply_period_rolling(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
    roller: Callable[[Sequence[float | None], int], list[float | None]],
) -> list[float | None]:
    freq = str(params.get("freq", "M"))
    window = int(params["window"])
    result: list[float | None] = [None] * len(rows)

    grouped: dict[str, list[list[int]]] = defaultdict(list)
    for indexes in _market_period_groups(rows, freq=freq):
        grouped[rows[indexes[0]]["market"]].append(indexes)

    for market, period_groups in sorted(grouped.items()):
        del market
        aggregates = []
        for indexes in period_groups:
            valid = [values[idx] for idx in indexes if values[idx] is not None]
            aggregates.append(None if not valid else float(valid[-1]))
        rolled = roller(aggregates, window)
        for indexes, rolled_value in zip(period_groups, rolled):
            for idx in indexes:
                result[idx] = rolled_value
    return result


def transform_momentum(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    window = int(params["window"])
    return _apply_by_market(rows, values, lambda group, _: _log_return(group, window), params)


def transform_simple_return(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    window = int(params["window"])
    return _apply_by_market(rows, values, lambda group, _: _pct_change(group, window), params)


def transform_rolling_mean(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    window = int(params["window"])
    return _apply_by_market(rows, values, lambda group, _: _rolling_mean(group, window), params)


def transform_ewma(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    window = int(params["window"])
    return _apply_by_market(rows, values, lambda group, _: _ewma(group, window), params)


def transform_rolling_sum(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    window = int(params["window"])
    return _apply_by_market(rows, values, lambda group, _: _rolling_sum(group, window), params)


def transform_rolling_std(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    window = int(params["window"])
    return _apply_by_market(rows, values, lambda group, _: _rolling_std(group, window), params)


def transform_delta(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    periods = int(params.get("periods", 1))
    return _apply_by_market(rows, values, lambda group, _: _difference(group, periods), params)


def transform_vwma(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    window = int(params["window"])
    volume_column = str(params.get("volume_column", "candle_acc_trade_volume"))
    volume_values = _values_from_column(rows, volume_column)

    result: list[float | None] = [None] * len(rows)
    for indexes in _market_groups(rows):
        market_prices = [values[idx] for idx in indexes]
        market_volumes = [volume_values[idx] for idx in indexes]
        transformed = _rolling_vwma(market_prices, market_volumes, window)
        if len(transformed) != len(indexes):
            raise ValueError("Transform length mismatch in market scope")
        for idx, value in zip(indexes, transformed):
            result[idx] = value
    return result


def transform_volatility(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    window = int(params["window"])

    def group_volatility(group: list[float | None], _: dict[str, int | float | str]) -> list[float | None]:
        # Use return-based volatility so assets with different price levels remain comparable.
        returns = _log_return(group, 1)
        return _rolling_std(returns, window)

    return _apply_by_market(rows, values, group_volatility, params)


def transform_adx(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    window = int(params["window"])
    high_column = str(params.get("high_column", "high_price"))
    low_column = str(params.get("low_column", "low_price"))

    high_values = _values_from_column(rows, high_column)
    low_values = _values_from_column(rows, low_column)
    close_values = values

    result: list[float | None] = [None] * len(rows)
    for indexes in _market_groups(rows):
        market_highs = [high_values[idx] for idx in indexes]
        market_lows = [low_values[idx] for idx in indexes]
        market_closes = [close_values[idx] for idx in indexes]
        transformed = _adx(market_highs, market_lows, market_closes, window)
        if len(transformed) != len(indexes):
            raise ValueError("Transform length mismatch in market scope")
        for idx, value in zip(indexes, transformed):
            result[idx] = value
    return result


def transform_atr(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    window = int(params["window"])
    high_column = str(params.get("high_column", "high_price"))
    low_column = str(params.get("low_column", "low_price"))

    high_values = _values_from_column(rows, high_column)
    low_values = _values_from_column(rows, low_column)
    close_values = values

    result: list[float | None] = [None] * len(rows)
    for indexes in _market_groups(rows):
        market_highs = [high_values[idx] for idx in indexes]
        market_lows = [low_values[idx] for idx in indexes]
        market_closes = [close_values[idx] for idx in indexes]
        transformed = _atr(market_highs, market_lows, market_closes, window)
        if len(transformed) != len(indexes):
            raise ValueError("Transform length mismatch in market scope")
        for idx, value in zip(indexes, transformed):
            result[idx] = value
    return result


def transform_cross_rank(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    descending = bool(params.get("descending", True))

    def rank_group(group: list[float | None], _: dict[str, int | float | str]) -> list[float | None]:
        valid_positions = [(idx, value) for idx, value in enumerate(group) if value is not None]
        ordered = sorted(valid_positions, key=lambda item: float(item[1]), reverse=descending)
        ranked: list[float | None] = [None] * len(group)
        for rank, (idx, _) in enumerate(ordered, start=1):
            ranked[idx] = float(rank)
        return ranked

    return _apply_by_date(rows, values, rank_group, params)


def transform_cross_percentile(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    descending = bool(params.get("descending", True))

    def percentile_group(group: list[float | None], _: dict[str, int | float | str]) -> list[float | None]:
        valid_positions = [(idx, value) for idx, value in enumerate(group) if value is not None]
        ordered = sorted(valid_positions, key=lambda item: float(item[1]), reverse=descending)
        size = len(ordered)
        ranked: list[float | None] = [None] * len(group)
        for rank, (idx, _) in enumerate(ordered, start=1):
            ranked[idx] = 1.0 if size == 1 else (rank - 1) / (size - 1)
        return ranked

    return _apply_by_date(rows, values, percentile_group, params)


def transform_calendar_mean(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    return _apply_by_market_period(rows, values, params, lambda valid, _: sum(valid) / len(valid))


def transform_calendar_sum(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    return _apply_by_market_period(rows, values, params, lambda valid, _: sum(valid))


def transform_calendar_last(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    return _apply_by_market_period(rows, values, params, lambda valid, _: valid[-1])


def transform_calendar_rolling_mean(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    return _apply_period_rolling(rows, values, params, _rolling_mean)


def transform_calendar_rolling_sum(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    return _apply_period_rolling(rows, values, params, _rolling_sum)


def transform_intraday_range_pct(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    del values, params
    result: list[float | None] = []
    for row in rows:
        high_value = _parse_float(row["high_price"])
        low_value = _parse_float(row["low_price"])
        open_value = _parse_float(row["opening_price"])
        range_value = None if high_value is None or low_value is None else high_value - low_value
        result.append(_safe_ratio(range_value, open_value))
    return result


def transform_age_days(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    del values, params
    result: list[float | None] = [None] * len(rows)
    for indexes in _market_groups(rows):
        for offset, idx in enumerate(indexes, start=1):
            result[idx] = float(offset)
    return result


def transform_subtract_reference(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    reference_values = _resolve_source_values(rows, str(params["reference"]))
    result: list[float | None] = []
    for current, reference in zip(values, reference_values):
        if current is None or reference is None:
            result.append(None)
        else:
            result.append(current - reference)
    return result


def transform_ratio_to_reference(
    rows: list[dict[str, str]],
    values: list[float | None],
    params: dict[str, int | float | str],
) -> list[float | None]:
    reference_values = _resolve_source_values(rows, str(params["reference"]))
    result: list[float | None] = []
    for current, reference in zip(values, reference_values):
        if current is None or reference is None or reference == 0:
            result.append(None)
        else:
            result.append(current / reference)
    return result


TRANSFORM_REGISTRY: dict[str, TransformFn] = {
    "momentum": transform_momentum,
    "simple_return": transform_simple_return,
    "delta": transform_delta,
    "rolling_mean": transform_rolling_mean,
    "rolling_std": transform_rolling_std,
    "ewma": transform_ewma,
    "rolling_sum": transform_rolling_sum,
    "vwma": transform_vwma,
    "volatility": transform_volatility,
    "adx": transform_adx,
    "atr": transform_atr,
    "cross_rank": transform_cross_rank,
    "cross_percentile": transform_cross_percentile,
    "calendar_mean": transform_calendar_mean,
    "calendar_sum": transform_calendar_sum,
    "calendar_last": transform_calendar_last,
    "calendar_rolling_mean": transform_calendar_rolling_mean,
    "calendar_rolling_sum": transform_calendar_rolling_sum,
    "intraday_range_pct": transform_intraday_range_pct,
    "age_days": transform_age_days,
    "subtract_reference": transform_subtract_reference,
    "ratio_to_reference": transform_ratio_to_reference,
}


def base_feature_rows(rows: Iterable[CandleRow]) -> list[dict[str, str]]:
    base_rows = [
        {
            "date_utc": row.date_utc,
            "date_kst": row.date_kst,
            "market": row.market,
            "korean_name": row.korean_name,
            "english_name": row.english_name,
            "market_warning": row.market_warning,
            "opening_price": _format_value(row.opening_price),
            "high_price": _format_value(row.high_price),
            "low_price": _format_value(row.low_price),
            "trade_price": _format_value(row.trade_price),
            "candle_acc_trade_volume": _format_value(row.candle_acc_trade_volume),
            "candle_acc_trade_price": _format_value(row.candle_acc_trade_price),
            "timestamp": str(row.timestamp),
        }
        for row in rows
    ]
    return sorted(base_rows, key=lambda row: (row["date_utc"], row["market"]))


def apply_feature_spec(
    rows: list[dict[str, str]],
    spec: FeatureSpec,
) -> list[float | None]:
    if spec.compare is not None:
        compare = spec.compare
        if compare.operator not in COMPARE_OPERATORS:
            raise ValueError(f"Unsupported compare operator: {compare.operator}")
        if compare.left_feature not in rows[0]:
            raise ValueError(f"Unknown compare left feature: {compare.left_feature}")
        if compare.right_feature is None and compare.right_value is None:
            raise ValueError("CompareSpec must define right_feature or right_value")
        if compare.right_feature is not None and compare.right_feature not in rows[0]:
            raise ValueError(f"Unknown compare right feature: {compare.right_feature}")

        left_values = _values_from_column(rows, compare.left_feature)
        if compare.right_feature is not None:
            right_values = _values_from_column(rows, compare.right_feature)
        else:
            right_values = [compare.right_value] * len(rows)

        result: list[float | None] = []
        for left, right in zip(left_values, right_values):
            if left is None or right is None:
                result.append(None)
            else:
                result.append(1.0 if _compare_value(compare.operator, left, float(right)) else 0.0)
        return result

    if spec.logical is not None:
        logical = spec.logical
        if logical.operator not in LOGICAL_OPERATORS:
            raise ValueError(f"Unsupported logical operator: {logical.operator}")
        if not logical.features:
            raise ValueError("LogicalSpec must contain at least one feature")
        for feature_column in logical.features:
            if feature_column not in rows[0]:
                raise ValueError(f"Unknown logical feature column: {feature_column}")
        value_sets = [_values_from_column(rows, feature_column) for feature_column in logical.features]
        result: list[float | None] = []
        for idx in range(len(rows)):
            values = [value_set[idx] for value_set in value_sets]
            if any(value is None for value in values):
                result.append(None)
                continue
            flags = [value != 0.0 for value in values if value is not None]
            if logical.operator == "and":
                result.append(1.0 if all(flags) else 0.0)
            elif logical.operator == "or":
                result.append(1.0 if any(flags) else 0.0)
            else:
                if len(flags) != 1:
                    raise ValueError("LogicalSpec with operator 'not' must contain exactly one feature")
                result.append(0.0 if flags[0] else 1.0)
        return result

    if spec.state is not None:
        state = spec.state
        if state.entry_feature not in rows[0]:
            raise ValueError(f"Unknown state entry feature column: {state.entry_feature}")
        if state.exit_feature not in rows[0]:
            raise ValueError(f"Unknown state exit feature column: {state.exit_feature}")
        entry_values = _values_from_column(rows, state.entry_feature)
        exit_values = _values_from_column(rows, state.exit_feature)
        return _holding_state(rows, entry_values, exit_values)

    if spec.components:
        combine = spec.combine or "weighted_sum"
        if combine != "weighted_sum":
            raise ValueError(f"Unsupported score combine mode: {combine}")
        values_by_component: list[list[float | None]] = []
        weights: list[float] = []
        for component in spec.components:
            if component.feature_column not in rows[0]:
                raise ValueError(f"Unknown component feature column: {component.feature_column}")
            values_by_component.append(_values_from_column(rows, component.feature_column))
            weights.append(component.weight)
        return _weighted_sum(values_by_component, weights)

    if spec.source is None:
        raise ValueError("FeatureSpec must define source, components, logical, compare, or state")
    current_values = _resolve_source_values(rows, spec.source)
    if not spec.steps and not _source_exists(rows, spec.source):
        raise ValueError(f"Unknown source column: {spec.source}")

    for step in spec.steps:
        if step.kind not in TRANSFORM_REGISTRY:
            raise ValueError(f"Unknown transform: {step.kind}")
        current_values = TRANSFORM_REGISTRY[step.kind](rows, current_values, step.params)
    return current_values


def build_feature_table(
    rows: Iterable[CandleRow],
    feature_specs: Sequence[FeatureSpec],
) -> list[dict[str, str]]:
    feature_rows = base_feature_rows(rows)
    if not feature_rows:
        return []

    for spec in feature_specs:
        if (
            spec.compare is None
            and spec.logical is None
            and spec.state is None
            and not spec.components
            and spec.source is not None
            and not _source_exists(feature_rows, spec.source)
        ):
            raise ValueError(f"Unknown source column: {spec.source}")
        values = apply_feature_spec(feature_rows, spec)
        column_name = spec.resolved_column_name()
        for row, value in zip(feature_rows, values):
            row[column_name] = _format_value(value)
    return feature_rows


def feature_columns(feature_specs: Sequence[FeatureSpec]) -> list[str]:
    return BASE_COLUMNS + [spec.resolved_column_name() for spec in feature_specs]
