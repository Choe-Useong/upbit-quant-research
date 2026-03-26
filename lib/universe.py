from __future__ import annotations

import csv
import itertools
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq

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
    start_min_cross_section_size: int = 0
    mode: str = "top_n"
    top_n: int = 30
    quantiles: int = 5
    bucket_values: tuple[int, ...] = (1,)
    ascending: bool = False
    exclude_warnings: bool = False
    min_age_days: int | None = None
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
        if self.start_min_cross_section_size > 0:
            lag_part += f"_startcs{self.start_min_cross_section_size}"
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


def _base_cross_section_size_for_date(
    rows: list[dict[str, str]],
    lagged_value_map: dict[tuple[str, int], list[float | None]],
    indexes: list[int],
    spec: UniverseSpec,
) -> int:
    sort_column = spec.sort_column or spec.feature_column
    sort_values = lagged_value_map[(sort_column, spec.lag)]
    size = 0
    for idx in indexes:
        if not _row_passes_filters(rows[idx], spec):
            continue
        if sort_values[idx] is None:
            continue
        size += 1
    return size


def _validate_universe_spec(spec: UniverseSpec) -> None:
    if spec.mode not in UNIVERSE_MODES:
        raise ValueError(f"Unsupported universe mode: {spec.mode}")
    if spec.signal_lag < 0:
        raise ValueError("signal_lag must be >= 0")
    if spec.start_min_cross_section_size < 0:
        raise ValueError("start_min_cross_section_size must be >= 0")
    for filter_spec in spec.value_filters:
        if filter_spec.operator not in FILTER_OPERATORS:
            raise ValueError(f"Unsupported filter operator: {filter_spec.operator}")
    for rank_filter in spec.rank_filters:
        if rank_filter.mode not in UNIVERSE_MODES:
            raise ValueError(f"Unsupported rank filter mode: {rank_filter.mode}")


def _build_effective_universe_spec(spec: UniverseSpec) -> UniverseSpec:
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
    return UniverseSpec(
        feature_column=spec.feature_column,
        sort_column=spec.sort_column,
        lag=effective_sort_lag,
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
        value_filters=effective_value_filters,
        rank_filters=effective_rank_filters,
        name=spec.name,
    )


def _required_feature_lags(spec: UniverseSpec) -> set[tuple[str, int]]:
    return {
        (spec.sort_column or spec.feature_column, spec.lag),
        *[(filter_spec.feature_column, filter_spec.lag) for filter_spec in spec.value_filters],
        *[(rank_filter.feature_column, rank_filter.lag) for rank_filter in spec.rank_filters],
    }


def _selected_stream_rows_for_date(
    date_rows: list[dict[str, object]],
    spec: UniverseSpec,
) -> list[dict[str, str]]:
    sort_column = spec.sort_column or spec.feature_column
    candidates: list[dict[str, object]] = []
    for row in date_rows:
        raw_row = row["raw"]
        assert isinstance(raw_row, dict)
        if not _row_passes_filters(raw_row, spec):
            continue
        lagged_values = row["lagged_values"]
        assert isinstance(lagged_values, dict)
        final_value = lagged_values[(sort_column, spec.lag)]
        if final_value is None:
            continue
        blocked = False
        for filter_spec in spec.value_filters:
            value = lagged_values[(filter_spec.feature_column, filter_spec.lag)]
            if value is None or not _compare(filter_spec.operator, value, filter_spec.value):
                blocked = True
                break
        if blocked:
            continue
        candidates.append(row)

    for rank_filter in spec.rank_filters:
        scoped = []
        for row in candidates:
            lagged_values = row["lagged_values"]
            assert isinstance(lagged_values, dict)
            value = lagged_values[(rank_filter.feature_column, rank_filter.lag)]
            if value is not None:
                scoped.append((row, value))
        ordered = sorted(scoped, key=lambda item: item[1], reverse=not rank_filter.ascending)
        total = len(ordered)
        filtered: list[dict[str, object]] = []
        for rank, (row, _) in enumerate(ordered, start=1):
            bucket = _bucket_for_rank(rank, total, rank_filter.quantiles)
            keep = False
            if rank_filter.mode == "top_n":
                keep = rank <= rank_filter.top_n
            elif rank_filter.mode == "quantile":
                keep = bucket in rank_filter.bucket_values
            else:
                raise ValueError(f"Unsupported rank filter mode: {rank_filter.mode}")
            if keep:
                filtered.append(row)
        candidates = filtered

    ordered = sorted(
        [
            (
                row,
                row["lagged_values"][(sort_column, spec.lag)],
            )
            for row in candidates
        ],
        key=lambda item: item[1],
        reverse=not spec.ascending,
    )
    total = len(ordered)
    selected: list[dict[str, str]] = []
    for rank, (stream_row, value) in enumerate(ordered, start=1):
        assert value is not None
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

        raw_row = stream_row["raw"]
        assert isinstance(raw_row, dict)
        selected.append(
            {
                "date_utc": raw_row["date_utc"],
                "date_kst": raw_row["date_kst"],
                "market": raw_row["market"],
                "korean_name": raw_row["korean_name"],
                "english_name": raw_row["english_name"],
                "market_warning": raw_row["market_warning"],
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


def _base_cross_section_size_stream_for_date(
    date_rows: list[dict[str, object]],
    spec: UniverseSpec,
) -> int:
    sort_column = spec.sort_column or spec.feature_column
    size = 0
    for row in date_rows:
        raw_row = row["raw"]
        assert isinstance(raw_row, dict)
        if not _row_passes_filters(raw_row, spec):
            continue
        lagged_values = row["lagged_values"]
        assert isinstance(lagged_values, dict)
        if lagged_values[(sort_column, spec.lag)] is None:
            continue
        size += 1
    return size


def _iter_feature_csv_grouped_by_date(feature_csv_path: Path) -> Iterator[list[dict[str, str]]]:
    with feature_csv_path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        current_date: str | None = None
        current_rows: list[dict[str, str]] = []
        for row in reader:
            row_date = row["date_utc"]
            if current_date is None:
                current_date = row_date
            if row_date != current_date:
                yield current_rows
                current_rows = []
                current_date = row_date
            current_rows.append(row)
        if current_rows:
            yield current_rows


def _iter_feature_parquet_grouped_by_date(feature_parquet_path: Path) -> Iterator[list[dict[str, str]]]:
    parquet_file = pq.ParquetFile(feature_parquet_path)
    current_date: str | None = None
    current_rows: list[dict[str, str]] = []
    for batch in parquet_file.iter_batches(batch_size=65536):
        for raw_row in batch.to_pylist():
            row = {
                str(key): ("" if value is None else str(value))
                for key, value in raw_row.items()
            }
            row_date = row["date_utc"]
            if current_date is None:
                current_date = row_date
            if row_date != current_date:
                yield current_rows
                current_rows = []
                current_date = row_date
            current_rows.append(row)
    if current_rows:
        yield current_rows


def _iter_feature_table_grouped_by_date(feature_table_path: Path) -> Iterator[list[dict[str, str]]]:
    if feature_table_path.suffix.lower() == ".parquet":
        yield from _iter_feature_parquet_grouped_by_date(feature_table_path)
        return
    yield from _iter_feature_csv_grouped_by_date(feature_table_path)


def write_universe_table_from_feature_csv(
    feature_csv_path: str | Path,
    output_csv_path: str | Path,
    spec: UniverseSpec,
) -> int:
    _validate_universe_spec(spec)
    effective_spec = _build_effective_universe_spec(spec)
    required_pairs = _required_feature_lags(effective_spec)
    max_lag_by_feature: dict[str, int] = {}
    for feature_column, lag in required_pairs:
        max_lag_by_feature[feature_column] = max(max_lag_by_feature.get(feature_column, 0), lag)

    feature_csv = Path(feature_csv_path)
    output_csv = Path(output_csv_path)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    output_is_parquet = output_csv.suffix.lower() == ".parquet"
    universe_schema = pa.schema([(column, pa.string()) for column in universe_columns()])

    group_iter = _iter_feature_table_grouped_by_date(feature_csv)
    first_group = next(group_iter, None)
    if first_group is None:
        if output_is_parquet:
            pq.write_table(pa.Table.from_pylist([], schema=universe_schema), output_csv)
        else:
            with output_csv.open("w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.DictWriter(handle, fieldnames=universe_columns())
                writer.writeheader()
        return 0
    fieldnames = list(first_group[0].keys())
    for feature_column in max_lag_by_feature:
        if feature_column not in fieldnames:
            raise ValueError(f"Unknown feature column: {feature_column}")

    histories: dict[str, dict[str, list[float | None]]] = defaultdict(lambda: defaultdict(list))
    started = effective_spec.start_min_cross_section_size == 0
    row_count = 0

    def iter_selected_rows() -> Iterator[list[dict[str, str]]]:
        nonlocal started
        for date_group in itertools.chain([first_group], group_iter):
            stream_rows: list[dict[str, object]] = []
            for raw_row in date_group:
                market = raw_row["market"]
                lagged_values: dict[tuple[str, int], float | None] = {}
                current_values: dict[str, float | None] = {}
                for feature_column, max_lag in max_lag_by_feature.items():
                    current_value = _parse_float(raw_row.get(feature_column, ""))
                    current_values[feature_column] = current_value
                    history = histories[market][feature_column]
                    for lag in range(max_lag + 1):
                        pair = (feature_column, lag)
                        if pair not in required_pairs:
                            continue
                        if lag == 0:
                            lagged_values[pair] = current_value
                        elif len(history) >= lag:
                            lagged_values[pair] = history[-lag]
                        else:
                            lagged_values[pair] = None

                stream_rows.append({"raw": raw_row, "lagged_values": lagged_values})

                for feature_column, current_value in current_values.items():
                    history = histories[market][feature_column]
                    history.append(current_value)
                    max_lag = max_lag_by_feature[feature_column]
                    if max_lag > 0 and len(history) > max_lag:
                        del history[:-max_lag]

            if not started:
                base_cross_section_size = _base_cross_section_size_stream_for_date(stream_rows, effective_spec)
                if base_cross_section_size < effective_spec.start_min_cross_section_size:
                    continue
                started = True

            yield _selected_stream_rows_for_date(stream_rows, effective_spec)

    if output_is_parquet:
        writer: pq.ParquetWriter | None = None
        try:
            for selected_rows in iter_selected_rows():
                if not selected_rows:
                    continue
                table = pa.Table.from_pylist(selected_rows, schema=universe_schema)
                if writer is None:
                    writer = pq.ParquetWriter(output_csv, universe_schema)
                writer.write_table(table)
                row_count += len(selected_rows)
        finally:
            if writer is not None:
                writer.close()
        if writer is None:
            pq.write_table(pa.Table.from_pylist([], schema=universe_schema), output_csv)
    else:
        with output_csv.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=universe_columns())
            writer.writeheader()
            for selected_rows in iter_selected_rows():
                for row in selected_rows:
                    writer.writerow(row)
                    row_count += 1

    return row_count


def build_universe_table(
    feature_rows: Iterable[dict[str, str]],
    spec: UniverseSpec,
) -> list[dict[str, str]]:
    _validate_universe_spec(spec)

    rows = sorted(feature_rows, key=lambda row: (row["date_utc"], row["market"]))
    if not rows:
        return []
    effective_spec = _build_effective_universe_spec(spec)
    required_pairs = _required_feature_lags(effective_spec)
    for feature_column, _ in required_pairs:
        if feature_column not in rows[0]:
            raise ValueError(f"Unknown feature column: {feature_column}")

    lagged_value_map = {
        (feature_column, lag): _lag_feature_values(rows, feature_column, lag)
        for feature_column, lag in required_pairs
    }
    selected_rows: list[dict[str, str]] = []
    started = effective_spec.start_min_cross_section_size == 0
    for indexes in _date_groups(rows):
        if not started:
            base_cross_section_size = _base_cross_section_size_for_date(rows, lagged_value_map, indexes, effective_spec)
            if base_cross_section_size < effective_spec.start_min_cross_section_size:
                continue
            started = True
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
