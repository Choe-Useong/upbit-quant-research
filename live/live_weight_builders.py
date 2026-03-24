from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.features import build_feature_table
from lib.universe import build_universe_table
from lib.weights import build_weight_table
from lib.upbit_collector import CandleRow, Market, fetch_minute_candle_batch
from scripts.build_features import load_feature_specs
from scripts.build_universe import load_universe_spec
from scripts.build_weights import load_weight_spec


def _as_binary(value: str) -> float:
    normalized = (value or "").strip().lower()
    return 1.0 if normalized in {"1", "1.0", "true"} else 0.0


def _load_market_score_rules(path: Path) -> tuple[str, dict[str, dict[str, object]]]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return (
        payload.get("output_column", "custom_score"),
        {
            item["market"]: {
                "mode": item.get("mode", "weighted_sum"),
                "components": item.get("components", []),
            }
            for item in payload.get("rules", [])
        },
    )


def _infer_required_history_bars(feature_specs: list, universe_spec, refresh_candles: int) -> int:
    max_window = 1
    for spec in feature_specs:
        for step in getattr(spec, "steps", ()) or ():
            window = step.params.get("window")
            if isinstance(window, int):
                max_window = max(max_window, int(window))
    lag_values = [int(getattr(universe_spec, "lag", 0) or 0)]
    lag_values.extend(int(getattr(item, "lag", 0) or 0) for item in getattr(universe_spec, "value_filters", ()) or ())
    lag_values.extend(int(getattr(item, "lag", 0) or 0) for item in getattr(universe_spec, "rank_filters", ()) or ())
    max_lag = max(lag_values) if lag_values else 0
    return max(refresh_candles + max_window + max_lag + 10, 500)


def _read_tail_candles_csv(path: Path, tail_rows: int) -> list[CandleRow]:
    frame = pd.read_csv(path, encoding="utf-8-sig")
    if tail_rows > 0 and len(frame) > tail_rows:
        frame = frame.tail(tail_rows).copy()
    frame = frame.sort_values("date_utc")
    rows: list[CandleRow] = []
    for row in frame.to_dict(orient="records"):
        rows.append(
            CandleRow(
                market=str(row["market"]),
                korean_name=str(row["korean_name"]),
                english_name=str(row["english_name"]),
                market_warning=str(row["market_warning"]),
                date_utc=str(row["date_utc"]),
                date_kst=str(row["date_kst"]),
                opening_price=float(row["opening_price"]),
                high_price=float(row["high_price"]),
                low_price=float(row["low_price"]),
                trade_price=float(row["trade_price"]),
                candle_acc_trade_volume=float(row["candle_acc_trade_volume"]),
                candle_acc_trade_price=float(row["candle_acc_trade_price"]),
                timestamp=None if row.get("timestamp") in {"", None} or pd.isna(row.get("timestamp")) else int(row["timestamp"]),
            )
        )
    return rows


def _merge_live_rows(candle_dir: Path, market: str, refresh_candles: int, base_tail_rows: int) -> list[CandleRow]:
    path = candle_dir / f"{market}.csv"
    local_rows = _read_tail_candles_csv(path, base_tail_rows)
    meta = local_rows[-1] if local_rows else None
    if meta is None:
        raise FileNotFoundError(f"Missing local candle file: {path}")
    market_meta = Market(
        market=market,
        korean_name=meta.korean_name,
        english_name=meta.english_name,
        market_warning=meta.market_warning,
    )
    latest_rows = fetch_minute_candle_batch(market_meta, unit=60, count=refresh_candles)
    merged_by_date = {row.date_utc: row for row in local_rows}
    for row in latest_rows:
        merged_by_date[row.date_utc] = row
    return [merged_by_date[key] for key in sorted(merged_by_date)]


def _score_feature_rows(rows: list[dict[str, str]], score_column: str, market_rules: dict[str, dict[str, object]]) -> list[dict[str, str]]:
    scored_rows: list[dict[str, str]] = []
    for row in rows:
        rule = market_rules.get(row["market"], {"mode": "weighted_sum", "components": []})
        components = rule["components"]
        score = 0.0
        if rule["mode"] == "all_true":
            score = 1.0 if components and all(_as_binary(row.get(component["feature_column"], "")) > 0.0 for component in components) else 0.0
        else:
            for component in components:
                score += float(component.get("weight", 1.0)) * _as_binary(row.get(component["feature_column"], ""))
        next_row = dict(row)
        next_row[score_column] = f"{score:.12g}"
        scored_rows.append(next_row)
    return scored_rows


def apply_market_filters(markets: list[str], execution_config: dict[str, object]) -> list[str]:
    filtered = list(markets)
    only_markets = set(execution_config.get("only_markets", []))
    exclude_markets = set(execution_config.get("exclude_markets", []))
    if only_markets:
        filtered = [market for market in filtered if market in only_markets]
    if exclude_markets:
        filtered = [market for market in filtered if market not in exclude_markets]
    return filtered


def build_latest_pipeline_weights(execution_config: dict[str, object], refresh_candles: int) -> tuple[list[dict[str, str]], dict[str, float]]:
    feature_specs = load_feature_specs(Path(execution_config["features_spec_json"]))
    universe_spec = load_universe_spec(Path(execution_config["universe_spec_json"]))
    weight_spec = load_weight_spec(Path(execution_config["weights_spec_json"]))
    score_column, market_rules = _load_market_score_rules(Path(execution_config["market_scores_spec_json"]))
    required_history_bars = _infer_required_history_bars(feature_specs, universe_spec, refresh_candles)

    markets = apply_market_filters(list(universe_spec.allowed_markets), execution_config)
    candle_rows: list[CandleRow] = []
    latest_price_by_market: dict[str, float] = {}
    for market in markets:
        merged_rows = _merge_live_rows(
            Path(execution_config["candle_dir"]),
            market,
            refresh_candles,
            required_history_bars,
        )
        candle_rows.extend(merged_rows)
        latest_price_by_market[market] = float(merged_rows[-1].trade_price)

    feature_rows = build_feature_table(candle_rows, feature_specs)
    scored_rows = _score_feature_rows(feature_rows, score_column, market_rules)
    universe_rows = build_universe_table(scored_rows, universe_spec)
    weight_rows = build_weight_table(universe_rows, weight_spec)
    if not weight_rows:
        return [], latest_price_by_market
    latest_date = max(row["date_utc"] for row in weight_rows)
    latest_weight_rows = [row for row in weight_rows if row["date_utc"] == latest_date]
    return latest_weight_rows, latest_price_by_market


def _load_latest_weights_rows(weights_csv: Path) -> tuple[list[dict[str, str]], list[str]]:
    frame = pd.read_csv(weights_csv, encoding="utf-8-sig", dtype=str)
    if frame.empty:
        return [], []
    markets = sorted(frame["market"].dropna().astype(str).str.upper().unique().tolist())
    latest_date = frame["date_utc"].dropna().astype(str).max()
    latest_rows = frame.loc[frame["date_utc"].astype(str) == latest_date].copy()
    latest_rows["market"] = latest_rows["market"].astype(str).str.upper()
    return latest_rows.to_dict(orient="records"), markets


def build_latest_csv_weights(execution_config: dict[str, object], refresh_candles: int) -> tuple[list[dict[str, str]], dict[str, float]]:
    latest_weight_rows, historical_markets = _load_latest_weights_rows(Path(execution_config["weights_csv"]))
    allowed_latest_markets = set(apply_market_filters([row["market"] for row in latest_weight_rows], execution_config))
    latest_weight_rows = [row for row in latest_weight_rows if row["market"] in allowed_latest_markets]
    managed_markets = list(execution_config.get("managed_markets", [])) or historical_markets
    managed_markets = apply_market_filters(managed_markets, execution_config)
    base_tail_rows = max(refresh_candles + 2, 50)
    latest_price_by_market: dict[str, float] = {}
    for market in managed_markets:
        merged_rows = _merge_live_rows(
            Path(execution_config["candle_dir"]),
            market,
            refresh_candles,
            base_tail_rows,
        )
        latest_price_by_market[market] = float(merged_rows[-1].trade_price)
    return latest_weight_rows, latest_price_by_market


def _scale_sleeve_rows(rows: list[dict[str, str]], capital_weight: float, weight_scale_mode: str, sleeve_name: str) -> list[dict[str, str]]:
    if not rows or capital_weight <= 0.0:
        return []
    scale = 1.0
    if weight_scale_mode == "normalize_to_cap":
        total = sum(float(row["target_weight"]) for row in rows)
        scale = (1.0 / total) if total > 0.0 else 0.0
    elif weight_scale_mode != "keep_source":
        raise ValueError(f"Unsupported weight_scale_mode: {weight_scale_mode}")

    scaled_rows: list[dict[str, str]] = []
    for row in rows:
        next_row = dict(row)
        next_row["sleeve_name"] = sleeve_name
        next_row["source_target_weight"] = row["target_weight"]
        next_row["capital_weight"] = f"{capital_weight:.12g}"
        next_row["weight_scale_mode"] = weight_scale_mode
        next_row["target_weight"] = f"{float(row['target_weight']) * scale * capital_weight:.12g}"
        scaled_rows.append(next_row)
    return scaled_rows


def build_latest_sleeve_weights(execution_config: dict[str, object], refresh_candles: int) -> tuple[list[dict[str, str]], dict[str, float]]:
    combined_rows: list[dict[str, str]] = []
    latest_price_by_market: dict[str, float] = {}
    for sleeve in execution_config["sleeves"]:
        sleeve_rows, sleeve_prices = build_latest_weights(sleeve["source"], refresh_candles)
        latest_price_by_market.update(sleeve_prices)
        combined_rows.extend(
            _scale_sleeve_rows(
                sleeve_rows,
                float(sleeve["capital_weight"]),
                str(sleeve["weight_scale_mode"]),
                str(sleeve["name"]),
            )
        )
    if not combined_rows:
        return [], latest_price_by_market

    combined_by_market: dict[str, dict[str, str]] = {}
    for row in combined_rows:
        market = str(row["market"])
        if market not in combined_by_market:
            next_row = dict(row)
            next_row["target_weight"] = f"{float(row['target_weight']):.12g}"
            combined_by_market[market] = next_row
            continue
        existing = combined_by_market[market]
        existing["target_weight"] = f"{float(existing['target_weight']) + float(row['target_weight']):.12g}"
        existing["sleeve_name"] = f"{existing.get('sleeve_name', '')},{row['sleeve_name']}".strip(",")
        existing["source_target_weight"] = ""
        existing["capital_weight"] = ""
        existing["weight_scale_mode"] = "merged"

    allowed_markets = apply_market_filters(sorted(combined_by_market), execution_config)
    allowed_market_set = set(allowed_markets)
    latest_rows = sorted(
        [row for market, row in combined_by_market.items() if market in allowed_market_set],
        key=lambda row: row["market"],
    )
    latest_price_by_market = {market: price for market, price in latest_price_by_market.items() if market in allowed_market_set}
    return latest_rows, latest_price_by_market


def _rescale_rows_to_target_total(rows: list[dict[str, str]], target_total: float) -> list[dict[str, str]]:
    if not rows:
        return rows
    current_total = sum(float(row["target_weight"]) for row in rows)
    if current_total <= 0.0:
        return rows
    scale = target_total / current_total
    scaled_rows: list[dict[str, str]] = []
    for row in rows:
        next_row = dict(row)
        next_row["target_weight"] = f"{float(row['target_weight']) * scale:.12g}"
        scaled_rows.append(next_row)
    return scaled_rows


def _apply_market_caps(rows: list[dict[str, str]], market_caps: dict[str, float], overflow_mode: str) -> list[dict[str, str]]:
    if not rows or not market_caps:
        return rows
    if overflow_mode not in {"keep_cash", "redistribute"}:
        raise ValueError(f"Unsupported cap_overflow_mode: {overflow_mode}")

    base_weights = {str(row["market"]): float(row["target_weight"]) for row in rows}
    if overflow_mode == "keep_cash":
        adjusted_rows: list[dict[str, str]] = []
        for row in rows:
            market = str(row["market"])
            next_row = dict(row)
            limit = market_caps.get(market)
            weight = base_weights[market]
            next_row["target_weight"] = f"{min(weight, limit) if limit is not None else weight:.12g}"
            adjusted_rows.append(next_row)
        return adjusted_rows

    caps = {market: float(limit) for market, limit in market_caps.items()}
    target_total = sum(base_weights.values())
    result: dict[str, float] = {}
    free_markets = set(base_weights)
    remaining_total = target_total

    while free_markets:
        base_sum = sum(base_weights[market] for market in free_markets)
        if base_sum <= 0.0:
            break
        scaled = {market: remaining_total * base_weights[market] / base_sum for market in free_markets}
        breached = [market for market, weight in scaled.items() if market in caps and weight > caps[market]]
        if not breached:
            for market, weight in scaled.items():
                result[market] = weight
            break
        for market in breached:
            capped_weight = caps[market]
            result[market] = capped_weight
            remaining_total -= capped_weight
            free_markets.remove(market)

    adjusted_rows: list[dict[str, str]] = []
    for row in rows:
        market = str(row["market"])
        next_row = dict(row)
        next_row["target_weight"] = f"{result.get(market, 0.0):.12g}"
        adjusted_rows.append(next_row)
    return adjusted_rows


def postprocess_latest_weight_rows(execution_config: dict[str, object], latest_weight_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    rows = [dict(row) for row in latest_weight_rows]
    if execution_config["strategy_type"] == "sleeve_portfolio":
        portfolio_inactive_mode = str(execution_config.get("portfolio_inactive_mode", "keep_cash"))
        if portfolio_inactive_mode == "redistribute":
            target_total = min(sum(float(item["capital_weight"]) for item in execution_config["sleeves"]), 1.0)
            rows = _rescale_rows_to_target_total(rows, target_total)
        elif portfolio_inactive_mode != "keep_cash":
            raise ValueError(f"Unsupported portfolio_inactive_mode: {portfolio_inactive_mode}")
    rows = _apply_market_caps(
        rows,
        dict(execution_config.get("market_caps", {})),
        str(execution_config.get("cap_overflow_mode", "keep_cash")),
    )
    return rows


def build_latest_weights(execution_config: dict[str, object], refresh_candles: int) -> tuple[list[dict[str, str]], dict[str, float]]:
    strategy_type = str(execution_config["strategy_type"])
    if strategy_type == "portfolio_pipeline":
        return build_latest_pipeline_weights(execution_config, refresh_candles)
    if strategy_type == "weights_csv":
        return build_latest_csv_weights(execution_config, refresh_candles)
    if strategy_type == "sleeve_portfolio":
        return build_latest_sleeve_weights(execution_config, refresh_candles)
    raise ValueError(f"Unsupported strategy_type: {strategy_type}")
