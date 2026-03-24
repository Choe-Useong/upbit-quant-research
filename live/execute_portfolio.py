#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from live.check_balance import ENV_PATH, UPBIT_BASE_URL, _authorized_get, _encode_jwt_hs512, _load_dotenv
from live.live_weight_builders import build_latest_weights, postprocess_latest_weight_rows


DEFAULT_EXECUTION_CONFIG = ROOT_DIR / "configs" / "live" / "main_portfolio_60m_execution.json"
MIN_ORDER_KRW = 5000.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preview or execute a live Upbit portfolio from an execution config.")
    parser.add_argument("--mode", choices=["preview", "live"], default="preview")
    parser.add_argument("--execution-config-json", default=str(DEFAULT_EXECUTION_CONFIG))
    parser.add_argument("--ignore-unmanaged", action="store_true")
    parser.add_argument("--min-order-krw", type=float, default=None)
    parser.add_argument("--refresh-candles", type=int, default=None)
    return parser


def _load_leaf_execution_config(payload: dict[str, object], *, base_dir: Path) -> dict[str, object]:
    strategy_type = str(payload.get("strategy_type", "portfolio_pipeline"))
    config = {
        "strategy_type": strategy_type,
        "portfolio_name": str(payload.get("portfolio_name", "live_portfolio")),
        "candle_dir": (base_dir / str(payload.get("candle_dir", "data/upbit/minutes/60"))).resolve(),
        "refresh_candles": int(payload.get("refresh_candles", 240)),
        "min_order_krw": float(payload.get("min_order_krw", MIN_ORDER_KRW)),
        "only_markets": [str(item).upper() for item in payload.get("only_markets", [])],
        "exclude_markets": [str(item).upper() for item in payload.get("exclude_markets", [])],
        "market_caps": {str(key).upper(): float(value) for key, value in payload.get("market_caps", {}).items()},
        "cap_overflow_mode": str(payload.get("cap_overflow_mode", "keep_cash")),
    }
    if strategy_type == "portfolio_pipeline":
        config.update(
            {
                "features_spec_json": (base_dir / str(payload["features_spec_json"])).resolve(),
                "market_scores_spec_json": (base_dir / str(payload["market_scores_spec_json"])).resolve(),
                "universe_spec_json": (base_dir / str(payload["universe_spec_json"])).resolve(),
                "weights_spec_json": (base_dir / str(payload["weights_spec_json"])).resolve(),
            }
        )
    elif strategy_type == "weights_csv":
        config.update(
            {
                "weights_csv": (base_dir / str(payload["weights_csv"])).resolve(),
                "managed_markets": [str(item).upper() for item in payload.get("managed_markets", [])],
            }
        )
    else:
        raise ValueError(f"Unsupported strategy_type: {strategy_type}")
    return config


def _merge_leaf_overrides(config: dict[str, object], payload: dict[str, object], *, base_dir: Path) -> dict[str, object]:
    merged = dict(config)
    if "portfolio_name" in payload:
        merged["portfolio_name"] = str(payload["portfolio_name"])
    if "candle_dir" in payload:
        merged["candle_dir"] = (base_dir / str(payload["candle_dir"])).resolve()
    if "refresh_candles" in payload:
        merged["refresh_candles"] = int(payload["refresh_candles"])
    if "min_order_krw" in payload:
        merged["min_order_krw"] = float(payload["min_order_krw"])
    if "only_markets" in payload:
        merged["only_markets"] = [str(item).upper() for item in payload.get("only_markets", [])]
    if "exclude_markets" in payload:
        merged["exclude_markets"] = [str(item).upper() for item in payload.get("exclude_markets", [])]
    if "market_caps" in payload:
        merged["market_caps"] = {str(key).upper(): float(value) for key, value in payload.get("market_caps", {}).items()}
    if "cap_overflow_mode" in payload:
        merged["cap_overflow_mode"] = str(payload["cap_overflow_mode"])
    if merged["strategy_type"] == "weights_csv" and "managed_markets" in payload:
        merged["managed_markets"] = [str(item).upper() for item in payload.get("managed_markets", [])]
    return merged


def _load_sleeve_config(item: dict[str, object], *, base_dir: Path, index: int) -> dict[str, object]:
    if "execution_config_json" in item:
        source_path = (base_dir / str(item["execution_config_json"])).resolve()
        source_config = _load_execution_config(source_path)
        if source_config["strategy_type"] == "sleeve_portfolio":
            raise ValueError("Nested sleeve_portfolio configs are not supported")
    elif "source" in item:
        source_config = _load_leaf_execution_config(dict(item["source"]), base_dir=base_dir)
    else:
        raise ValueError("Each sleeve requires either execution_config_json or source")

    sleeve_source = _merge_leaf_overrides(source_config, item, base_dir=base_dir)
    inactive_weight_mode = str(item.get("inactive_weight_mode", "")).strip().lower()
    weight_scale_mode = str(item.get("weight_scale_mode", "keep_source"))
    if inactive_weight_mode:
        if inactive_weight_mode == "keep_cash":
            weight_scale_mode = "keep_source"
        elif inactive_weight_mode == "redistribute":
            weight_scale_mode = "normalize_to_cap"
        else:
            raise ValueError(f"Unsupported inactive_weight_mode: {inactive_weight_mode}")
    return {
        "name": str(item.get("name", f"sleeve_{index}")),
        "capital_weight": float(item.get("capital_weight", 0.0)),
        "weight_scale_mode": weight_scale_mode,
        "source": sleeve_source,
    }


def _load_execution_config(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if payload.get("strategy_type") == "sleeve_portfolio" or "sleeves" in payload:
        sleeves = [_load_sleeve_config(dict(item), base_dir=ROOT_DIR, index=index) for index, item in enumerate(payload.get("sleeves", []), start=1)]
        if not sleeves:
            raise ValueError("sleeve_portfolio requires at least one sleeve")
        return {
            "strategy_type": "sleeve_portfolio",
            "portfolio_name": str(payload.get("portfolio_name", "sleeve_portfolio")),
            "refresh_candles": int(payload.get("refresh_candles", 240)),
            "min_order_krw": float(payload.get("min_order_krw", MIN_ORDER_KRW)),
            "only_markets": [str(item).upper() for item in payload.get("only_markets", [])],
            "exclude_markets": [str(item).upper() for item in payload.get("exclude_markets", [])],
            "market_caps": {str(key).upper(): float(value) for key, value in payload.get("market_caps", {}).items()},
            "cap_overflow_mode": str(payload.get("cap_overflow_mode", "keep_cash")),
            "portfolio_inactive_mode": str(payload.get("portfolio_inactive_mode", "keep_cash")),
            "sleeves": sleeves,
        }
    return _load_leaf_execution_config(payload, base_dir=ROOT_DIR)


def _build_query_string(params: dict[str, object]) -> str:
    return urllib.parse.urlencode([(key, str(value)) for key, value in params.items() if value is not None], doseq=True)


def _authorized_post(path: str, body: dict[str, object]) -> dict[str, object]:
    import os

    access_key = os.environ.get("UPBIT_ACCESS_KEY", "").strip()
    secret_key = os.environ.get("UPBIT_SECRET_KEY", "").strip()
    if not access_key or not secret_key:
        raise RuntimeError("Missing UPBIT_ACCESS_KEY or UPBIT_SECRET_KEY")

    query_string = _build_query_string(body)
    token = _encode_jwt_hs512(
        {
            "access_key": access_key,
            "nonce": str(uuid.uuid4()),
            "query_hash": hashlib.sha512(query_string.encode("utf-8")).hexdigest(),
            "query_hash_alg": "SHA512",
        },
        secret_key,
    )
    req = urllib.request.Request(
        f"{UPBIT_BASE_URL}{path}",
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _safe_float(value: object) -> float:
    return float(value or 0.0)


def _place_sell_market_order(market: str, volume: float) -> dict[str, object]:
    body = {
        "market": market,
        "side": "ask",
        "volume": f"{volume:.16f}".rstrip("0").rstrip("."),
        "ord_type": "market",
        "identifier": f"live-sell-{market.lower()}-{uuid.uuid4().hex[:10]}",
    }
    return _authorized_post("/v1/orders", body)


def _place_buy_market_order(market: str, price_krw: float) -> dict[str, object]:
    body = {
        "market": market,
        "side": "bid",
        "price": f"{price_krw:.0f}",
        "ord_type": "price",
        "identifier": f"live-buy-{market.lower()}-{uuid.uuid4().hex[:10]}",
    }
    return _authorized_post("/v1/orders", body)


def _build_plan(execution_config: dict[str, object], refresh_candles: int, min_order_krw: float) -> dict[str, object]:
    latest_weight_rows, latest_price_by_market = build_latest_weights(execution_config, refresh_candles)
    latest_weight_rows = postprocess_latest_weight_rows(execution_config, latest_weight_rows)
    balances = _authorized_get("/v1/accounts")

    target_weight_by_market = {row["market"]: float(row["target_weight"]) for row in latest_weight_rows}
    managed_markets = sorted(set(list(target_weight_by_market.keys()) + list(latest_price_by_market.keys())))
    balance_by_currency = {str(item.get("currency", "")).upper(): item for item in balances}
    available_krw = _safe_float(balance_by_currency.get("KRW", {}).get("balance")) + _safe_float(
        balance_by_currency.get("KRW", {}).get("locked")
    )

    managed_holdings: list[dict[str, object]] = []
    unmanaged_holdings: list[dict[str, object]] = []
    for item in balances:
        currency = str(item.get("currency", "")).upper()
        if currency == "KRW":
            continue
        qty = _safe_float(item.get("balance")) + _safe_float(item.get("locked"))
        if qty <= 0.0:
            continue
        market = f"KRW-{currency}"
        latest_price = latest_price_by_market.get(market)
        current_value_krw = qty * latest_price if latest_price is not None else None
        row = {
            "market": market,
            "asset": currency,
            "quantity": qty,
            "latest_price": latest_price,
            "current_value_krw": current_value_krw,
        }
        if market in managed_markets:
            managed_holdings.append(row)
        else:
            unmanaged_holdings.append(row)

    managed_equity_krw = available_krw + sum(
        float(item["current_value_krw"]) for item in managed_holdings if item["current_value_krw"] is not None
    )

    actions: list[dict[str, object]] = []
    latest_weight_date = max((row["date_utc"] for row in latest_weight_rows), default=None)
    for market in managed_markets:
        asset = market.replace("KRW-", "")
        latest_price = float(latest_price_by_market[market])
        balance_item = balance_by_currency.get(asset, {})
        current_qty = _safe_float(balance_item.get("balance")) + _safe_float(balance_item.get("locked"))
        current_value_krw = current_qty * latest_price
        target_weight = target_weight_by_market.get(market, 0.0)
        target_value_krw = managed_equity_krw * target_weight
        delta_value_krw = target_value_krw - current_value_krw

        if abs(delta_value_krw) < min_order_krw:
            action = "hold"
        elif delta_value_krw > 0:
            action = "buy"
        else:
            action = "sell"

        actions.append(
            {
                "market": market,
                "asset": asset,
                "latest_weight_date_utc": latest_weight_date,
                "latest_price": latest_price,
                "current_qty": current_qty,
                "current_value_krw": current_value_krw,
                "target_weight": target_weight,
                "target_value_krw": target_value_krw,
                "delta_value_krw": delta_value_krw,
                "action": action,
            }
        )

    return {
        "portfolio_name": str(execution_config["portfolio_name"]),
        "strategy_type": str(execution_config["strategy_type"]),
        "execution_config_json": str(execution_config.get("_path", "")),
        "managed_equity_krw": managed_equity_krw,
        "available_krw": available_krw,
        "min_order_krw": min_order_krw,
        "managed_markets": managed_markets,
        "latest_weight_date_utc": latest_weight_date,
        "latest_weight_rows": latest_weight_rows,
        "unmanaged_holdings": unmanaged_holdings,
        "actions": actions,
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _load_dotenv(ENV_PATH)
    execution_config_path = Path(args.execution_config_json)
    execution_config = _load_execution_config(execution_config_path)
    execution_config["_path"] = str(execution_config_path)
    refresh_candles = args.refresh_candles if args.refresh_candles is not None else int(execution_config["refresh_candles"])
    min_order_krw = args.min_order_krw if args.min_order_krw is not None else float(execution_config["min_order_krw"])

    try:
        plan = _build_plan(execution_config, refresh_candles, min_order_krw)
    except Exception as exc:
        print(f"Portfolio execution plan failed: {exc}", file=sys.stderr)
        return 1

    if plan["unmanaged_holdings"] and not args.ignore_unmanaged:
        print(json.dumps({**plan, "execution_blocked": True, "reason": "unmanaged_holdings_present"}, ensure_ascii=False, indent=2))
        return 2

    if args.mode == "preview":
        print(json.dumps({**plan, "mode": "preview"}, ensure_ascii=False, indent=2))
        return 0

    execution_results: list[dict[str, object]] = []
    sell_actions = [row for row in plan["actions"] if row["action"] == "sell"]
    buy_actions = [row for row in plan["actions"] if row["action"] == "buy"]
    hold_actions = [row for row in plan["actions"] if row["action"] == "hold"]

    for action in sell_actions:
        current_qty = float(action["current_qty"])
        latest_price = float(action["latest_price"])
        target_qty = float(action["target_value_krw"]) / latest_price if latest_price > 0 else 0.0
        sell_qty = max(current_qty - target_qty, 0.0)
        if sell_qty * latest_price < min_order_krw:
            execution_results.append({**action, "submitted": False, "reason": "below_min_order"})
            continue
        try:
            response = _place_sell_market_order(str(action["market"]), sell_qty)
            execution_results.append({**action, "submitted": True, "order_response": response})
        except Exception as exc:
            execution_results.append({**action, "submitted": False, "error": str(exc)})

    try:
        refreshed_balances = _authorized_get("/v1/accounts")
        refreshed_available_krw = _safe_float(
            next((item.get("balance") for item in refreshed_balances if str(item.get("currency", "")).upper() == "KRW"), 0.0)
        )
    except Exception:
        refreshed_available_krw = float(plan["available_krw"])

    remaining_cash = refreshed_available_krw
    for action in buy_actions:
        buy_value = min(float(action["delta_value_krw"]), remaining_cash)
        if buy_value < min_order_krw:
            execution_results.append({**action, "submitted": False, "reason": "below_min_order_or_no_cash"})
            continue
        try:
            response = _place_buy_market_order(str(action["market"]), buy_value)
            remaining_cash -= buy_value
            execution_results.append({**action, "submitted": True, "order_response": response})
        except Exception as exc:
            execution_results.append({**action, "submitted": False, "error": str(exc)})

    for action in hold_actions:
        execution_results.append({**action, "submitted": False, "reason": "hold"})

    print(json.dumps({**plan, "mode": "live", "execution_results": execution_results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
