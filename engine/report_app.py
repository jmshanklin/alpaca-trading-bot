import os
from flask import Flask, jsonify
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta

app = Flask(__name__)

# --- Alpaca connection ---
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")

def _get_attr(obj, name, default=None):
    # Works whether Alpaca returns an object or dict-like
    try:
        return getattr(obj, name)
    except Exception:
        pass
    try:
        return obj.get(name, default)
    except Exception:
        return default


def aggregate_fills_by_order_id(activities, only_symbol="TSLA", only_side="buy"):
    """
    Turn Alpaca FILL activities into one row per order_id.
    Filters to TSLA buys by default.
    """
    grouped = {}

    for act in activities:
        symbol = _get_attr(act, "symbol")
        side = _get_attr(act, "side")
        if only_symbol and symbol != only_symbol:
            continue
        if only_side and side != only_side:
            continue

        oid = _get_attr(act, "order_id")
        qty = float(_get_attr(act, "qty", 0) or 0)
        price = float(_get_attr(act, "price", 0) or 0)
        ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")

        if oid is None:
            # fallback grouping if order_id missing: time+price+qty
            oid = f"noid:{symbol}:{side}:{ts}:{price}:{qty}"

        g = grouped.setdefault(oid, {
            "order_id": oid,
            "time": ts,
            "side": side,
            "symbol": symbol,
            "filled_qty": 0.0,
            "pv": 0.0,
        })

        g["filled_qty"] += qty
        g["pv"] += qty * price

        # keep earliest time if comparable
        if ts and g["time"] and ts < g["time"]:
            g["time"] = ts

    rows = []
    for _, g in grouped.items():
        if g["filled_qty"] <= 0:
            continue
        vwap = g["pv"] / g["filled_qty"]
        rows.append({
            "time": g["time"].isoformat() if hasattr(g["time"], "isoformat") else str(g["time"]),
            "symbol": g["symbol"],
            "side": g["side"],
            "filled_qty": int(round(g["filled_qty"])),
            "vwap": round(float(vwap), 4),
            "order_id": g["order_id"],
        })

    rows.sort(key=lambda r: r["time"], reverse=True)
    return rows

@app.route("/")
def home():
    return "Alpaca Report Service Running"

@app.route("/report")
def report():
    """Report: account + TSLA position + (optional) recent fills. Always returns JSON."""
    try:
        acct = api.get_account()

        # TSLA position (may not exist)
        position_data = None
        try:
            pos = api.get_position("TSLA")
            position_data = {
                "symbol": pos.symbol,
                "qty": float(pos.qty),
                "avg_entry": float(pos.avg_entry_price),
                "market_value": float(pos.market_value),
                "unrealized_pl": float(pos.unrealized_pl),
                "current_price": float(pos.current_price),
            }
        except Exception as e:
            position_data = None

        data = {
            "ok": True,
            "account": {
                "equity": float(acct.equity),
                "cash": float(acct.cash),
                "buying_power": float(acct.buying_power),
            },
            "position": position_data,
        }

        # --- Recent TSLA BUY triggers (aggregated fills) ---
        try:
            after = (datetime.utcnow() - timedelta(days=5)).isoformat() + "Z"
            activities = api.get_activities(activity_types="FILL", after=after)
            buys = aggregate_fills_by_order_id(activities, only_symbol="TSLA", only_side="buy")
            data["recent_buy_triggers"] = buys[:50]
        except Exception as e:
            data["recent_buy_triggers_error"] = str(e)
            data["recent_buy_triggers"] = []

        return jsonify(data)

    except Exception as e:
        # This makes debugging painless (you'll see the error in the browser)
        return jsonify({"ok": False, "error": str(e)}), 500

