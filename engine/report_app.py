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

def aggregate_fills_by_order_id(activities):
    """Group Alpaca fills into one row per order_id."""
    grouped = {}

    for act in activities:
        oid = act.order_id
        qty = float(act.qty)
        price = float(act.price)
        ts = act.transaction_time

        if oid not in grouped:
            grouped[oid] = {
                "order_id": oid,
                "filled_qty": 0,
                "pv": 0,
                "ts": ts,
                "side": act.side,
            }

        grouped[oid]["filled_qty"] += qty
        grouped[oid]["pv"] += qty * price

        if ts < grouped[oid]["ts"]:
            grouped[oid]["ts"] = ts

    rows = []
    for oid, g in grouped.items():
        vwap = g["pv"] / g["filled_qty"]
        rows.append({
            "time": g["ts"].isoformat(),
            "side": g["side"],
            "filled_qty": g["filled_qty"],
            "vwap": round(vwap, 2),
        })

    rows.sort(key=lambda r: r["time"], reverse=True)
    return rows

@app.route("/")
def home():
    return "Alpaca Report Service Running"


@app.route("/report")
def report():
    """Minimal Phase 1 report: account + TSLA position"""

    # Get account
    acct = api.get_account()

    # Try to get TSLA position (may not exist if flat)
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
    except Exception:
        position_data = None
        
    # --- Get recent fills (last 3 days) ---
    after = (datetime.utcnow() - timedelta(days=3)).isoformat()
    activities = api.get_activities(
        activity_types="FILL",
        after=after
    )

    aggregated_fills = aggregate_fills_by_order_id(activities)

    data = {
        "account": {
            "equity": float(acct.equity),
            "cash": float(acct.cash),
            "buying_power": float(acct.buying_power),
        },
        "position": position_data,
        "recent_buy_triggers": aggregated_fills[:50]
    }

    return jsonify(data)
