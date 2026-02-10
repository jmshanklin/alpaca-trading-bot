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
            "total_dollars": round(float(g["pv"]), 2),
            "order_id": g["order_id"],
        })             

    rows.sort(key=lambda r: r["time"], reverse=True)
    return rows
    
def find_last_tsla_sell_time(activities):
    """Return timestamp of most recent TSLA sell fill, else None."""
    last = None
    for act in activities:
        symbol = _get_attr(act, "symbol")
        side = _get_attr(act, "side")
        ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
        if symbol == "TSLA" and side == "sell" and ts:
            if last is None or ts > last:
                last = ts
    return last

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
            
        # --- Active Group (computed from activities; one-group mode) ---
        try:
            # pull both buys + sells so we can find last sell time
            after2 = (datetime.utcnow() - timedelta(days=10)).isoformat() + "Z"
            acts2 = api.get_activities(activity_types="FILL", after=after2)

            last_sell_ts = find_last_tsla_sell_time(acts2)

            # filter to TSLA buys AFTER last sell
            tsla_buys_after = []
            for act in acts2:
                symbol = _get_attr(act, "symbol")
                side = _get_attr(act, "side")
                ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
                if symbol != "TSLA" or side != "buy" or not ts:
                    continue
                if last_sell_ts and ts <= last_sell_ts:
                    continue
                tsla_buys_after.append(act)

            # aggregate those buys into buy-triggers
            group_buys = aggregate_fills_by_order_id(tsla_buys_after, only_symbol="TSLA", only_side="buy")

            active_group = None
            if group_buys:
                # anchor is the OLDEST buy trigger in the current group
                anchor_row = group_buys[-1]
                anchor_price = float(anchor_row["vwap"])

                # strategy settings (hard-coded for now; later we read from config)
                BUY_QTY = 12
                SELL_OFFSET = 2.0   # sell at anchor + $2
                FIRST_INCREMENT_COUNT = 5  # 5 buys per increment step

                # count how many buy triggers are in the group
                buys_count = len(group_buys)

                # determine which increment stage we are in:
                # stage 1 = $1 drops, stage 2 = $2 drops, stage 3 = $3 drops, etc.
                stage = (buys_count - 1) // FIRST_INCREMENT_COUNT + 1
                drop_increment = float(stage)  # $1, $2, $3...

                buys_in_this_stage = (buys_count - 1) % FIRST_INCREMENT_COUNT + 1
                buys_remaining_before_increment_increases = FIRST_INCREMENT_COUNT - buys_in_this_stage

                # next buy trigger based on anchor and completed drops
                # completed stage drops:
                # stage 1 drops: 1,2,3,4,5
                # stage 2 drops: 2,4,6,8,10
                # etc.
                completed_drops = 0.0
                remaining = buys_count

                # sum full stages
                full_stages = (buys_count - 1) // FIRST_INCREMENT_COUNT
                for s in range(1, full_stages + 1):
                    completed_drops += s * FIRST_INCREMENT_COUNT

                # add partial stage drops
                partial = (buys_count - 1) % FIRST_INCREMENT_COUNT
                completed_drops += stage * partial

                next_buy_price = anchor_price - (completed_drops + stage)

                active_group = {
                    "group_start_time": anchor_row["time"],
                    "anchor_vwap": round(anchor_price, 4),
                    "sell_target": round(anchor_price + SELL_OFFSET, 4),
                    "buys_count": buys_count,
                    "drop_increment": drop_increment,
                    "buys_in_this_increment": buys_in_this_stage,
                    "buys_remaining_before_increment_increases": buys_remaining_before_increment_increases,
                    "next_buy_price": round(next_buy_price, 4),
                    "shares_expected": buys_count * BUY_QTY,
                }

            data["active_group"] = active_group
            # --- Active Group Triggers table (Excel-style) ---
            active_group_triggers = []
            if group_buys:
                # group_buys is newest-first; we want oldest-first for ladder math
                ladder = list(reversed(group_buys))

                prev_vwap = None
                for i, row in enumerate(ladder, start=1):
                    vwap = float(row["vwap"])

                    # Intended drop: 1 for triggers 2-5, 2 for 6-10, 3 for 11-15, 4 for 16-20, ...
                    intended_drop = ((i - 1) // 5) + 1 if i > 1 else None

                    actual_drop = None
                    if prev_vwap is not None:
                        actual_drop = round(prev_vwap - vwap, 4)

                    active_group_triggers.append({
                        "trigger": i,
                        "time": row.get("time"),
                        "shares": row.get("filled_qty"),
                        "avg_price": round(vwap, 4),
                        "total_dollars": row.get("total_dollars"),
                        "actual_drop": actual_drop,
                        "intended_drop": intended_drop,
                        "order_id": row.get("order_id"),
                    })

                    prev_vwap = vwap

            data["active_group_triggers"] = active_group_triggers

            data["active_group_last_sell_time"] = (last_sell_ts.isoformat() if last_sell_ts else None)

        except Exception as e:
            data["active_group_error"] = str(e)
            data["active_group"] = None

        return jsonify(data)

    except Exception as e:
        # This makes debugging painless (you'll see the error in the browser)
        return jsonify({"ok": False, "error": str(e)}), 500

