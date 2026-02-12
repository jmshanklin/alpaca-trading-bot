import os
import json
import time
import threading
from datetime import datetime, timedelta

import requests
import alpaca_trade_api as tradeapi
import pytz
from flask import Flask, Response, jsonify

app = Flask(__name__)

# --- Alpaca connection ---
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")

# -------------------------
# PUSHOVER ALERTS
# -------------------------
ENABLE_PUSH_ALERTS = os.getenv("ENABLE_PUSH_ALERTS", "0") == "1"
PUSHOVER_USER_KEY = os.getenv("PUSHOVER_USER_KEY")
PUSHOVER_APP_TOKEN = os.getenv("PUSHOVER_APP_TOKEN")


def send_push(title: str, message: str):
    """Send push notification to phone via Pushover."""
    if not ENABLE_PUSH_ALERTS:
        return

    if not PUSHOVER_USER_KEY or not PUSHOVER_APP_TOKEN:
        print("Pushover keys missing")
        return

    try:
        requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": PUSHOVER_APP_TOKEN,
                "user": PUSHOVER_USER_KEY,
                "title": title,
                "message": message,
                "priority": 1,
            },
            timeout=10,
        )
    except Exception as e:
        print("Push send error:", e)


_WATCHER_THREAD_STARTED = False  # guard: only start once per process

# -------------------------
# BUY/SELL FILL WATCHER -> PUSH ALERTS
# -------------------------
_PUSH_STATE_PATH = "/tmp/pushover_last_fill.json"  # survives during runtime; resets if service restarts

WATCHER_STATUS = {
    "started": False,
    "initialized": False,
    "last_seen_time": None,
    "last_seen_id": None,
    "last_error": None,
}


# -------------------------
# Helpers
# -------------------------
def _get_attr(obj, name, default=None):
    """Works whether Alpaca returns an object or dict-like."""
    try:
        return getattr(obj, name)
    except Exception:
        pass
    try:
        return obj.get(name, default)
    except Exception:
        return default


def _parse_iso_time(s):
    """Parse ISO timestamps; returns timezone-aware datetime when possible."""
    try:
        # Handles "...Z" and "+00:00"
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _as_dt(ts):
    """
    Normalize Alpaca time values to a timezone-aware datetime.
    Handles:
      - datetime
      - ISO string
      - pandas Timestamp-like objects (has .to_pydatetime())
    """
    if ts is None:
        return None

    # pandas Timestamp or similar
    try:
        if hasattr(ts, "to_pydatetime"):
            ts = ts.to_pydatetime()
    except Exception:
        pass

    # ISO string
    if isinstance(ts, str):
        ts = _parse_iso_time(ts)

    if not isinstance(ts, datetime):
        return None

    # Ensure tz-aware (assume UTC if missing)
    try:
        if ts.tzinfo is None:
            ts = pytz.utc.localize(ts)
    except Exception:
        pass

    return ts


def to_central(ts):
    """UTC -> Central Time formatted string."""
    if not ts:
        return None
    try:
        utc = pytz.utc
        central = pytz.timezone("US/Central")
        if isinstance(ts, str):
            ts = _parse_iso_time(ts)
        ts = _as_dt(ts) or ts
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                ts = utc.localize(ts)
            ct_time = ts.astimezone(central)
            return ct_time.strftime("%b %d, %Y %I:%M:%S %p CT")
        return str(ts)
    except Exception:
        return str(ts)


def fmt_ct_any(ts):
    """Accepts datetime OR ISO string OR Timestamp-like."""
    dt = _as_dt(ts)
    if dt:
        return to_central(dt)
    return str(ts) if ts is not None else None


def money(x):
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return str(x)


def money0(x):
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)


def _load_push_state():
    try:
        with open(_PUSH_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_push_state(state: dict):
    try:
        with open(_PUSH_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception:
        pass


def _get_fill_time(act):
    raw = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
    dt = _as_dt(raw)
    return dt or datetime.min.replace(tzinfo=pytz.utc)


def _fill_unique_id(act):
    # activity id is best; fallback to order_id
    return str(_get_attr(act, "id") or _get_attr(act, "activity_id") or _get_attr(act, "order_id") or "")


def _watch_fills_and_push(symbol="TSLA", poll_seconds=15):
    global WATCHER_STATUS

    state = _load_push_state()
    initialized = state.get("initialized", False)

    last_seen_id = state.get("last_seen_id")
    last_seen_time = state.get("last_seen_time")  # ISO string

    while True:
        try:
            after = (datetime.utcnow() - timedelta(days=2)).isoformat() + "Z"
            acts = api.get_activities(activity_types="FILL", after=after)

            # Filter to TSLA buy/sell fills
            fills = []
            for a in acts:
                if _get_attr(a, "symbol") != symbol:
                    continue
                side = _get_attr(a, "side")
                if side not in ("buy", "sell"):
                    continue
                fills.append(a)

            # newest-first
            fills.sort(key=_get_fill_time, reverse=True)

            if fills:
                newest = fills[0]
                newest_id = _fill_unique_id(newest)
                newest_time = _get_fill_time(newest)

                # First run: set baseline only (no alerts)
                if not initialized:
                    state["initialized"] = True
                    state["last_seen_id"] = newest_id
                    state["last_seen_time"] = newest_time.isoformat()
                    _save_push_state(state)

                    print("Fill watcher initialized (no startup spam).")

                    WATCHER_STATUS["initialized"] = True
                    WATCHER_STATUS["last_seen_time"] = state.get("last_seen_time")
                    WATCHER_STATUS["last_seen_id"] = state.get("last_seen_id")

                    initialized = True
                    last_seen_id = state.get("last_seen_id")
                    last_seen_time = state.get("last_seen_time")

                    time.sleep(poll_seconds)
                    continue

                # Determine "new" fills since last_seen_time
                last_dt = _as_dt(last_seen_time)

                new_items = []
                for a in fills:
                    aid = _fill_unique_id(a)
                    atime = _get_fill_time(a)

                    if last_dt and atime <= last_dt:
                        continue
                    if last_seen_id and aid == last_seen_id:
                        continue

                    new_items.append(a)

                # send oldest-first so notifications arrive in order
                new_items.sort(key=_get_fill_time)

                for a in new_items:
                    side = _get_attr(a, "side")
                    qty = _get_attr(a, "qty")
                    price = _get_attr(a, "price")
                    atime = _get_fill_time(a)

                    side_upper = str(side).upper()
                    title = f"TSLA {side_upper} FILL"
                    msg = f"Qty: {qty} @ ${money(price)}\nTime: {to_central(atime)}"
                    send_push(title, msg)

                    # advance state after each sent
                    state["last_seen_id"] = _fill_unique_id(a)
                    state["last_seen_time"] = atime.isoformat()
                    _save_push_state(state)

                    # update watcher status
                    WATCHER_STATUS["last_seen_time"] = state.get("last_seen_time")
                    WATCHER_STATUS["last_seen_id"] = state.get("last_seen_id")

                # keep local copies updated too
                last_seen_id = state.get("last_seen_id")
                last_seen_time = state.get("last_seen_time")

        except Exception as e:
            print("Fill watcher error:", str(e))
            WATCHER_STATUS["last_error"] = str(e)

        time.sleep(poll_seconds)


def start_fill_watcher():
    global _WATCHER_THREAD_STARTED

    if not ENABLE_PUSH_ALERTS:
        print("Push alerts disabled (ENABLE_PUSH_ALERTS != 1).")
        return

    if _WATCHER_THREAD_STARTED:
        return

    t = threading.Thread(
        target=_watch_fills_and_push,
        kwargs={"symbol": "TSLA", "poll_seconds": 15},
        daemon=True,
    )
    t.start()
    _WATCHER_THREAD_STARTED = True

    WATCHER_STATUS["started"] = True
    print("Fill watcher started.")


# -------------------------
# Aggregation + Cycles
# -------------------------
def aggregate_fills_by_order_id(activities, only_symbol="TSLA", only_side="buy"):
    """
    Turn Alpaca FILL activities into one row per order_id.
    Returns rows newest-first.
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

        raw_ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
        ts = _as_dt(raw_ts)

        if oid is None:
            oid = f"noid:{symbol}:{side}:{raw_ts}:{price}:{qty}"

        g = grouped.setdefault(
            oid,
            {
                "order_id": oid,
                "time_dt": ts,
                "side": side,
                "symbol": symbol,
                "filled_qty": 0.0,
                "pv": 0.0,
            },
        )

        g["filled_qty"] += qty
        g["pv"] += qty * price

        # keep earliest time if comparable
        if ts and g["time_dt"] and ts < g["time_dt"]:
            g["time_dt"] = ts
        elif ts and g["time_dt"] is None:
            g["time_dt"] = ts

    rows = []
    for _, g in grouped.items():
        if g["filled_qty"] <= 0:
            continue
        vwap = g["pv"] / g["filled_qty"]
        tdt = g.get("time_dt")
        rows.append(
            {
                "time": tdt.isoformat() if isinstance(tdt, datetime) else "",
                "symbol": g["symbol"],
                "side": g["side"],
                "filled_qty": int(round(g["filled_qty"])),
                "vwap": round(float(vwap), 4),
                "total_dollars": round(float(g["pv"]), 2),
                "order_id": g["order_id"],
            }
        )

    def _t_key(r):
        dt = _as_dt(r.get("time"))
        return dt or datetime.min.replace(tzinfo=pytz.utc)

    rows.sort(key=_t_key, reverse=True)  # newest-first
    return rows


def aggregate_fills_all_sides_by_order_id(activities, only_symbol="TSLA"):
    """
    Aggregates both buys and sells, one row per order_id.
    Returns rows newest-first.
    """
    grouped = {}

    for act in activities:
        symbol = _get_attr(act, "symbol")
        if only_symbol and symbol != only_symbol:
            continue

        side = _get_attr(act, "side")
        oid = _get_attr(act, "order_id")
        qty = float(_get_attr(act, "qty", 0) or 0)
        price = float(_get_attr(act, "price", 0) or 0)

        raw_ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
        ts = _as_dt(raw_ts)

        if oid is None:
            oid = f"noid:{symbol}:{side}:{raw_ts}:{price}:{qty}"

        g = grouped.setdefault(
            oid,
            {
                "order_id": oid,
                "time_dt": ts,
                "side": side,
                "symbol": symbol,
                "filled_qty": 0.0,
                "pv": 0.0,
            },
        )

        g["filled_qty"] += qty
        g["pv"] += qty * price

        # keep earliest time
        if ts and g["time_dt"] and ts < g["time_dt"]:
            g["time_dt"] = ts
        elif ts and g["time_dt"] is None:
            g["time_dt"] = ts

    rows = []
    for _, g in grouped.items():
        if g["filled_qty"] <= 0:
            continue
        vwap = g["pv"] / g["filled_qty"]
        tdt = g.get("time_dt")
        rows.append(
            {
                "time": tdt.isoformat() if isinstance(tdt, datetime) else "",
                "symbol": g["symbol"],
                "side": g["side"],
                "filled_qty": int(round(g["filled_qty"])),
                "vwap": round(float(vwap), 4),
                "total_dollars": round(float(g["pv"]), 2),
                "order_id": g["order_id"],
            }
        )

    def _t_key(r):
        dt = _as_dt(r.get("time"))
        return dt or datetime.min.replace(tzinfo=pytz.utc)

    rows.sort(key=_t_key, reverse=True)  # newest-first
    return rows


def build_trade_cycles_from_order_rows(order_rows):
    """
    Build closed trade cycles:
    multiple BUY orders -> one SELL order closes the group.
    Output: cycles newest-first.
    """
    ordered = list(reversed(order_rows))  # oldest-first
    cycles = []
    cur = None

    def start_cycle(buy_row):
        t = buy_row.get("time")
        return {
            "anchor_time_raw": t,
            "anchor_time": fmt_ct_any(t),
            "anchor_order_id": buy_row.get("order_id"),
            "anchor_vwap": float(buy_row.get("vwap") or 0),
            "buy_orders": 0,
            "shares": 0,
            "cost": 0.0,
            "last_buy_time_raw": t,
        }

    for r in ordered:
        side = r.get("side")
        qty = int(r.get("filled_qty") or 0)
        vwap = float(r.get("vwap") or 0)
        dollars = float(r.get("total_dollars") or 0)
        t = r.get("time")

        if side == "buy":
            if cur is None:
                cur = start_cycle(r)
            cur["buy_orders"] += 1
            cur["shares"] += qty
            cur["cost"] += dollars
            cur["last_buy_time_raw"] = t

        elif side == "sell":
            if cur is None or cur["shares"] <= 0:
                continue

            proceeds = dollars
            avg_entry = (cur["cost"] / cur["shares"]) if cur["shares"] else None
            avg_exit = vwap
            realized_pl = proceeds - cur["cost"]
            realized_pl_pct = (realized_pl / cur["cost"] * 100.0) if cur["cost"] else None

            cycles.append(
                {
                    "anchor_time": cur["anchor_time"],
                    "anchor_time_raw": cur["anchor_time_raw"],
                    "anchor_order_id": cur["anchor_order_id"],
                    "anchor_vwap": round(cur["anchor_vwap"], 4),
                    "shares": cur["shares"],
                    "buy_orders": cur["buy_orders"],
                    "avg_entry": round(avg_entry, 4) if avg_entry is not None else None,
                    "sell_time": fmt_ct_any(t),
                    "sell_time_raw": t,
                    "sell_order_id": r.get("order_id"),
                    "avg_exit": round(avg_exit, 4) if avg_exit is not None else None,
                    "proceeds": round(proceeds, 2),
                    "cost": round(cur["cost"], 2),
                    "realized_pl": round(realized_pl, 2),
                    "realized_pl_pct": round(realized_pl_pct, 3) if realized_pl_pct is not None else None,
                }
            )

            cur = None

    def _sell_sort_key(c):
        dt = _as_dt(c.get("sell_time_raw"))
        return dt or datetime.min.replace(tzinfo=pytz.utc)

    cycles.sort(key=_sell_sort_key, reverse=True)  # newest-first
    return cycles


def compute_cycles(days=30, symbol="TSLA"):
    after = (datetime.utcnow() - timedelta(days=days)).isoformat() + "Z"
    acts = api.get_activities(activity_types="FILL", after=after)
    orders = aggregate_fills_all_sides_by_order_id(acts, only_symbol=symbol)
    return build_trade_cycles_from_order_rows(orders)


def find_last_tsla_sell_time(activities):
    """Return datetime (UTC, tz-aware) of most recent TSLA sell fill, else None."""
    last = None
    for act in activities:
        symbol = _get_attr(act, "symbol")
        side = _get_attr(act, "side")
        raw = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
        ts = _as_dt(raw)
        if symbol == "TSLA" and side == "sell" and ts:
            if last is None or ts > last:
                last = ts
    return last


def get_tsla_price_fallback():
    """
    Try to get TSLA price even if no TSLA position exists.
    Uses get_latest_trade if available, else None.
    """
    try:
        t = api.get_latest_trade("TSLA")
        p = _get_attr(t, "price", None)
        return float(p) if p is not None else None
    except Exception:
        return None


# -------------------------
# Routes
# -------------------------
@app.route("/")
def home():
    return "Alpaca Report Service Running"


@app.route("/push_test")
def push_test():
    send_push("TSLA BOT TEST 🚀", "If you see this, push alerts work!")
    return jsonify({"ok": True, "message": "sent"})


@app.route("/watcher_status")
def watcher_status():
    return jsonify({"ok": True, "watcher": WATCHER_STATUS})


@app.route("/pushover_status")
def pushover_status():
    return jsonify(
        {
            "ok": True,
            "watcher": WATCHER_STATUS,
            "push_enabled": ENABLE_PUSH_ALERTS,
            "has_user_key": bool(PUSHOVER_USER_KEY),
            "has_app_token": bool(PUSHOVER_APP_TOKEN),
        }
    )


@app.route("/watcher_debug")
def watcher_debug():
    """
    TEMP debug:
    Shows what the watcher is seeing and how it decides what's "new".
    """
    try:
        after = (datetime.utcnow() - timedelta(days=2)).isoformat() + "Z"
        acts = api.get_activities(activity_types="FILL", after=after)

        fills = []
        for a in acts:
            if _get_attr(a, "symbol") != "TSLA":
                continue
            side = _get_attr(a, "side")
            if side not in ("buy", "sell"):
                continue
            fills.append(a)

        fills.sort(key=_get_fill_time, reverse=True)

        last_seen_time = None
        try:
            last_seen_time = _load_push_state().get("last_seen_time")
        except Exception:
            last_seen_time = None

        last_dt = _as_dt(last_seen_time)

        top = []
        new_candidates = []

        for a in fills[:15]:
            aid = _fill_unique_id(a)
            atime = _get_fill_time(a)
            row = {
                "id": aid,
                "time_utc": atime.isoformat() if hasattr(atime, "isoformat") else str(atime),
                "time_ct": to_central(atime),
                "side": _get_attr(a, "side"),
                "qty": _get_attr(a, "qty"),
                "price": _get_attr(a, "price"),
            }
            top.append(row)

            if last_dt and atime > last_dt:
                new_candidates.append(row)

        return jsonify(
            {
                "ok": True,
                "watcher_status": WATCHER_STATUS,
                "state_last_seen_time": last_seen_time,
                "state_last_seen_dt_parsed": last_dt.isoformat() if last_dt else None,
                "tsla_fill_count_last_2d": len(fills),
                "top_15_tsla_fills_newest_first": top,
                "would_count_as_new_right_now": new_candidates,
            }
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/report")
def report():
    """Report: account + TSLA price + TSLA position + active group + ladder triggers. Always returns JSON."""
    try:
        acct = api.get_account()

        # TSLA position (may not exist)
        position_data = None
        tsla_price = None
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
            tsla_price = float(pos.current_price)
        except Exception:
            position_data = None
            tsla_price = get_tsla_price_fallback()

        now_ct = to_central(datetime.utcnow().replace(tzinfo=pytz.utc))

        data = {
            "ok": True,
            "server_time_ct": now_ct,
            "tsla_price": tsla_price,
            "account": {
                "equity": float(_get_attr(acct, "equity", 0) or 0),
                "cash": float(_get_attr(acct, "cash", 0) or 0),
                "buying_power": float(_get_attr(acct, "buying_power", 0) or 0),
                "regt_buying_power": float(_get_attr(acct, "regt_buying_power", 0) or 0),
                "daytrading_buying_power": float(_get_attr(acct, "daytrading_buying_power", 0) or 0),
                "effective_buying_power": float(_get_attr(acct, "effective_buying_power", 0) or 0),
                "non_marginable_buying_power": float(_get_attr(acct, "non_marginable_buying_power", 0) or 0),
                "long_market_value": float(_get_attr(acct, "long_market_value", 0) or 0),
                "initial_margin": float(_get_attr(acct, "initial_margin", 0) or 0),
                "maintenance_margin": float(_get_attr(acct, "maintenance_margin", 0) or 0),
            },
            "position": position_data,
        }

        # --- Active Group (computed from activities; one-group mode) ---
        try:
            after2 = (datetime.utcnow() - timedelta(days=10)).isoformat() + "Z"
            acts2 = api.get_activities(activity_types="FILL", after=after2)

            last_sell_ts = find_last_tsla_sell_time(acts2)  # datetime or None

            # filter to TSLA buys AFTER last sell
            tsla_buys_after = []
            for act in acts2:
                symbol = _get_attr(act, "symbol")
                side = _get_attr(act, "side")
                raw_ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
                ts = _as_dt(raw_ts)

                if symbol != "TSLA" or side != "buy" or not ts:
                    continue
                if last_sell_ts and ts <= last_sell_ts:
                    continue
                tsla_buys_after.append(act)

            group_buys = aggregate_fills_by_order_id(tsla_buys_after, only_symbol="TSLA", only_side="buy")
            # group_buys is newest-first

            active_group = None
            active_group_triggers = []

            if group_buys:
                # Anchor lock: FIRST buy trigger after last sell (oldest by time)
                def _row_time_dt(r):
                    dt = _as_dt(r.get("time"))
                    return dt or datetime.max.replace(tzinfo=pytz.utc)

                anchor_row = min(group_buys, key=_row_time_dt)
                anchor_price = float(anchor_row["vwap"])
                group_start_time = anchor_row.get("time")

                # strategy settings (hard-coded for now)
                BUY_QTY = 12
                SELL_OFFSET = 2.0
                FIRST_INCREMENT_COUNT = 5

                buys_count = len(group_buys)
                stage = (buys_count - 1) // FIRST_INCREMENT_COUNT + 1
                drop_increment = float(stage)

                buys_in_this_stage = (buys_count - 1) % FIRST_INCREMENT_COUNT + 1

                # compute next buy price
                completed_drops = 0.0
                full_stages = (buys_count - 1) // FIRST_INCREMENT_COUNT
                for s in range(1, full_stages + 1):
                    completed_drops += s * FIRST_INCREMENT_COUNT
                partial = (buys_count - 1) % FIRST_INCREMENT_COUNT
                completed_drops += stage * partial
                next_buy_price = anchor_price - (completed_drops + stage)

                current_price = None
                if position_data and position_data.get("current_price") is not None:
                    current_price = float(position_data["current_price"])
                elif tsla_price is not None:
                    current_price = float(tsla_price)

                sell_target = anchor_price + SELL_OFFSET

                distance_to_sell = None
                distance_to_next_buy = None
                if current_price is not None:
                    distance_to_sell = round(sell_target - current_price, 4)
                    distance_to_next_buy = round(current_price - next_buy_price, 4)

                active_group = {
                    "group_start_time": fmt_ct_any(group_start_time),
                    "anchor_vwap": round(anchor_price, 4),
                    "sell_target": round(sell_target, 4),
                    "buys_count": buys_count,
                    "drop_increment": drop_increment,
                    "buys_in_this_increment": buys_in_this_stage,
                    "next_buy_price": round(next_buy_price, 4),
                    "current_price": round(current_price, 4) if current_price is not None else None,
                    "distance_to_sell": distance_to_sell,
                    "distance_to_next_buy": distance_to_next_buy,
                    "anchor_time": fmt_ct_any(anchor_row.get("time")),
                    "anchor_order_id": anchor_row.get("order_id"),
                }

                # Build chronological first (oldest-first) so actual_drop is correct.
                ladder_oldest_first = list(reversed(group_buys))  # oldest-first
                prev_vwap = None
                tmp_rows = []
                for i, row in enumerate(ladder_oldest_first, start=1):
                    vwap = float(row["vwap"])
                    intended_drop = ((i - 1) // 5) + 1 if i > 1 else None
                    actual_drop = round(prev_vwap - vwap, 4) if prev_vwap is not None else None

                    tmp_rows.append(
                        {
                            "trigger": i,
                            "time": fmt_ct_any(row.get("time")),
                            "shares": row.get("filled_qty"),
                            "avg_price": round(vwap, 4),
                            "total_dollars": row.get("total_dollars"),
                            "actual_drop": actual_drop,
                            "intended_drop": intended_drop,
                            "order_id": row.get("order_id"),
                        }
                    )
                    prev_vwap = vwap

                # Display newest-first (most recent fill first)
                active_group_triggers = list(reversed(tmp_rows))

            data["active_group"] = active_group
            data["active_group_triggers"] = active_group_triggers
            data["active_group_last_sell_time"] = (last_sell_ts.isoformat() if last_sell_ts else None)

        except Exception as e:
            data["active_group_error"] = str(e)
            data["active_group"] = None
            data["active_group_triggers"] = []

        return jsonify(data)

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/cycles")
def cycles():
    """Closed trade cycles newest-first."""
    try:
        cyc = compute_cycles(days=30, symbol="TSLA")
        return jsonify({"ok": True, "cycles": cyc})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/table")
def table_view():
    """
    HTML dashboard page:
    - Big TSLA price banner (top)
    - Potential Profit box (Total + Per-share) (updates every 15 seconds)
    - Account + Summary boxes side-by-side (centered as a pair)
    - Ladder with WAITING row at TOP + newest fills below
    - Cycles newest-first
    Auto-refresh: fetch /report + /cycles every 15 seconds
    """
    r = report()
    if isinstance(r, tuple):
        resp, _status = r
        data = resp.get_json() if hasattr(resp, "get_json") else {}
    else:
        data = r.get_json() if hasattr(r, "get_json") else {}

    ag = data.get("active_group") or {}
    rows = data.get("active_group_triggers") or []  # newest-first fills
    acct = data.get("account") or {}
    pos = data.get("position") or {}
    tsla_price = data.get("tsla_price")

    html = []
    html.append("<html><head><title>TSLA Ladder</title>")
    html.append(
        """
    <style>
      body { font-family: Arial, sans-serif; padding: 16px; }
      .box { padding: 12px; border: 1px solid #ccc; border-radius: 8px; margin-bottom: 16px; background: #fff; }
      .row { margin: 4px 0; }
      .muted { color: #666; }

      table { border-collapse: collapse; width: 100%; margin-bottom: 24px; }
      th, td { border: 1px solid #ddd; padding: 8px; text-align: right; }
      th { background: #f5f5f5; text-align: right; }
      td:first-child, th:first-child { text-align: center; }
      td:nth-child(2), th:nth-child(2) { text-align: left; }

      /* BIG live TSLA price banner */
      .price-wrap { display: flex; justify-content: center; margin-bottom: 12px; }
      .price-box { max-width: 520px; width: 100%; text-align: center; }
      .price-label { font-size: 16px; color: #666; margin-bottom: 4px; }
      .price-banner { font-size: 42px; font-weight: bold; margin-bottom: 4px; }

      /* Small metric box (Potential Profit) */
      .metric-wrap { display: flex; justify-content: center; margin: 0 0 16px; }
      .metric-box { max-width: 520px; width: 100%; text-align: center; }
      .metric-title { font-size: 16px; color: #666; margin-bottom: 6px; }
      .metric-value { font-size: 28px; font-weight: bold; margin-bottom: 4px; }
      .metric-sub { font-size: 14px; color: #666; }

      /* Top row: Account (left) + Summary (right), centered as a pair */
      .top-row {
        display: flex;
        justify-content: center;
        gap: 24px;
        align-items: flex-start;
        margin-bottom: 16px;
        flex-wrap: wrap;
      }

      /* Two-column rows (shared) */
      .grid-row {
        display: grid;
        grid-template-columns: 1fr auto;
        gap: 16px;
        padding: 2px 0;
      }
      .grid-label { text-align: left; }
      .grid-value { text-align: right; font-variant-numeric: tabular-nums; }

      /* Account + Summary widths */
      .acct-grid { max-width: 400px; margin: 0; width: 100%; }
      .summary-grid { max-width: 400px; margin: 0; width: 100%; }

      /* Small section heading inside boxes */
      .box-title { font-weight: bold; margin-bottom: 2px; }
      .box-sub { color: #666; font-size: 13px; margin-bottom: 8px; }
      .section-gap { margin-top: 10px; }
    </style>
    """
    )
    html.append("</head><body>")

    # TSLA price indicator (TOP)
    html.append("<div class='price-wrap'>")
    html.append("<div class='box price-box'>")
    html.append("<div class='price-label'>TSLA Price</div>")
    html.append(
        f"<div class='price-banner'>$<span id='tsla-price'>{money(tsla_price) if tsla_price is not None else ''}</span></div>"
    )
    html.append("</div></div>")

    # Potential Profit box
    html.append("<div class='metric-wrap'>")
    html.append("<div class='box metric-box'>")
    html.append("<div class='metric-title'>Potential Profit (to Sell Target)</div>")
    html.append("<div class='metric-value'>$<span id='potential-profit'></span></div>")
    html.append("<div class='metric-sub'>Per share: $<span id='potential-per-share'></span></div>")
    html.append("</div></div>")

    # Side-by-side boxes
    html.append("<div class='top-row'>")

    # Account box
    html.append("<div class='box acct-grid'>")
    html.append("<div class='box-title'>Account</div>")
    html.append("<div class='box-sub'>Updates every 15 seconds (time-based), independent of trade activity.</div>")

    def acct_row(label, key, fmt="money"):
        val = acct.get(key)
        sval = money0(val) if fmt == "money0" else money(val)
        return (
            "<div class='grid-row'>"
            f"<div class='grid-label'>{label}</div>"
            f"<div class='grid-value'>$<span id='acct-{key}'>{sval}</span></div>"
            "</div>"
        )

    html.append(acct_row("Equity", "equity"))
    html.append(acct_row("Cash", "cash"))
    html.append(acct_row("Buying Power", "buying_power", fmt="money0"))
    html.append(acct_row("RegT Buying Power", "regt_buying_power", fmt="money0"))
    html.append(acct_row("Day Trading Buying Power", "daytrading_buying_power", fmt="money0"))
    html.append(acct_row("Effective Buying Power", "effective_buying_power", fmt="money0"))
    html.append(acct_row("Non-Marginable Buying Power", "non_marginable_buying_power", fmt="money0"))

    html.append("<div class='section-gap'></div>")
    html.append(acct_row("Long Market Value", "long_market_value"))
    html.append(acct_row("Initial Margin", "initial_margin"))
    html.append(acct_row("Maintenance Margin", "maintenance_margin"))
    html.append("</div>")  # end Account box

    # Summary box
    html.append("<div class='box summary-grid'>")
    html.append("<div class='box-title'>Summary</div>")
    html.append("<div class='box-sub'>Bot status + ladder metrics (updates every 15 seconds).</div>")

    def sum_row(label, value, value_id=None, prefix=""):
        v = value if value is not None else ""
        if value_id:
            return (
                "<div class='grid-row'>"
                f"<div class='grid-label'>{label}</div>"
                f"<div class='grid-value'>{prefix}<span id='{value_id}'>{v}</span></div>"
                "</div>"
            )
        return (
            "<div class='grid-row'>"
            f"<div class='grid-label'>{label}</div>"
            f"<div class='grid-value'>{prefix}{v}</div>"
            "</div>"
        )

    html.append(sum_row("Last updated", data.get("server_time_ct", ""), "last-updated"))
    html.append("<div class='section-gap'></div>")
    html.append(sum_row("Anchor", ag.get("anchor_vwap"), "ag-anchor-vwap"))
    html.append(sum_row("Anchor time", ag.get("anchor_time"), "ag-anchor-time"))
    html.append(sum_row("Sell Target", ag.get("sell_target"), "ag-sell-target"))
    html.append(sum_row("Distance to Sell", ag.get("distance_to_sell"), "ag-distance-sell"))
    html.append(sum_row("Next Buy", ag.get("next_buy_price"), "ag-next-buy"))
    html.append(sum_row("Distance to Next Buy", ag.get("distance_to_next_buy"), "ag-distance-next"))
    html.append(sum_row("Buys", ag.get("buys_count"), "ag-buys"))
    html.append(sum_row("Drop Increment", ag.get("drop_increment"), "ag-drop-inc"))

    if pos and isinstance(pos, dict) and pos.get("qty") is not None:
        html.append("<div class='section-gap'></div>")
        html.append("<div class='box-title'>TSLA Position</div>")
        html.append(sum_row("Qty", pos.get("qty"), "pos-qty"))
        html.append(sum_row("Avg Entry", money(pos.get("avg_entry")), "pos-avg", prefix="$"))
        html.append(sum_row("Mkt Value", money(pos.get("market_value")), "pos-mv", prefix="$"))
        html.append(sum_row("uP/L", money(pos.get("unrealized_pl")), "pos-upl", prefix="$"))

    html.append("</div>")  # end Summary box
    html.append("</div>")  # end top-row

    # Ladder table
    html.append("<h2>Active Group Ladder</h2>")
    html.append("<table>")
    html.append(
        "<thead><tr>"
        "<th>#</th><th>Time (CT)</th><th>Shares</th><th>Total $</th><th>Avg Price</th><th>Intended Drop</th><th>Actual Drop</th>"
        "</tr></thead>"
    )
    html.append("<tbody id='ladder-body'>")

    # WAITING row at TOP
    if ag and ag.get("next_buy_price") is not None and ag.get("buys_count") is not None:
        next_trigger = int(ag["buys_count"]) + 1
        intended_drop_next = ((next_trigger - 1) // 5) + 1
        html.append(
            "<tr>"
            f"<td><b>{next_trigger}</b></td>"
            f"<td><b>WAITING</b></td>"
            f"<td>12</td>"
            f"<td>—</td>"
            f"<td><b>{ag.get('next_buy_price')}</b></td>"
            f"<td><b>{intended_drop_next}</b></td>"
            f"<td>—</td>"
            "</tr>"
        )

    # Then newest-first filled rows
    for row in rows:
        html.append(
            "<tr>"
            f"<td>{row.get('trigger')}</td>"
            f"<td>{row.get('time')}</td>"
            f"<td>{row.get('shares')}</td>"
            f"<td>{row.get('total_dollars')}</td>"
            f"<td>{row.get('avg_price')}</td>"
            f"<td>{row.get('intended_drop')}</td>"
            f"<td>{row.get('actual_drop')}</td>"
            "</tr>"
        )

    html.append("</tbody></table>")

    # Cycles table (filled by JS)
    html.append("<h2>Closed Trade Cycles</h2>")
    html.append(
        """
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Start</th>
          <th>End</th>
          <th>Buys</th>
          <th>Shares</th>
          <th>Avg Entry</th>
          <th>Avg Exit</th>
          <th>P/L $</th>
          <th>P/L %</th>
        </tr>
      </thead>
      <tbody id="cycles-body"></tbody>
    </table>
    """
    )

    # JS: refresh
    html.append(
        """
    <script>
    const REFRESH_MS = 15000;

    function fmtMoney(x) {
      if (x == null || x === "") return "";
      const n = Number(x);
      if (Number.isNaN(n)) return String(x);
      return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function fmtMoney0(x) {
      if (x == null || x === "") return "";
      const n = Number(x);
      if (Number.isNaN(n)) return String(x);
      return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
    }

    function num(x) {
      const n = Number(x);
      return Number.isFinite(n) ? n : null;
    }

    function setText(id, v) {
      const el = document.getElementById(id);
      if (el) el.textContent = (v ?? "");
    }

    async function refreshReport() {
      try {
        const res = await fetch("/report?t=" + Date.now(), { cache: "no-store" });
        const data = await res.json();
        if (!data.ok) return;

        setText("tsla-price", fmtMoney(data.tsla_price));
        setText("last-updated", data.server_time_ct);

        const ag = data.active_group || {};
        setText("ag-anchor-vwap", ag.anchor_vwap);
        setText("ag-anchor-time", ag.anchor_time);
        setText("ag-sell-target", ag.sell_target);
        setText("ag-distance-sell", ag.distance_to_sell);
        setText("ag-next-buy", ag.next_buy_price);
        setText("ag-distance-next", ag.distance_to_next_buy);
        setText("ag-buys", ag.buys_count);
        setText("ag-drop-inc", ag.drop_increment);

        const pos = data.position || {};
        if (pos && Object.keys(pos).length) {
          setText("pos-qty", pos.qty);
          setText("pos-avg", fmtMoney(pos.avg_entry));
          setText("pos-mv", fmtMoney(pos.market_value));
          setText("pos-upl", fmtMoney(pos.unrealized_pl));
        }

        // Potential Profit: (Sell Target - Avg Entry) * Qty
        const sellTarget = num(ag.sell_target);
        const avgEntry = num(pos.avg_entry);
        const qty = num(pos.qty);

        let perShare = "";
        let total = "";
        if (sellTarget != null && avgEntry != null && qty != null) {
          const ps = (sellTarget - avgEntry);
          perShare = fmtMoney(ps);
          total = fmtMoney(ps * qty);
        }
        setText("potential-per-share", perShare);
        setText("potential-profit", total);

        // Account
        const acct = data.account || {};
        const setAcct = (key, fmt0=false) => {
          const el = document.getElementById("acct-" + key);
          if (!el) return;
          el.textContent = fmt0 ? fmtMoney0(acct[key]) : fmtMoney(acct[key]);
        };

        setAcct("equity");
        setAcct("cash");
        setAcct("buying_power", true);
        setAcct("regt_buying_power", true);
        setAcct("daytrading_buying_power", true);
        setAcct("effective_buying_power", true);
        setAcct("non_marginable_buying_power", true);
        setAcct("long_market_value");
        setAcct("initial_margin");
        setAcct("maintenance_margin");

        // Ladder
        const ladderBody = document.getElementById("ladder-body");
        if (ladderBody) {
          ladderBody.innerHTML = "";

          const rows = data.active_group_triggers || [];

          if (ag && ag.next_buy_price != null && ag.buys_count != null) {
            const nextTrigger = Number(ag.buys_count) + 1;
            const intendedDropNext = Math.floor((nextTrigger - 1) / 5) + 1;

            const trW = document.createElement("tr");
            trW.innerHTML = `
              <td><b>${nextTrigger}</b></td>
              <td><b>WAITING</b></td>
              <td>12</td>
              <td>—</td>
              <td><b>${ag.next_buy_price}</b></td>
              <td><b>${intendedDropNext}</b></td>
              <td>—</td>
            `;
            ladderBody.appendChild(trW);
          }

          rows.forEach((r) => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
              <td>${r.trigger ?? ""}</td>
              <td>${r.time ?? ""}</td>
              <td>${r.shares ?? ""}</td>
              <td>${r.total_dollars ?? ""}</td>
              <td>${r.avg_price ?? ""}</td>
              <td>${r.intended_drop ?? ""}</td>
              <td>${r.actual_drop ?? ""}</td>
            `;
            ladderBody.appendChild(tr);
          });
        }

      } catch (e) {
        console.error("refreshReport exception", e);
      }
    }

    async function loadCycles() {
      try {
        const res = await fetch("/cycles?t=" + Date.now(), { cache: "no-store" });
        const data = await res.json();
        if (!data.ok) return;

        const tbody = document.getElementById("cycles-body");
        if (!tbody) return;

        tbody.innerHTML = "";
        data.cycles.forEach((c, i) => {
          const row = document.createElement("tr");
          row.innerHTML = `
            <td>${i + 1}</td>
            <td>${c.anchor_time ?? ""}</td>
            <td>${c.sell_time ?? ""}</td>
            <td>${c.buy_orders ?? ""}</td>
            <td>${c.shares ?? ""}</td>
            <td>${c.avg_entry != null ? "$" + c.avg_entry : ""}</td>
            <td>${c.avg_exit != null ? "$" + c.avg_exit : ""}</td>
            <td>${c.realized_pl != null ? "$" + c.realized_pl : ""}</td>
            <td>${c.realized_pl_pct != null ? c.realized_pl_pct + "%" : ""}</td>
          `;
          tbody.appendChild(row);
        });
      } catch (e) {
        console.error("loadCycles exception", e);
      }
    }

    refreshReport();
    loadCycles();

    setInterval(() => {
      refreshReport();
      loadCycles();
    }, REFRESH_MS);
    </script>
    """
    )

    html.append("</body></html>")
    return Response("\n".join(html), mimetype="text/html")


# Start the BUY/SELL push watcher when the app loads
start_fill_watcher()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
