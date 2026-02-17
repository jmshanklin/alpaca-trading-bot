import os
import json
import time
import threading
import atexit
from datetime import datetime, timedelta
try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except Exception as e:
    psycopg2 = None
    HAS_PSYCOPG2 = False
    print("psycopg2 not available:", e, flush=True)

import requests
import alpaca_trade_api as tradeapi
import pytz
from flask import Flask, Response, jsonify

app = Flask(__name__)

# ---------------------------------------------------------
# DIAGNOSTIC ROUTE — proves THIS file is running on Render
# ---------------------------------------------------------
@app.route("/diag")
def diag():
    return jsonify({
        "ok": True,
        "service": "alpaca-report",
        "file": "engine/report_app.py",
        "time_utc": datetime.utcnow().isoformat() + "Z",

        # watcher state
        "watcher_started": WATCHER_STATUS.get("started"),
        "watcher_initialized": WATCHER_STATUS.get("initialized"),
        "watcher_last_seen_time": WATCHER_STATUS.get("last_seen_time"),
        "watcher_last_error": WATCHER_STATUS.get("last_error"),

        # push settings
        "ENABLE_PUSH_ALERTS": ENABLE_PUSH_ALERTS,
        "has_PUSHOVER_USER_KEY": bool(PUSHOVER_USER_KEY),
        "has_PUSHOVER_APP_TOKEN": bool(PUSHOVER_APP_TOKEN),

        # bot sell detection config
        "BOT_SELL_CLIENT_PREFIXES": BOT_SELL_CLIENT_PREFIXES,

        # Alpaca endpoint info
        "alpaca_base_url": BASE_URL,
        "alpaca_key_loaded": bool(API_KEY),
    })
    
@app.route("/pid")
def pid():
    return jsonify({
        "ok": True,
        "pid": os.getpid(),
    })

@app.route("/gunicorn")
def gunicorn_info():
    return jsonify({
        "ok": True,
        "pid": os.getpid(),
        "WEB_CONCURRENCY": os.getenv("WEB_CONCURRENCY"),
        "GUNICORN_CMD_ARGS": os.getenv("GUNICORN_CMD_ARGS"),
    })

# --- Alpaca connection -------------
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")

# --- Postgres connection (shared with engine) ---
DATABASE_URL = os.getenv("DATABASE_URL")

def db_connect():
    """
    Returns a psycopg2 connection or None if DATABASE_URL not set.
    Render Postgres typically requires sslmode=require.
    """
    if not DATABASE_URL:
        return None
    return psycopg2.connect(DATABASE_URL, sslmode="require")
    
def db_connect():
    if (not HAS_PSYCOPG2) or (not DATABASE_URL):
        return None

    conn = psycopg2.connect(DATABASE_URL, sslmode="require")

    # ✅ Force unqualified table names like "trade_journal" to resolve to public.trade_journal
    with conn.cursor() as cur:
        cur.execute("SET search_path TO public;")
    return conn

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
    ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
    ts = _normalize_ts(ts)
    return ts or datetime.min


def _fill_unique_id(act):
    # activity id is best; fallback to order_id
    return str(_get_attr(act, "id") or _get_attr(act, "activity_id") or _get_attr(act, "order_id") or "")


def _watch_fills_and_push(symbol="TSLA", poll_seconds=15):
    global WATCHER_STATUS

    state = _load_push_state()
    initialized = bool(state.get("initialized", False) or state.get("last_seen_time"))

    last_seen_id = state.get("last_seen_id")
    last_seen_time = state.get("last_seen_time")  # ISO string
    
    # --- Sync in-memory status from persisted state on startup ---
    WATCHER_STATUS["initialized"] = bool(initialized)
    WATCHER_STATUS["last_seen_id"] = last_seen_id
    WATCHER_STATUS["last_seen_time"] = last_seen_time
    WATCHER_STATUS["started"] = True

    while True:
        try:
            after = (datetime.utcnow() - timedelta(days=10)).isoformat() + "Z"
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
                last_dt = _parse_iso_time(last_seen_time) if isinstance(last_seen_time, str) else None

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
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _normalize_ts(ts):
    """
    Normalize Alpaca timestamps so comparisons never crash.
    - strings -> datetime
    - pandas Timestamp -> python datetime
    - datetime passes through
    """
    if ts is None:
        return None
    if isinstance(ts, str):
        return _parse_iso_time(ts) or None
    # pandas Timestamp has to_pydatetime()
    if hasattr(ts, "to_pydatetime"):
        try:
            return ts.to_pydatetime()
        except Exception:
            return ts
    return ts


def to_central(ts):
    """UTC -> Central Time formatted string."""
    if not ts:
        return None
    try:
        utc = pytz.utc
        central = pytz.timezone("US/Central")
        if ts.tzinfo is None:
            ts = utc.localize(ts)
        ct_time = ts.astimezone(central)
        return ct_time.strftime("%b %d, %Y %I:%M:%S %p CT")
    except Exception:
        return str(ts)


def fmt_ct_any(ts):
    """Accepts datetime OR ISO string."""
    if ts is None:
        return None
    if isinstance(ts, str):
        dt = _parse_iso_time(ts)
        return to_central(dt) if dt else ts
    return to_central(ts)


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


# -----------------------------------------------
# ---------Bot sell / buy detection--------------
# -----------------------------------------------
# Your bot uses: client_order_id = f"grid-sell-{cfg.symbol}-..."
# For TSLA that becomes: "grid-sell-TSLA-..."
BOT_SELL_CLIENT_PREFIXES = [
    p.strip()
    for p in (os.getenv("BOT_SELL_CLIENT_PREFIXES", "grid-sell-TSLA-") or "").split(",")
    if p.strip()
]

BOT_BUY_CLIENT_PREFIXES = [
    p.strip()
    for p in (os.getenv("BOT_BUY_CLIENT_PREFIXES", "grid-buy-TSLA-") or "").split(",")
    if p.strip()
]

# Small in-memory cache to reduce repeated order lookups
_ORDER_CLIENT_ID_CACHE = {}  # order_id -> client_order_id (or None)
_ORDER_CLIENT_ID_CACHE_MAX = 500


def _get_client_order_id_for_order(order_id: str):
    if not order_id:
        return None

    if order_id in _ORDER_CLIENT_ID_CACHE:
        return _ORDER_CLIENT_ID_CACHE.get(order_id)

    try:
        o = api.get_order(order_id)
        cid = _get_attr(o, "client_order_id")
    except Exception:
        cid = None

    # simple bounded cache
    if len(_ORDER_CLIENT_ID_CACHE) >= _ORDER_CLIENT_ID_CACHE_MAX:
        try:
            _ORDER_CLIENT_ID_CACHE.pop(next(iter(_ORDER_CLIENT_ID_CACHE)))
        except Exception:
            _ORDER_CLIENT_ID_CACHE.clear()

    _ORDER_CLIENT_ID_CACHE[order_id] = cid
    return cid

def _is_bot_sell_fill(act):
    """
    True only when this SELL fill belongs to the bot's SELL-ALL order,
    identified by client_order_id prefix.
    """
    try:
        if _get_attr(act, "symbol") != "TSLA":
            return False
        if _get_attr(act, "side") != "sell":
            return False
        order_id = _get_attr(act, "order_id")
        cid = _get_client_order_id_for_order(str(order_id)) if order_id else None
        if not cid:
            return False
        for pfx in BOT_SELL_CLIENT_PREFIXES:
            if cid.startswith(pfx):
                return True
        return False
    except Exception:
        return False
        
def _is_bot_buy_fill(act):
    """
    True only when this BUY fill belongs to the bot's BUY order,
    identified by client_order_id prefix.
    """
    try:
        if _get_attr(act, "symbol") != "TSLA":
            return False
        if _get_attr(act, "side") != "buy":
            return False
        order_id = _get_attr(act, "order_id")
        cid = _get_client_order_id_for_order(str(order_id)) if order_id else None
        if not cid:
            return False
        for pfx in BOT_BUY_CLIENT_PREFIXES:
            if cid.startswith(pfx):
                return True
        return False
    except Exception:
        return False
        
def fetch_active_bot_group_from_db(symbol="TSLA"):
    """
    Uses public.trade_journal written by engine.py.
    Returns:
      group_id (str|None),
      buys (list of dict) oldest-first with est_price, qty, ts_utc,
      last_bot_sell_ts (datetime|None)
    """
    conn = db_connect()
    if conn is None:
        return None, [], None

    buy_prefix_like = f"grid-buy-{symbol}-%"
    sell_prefix_like = f"grid-sell-{symbol}-%"

    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # pick most recent group that has bot BUYs and is not closed by a later bot SELL
                cur.execute(
                    """
                    WITH last_buy AS (
                      SELECT group_id, MAX(ts_utc) AS last_buy_ts
                      FROM trade_journal
                      WHERE symbol=%s
                        AND side='BUY'
                        AND client_order_id LIKE %s
                      GROUP BY group_id
                    ),
                    last_sell AS (
                      SELECT group_id, MAX(ts_utc) AS last_sell_ts
                      FROM trade_journal
                      WHERE symbol=%s
                        AND side='SELL'
                        AND client_order_id LIKE %s
                      GROUP BY group_id
                    )
                    SELECT b.group_id, b.last_buy_ts, s.last_sell_ts
                    FROM last_buy b
                    LEFT JOIN last_sell s ON s.group_id=b.group_id
                    WHERE s.last_sell_ts IS NULL OR s.last_sell_ts < b.last_buy_ts
                    ORDER BY b.last_buy_ts DESC
                    LIMIT 1;
                    """,
                    (symbol, buy_prefix_like, symbol, sell_prefix_like),
                )
                row = cur.fetchone()
                if not row:
                    return None, [], None

                group_id = row["group_id"]

                cur.execute(
                    """
                    SELECT ts_utc, qty, est_price, order_id, client_order_id
                    FROM trade_journal
                    WHERE symbol=%s
                      AND group_id=%s
                      AND side='BUY'
                      AND client_order_id LIKE %s
                    ORDER BY ts_utc ASC;
                    """,
                    (symbol, group_id, buy_prefix_like),
                )
                buys = [dict(r) for r in cur.fetchall()]

                cur.execute(
                    """
                    SELECT MAX(ts_utc) AS last_sell_ts
                    FROM trade_journal
                    WHERE symbol=%s
                      AND side='SELL'
                      AND client_order_id LIKE %s;
                    """,
                    (symbol, sell_prefix_like),
                )
                last_sell = cur.fetchone()
                last_sell_ts = last_sell["last_sell_ts"] if last_sell else None

                return group_id, buys, last_sell_ts
    finally:
        try:
            conn.close()
        except Exception:
            pass

def fetch_active_bot_group_from_db(symbol="TSLA"):
    """
    Returns:
      group_id (str|None),
      buys (list of dict) oldest-first with est_price, qty, ts_utc,
      last_bot_sell_ts (datetime|None)
    """
    conn = db_connect()
    if conn is None:
        return None, [], None

    buy_pfx = "grid-buy-" + symbol + "-"
    sell_pfx = "grid-sell-" + symbol + "-"

    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Find the most recent group_id that has a bot BUY and has NOT been closed by a bot SELL
            cur.execute(
                """
                WITH last_buy AS (
                  SELECT group_id, MAX(ts_utc) AS last_buy_ts
                  FROM trade_journal
                  WHERE symbol=%s
                    AND side='BUY'
                    AND client_order_id LIKE %s
                  GROUP BY group_id
                ),
                last_sell AS (
                  SELECT group_id, MAX(ts_utc) AS last_sell_ts
                  FROM trade_journal
                  WHERE symbol=%s
                    AND side='SELL'
                    AND client_order_id LIKE %s
                  GROUP BY group_id
                )
                SELECT b.group_id, b.last_buy_ts, s.last_sell_ts
                FROM last_buy b
                LEFT JOIN last_sell s ON s.group_id=b.group_id
                WHERE s.last_sell_ts IS NULL OR s.last_sell_ts < b.last_buy_ts
                ORDER BY b.last_buy_ts DESC
                LIMIT 1;
                """,
                (symbol, buy_pfx + "%", symbol, sell_pfx + "%"),
            )
            row = cur.fetchone()
            if not row:
                return None, [], None

            group_id = row["group_id"]

            # Get all bot BUY journal rows for this group, oldest-first
            cur.execute(
                """
                SELECT ts_utc, qty, est_price, order_id, client_order_id
                FROM trade_journal
                WHERE symbol=%s
                  AND group_id=%s
                  AND side='BUY'
                  AND client_order_id LIKE %s
                ORDER BY ts_utc ASC;
                """,
                (symbol, group_id, buy_pfx + "%"),
            )
            buys = [dict(r) for r in cur.fetchall()]

            # Last bot sell time overall (optional; for debugging/labels)
            cur.execute(
                """
                SELECT MAX(ts_utc) AS last_sell_ts
                FROM trade_journal
                WHERE symbol=%s
                  AND side='SELL'
                  AND client_order_id LIKE %s;
                """,
                (symbol, sell_pfx + "%"),
            )
            last_sell = cur.fetchone()
            last_sell_ts = last_sell["last_sell_ts"] if last_sell else None

    try:
        conn.close()
    except Exception:
        pass

    return group_id, buys, last_sell_ts
    
def build_ladder_from_journal_buys(buys):
    """
    buys: oldest-first journal rows with est_price, qty, ts_utc
    Returns newest-first ladder rows where "avg_price" is actually trigger price.
    """
    tmp = []
    prev_price = None

    for i, b in enumerate(buys, start=1):
        p = float(b["est_price"]) if b.get("est_price") is not None else None

        actual_drop = round(prev_price - p, 4) if (prev_price is not None and p is not None) else None
        intended_drop = ((i - 1) // 5) + 1 if i > 1 else None

        ts = b.get("ts_utc")
        # ts_utc from psycopg2 is usually a datetime already; fmt_ct_any accepts datetime or iso
        time_disp = fmt_ct_any(ts) if ts is not None else None

        tmp.append(
            {
                "trigger": i,
                "time": time_disp,
                "shares": int(b.get("qty") or 0),
                "avg_price": round(p, 4) if p is not None else None,  # this is TRIGGER price now
                "total_dollars": round((p * float(b.get("qty") or 0)), 2) if p is not None else None,
                "actual_drop": actual_drop,
                "intended_drop": intended_drop,
                "order_id": b.get("order_id"),
            }
        )
        prev_price = p

    return list(reversed(tmp))  # newest-first for your UI

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

        ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
        ts = _normalize_ts(ts)

        if oid is None:
            oid = f"noid:{symbol}:{side}:{ts}:{price}:{qty}"

        g = grouped.setdefault(
            oid,
            {"order_id": oid, "time": ts, "side": side, "symbol": symbol, "filled_qty": 0.0, "pv": 0.0},
        )

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
        t = g["time"]
        rows.append(
            {
                "time": t.isoformat() if hasattr(t, "isoformat") else str(t),
                "symbol": g["symbol"],
                "side": g["side"],
                "filled_qty": int(round(g["filled_qty"])),
                "vwap": round(float(vwap), 4),
                "total_dollars": round(float(g["pv"]), 2),
                "order_id": g["order_id"],
            }
        )

    rows.sort(key=lambda r: r["time"], reverse=True)  # newest-first
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

        ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
        ts = _normalize_ts(ts)

        if oid is None:
            oid = f"noid:{symbol}:{side}:{ts}:{price}:{qty}"

        g = grouped.setdefault(
            oid,
            {"order_id": oid, "time": ts, "side": side, "symbol": symbol, "filled_qty": 0.0, "pv": 0.0},
        )

        g["filled_qty"] += qty
        g["pv"] += qty * price

        # keep earliest time
        if ts and g["time"] and ts < g["time"]:
            g["time"] = ts

    rows = []
    for _, g in grouped.items():
        if g["filled_qty"] <= 0:
            continue
        vwap = g["pv"] / g["filled_qty"]
        t = g["time"]
        rows.append(
            {
                "time": t.isoformat() if hasattr(t, "isoformat") else str(t),
                "symbol": g["symbol"],
                "side": g["side"],
                "filled_qty": int(round(g["filled_qty"])),
                "vwap": round(float(vwap), 4),
                "total_dollars": round(float(g["pv"]), 2),
                "order_id": g["order_id"],
            }
        )

    rows.sort(key=lambda r: r["time"], reverse=True)  # newest-first
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
        raw = c.get("sell_time_raw")
        if isinstance(raw, str):
            dt = _parse_iso_time(raw)
            return dt or datetime.min
        return raw or datetime.min

    cycles.sort(key=_sell_sort_key, reverse=True)  # newest-first
    return cycles


def compute_cycles(days=30, symbol="TSLA"):
    after = (datetime.utcnow() - timedelta(days=days)).isoformat() + "Z"
    acts = api.get_activities(activity_types="FILL", after=after)
    orders = aggregate_fills_all_sides_by_order_id(acts, only_symbol=symbol)
    return build_trade_cycles_from_order_rows(orders)


def find_last_tsla_bot_sell_time(activities):
    """
    Return timestamp of most recent TSLA SELL fill that belongs to the BOT's
    sell-all order (client_order_id prefix match). Manual sells are ignored.
    """
    last = None
    for act in activities:
        ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
        ts = _normalize_ts(ts)
        if not ts:
            continue
        if _is_bot_sell_fill(act):
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
    state = _load_push_state()
    initialized = bool(state.get("initialized") or state.get("last_seen_time"))
    return jsonify({
        "ok": True,
        "watcher": {
            "started": True,  # means the service is up; watcher may be in another worker
            "initialized": initialized,
            "last_seen_time": state.get("last_seen_time"),
            "last_seen_id": state.get("last_seen_id"),
            "last_error": WATCHER_STATUS.get("last_error"),
        }
    })

@app.route("/watcher_debug")
def watcher_debug():
    """
    TEMP debug:
    Shows what the watcher is seeing and how it decides what's "new".
    """
    try:
        after = (datetime.utcnow() - timedelta(days=10)).isoformat() + "Z"
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

        last_dt = _parse_iso_time(last_seen_time) if isinstance(last_seen_time, str) else None

        top = []
        new_candidates = []

        for a in fills[:15]:
            aid = _fill_unique_id(a)
            atime = _get_fill_time(a)

            order_id = _get_attr(a, "order_id")
            cid = _get_client_order_id_for_order(str(order_id)) if order_id else None

            row = {
                "id": aid,
                "time_utc": atime.isoformat() if hasattr(atime, "isoformat") else str(atime),
                "time_ct": to_central(atime),
                "side": _get_attr(a, "side"),
                "qty": _get_attr(a, "qty"),
                "price": _get_attr(a, "price"),
                "order_id": str(order_id) if order_id else None,
                "client_order_id": cid,
                "is_bot_sell": _is_bot_sell_fill(a),
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
                "bot_sell_prefixes": BOT_SELL_CLIENT_PREFIXES,
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

        # If you do NOT currently hold TSLA, then there is no "active group" to show.
        # This avoids confusing displays after manual full exits.
        if position_data is None or float(position_data.get("qty", 0) or 0) <= 0:
            data["active_group"] = None
            data["active_group_triggers"] = []
            data["active_group_last_sell_time"] = None
            return jsonify(data)

        # --- Active Group (from DB journal; trigger-to-trigger truth) ---
        try:
            group_id, buys, last_bot_sell_ts = fetch_active_bot_group_from_db("TSLA")
        
            active_group = None
            active_group_triggers = []
        
            if buys:
                # buys is oldest-first journal rows with est_price + qty
                active_group_triggers = build_ladder_from_journal_buys(buys)  # newest-first for UI
        
                # Anchor = FIRST bot buy trigger price in this group
                anchor_price = float(buys[0]["est_price"]) if buys[0].get("est_price") is not None else None
                group_start_time = buys[0].get("ts_utc")
        
                # Current price from position if available, else fallback
                current_price = None
                if position_data and position_data.get("current_price") is not None:
                    current_price = float(position_data["current_price"])
                elif tsla_price is not None:
                    current_price = float(tsla_price)
        
                # Strategy settings (keep these aligned with engine env if/when you want)
                BUY_QTY = 12
                SELL_OFFSET = 2.0
                FIRST_INCREMENT_COUNT = 5
        
                buys_count = len(buys)
        
                # Stage math (your existing scheme)
                stage = (buys_count - 1) // FIRST_INCREMENT_COUNT + 1
                drop_increment = float(stage)
                buys_in_this_stage = (buys_count - 1) % FIRST_INCREMENT_COUNT + 1
        
                # Compute next buy price (same logic you had, but based on anchor trigger price)
                completed_drops = 0.0
                full_stages = (buys_count - 1) // FIRST_INCREMENT_COUNT
                for s in range(1, full_stages + 1):
                    completed_drops += s * FIRST_INCREMENT_COUNT
                partial = (buys_count - 1) % FIRST_INCREMENT_COUNT
                completed_drops += stage * partial
                next_buy_price = (anchor_price - (completed_drops + stage)) if anchor_price is not None else None
        
                sell_target = (anchor_price + SELL_OFFSET) if anchor_price is not None else None
        
                distance_to_sell = None
                distance_to_next_buy = None
                if current_price is not None and sell_target is not None:
                    distance_to_sell = round(sell_target - current_price, 4)
                if current_price is not None and next_buy_price is not None:
                    distance_to_next_buy = round(current_price - next_buy_price, 4)
        
                # Keep the same keys your UI expects
                active_group = {
                    "group_start_time": fmt_ct_any(group_start_time),
                    "anchor_vwap": round(anchor_price, 4) if anchor_price is not None else None,  # now anchor TRIGGER price
                    "sell_target": round(sell_target, 4) if sell_target is not None else None,
                    "buys_count": buys_count,
                    "drop_increment": drop_increment,
                    "buys_in_this_increment": buys_in_this_stage,
                    "next_buy_price": round(next_buy_price, 4) if next_buy_price is not None else None,
                    "current_price": round(current_price, 4) if current_price is not None else None,
                    "distance_to_sell": distance_to_sell,
                    "distance_to_next_buy": distance_to_next_buy,
                    "anchor_time": fmt_ct_any(group_start_time),
                    "anchor_order_id": buys[0].get("order_id"),
                    "buy_qty": BUY_QTY,
                }
        
            data["active_group"] = active_group
            data["active_group_triggers"] = active_group_triggers
            data["active_group_last_sell_time"] = (
                last_bot_sell_ts.isoformat() if last_bot_sell_ts else None
            )
        
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
    - Ladder (filled by JS)
    - Cycles (filled by JS)
    Auto-refresh: fetch /report + /cycles every 15 seconds
    """

    # IMPORTANT:
    # /table MUST return fast and MUST NOT call Alpaca.
    # The browser will fetch /report and /cycles via JS.

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
    html.append("<div class='price-banner'>$<span id='tsla-price'></span></div>")
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

    def acct_row(label, key):
        return (
            "<div class='grid-row'>"
            f"<div class='grid-label'>{label}</div>"
            f"<div class='grid-value'>$<span id='acct-{key}'></span></div>"
            "</div>"
        )

    html.append(acct_row("Equity", "equity"))
    html.append(acct_row("Cash", "cash"))
    html.append(acct_row("Buying Power", "buying_power"))
    html.append(acct_row("RegT Buying Power", "regt_buying_power"))
    html.append(acct_row("Day Trading Buying Power", "daytrading_buying_power"))
    html.append(acct_row("Effective Buying Power", "effective_buying_power"))
    html.append(acct_row("Non-Marginable Buying Power", "non_marginable_buying_power"))

    html.append("<div class='section-gap'></div>")
    html.append(acct_row("Long Market Value", "long_market_value"))
    html.append(acct_row("Initial Margin", "initial_margin"))
    html.append(acct_row("Maintenance Margin", "maintenance_margin"))
    html.append("</div>")  # end Account box

    # Summary box
    html.append("<div class='box summary-grid'>")
    html.append("<div class='box-title'>Summary</div>")
    html.append("<div class='box-sub'>Bot status + ladder metrics (updates every 15 seconds).</div>")

    def sum_row(label, value_id, prefix=""):
        return (
            "<div class='grid-row'>"
            f"<div class='grid-label'>{label}</div>"
            f"<div class='grid-value'>{prefix}<span id='{value_id}'></span></div>"
            "</div>"
        )

    html.append(sum_row("Last updated", "last-updated"))
    html.append("<div class='section-gap'></div>")
    html.append(sum_row("Anchor", "ag-anchor-vwap"))
    html.append(sum_row("Anchor time", "ag-anchor-time"))
    html.append(sum_row("Sell Target", "ag-sell-target"))
    html.append(sum_row("Distance to Sell", "ag-distance-sell"))
    html.append(sum_row("Next Buy", "ag-next-buy"))
    html.append(sum_row("Distance to Next Buy", "ag-distance-next"))
    html.append(sum_row("Buys", "ag-buys"))
    html.append(sum_row("Drop Increment", "ag-drop-inc"))

    html.append("<div class='section-gap'></div>")
    html.append("<div class='box-title'>TSLA Position</div>")
    html.append(sum_row("Qty", "pos-qty"))
    html.append(sum_row("Avg Entry", "pos-avg", prefix="$"))
    html.append(sum_row("Mkt Value", "pos-mv", prefix="$"))
    html.append(sum_row("uP/L", "pos-upl", prefix="$"))

    html.append("</div>")  # end Summary box
    html.append("</div>")  # end top-row

    # Ladder table (JS fills tbody)
    html.append("<h2>Active Group Ladder</h2>")
    html.append("<table>")
    html.append(
        "<thead><tr>"
        "<th>#</th><th>Time (CT)</th><th>Shares</th><th>Total $</th><th>Trigger Price</th><th>Intended Drop</th><th>Actual Drop (Trigger)</th>"
        "</tr></thead>"
    )

    html.append("<tbody id='ladder-body'></tbody>")
    html.append("</table>")

    # Cycles table (JS fills tbody)
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

    # JS: refresh (same logic as before; pulls /report and /cycles)
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
        } else {
          setText("pos-qty", "");
          setText("pos-avg", "");
          setText("pos-mv", "");
          setText("pos-upl", "");
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
          const buyQty = (ag.buy_qty ?? 12);

          if (ag && ag.next_buy_price != null && ag.buys_count != null) {
            const nextTrigger = Number(ag.buys_count) + 1;
            const intendedDropNext = Math.floor((nextTrigger - 1) / 5) + 1;

            const trW = document.createElement("tr");
            trW.innerHTML = `
              <td><b>${nextTrigger}</b></td>
              <td><b>WAITING</b></td>
              <td>${buyQty}</td>
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
        (data.cycles || []).forEach((c, i) => {
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

@app.route("/pushover_status")
def pushover_status():
    state = _load_push_state()
    return jsonify(
        {
            "ok": True,
            "watcher": {
                **WATCHER_STATUS,
                "initialized": bool(state.get("initialized", False) or state.get("last_seen_time")),
                "last_seen_id": state.get("last_seen_id"),
                "last_seen_time": state.get("last_seen_time"),
            },
            "push_enabled": ENABLE_PUSH_ALERTS,
            "has_user_key": bool(PUSHOVER_USER_KEY),
            "has_app_token": bool(PUSHOVER_APP_TOKEN),
            "bot_sell_prefixes": BOT_SELL_CLIENT_PREFIXES,
        }
    )
      
# -------------------------
# Watcher singleton (prevents duplicate watchers under gunicorn -w N)
# -------------------------
_WATCHER_LOCK_PATH = os.getenv("WATCHER_LOCK_PATH", "/tmp/tsla_fill_watcher.lock")
_WATCHER_LOCK_OWNER = False  # True only in the process that owns the lock


def _pid_is_alive(pid: int) -> bool:
    """Linux/Unix: os.kill(pid, 0) checks existence without killing."""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # If we can't check it, assume alive to avoid starting duplicates
        return True
    except Exception:
        return True


def _release_watcher_lock():
    """Remove lock file on clean shutdown if we own it."""
    global _WATCHER_LOCK_OWNER
    if not _WATCHER_LOCK_OWNER:
        return
    try:
        os.remove(_WATCHER_LOCK_PATH)
        print("Watcher lock released")
    except FileNotFoundError:
        pass
    except Exception as e:
        print("Watcher lock release error:", e)
    _WATCHER_LOCK_OWNER = False


def _acquire_watcher_lock() -> bool:
    """
    Return True only in ONE process.
    Uses atomic create to claim the lock.
    If a stale/bad lock exists, remove it and try again.
    """
    global _WATCHER_LOCK_OWNER

    # If lock exists, see if it's stale/bad
    if os.path.exists(_WATCHER_LOCK_PATH):
        try:
            with open(_WATCHER_LOCK_PATH, "r", encoding="utf-8") as f:
                s = (f.read() or "").strip()
            old_pid = int(s) if s.isdigit() else None
        except Exception:
            old_pid = None

        if old_pid is None:
            # unreadable/garbage lock -> treat as stale
            try:
                os.remove(_WATCHER_LOCK_PATH)
                print("Bad watcher lock removed (unreadable pid)")
            except Exception as e:
                print("Failed removing bad watcher lock:", e)
                return False

        elif not _pid_is_alive(old_pid):
            # stale lock -> remove it
            try:
                os.remove(_WATCHER_LOCK_PATH)
                print(f"Stale watcher lock removed (old pid {old_pid})")
            except Exception as e:
                print("Failed removing stale watcher lock:", e)
                return False

        else:
            # lock exists and looks alive -> do not start
            return False

    # Try to create lock atomically
    try:
        fd = os.open(_WATCHER_LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(fd, str(os.getpid()).encode("utf-8"))
        finally:
            os.close(fd)

        _WATCHER_LOCK_OWNER = True
        atexit.register(_release_watcher_lock)
        return True

    except FileExistsError:
        return False
    except Exception as e:
        print("Watcher lock error:", e)
        return False


def start_fill_watcher_singleton():
    """
    Starts the watcher only if this process wins the lock.
    """
    if _acquire_watcher_lock():
        print("Watcher lock acquired -> starting fill watcher")
        start_fill_watcher()
    else:
        print("Watcher lock NOT acquired -> watcher will NOT start in this worker")


# Start the BUY/SELL push watcher when the app loads
if os.getenv("START_WATCHER_ON_BOOT", "0") == "1":
    start_fill_watcher_singleton()
else:
    print("Watcher NOT started (START_WATCHER_ON_BOOT != 1).", flush=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
