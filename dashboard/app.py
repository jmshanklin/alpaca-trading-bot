import os
import time
from threading import Lock
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from alpaca_trade_api.rest import REST, TimeFrame
from alpaca_trade_api.rest import APIError


# ============================================================
# Timezones
# ============================================================
TZ_NY = ZoneInfo("America/New_York")      # RTH defined in NY time
TZ_CHI = ZoneInfo("America/Chicago")      # display timezone


# ============================================================
# Cache settings (Latest Bar)
# ============================================================
LATEST_BAR_CACHE_TTL = int(os.getenv("LATEST_BAR_CACHE_TTL", "8"))  # seconds
_latest_bar_cache = {"ts": 0.0, "data": None}
_latest_bar_lock = Lock()


# ============================================================
# App
# ============================================================
app = FastAPI()

# If this file is dashboard/main.py and your working dir is dashboard/, this is correct:
app.mount("/static", StaticFiles(directory="static"), name="static")


# ============================================================
# Helpers
# ============================================================
def _alpaca() -> REST:
    key = os.getenv("APCA_API_KEY_ID")
    secret = os.getenv("APCA_API_SECRET_KEY")
    base_url = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
    if not key or not secret:
        raise ValueError("Missing Alpaca API keys")
    return REST(key, secret, base_url)

def _symbol() -> str:
    return (os.getenv("ENGINE_SYMBOL") or "TSLA").upper()

def _feed() -> str:
    return (os.getenv("APCA_DATA_FEED") or "iex").lower()

def _sell_rise_usd() -> float:
    """
    Dollar amount ABOVE anchor (first buy of the current open cycle) for sell target.
    Default: 2.0
    """
    try:
        return float(os.getenv("SELL_RISE_USD", "2") or "2")
    except Exception:
        return 2.0

def _to_rfc3339_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _is_rth_ny(ts_utc: datetime) -> bool:
    ts_ny = ts_utc.astimezone(TZ_NY)
    minutes = ts_ny.hour * 60 + ts_ny.minute
    return (9 * 60 + 30) <= minutes <= (16 * 60)

def _fmt_ct(ts_utc: datetime) -> str:
    return ts_utc.astimezone(TZ_CHI).strftime("%-m/%-d, %-I:%M %p")

def _db_url() -> str:
    url = os.getenv("DATABASE_URL", "")
    if not url:
        raise ValueError("Missing DATABASE_URL")
    return url

def _db_conn():
    return psycopg2.connect(_db_url(), sslmode="require")

def _parse_dt(x):
    if x is None:
        return None
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    try:
        s = str(x).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _anchor_from_recent_fills(api: REST, symbol: str, lookback: int = 500):
    """
    Anchor = first BUY price after position goes from 0 -> >0.
    We reconstruct by walking fills oldest->newest and tracking running qty.
    Returns: (anchor_price, anchor_time_utc) or (None, None)
    """
    try:
        acts = api.list_activities(activity_types="FILL", limit=lookback)
    except Exception:
        return (None, None)

    fills = []
    for a in acts or []:
        try:
            if (getattr(a, "symbol", "") or "").upper() != symbol.upper():
                continue
            side = (getattr(a, "side", "") or "").lower()
            qty = float(getattr(a, "qty", 0) or 0)
            px = float(getattr(a, "price", 0) or 0)
            dt = _parse_dt(
                getattr(a, "transaction_time", None)
                or getattr(a, "time", None)
                or getattr(a, "filled_at", None)
            )
            if side not in ("buy", "sell") or qty <= 0 or px <= 0:
                continue
            fills.append((dt, side, qty, px))
        except Exception:
            continue

    fills.sort(key=lambda x: (x[0] or datetime(1970, 1, 1, tzinfo=timezone.utc)))

    running_qty = 0.0
    anchor_price = None
    anchor_time = None

    for (dt, side, qty, px) in fills:
        if side == "buy":
            if running_qty <= 0:
                anchor_price = px
                anchor_time = dt
            running_qty += qty
        else:
            running_qty = max(0.0, running_qty - qty)

    return (anchor_price, anchor_time)


# ============================================================
# Routes
# ============================================================
@app.get("/", response_class=HTMLResponse)
def home():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/version")
def version():
    return {
        "ok": True,
        "marker": "dashboard_v1_clean",
        "ttl": LATEST_BAR_CACHE_TTL,
        "symbol": _symbol(),
        "feed": _feed(),
        "tz_display": "America/Chicago",
        "rth_defined_in": "America/New_York",
    }

@app.get("/config")
def config():
    return {
        "ok": True,
        "symbol": _symbol(),
        "feed": _feed(),
        "base_url": os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets"),
        "has_key": bool(os.getenv("APCA_API_KEY_ID")),
        "has_secret": bool(os.getenv("APCA_API_SECRET_KEY")),
        "sell_rise_usd": _sell_rise_usd(),
    }

@app.get("/group_performance")
def group_performance(limit: int = 25):
    """
    One TSLA cycle (group_id) = one row.
    CLOSED iff (buy_qty > 0 AND sell_qty == buy_qty).
    PnL = sum(sells) - sum(buys), using est_price.
    Times shown in Chicago time.
    """
    symbol = _symbol()

    sql = """
    WITH base AS (
      SELECT
        ts_utc,
        symbol,
        side,
        qty::numeric AS qty,
        est_price::numeric AS est_price,
        group_id
      FROM trade_journal
      WHERE symbol = %s
        AND group_id IS NOT NULL
    ),
    agg AS (
      SELECT
        group_id,
        MIN(ts_utc) AS cycle_start_utc,
        MAX(ts_utc) AS cycle_last_utc,

        COALESCE(SUM(qty) FILTER (WHERE side = 'BUY'), 0) AS buy_qty,
        COALESCE(SUM(qty * est_price) FILTER (WHERE side = 'BUY'), 0) AS buy_notional,
        CASE
          WHEN COALESCE(SUM(qty) FILTER (WHERE side = 'BUY'), 0) > 0
          THEN COALESCE(SUM(qty * est_price) FILTER (WHERE side = 'BUY'), 0)
               / NULLIF(SUM(qty) FILTER (WHERE side = 'BUY'), 0)
          ELSE NULL
        END AS avg_buy_price,

        COALESCE(SUM(qty) FILTER (WHERE side = 'SELL'), 0) AS sell_qty,
        COALESCE(SUM(qty * est_price) FILTER (WHERE side = 'SELL'), 0) AS sell_notional,
        CASE
          WHEN COALESCE(SUM(qty) FILTER (WHERE side = 'SELL'), 0) > 0
          THEN COALESCE(SUM(qty * est_price) FILTER (WHERE side = 'SELL'), 0)
               / NULLIF(SUM(qty) FILTER (WHERE side = 'SELL'), 0)
          ELSE NULL
        END AS avg_sell_price,

        (COALESCE(SUM(qty * est_price) FILTER (WHERE side = 'SELL'), 0)
         - COALESCE(SUM(qty * est_price) FILTER (WHERE side = 'BUY'), 0)) AS pnl
      FROM base
      GROUP BY group_id
    )
    SELECT
      group_id,

      to_char(cycle_start_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago',
              'YYYY-MM-DD HH:MI:SS AM') AS cycle_start_ct,
      to_char(cycle_last_utc  AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago',
              'YYYY-MM-DD HH:MI:SS AM') AS cycle_last_ct,

      buy_qty, buy_notional, avg_buy_price,
      sell_qty, sell_notional, avg_sell_price,
      pnl,

      CASE
        WHEN buy_notional > 0 AND buy_qty > 0 AND sell_qty = buy_qty
          THEN ROUND((pnl / buy_notional) * 100.0, 2)
        ELSE NULL
      END AS pnl_pct,

      CASE
        WHEN buy_qty > 0 AND sell_qty = buy_qty THEN 'CLOSED'
        ELSE 'OPEN'
      END AS cycle_status,

      CASE
        WHEN buy_qty > 0 AND sell_qty = buy_qty AND pnl >= 0 THEN 'WIN'
        WHEN buy_qty > 0 AND sell_qty = buy_qty AND pnl < 0 THEN 'LOSS'
        ELSE NULL
      END AS win_loss

    FROM agg
    ORDER BY cycle_last_utc DESC
    LIMIT %s;
    """

    with _db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (symbol, limit))
            rows = cur.fetchall()

    return {"ok": True, "symbol": symbol, "rows": rows}


# ============================================================
# Latest Bar (last CLOSED bar; cached)
# ============================================================
@app.get("/latest_bar")
def latest_bar():
    now = time.time()

    with _latest_bar_lock:
        cached = _latest_bar_cache["data"]
        if cached is not None and (now - _latest_bar_cache["ts"]) < LATEST_BAR_CACHE_TTL:
            cached2 = dict(cached)
            cached2["cached"] = True
            return cached2

    api = _alpaca()
    symbol = _symbol()
    feed = _feed()

    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=7)

    start_rfc3339 = _to_rfc3339_z(start_utc)
    end_rfc3339 = _to_rfc3339_z(now_utc)

    try:
        bars = api.get_bars(
            symbol,
            TimeFrame.Minute,
            start=start_rfc3339,
            end=end_rfc3339,
            limit=1000,
            adjustment="raw",
            feed=feed,
        )
    except Exception as e:
        payload = {
            "ok": False,
            "symbol": symbol,
            "feed": feed,
            "error": "get_bars exception",
            "cached": False,
            "debug": {
                "start": start_rfc3339,
                "end": end_rfc3339,
                "exception_type": type(e).__name__,
                "exception_text": str(e),
            },
        }
        with _latest_bar_lock:
            _latest_bar_cache["ts"] = now
            _latest_bar_cache["data"] = payload
        return payload

    bars_list = list(bars) if bars else []
    if not bars_list:
        payload = {"ok": False, "symbol": symbol, "feed": feed, "error": "no bars returned", "cached": False}
        with _latest_bar_lock:
            _latest_bar_cache["ts"] = now
            _latest_bar_cache["data"] = payload
        return payload

    now_floor = now_utc.replace(second=0, microsecond=0)

    chosen = None
    for b in reversed(bars_list):
        bt = b.t
        if bt.tzinfo is None:
            bt = bt.replace(tzinfo=timezone.utc)
        if bt < now_floor:
            chosen = b
            break

    if not chosen:
        payload = {"ok": False, "symbol": symbol, "feed": feed, "error": "no closed bar found", "cached": False}
        with _latest_bar_lock:
            _latest_bar_cache["ts"] = now
            _latest_bar_cache["data"] = payload
        return payload

    t_utc = chosen.t if chosen.t.tzinfo else chosen.t.replace(tzinfo=timezone.utc)

    payload = {
        "ok": True,
        "symbol": symbol,
        "feed": feed,
        "t": t_utc.isoformat(),
        "t_ct": _fmt_ct(t_utc),
        "o": float(chosen.o),
        "h": float(chosen.h),
        "l": float(chosen.l),
        "c": float(chosen.c),
        "v": float(chosen.v or 0),
        "cached": False,
    }

    with _latest_bar_lock:
        _latest_bar_cache["ts"] = now
        _latest_bar_cache["data"] = payload

    return payload


# ============================================================
# Historical Bars (RTH only)
# ============================================================
@app.get("/bars")
def bars(limit: int = 300):
    api = _alpaca()
    symbol = _symbol()
    feed = _feed()

    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=14)

    start_rfc3339 = _to_rfc3339_z(start_utc)
    end_rfc3339 = _to_rfc3339_z(now_utc)

    try:
        raw_bars = api.get_bars(
            symbol,
            TimeFrame.Minute,
            start=start_rfc3339,
            end=end_rfc3339,
            limit=10000,
            adjustment="raw",
            feed=feed,
        )
    except Exception as e:
        return {
            "ok": False,
            "symbol": symbol,
            "feed": feed,
            "error": "get_bars exception",
            "debug": {"exception_type": type(e).__name__, "exception_text": str(e)},
        }

    bars_list = list(raw_bars) if raw_bars else []
    if not bars_list:
        return {"ok": False, "symbol": symbol, "feed": feed, "error": "no bars returned"}

    out = []
    for b in bars_list:
        ts = b.t
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        if not _is_rth_ny(ts):
            continue

        out.append({
            "time": int(ts.timestamp()),
            "open": float(b.o),
            "high": float(b.h),
            "low": float(b.l),
            "close": float(b.c),
        })

    out = out[-limit:]

    if not out:
        return {"ok": False, "symbol": symbol, "feed": feed, "error": "no RTH bars returned"}

    last_ts_utc = datetime.fromtimestamp(out[-1]["time"], tz=timezone.utc)

    return {
        "ok": True,
        "symbol": symbol,
        "feed": feed,
        "bars": out,
        "bars_count": len(out),
        "last_ct": _fmt_ct(last_ts_utc),
    }


# ============================================================
# Position (Avg Entry + Anchor + Sell Target)
# ============================================================
@app.get("/position")
def position():
    api = _alpaca()
    symbol = _symbol()
    sell_rise_usd = _sell_rise_usd()

    try:
        pos = api.get_position(symbol)
    except Exception:
        return {
            "ok": True,
            "symbol": symbol,
            "qty": 0,
            "avg_entry": None,
            "anchor_price": None,
            "anchor_time_utc": None,
            "sell_rise_usd": sell_rise_usd,
            "sell_target": None,
        }

    qty = float(pos.qty)
    avg_entry = float(pos.avg_entry_price) if qty > 0 else None

    anchor_price = None
    anchor_time = None

    if qty > 0:
        ap, at = _anchor_from_recent_fills(api, symbol, lookback=500)
        anchor_price, anchor_time = ap, at

    sell_target = (anchor_price + sell_rise_usd) if (qty > 0 and anchor_price is not None) else None

    return {
        "ok": True,
        "symbol": symbol,
        "qty": qty,
        "avg_entry": avg_entry,
        "anchor_price": anchor_price,
        "anchor_time_utc": anchor_time.isoformat() if anchor_time else None,
        "sell_rise_usd": sell_rise_usd,
        "sell_target": sell_target,
        "market_price": float(pos.current_price),
        "unrealized_pl": float(pos.unrealized_pl),
        "unrealized_plpc": float(pos.unrealized_plpc),
    }


# ============================================================
# Fills (Closed orders w/ fills, filtered to symbol)
# ============================================================
@app.get("/fills")
def fills(limit: int = 200):
    api = _alpaca()
    symbol = _symbol()

    try:
        orders = api.list_orders(
            status="closed",
            limit=limit,
            nested=True,
            direction="desc",
        )
    except APIError as e:
        return {"ok": False, "symbol": symbol, "error": str(e)}

    out = []
    for o in orders:
        if (getattr(o, "symbol", "") or "").upper() != symbol:
            continue
        if getattr(o, "filled_at", None) is None:
            continue

        filled_qty = float(getattr(o, "filled_qty", 0) or 0)
        if filled_qty <= 0:
            continue

        filled_avg_price = float(getattr(o, "filled_avg_price", 0) or 0)

        fa = getattr(o, "filled_at", None)
        filled_at = fa if isinstance(fa, str) else fa.isoformat()

        out.append({
            "id": getattr(o, "id", None),
            "symbol": symbol,
            "side": (getattr(o, "side", "") or "").lower(),
            "filled_at": filled_at,
            "filled_qty": filled_qty,
            "filled_avg_price": filled_avg_price,
        })

    return {"ok": True, "symbol": symbol, "fills": out}
