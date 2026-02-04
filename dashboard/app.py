import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from threading import Lock
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from alpaca_trade_api.rest import REST, TimeFrame
from alpaca_trade_api.rest import APIError

# ============================================================
# Timezones
# ============================================================
TZ_NY = ZoneInfo("America/New_York")      # market hours are defined in NY time
TZ_CHI = ZoneInfo("America/Chicago")      # your preferred display timezone

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
    # Keep default as IEX. (SIP will fail without entitlement.)
    return (os.getenv("ALPACA_DATA_FEED") or "iex").lower()
    
def _sell_pct() -> float:
    # default 0.002 = 0.2%
    try:
        return float(os.getenv("SELL_PCT", "0.002"))
    except Exception:
        return 0.002

def _to_rfc3339_z(dt: datetime) -> str:
    """Convert aware UTC datetime to RFC3339 with trailing Z."""
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _is_rth_ny(ts_utc: datetime) -> bool:
    """Regular Trading Hours: 9:30–16:00 NY time."""
    ts_ny = ts_utc.astimezone(TZ_NY)
    minutes = ts_ny.hour * 60 + ts_ny.minute
    return (9 * 60 + 30) <= minutes <= (16 * 60)

def _fmt_ct(ts_utc: datetime) -> str:
    """Human display in Chicago time."""
    return ts_utc.astimezone(TZ_CHI).strftime("%-m/%-d, %-I:%M %p")
    
def _db_url() -> str:
    url = os.getenv("DATABASE_URL", "")
    if not url:
        raise ValueError("Missing DATABASE_URL (set it in Render -> alpaca-dashboard -> Environment)")
    return url

def _db_conn():
    # Render Postgres typically requires SSL
    return psycopg2.connect(_db_url(), sslmode="require")

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
        "marker": "CLEAN_RTH_IEX_v1",
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
        "sell_pct": float(os.getenv("SELL_PCT", "0.002")),
    }
    
@app.get("/group_performance")
def group_performance(limit: int = 25):
    """
    One TSLA cycle (group_id) = one row
    PnL = sum(sells) - sum(buys), using est_price
    Times shown in Chicago time with AM/PM
    """
    symbol = _symbol()

    sql = """
    WITH base AS (
      SELECT
        id,
        ts_utc,
        symbol,
        side,
        qty,
        est_price,
        is_dry_run,
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
        WHEN buy_notional > 0 THEN ROUND((pnl / buy_notional) * 100.0, 2)
        ELSE NULL
      END AS pnl_pct,

      CASE
        WHEN sell_qty > 0 THEN 'CLOSED'
        ELSE 'OPEN'
      END AS cycle_status,

      CASE
        WHEN sell_qty > 0 AND pnl >= 0 THEN 'WIN'
        WHEN sell_qty > 0 AND pnl < 0 THEN 'LOSS'
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

    # 1) Serve cached response if fresh
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

    # Look back far enough to survive weekends/holidays
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
                "base_url": os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets"),
                "data_feed_env": os.getenv("ALPACA_DATA_FEED"),
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
        payload = {
            "ok": False,
            "symbol": symbol,
            "feed": feed,
            "error": "no bars returned",
            "cached": False,
            "debug": {
                "start": start_rfc3339,
                "end": end_rfc3339,
                "base_url": os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets"),
                "data_feed_env": os.getenv("ALPACA_DATA_FEED"),
            },
        }
        with _latest_bar_lock:
            _latest_bar_cache["ts"] = now
            _latest_bar_cache["data"] = payload
        return payload

    # Pick last CLOSED bar (strictly earlier than current minute)
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
        payload = {
            "ok": False,
            "symbol": symbol,
            "feed": feed,
            "error": "no closed bar found",
            "cached": False,
        }
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
        "t_ct": _fmt_ct(t_utc),   # display string in Chicago time
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
# Historical Bars (RTH only; works on IEX even when market closed)
# ============================================================
@app.get("/bars")
def bars(limit: int = 300):
    api = _alpaca()
    symbol = _symbol()
    feed = _feed()

    now_utc = datetime.now(timezone.utc)

    # Pull a wide window so weekends/holidays still include last trading session
    start_utc = now_utc - timedelta(days=14)

    start_rfc3339 = _to_rfc3339_z(start_utc)
    end_rfc3339 = _to_rfc3339_z(now_utc)

    try:
        raw_bars = api.get_bars(
            symbol,
            TimeFrame.Minute,
            start=start_rfc3339,
            end=end_rfc3339,
            limit=10000,          # ask for plenty; we'll slice to `limit` after filtering
            adjustment="raw",
            feed=feed,
        )
    except Exception as e:
        return {
            "ok": False,
            "symbol": symbol,
            "feed": feed,
            "error": "get_bars exception",
            "debug": {
                "start": start_rfc3339,
                "end": end_rfc3339,
                "base_url": os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets"),
                "data_feed_env": os.getenv("ALPACA_DATA_FEED"),
                "limit": limit,
                "exception_type": type(e).__name__,
                "exception_text": str(e),
            },
        }

    bars_list = list(raw_bars) if raw_bars else []
    if not bars_list:
        return {
            "ok": False,
            "symbol": symbol,
            "feed": feed,
            "error": "no bars returned",
            "debug": {
                "start": start_rfc3339,
                "end": end_rfc3339,
                "base_url": os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets"),
                "data_feed_env": os.getenv("ALPACA_DATA_FEED"),
                "limit": limit,
            },
        }

    out = []
    for b in bars_list:
        ts = b.t
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        # Filter to Regular Trading Hours
        if not _is_rth_ny(ts):
            continue

        out.append({
            "time": int(ts.timestamp()),  # epoch seconds (correct for chart)
            "open": float(b.o),
            "high": float(b.h),
            "low": float(b.l),
            "close": float(b.c),
        })

    # Keep only the most recent bars after filtering
    out = out[-limit:]

    if not out:
        return {
            "ok": False,
            "symbol": symbol,
            "feed": feed,
            "error": "no RTH bars returned",
            "debug": {
                "start": start_rfc3339,
                "end": end_rfc3339,
                "limit": limit,
            },
        }

    # Helpful display fields (Chicago time)
    last_ts_utc = datetime.fromtimestamp(out[-1]["time"], tz=timezone.utc)

    return {
        "ok": True,
        "symbol": symbol,
        "feed": feed,
        "bars": out,
        "bars_count": len(out),
        "last_ct": _fmt_ct(last_ts_utc),
    }

import os
from datetime import datetime, timezone
from typing import Optional, Tuple

# -------------------------
# Env helper
# -------------------------
def _sell_rise_usd() -> float:
    """
    Dollar amount ABOVE the anchor (first buy in the current group)
    that should trigger a SELL.
    Default: 2.0
    """
    try:
        return float(os.getenv("SELL_RISE_USD", "2") or "2")
    except Exception:
        return 2.0


def _parse_dt(x) -> Optional[datetime]:
    """
    Alpaca activities sometimes come as datetime, sometimes as string.
    Normalize to aware UTC datetime when possible.
    """
    if x is None:
        return None
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    try:
        # Alpaca often returns ISO like '2026-02-03T01:23:45.123Z'
        s = str(x).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _anchor_from_recent_fills(api, symbol: str, lookback: int = 500) -> Tuple[Optional[float], Optional[datetime]]:
    """
    Reconstruct the CURRENT group's anchor price:
    - Sort fills oldest->newest
    - Track running position qty
    - Every time running qty goes from 0 -> >0 on a BUY, that BUY price becomes the new anchor
    - Sells reduce running qty; when qty returns to 0, the next buy starts a new group

    Returns: (anchor_price, anchor_time_utc)
    """
    try:
        acts = api.list_activities(activity_types="FILL", limit=lookback)
    except Exception:
        return (None, None)

    # Filter to symbol and normalize
    fills = []
    for a in acts or []:
        try:
            if (getattr(a, "symbol", "") or "").upper() != symbol.upper():
                continue
            side = (getattr(a, "side", "") or "").lower()
            qty = float(getattr(a, "qty", 0) or 0)
            px = float(getattr(a, "price", 0) or 0)
            dt = _parse_dt(getattr(a, "transaction_time", None) or getattr(a, "time", None) or getattr(a, "filled_at", None))
            if side not in ("buy", "sell") or qty <= 0 or px <= 0:
                continue
            fills.append((dt, side, qty, px))
        except Exception:
            continue

    # Oldest -> newest
    fills.sort(key=lambda x: (x[0] or datetime(1970, 1, 1, tzinfo=timezone.utc)))

    running_qty = 0.0
    anchor_price = None
    anchor_time = None

    for (dt, side, qty, px) in fills:
        if side == "buy":
            # If we were flat, this buy starts a NEW group => set anchor
            if running_qty <= 0:
                anchor_price = px
                anchor_time = dt
            running_qty += qty

        elif side == "sell":
            running_qty = max(0.0, running_qty - qty)
            # When we go flat, next buy will set a new anchor
            if running_qty <= 0:
                pass

    # If we currently have a position but couldn't find an anchor, return None
    return (anchor_price, anchor_time)

from datetime import datetime
from typing import Optional

def _sell_rise_usd() -> float:
    # Dashboard env var (add to Render alpaca-dashboard): SELL_RISE_USD=2
    try:
        import os
        return float(os.getenv("SELL_RISE_USD", "2"))
    except Exception:
        return 2.0

def _anchor_from_fills(api, symbol: str, limit: int = 500) -> Optional[tuple[float, Optional[datetime]]]:
    """
    Reconstruct 'anchor' (first buy of current open cycle) from fills.
    Cycle starts when position goes from 0 -> >0.
    Returns: (anchor_price, anchor_time_utc) or None
    """
    try:
        orders = api.list_orders(
            status="all",
            limit=limit,
            nested=True,
            direction="asc"
        )
    except Exception:
        orders = []

    # Collect fills-like events from orders with filled_at
    events = []
    for o in orders:
        try:
            if getattr(o, "symbol", None) != symbol:
                continue
            filled_at = getattr(o, "filled_at", None)
            if not filled_at:
                continue
            side = (getattr(o, "side", "") or "").lower()
            qty = float(getattr(o, "filled_qty", 0) or 0)
            px = float(getattr(o, "filled_avg_price", 0) or 0)
            if side not in ("buy", "sell"):
                continue
            if qty <= 0 or px <= 0:
                continue
            events.append((filled_at, side, qty, px))
        except Exception:
            continue

    # Oldest -> newest
    events.sort(key=lambda x: x[0])

    running_qty = 0.0
    current_anchor_price = None
    current_anchor_time = None

    for filled_at, side, qty, px in events:
        if side == "buy":
            # If we were flat and we buy, this begins a new cycle => anchor
            if running_qty <= 0:
                current_anchor_price = px
                current_anchor_time = filled_at
            running_qty += qty

        elif side == "sell":
            running_qty = max(0.0, running_qty - qty)
            # If we sell to flat, cycle ended; next buy would start a new cycle
            if running_qty <= 0:
                current_anchor_price = None
                current_anchor_time = None

    # If we end with running_qty>0, we are in an open cycle and should have an anchor
    if running_qty > 0 and current_anchor_price is not None:
        return float(current_anchor_price), current_anchor_time

    return None

# ============================================================
# Position Info (UPDATED: uses SELL_RISE_USD + anchor price)
# ============================================================
@app.get("/position")
def position():
    api = _alpaca()
    symbol = _symbol()
    sell_rise_usd = _sell_rise_usd()

    # Try to get current position
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

    # If we have an open position, reconstruct anchor from fills/orders
    if qty > 0:
        anchor = _anchor_from_fills(api, symbol, limit=500)
        if anchor:
            anchor_price, anchor_time = anchor

    sell_target = (anchor_price + sell_rise_usd) if (qty > 0 and anchor_price is not None) else None

    return {
        "ok": True,
        "symbol": symbol,
        "qty": qty,
        "avg_entry": avg_entry,                 # keep Avg Entry line
        "anchor_price": anchor_price,           # NEW: anchor
        "anchor_time_utc": anchor_time.isoformat() if anchor_time else None,
        "sell_rise_usd": sell_rise_usd,         # NEW: $ rise above anchor
        "sell_target": sell_target,             # NEW: anchor + $2
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
            "side": (getattr(o, "side", "") or "").lower(),   # "buy" / "sell"
            "filled_at": filled_at,                           # ISO string (UTC from Alpaca)
            "filled_qty": filled_qty,
            "filled_avg_price": filled_avg_price,
        })

    return {"ok": True, "symbol": symbol, "fills": out}
