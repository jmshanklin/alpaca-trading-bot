import os
import time
from threading import Lock
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from alpaca_trade_api.rest import REST, TimeFrame


# =======================
# Cache settings
# =======================
LATEST_BAR_CACHE_TTL = int(os.getenv("LATEST_BAR_CACHE_TTL", "8"))  # seconds

_latest_bar_cache = {"ts": 0.0, "data": None}
_latest_bar_lock = Lock()


# =======================
# App
# =======================
app = FastAPI()

# Serve /static files
app.mount("/static", StaticFiles(directory="static"), name="static")


# =======================
# Helpers
# =======================
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
    return (os.getenv("ALPACA_DATA_FEED") or "iex").lower()


# =======================
# Routes
# =======================
@app.get("/", response_class=HTMLResponse)
def home():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/config")
def config():
    return {
        "ok": True,
        "symbol": _symbol(),
        "feed": _feed(),
        "base_url": os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets"),
        "has_key": bool(os.getenv("APCA_API_KEY_ID")),
        "has_secret": bool(os.getenv("APCA_API_SECRET_KEY")),
    }


# =======================
# Latest Bar (last CLOSED bar)
# =======================
@app.get("/latest_bar")
def latest_bar():
    now = time.time()

    # 1) Serve cached response if fresh
    with _latest_bar_lock:
        cached = _latest_bar_cache["data"]
        if cached is not None and (now - _latest_bar_cache["ts"]) < LATEST_BAR_CACHE_TTL:
            # mark it (copy not required here since we're just annotating)
            cached["cached"] = True
            return cached

    # 2) Otherwise fetch from Alpaca
    api = _alpaca()
    symbol = _symbol()
    feed = _feed()

    now_utc = datetime.now(timezone.utc)

    # Look back far enough so after-hours / weekends still return the last closed bar.
    start = now_utc - timedelta(days=7)

    bars = api.get_bars(
        symbol,
        TimeFrame.Minute,
        start=start.isoformat(),
        end=now_utc.isoformat(),
        limit=1000,
        adjustment="raw",
        feed=feed,
    )

    bars_list = list(bars) if bars else []
    if not bars_list:
        payload = {
            "ok": False,
            "symbol": symbol,
            "feed": feed,
            "error": "no bars returned",
            "cached": False,
        }
        with _latest_bar_lock:
            _latest_bar_cache["ts"] = now
            _latest_bar_cache["data"] = payload
        return payload

    # Pick last CLOSED bar (strictly earlier than the current minute)
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

    payload = {
        "ok": True,
        "symbol": symbol,
        "feed": feed,
        "t": chosen.t.isoformat(),
        "o": float(chosen.o),
        "h": float(chosen.h),
        "l": float(chosen.l),
        "c": float(chosen.c),
        "v": float(chosen.v or 0),
        "cached": False,
    }

    # 3) Store into cache and return
    with _latest_bar_lock:
        _latest_bar_cache["ts"] = now
        _latest_bar_cache["data"] = payload

    return payload


# =======================
# Historical Bars
# =======================
@app.get("/bars")
def bars(limit: int = 300):
    api = _alpaca()
    symbol = _symbol()
    feed = _feed()

    now_utc = datetime.now(timezone.utc)
    # Look back far enough so after-hours / weekends still return the last closed bar.
    start = now_utc - timedelta(days=7)
    
    # Force RFC3339 timestamps with trailing 'Z' (helps Alpaca parse consistently)
    start_rfc3339 = start.isoformat().replace("+00:00", "Z")
    end_rfc3339 = now_utc.isoformat().replace("+00:00", "Z")
    
    bars = api.get_bars(
        symbol,
        TimeFrame.Minute,
        start=start_rfc3339,
        end=end_rfc3339,
        limit=1000,
        adjustment="raw",
        feed=feed,
    )

    bars_list = list(bars) if bars else []
    if not bars_list:
        return {"ok": False, "symbol": symbol, "feed": feed, "error": "no bars returned"}

    out = []
    for b in bars_list:
        ts = b.t
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        out.append(
            {
                "time": int(ts.timestamp()),
                "open": float(b.o),
                "high": float(b.h),
                "low": float(b.l),
                "close": float(b.c),
            }
        )

    return {"ok": True, "symbol": symbol, "feed": feed, "bars": out}


# =======================
# Position Info
# =======================
@app.get("/position")
def position():
    api = _alpaca()
    symbol = _symbol()

    try:
        pos = api.get_position(symbol)
    except Exception:
        return {"ok": True, "symbol": symbol, "qty": 0}

    return {
        "ok": True,
        "symbol": symbol,
        "qty": float(pos.qty),
        "avg_entry": float(pos.avg_entry_price),
        "market_price": float(pos.current_price),
        "unrealized_pl": float(pos.unrealized_pl),
        "unrealized_plpc": float(pos.unrealized_plpc),
    }
