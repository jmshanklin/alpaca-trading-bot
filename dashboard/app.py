import os
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from alpaca_trade_api.rest import REST, TimeFrame


# =========================
# App + Static Frontend
# =========================
app = FastAPI()

# Serve /static files (JS, CSS, etc.)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
def home():
    # Serve your dashboard page
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/health")
def health():
    return {"ok": True}


# =========================
# Alpaca Client Helper
# =========================
def _alpaca() -> REST:
    """
    Accept either env var naming style:
      - Preferred (matches many bots): APCA_API_KEY_ID / APCA_API_SECRET_KEY / APCA_API_BASE_URL
      - Alternate: ALPACA_KEY_ID / ALPACA_SECRET_KEY / ALPACA_BASE_URL
    """
    key = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_KEY_ID")
    secret = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_SECRET_KEY")
    base_url = (
        os.getenv("APCA_API_BASE_URL")
        or os.getenv("ALPACA_BASE_URL")
        or "https://paper-api.alpaca.markets"
    )

    if not key or not secret:
        # Fail fast with a clear message (shows up in logs + API response)
        raise ValueError("Missing Alpaca credentials. Set APCA_API_KEY_ID and APCA_API_SECRET_KEY (or ALPACA_KEY_ID/ALPACA_SECRET_KEY).")

    return REST(key, secret, base_url)

# =========================
# Market Data Endpoints
# =========================
from datetime import datetime, timezone, timedelta
from alpaca_trade_api.rest import REST, TimeFrame
import os

def _alpaca():
    key = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID")
    secret = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
    base_url = os.getenv("ALPACA_BASE_URL") or os.getenv("APCA_API_BASE_URL") or "https://paper-api.alpaca.markets"
    return REST(key, secret, base_url)

def _get_minute_bars(symbol: str, feed: str, limit: int, now_utc: datetime):
    """
    Return up to `limit` 1-min bars ending near now_utc (UTC), sorted oldest->newest.
    Tries a short lookback first; falls back to longer (helps after-hours / gaps).
    If feed is invalid for your account (e.g., sip), we retry with iex automatically.
    """
    api = _alpaca()
    last_error = None

    # Buffer prevents "no bars" when we ask too tight a window
    lookbacks = [
        timedelta(minutes=max(limit + 30, 120)),  # enough for 300 bars etc.
        timedelta(days=5),
    ]

    def try_fetch(use_feed: str):
        nonlocal last_error
        for lb in lookbacks:
            start = now_utc - lb
            try:
                bars = api.get_bars(
                    symbol,
                    TimeFrame.Minute,
                    start=start.isoformat(),
                    end=now_utc.isoformat(),
                    limit=min(max(limit, 1), 10000),
                    adjustment="raw",
                    feed=use_feed,
                )
                bars_list = list(bars) if bars else []
                if bars_list:
                    # Ensure oldest -> newest
                    bars_list.sort(key=lambda b: getattr(b, "t"))
                    # Keep only last `limit`
                    return bars_list[-limit:]
            except Exception as e:
                last_error = str(e)

        return []

    bars_list = try_fetch(feed)

    # If user set ALPACA_DATA_FEED=sip but account doesn't have it,
    # Alpaca will error; retry with iex automatically.
    if not bars_list and feed != "iex":
        bars_list = try_fetch("iex")

    return bars_list, last_error


@app.get("/latest_bar")
def latest_bar():
    symbol = (os.getenv("ENGINE_SYMBOL") or os.getenv("SYMBOL") or "TSLA").upper()
    feed = (os.getenv("ALPACA_DATA_FEED") or "iex").lower()

    now_utc = datetime.now(timezone.utc)
    bars_list, last_error = _get_minute_bars(symbol, feed, limit=50, now_utc=now_utc)

    if not bars_list:
        return {"ok": False, "symbol": symbol, "error": last_error or "no bars returned"}

    # Choose latest CLOSED bar (strictly < current minute)
    now_floor = now_utc.replace(second=0, microsecond=0)
    chosen = None
    for b in reversed(bars_list):
        bt = getattr(b, "t", None)
        if bt is None:
            continue
        if bt.tzinfo is None:
            bt = bt.replace(tzinfo=timezone.utc)
        if bt < now_floor:
            chosen = b
            break

    if chosen is None:
        return {"ok": False, "symbol": symbol, "error": "no closed bar found yet"}

    return {
        "ok": True,
        "symbol": symbol,
        "t": chosen.t.isoformat() if getattr(chosen, "t", None) else None,
        "o": float(chosen.o),
        "h": float(chosen.h),
        "l": float(chosen.l),
        "c": float(chosen.c),
        "v": float(getattr(chosen, "v", 0.0) or 0.0),
        "feed": feed,
    }


@app.get("/bars")
def bars(limit: int = 300):
    symbol = (os.getenv("ENGINE_SYMBOL") or os.getenv("SYMBOL") or "TSLA").upper()
    feed = (os.getenv("ALPACA_DATA_FEED") or "iex").lower()

    # Clamp to something sane
    limit = max(10, min(int(limit), 2000))

    now_utc = datetime.now(timezone.utc)
    bars_list, last_error = _get_minute_bars(symbol, feed, limit=limit, now_utc=now_utc)

    if not bars_list:
        return {"ok": False, "symbol": symbol, "error": last_error or "no bars returned"}

    # Convert to lightweight-charts format: epoch seconds UTC
    out = []
    for b in bars_list:
        bt = getattr(b, "t", None)
        if bt is None:
            continue
        if bt.tzinfo is None:
            bt = bt.replace(tzinfo=timezone.utc)
        out.append({
            "time": int(bt.timestamp()),
            "open": float(b.o),
            "high": float(b.h),
            "low": float(b.l),
            "close": float(b.c),
        })

    return {"ok": True, "symbol": symbol, "feed": feed, "bars": out}

    # Convert to lightweight-charts format (seconds)
    out = []
    for b in bars_list:
        bt = getattr(b, "t", None)
        if bt is None:
            continue
        if bt.tzinfo is None:
            bt = bt.replace(tzinfo=timezone.utc)

        out.append({
            "time": int(bt.timestamp()),
            "open": float(b.o),
            "high": float(b.h),
            "low": float(b.l),
            "close": float(b.c),
        })

    return {"ok": True, "symbol": symbol, "feed": feed, "bars": out}

@app.get("/position")
def position():
    symbol = (os.getenv("ENGINE_SYMBOL") or os.getenv("SYMBOL") or "TSLA").upper()
    api = _alpaca()

    try:
        pos = api.get_position(symbol)

        qty = float(pos.qty)
        avg_entry = float(pos.avg_entry_price)
        market_price = float(pos.current_price)

        unrealized_pl = float(pos.unrealized_pl)
        unrealized_plpc = float(pos.unrealized_plpc)

        return {
            "ok": True,
            "symbol": symbol,
            "qty": qty,
            "avg_entry": avg_entry,
            "market_price": market_price,
            "unrealized_pl": unrealized_pl,
            "unrealized_plpc": unrealized_plpc
        }

    except Exception:
        # No open position
        return {
            "ok": True,
            "symbol": symbol,
            "qty": 0,
            "avg_entry": None,
            "market_price": None,
            "unrealized_pl": 0.0,
            "unrealized_plpc": 0.0
        }
        
from alpaca_trade_api.rest import APIError

def _get_feed():
    return (os.getenv("ALPACA_DATA_FEED") or "iex").lower()

def _bars_with_fallback(api, symbol, tf, start, end, limit, feed):
    try:
        return api.get_bars(
            symbol, tf,
            start=start, end=end,
            limit=limit,
            adjustment="raw",
            feed=feed
        )
    except APIError as e:
        msg = str(e).lower()
        # If SIP is not allowed, retry with IEX automatically
        if "sip" in feed and ("subscription" in msg or "not permitted" in msg or "does not permit" in msg):
            return api.get_bars(
                symbol, tf,
                start=start, end=end,
                limit=limit,
                adjustment="raw",
                feed="iex"
            )
        raise
