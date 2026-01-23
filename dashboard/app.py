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
# Market Data Endpoint
# =========================
@app.get("/latest_bar")
def latest_bar():
    symbol = (os.getenv("ENGINE_SYMBOL") or os.getenv("SYMBOL") or "TSLA").upper()
    feed = (os.getenv("ALPACA_DATA_FEED") or "iex").lower()

    api = _alpaca()

    # Get enough lookback to survive "no bars" situations (closed market / feed gaps).
    now_utc = datetime.now(timezone.utc)

    chosen = None
    last_error = None

    # Try short lookback first, then fall back to a longer window (handles after-hours/closed market)
    for lookback, limit in ((timedelta(minutes=15), 1000), (timedelta(days=5), 1000)):
        start = now_utc - lookback
        try:
            bars = api.get_bars(
                symbol,
                TimeFrame.Minute,
                start=start.isoformat(),
                end=now_utc.isoformat(),
                limit=limit,
                adjustment="raw",
                feed=feed,
            )
            bars_list = list(bars) if bars else []
        except Exception as e:
            last_error = str(e)
            bars_list = []

        if not bars_list:
            continue

        # Choose the latest CLOSED bar (timestamp strictly < current minute)
        now_floor = now_utc.replace(second=0, microsecond=0)
        for b in reversed(bars_list):
            bt = getattr(b, "t", None)
            if bt is None:
                continue
            if bt.tzinfo is None:
                bt = bt.replace(tzinfo=timezone.utc)
            if bt < now_floor:
                chosen = b
                break

        if chosen is not None:
            break

    if chosen is None:
        payload = {"ok": False, "symbol": symbol, "error": "no bars returned"}
        if last_error:
            payload["alpaca_error"] = last_error
        return payload

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
    
from datetime import datetime, timezone, timedelta

@app.get("/bars")
def bars(limit: int = 300):
    symbol = (os.getenv("ENGINE_SYMBOL") or os.getenv("SYMBOL") or "TSLA").upper()
    feed = (os.getenv("ALPACA_DATA_FEED") or "iex").lower()
    api = _alpaca()

    now_utc = datetime.now(timezone.utc)

    # Look back far enough to still get bars when the market is closed
    # (Alpaca will just return the most recent bars within this window)
    start = now_utc - timedelta(days=5)

    bars = api.get_bars(
        symbol,
        TimeFrame.Minute,
        start=start.isoformat(),
        end=now_utc.isoformat(),
        limit=limit,
        adjustment="raw",
        feed=feed,
    )

    bars_list = list(bars) if bars else []
    if not bars_list:
        return {"ok": False, "symbol": symbol, "feed": feed, "error": "no bars returned"}

    out = []
    for b in bars_list:
        bt = getattr(b, "t", None)
        if bt is None:
            continue
        if bt.tzinfo is None:
            bt = bt.replace(tzinfo=timezone.utc)

        out.append({
            "time": int(bt.timestamp()),   # lightweight-charts wants seconds
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
