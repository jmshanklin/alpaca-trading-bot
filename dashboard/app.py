import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

# Serve /static files (your JS, etc.)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def home():
    # Serve your dashboard page
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/health")
def health():
    return {"ok": True}
    
from datetime import datetime, timezone, timedelta
import os

from alpaca_trade_api.rest import REST, TimeFrame  # make sure this import exists

def _alpaca():
    # Accept either naming style
    key = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID")
    secret = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
    base_url = os.getenv("ALPACA_BASE_URL") or os.getenv("APCA_API_BASE_URL") or "https://paper-api.alpaca.markets"
    return REST(key, secret, base_url)

@app.get("/latest_bar")
def latest_bar():
    symbol = (os.getenv("ENGINE_SYMBOL") or os.getenv("SYMBOL") or "TSLA").upper()
    feed = (os.getenv("ALPACA_DATA_FEED") or "iex").lower()

    api = _alpaca()

    now_utc = datetime.now(timezone.utc)
    start = now_utc - timedelta(minutes=15)

    bars = api.get_bars(
        symbol,
        TimeFrame.Minute,
        start=start.isoformat(),
        end=now_utc.isoformat(),
        limit=15,
        adjustment="raw",
        feed=feed,
    )

    bars_list = list(bars) if bars else []
    if not bars_list:
        return {"ok": False, "symbol": symbol, "error": "no bars returned"}

    # Choose the latest CLOSED bar (timestamp strictly < current minute)
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

from alpaca_trade_api.rest import REST, TimeFrame
import os

ALPACA_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET = os.getenv("ALPACA_API_SECRET")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

alpaca = REST(ALPACA_KEY, ALPACA_SECRET, ALPACA_BASE_URL)

@app.get("/latest_bar")
def latest_bar():
    bars = alpaca.get_bars(
        "TSLA",
        TimeFrame.Minute,
        limit=1
    )

    bar = bars[0]
    return {
        "t": bar.t.isoformat(),
        "o": bar.o,
        "h": bar.h,
        "l": bar.l,
        "c": bar.c,
        "v": bar.v
    }

