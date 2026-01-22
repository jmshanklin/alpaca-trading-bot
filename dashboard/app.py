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

