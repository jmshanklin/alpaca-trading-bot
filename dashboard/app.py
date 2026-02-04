import os
from datetime import datetime, timezone
from typing import Optional, Tuple

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

from alpaca_trade_api.rest import REST


# ============================================================
# App
# ============================================================
app = FastAPI()

# Render Root Directory is set to "dashboard", so this is correct:
app.mount("/static", StaticFiles(directory="static"), name="static")


# ============================================================
# Helpers
# ============================================================
def _alpaca() -> REST:
    key = os.getenv("APCA_API_KEY_ID")
    secret = os.getenv("APCA_API_SECRET_KEY")
    base_url = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
    if not key or not secret:
        raise ValueError("Missing Alpaca API keys (APCA_API_KEY_ID / APCA_API_SECRET_KEY)")
    return REST(key, secret, base_url)


def _symbol() -> str:
    return (os.getenv("ENGINE_SYMBOL") or "TSLA").upper()


def _sell_rise_usd() -> float:
    """
    Dollar amount ABOVE anchor (first buy of the current open cycle)
    that should trigger a SELL.
    Default: 2.0
    """
    try:
        return float(os.getenv("SELL_RISE_USD", "2") or "2")
    except Exception:
        return 2.0


def _parse_dt(x) -> Optional[datetime]:
    """
    Alpaca timestamps can be datetime or string. Normalize to aware UTC datetime.
    """
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


def _anchor_from_orders(api: REST, symbol: str, limit: int = 2000) -> Tuple[Optional[float], Optional[datetime]]:
    """
    Anchor reconstruction using ORDERS (most reliable):
    - Pull orders oldest -> newest
    - Track running_qty
    - When running_qty goes 0 -> >0 on a BUY, that BUY price/time becomes anchor
    - When running_qty returns to 0 (sell-to-flat), next BUY becomes new anchor

    Returns: (anchor_price, anchor_time_utc) or (None, None)
    """
    try:
        orders = api.list_orders(
            status="all",
            limit=limit,
            nested=True,
            direction="asc",
        )
    except Exception:
        return (None, None)

    events = []
    sym = symbol.upper()

    for o in orders or []:
        try:
            if (getattr(o, "symbol", "") or "").upper() != sym:
                continue

            filled_at = getattr(o, "filled_at", None)
            dt = _parse_dt(filled_at)
            if dt is None:
                continue

            side = (getattr(o, "side", "") or "").lower()
            qty = float(getattr(o, "filled_qty", 0) or 0)
            px = float(getattr(o, "filled_avg_price", 0) or 0)

            if side not in ("buy", "sell") or qty <= 0 or px <= 0:
                continue

            events.append((dt, side, qty, px))
        except Exception:
            continue

    # Oldest -> newest
    events.sort(key=lambda x: x[0])

    running_qty = 0.0
    anchor_price = None
    anchor_time = None

    for dt, side, qty, px in events:
        if side == "buy":
            # Flat -> long starts a new cycle => anchor
            if running_qty <= 0:
                anchor_price = px
                anchor_time = dt
            running_qty += qty
        else:
            running_qty = max(0.0, running_qty - qty)
            # If we sold to flat, next buy will start a new cycle (new anchor)
            if running_qty <= 0:
                # We keep anchor_price/time as-is here; if position is currently flat,
                # /position will not use it anyway.
                pass

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


@app.get("/favicon.ico")
def favicon():
    # Stops the browser console 404 spam
    return Response(status_code=204)


@app.get("/position")
def position():
    """
    Returns the ONLY numbers we care about now:
    - qty, avg_entry, market_price, unrealized
    - anchor_price (first buy of current open cycle)
    - sell_target = anchor_price + SELL_RISE_USD
    """
    api = _alpaca()
    symbol = _symbol()
    rise = _sell_rise_usd()

    # If no position exists, return a clean "flat" payload
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
            "sell_rise_usd": rise,
            "sell_target": None,
            "market_price": None,
            "unrealized_pl": None,
            "unrealized_plpc": None,
        }

    qty = float(getattr(pos, "qty", 0) or 0)
    avg_entry = float(getattr(pos, "avg_entry_price", 0) or 0) if qty > 0 else None

    market_price = float(getattr(pos, "current_price", 0) or 0) if qty > 0 else None
    unrealized_pl = float(getattr(pos, "unrealized_pl", 0) or 0) if qty > 0 else None
    unrealized_plpc = float(getattr(pos, "unrealized_plpc", 0) or 0) if qty > 0 else None

    anchor_price = None
    anchor_time = None
    sell_target = None

    # Only compute anchor when we actually have an open position
    if qty > 0:
        anchor_price, anchor_time = _anchor_from_orders(api, symbol, limit=2000)
        if anchor_price is not None:
            sell_target = float(anchor_price + rise)

    return {
        "ok": True,
        "symbol": symbol,
        "qty": qty,
        "avg_entry": avg_entry,
        "anchor_price": anchor_price,
        "anchor_time_utc": anchor_time.isoformat() if anchor_time else None,
        "sell_rise_usd": rise,
        "sell_target": sell_target,
        "market_price": market_price,
        "unrealized_pl": unrealized_pl,
        "unrealized_plpc": unrealized_plpc,
    }
