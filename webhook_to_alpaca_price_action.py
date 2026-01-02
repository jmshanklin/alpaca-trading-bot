# webhook_to_alpaca_price_action.py
# --------------------------------------------------------------------
# TradingBot webhook for Alpaca (Render + Flask, Python 3.11+)
#
# Expected JSON (examples):
# Stocks:
#   {"symbol":"TSLA","side":"buy","qty":1}
#
# Crypto test (market closed use-case):
#   {"symbol":"BTCUSD","side":"buy","qty":0.001}
#
# Optional fields:
#   "time_in_force": "day" (stocks) | "gtc"/"ioc" (crypto)
#   "client_id": "optional-id"
# Key auth:
#   Put WEBHOOK_KEY in Render env, then call:
#     https://<service>.onrender.com/webhook?key=<WEBHOOK_KEY>
# --------------------------------------------------------------------

from flask import Flask, request, jsonify
import os
import uuid
import hmac
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import alpaca_trade_api as tradeapi

# ---------- Logging ----------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

def log(message: str, level: str = "info", **fields):
    masked = {}
    for k, v in fields.items():
        if k.lower() in {"key", "webhook_key", "apca_api_secret_key", "alpaca_secret_key"}:
            masked[k] = "***"
        else:
            masked[k] = v
    trailer = ""
    if masked:
        trailer = " | " + ", ".join(f"{k}={v}" for k, v in masked.items())
    msg = f"{message}{trailer}"
    lvl = level.lower()
    if lvl == "warning":
        logging.warning(msg)
    elif lvl == "error":
        logging.error(msg)
    else:
        logging.info(msg)

# ---------- Config (env vars on Render) ----------
ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_BASE_URL   = (
    os.getenv("ALPACA_BASE_URL")
    or os.getenv("APCA_API_BASE_URL")
    or "https://paper-api.alpaca.markets"
)

WEBHOOK_KEY = os.getenv("WEBHOOK_KEY", "")

# ---------- Alpaca client ----------
api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)

app = Flask(__name__)

# ---------- Health ----------
@app.route("/", methods=["GET"])
def root():
    return jsonify({"status": "ok", "service": "TradingBot", "endpoints": ["/webhook", "/ping", "/healthz"]}), 200

@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"status": "ok", "service": "TradingBot"}), 200

# Render Health Check Path (your settings show /healthz)
@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True}), 200

def get_provided_key(data: dict) -> str:
    # Accept key via query OR JSON body
    return (
        request.args.get("key")
        or request.args.get("token")
        or (data.get("key") if isinstance(data, dict) else None)
        or (data.get("secret") if isinstance(data, dict) else None)
        or ""
    )

def is_crypto_symbol(symbol: str) -> bool:
    # Best: ask Alpaca
    try:
        a = api.get_asset(symbol)
        # alpaca_trade_api asset has .asset_class
        return str(getattr(a, "asset_class", "")).lower() == "crypto"
    except Exception:
        # Fallback heuristic if get_asset fails for some reason
        s = (symbol or "").upper().strip()
        return len(s) >= 6 and s.endswith("USD") and s not in {"TSLAUSD", "AAPLUSD"}

def parse_qty(symbol: str, qty_in) -> str:
    """
    Alpaca accepts qty as string; we return a normalized string:
      - crypto: allow decimals (e.g., 0.001)
      - stocks: whole shares (int)
    """
    if qty_in is None:
        raise ValueError("qty is required")

    if is_crypto_symbol(symbol):
        try:
            q = Decimal(str(qty_in))
            if q <= 0:
                raise ValueError("qty must be > 0")
            # normalize (no scientific notation)
            return format(q.normalize(), "f")
        except (InvalidOperation, ValueError):
            raise ValueError("qty must be a valid decimal number for crypto")
    else:
        try:
            q = int(qty_in)
            if q <= 0:
                raise ValueError("qty must be > 0")
            return str(q)
        except Exception:
            raise ValueError("qty must be an integer for stocks")

def normalize_time_in_force(symbol: str, tif: str) -> str:
    tif = (tif or "").lower().strip()
    if is_crypto_symbol(symbol):
        # Crypto does NOT accept "day"
        if tif in {"", "day"}:
            return "gtc"
        if tif in {"gtc", "ioc"}:
            return tif
        # if someone sends something else, force safe default
        return "gtc"
    else:
        # Stocks default to day if missing
        return tif if tif else "day"

@app.route("/webhook", methods=["POST"])
def webhook():
    req_id = str(uuid.uuid4())[:8]
    data = request.get_json(silent=True) or {}

    ua = request.headers.get("User-Agent", "unknown")
    src = "TradingView" if "tradingview" in ua.lower() else ("powershell/curl" if "curl" in ua.lower() or "powershell" in ua.lower() else "unknown")

    safe = {k: ("***" if k.lower() in {"key", "secret"} else v) for k, v in (data.items() if isinstance(data, dict) else [])}
    log("received", req_id=req_id, source=src, **safe)

    # --- webhook auth ---
    if WEBHOOK_KEY:
        provided = get_provided_key(data)
        if not hmac.compare_digest(str(provided), str(WEBHOOK_KEY)):
            log("unauthorized", level="warning", req_id=req_id)
            return jsonify({"status": "error", "message": "unauthorized"}), 401

    # --- payload normalization ---
    symbol = (data.get("symbol") or "TSLA").upper().strip()
    side = (data.get("side") or data.get("action") or "buy").lower().strip()

    if side not in {"buy", "sell", "close"}:
        return jsonify({"status": "error", "message": "side/action must be buy/sell/close"}), 400

    tif_in = data.get("time_in_force")
    tif = normalize_time_in_force(symbol, tif_in)

    # idempotency
    client_id = data.get("client_id") or f"{symbol}-{side}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    try:
        if side == "close":
            api.close_position(symbol)
            log("close_position", req_id=req_id, symbol=symbol)
            return jsonify({"status": "success", "action": "close", "symbol": symbol}), 200

        qty = parse_qty(symbol, data.get("qty", 1))

        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type="market",
            time_in_force=tif,
            client_order_id=client_id,
        )
        log("order_submitted", req_id=req_id, id=order.id, symbol=symbol, side=side, qty=qty, time_in_force=tif)
        return jsonify({"status": "success", "order_id": order.id, "client_id": client_id}), 200

    except Exception as e:
        msg = str(e)
        log("order_error", level="error", req_id=req_id, error=msg)
        # Keep 200 so TradingView doesn’t spam retries
        return jsonify({"status": "error", "message": msg}), 200
