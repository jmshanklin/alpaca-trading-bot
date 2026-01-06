# webhook_to_alpaca_price_action.py
# ------------------------------------------------------------
# TradingView webhook -> Alpaca (Render + Flask)
#
# Goals:
# - Accept TradingView webhook JSON (including text/plain bodies)
# - Support equities (TSLA) with time_in_force="day"
# - Support crypto (BTCUSD) with time_in_force forced to "gtc"
# - Allow fractional qty for crypto (e.g., 0.001)
#
# Accepts either "side" or "action" from TradingView:
#   {"symbol":"BTCUSD","side":"buy","qty":0.001}
#   {"symbol":"BTCUSD","action":"buy","qty":0.001}
# ------------------------------------------------------------

from flask import Flask, request, jsonify
import os
import uuid
import hmac
import json
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
        if k.lower() in {"key", "webhook_key", "apca_api_secret_key", "alpaca_secret_key", "authorization"}:
            masked[k] = "***"
        else:
            masked[k] = v
    trailer = (" | " + ", ".join(f"{k}={v}" for k, v in masked.items())) if masked else ""
    msg = f"{message}{trailer}"
    lvl = level.lower()
    getattr(logging, "warning" if lvl == "warning" else ("error" if lvl == "error" else "info"))(msg)

# ---------- Config (Render env vars) ----------
ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_BASE_URL   = os.getenv("ALPACA_BASE_URL") or os.getenv("APCA_API_BASE_URL") or "https://paper-api.alpaca.markets"

WEBHOOK_KEY    = os.getenv("WEBHOOK_KEY", "")
SELFTEST_TOKEN = os.getenv("SELFTEST_TOKEN", "let_me_in")

api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)
app = Flask(__name__)

# ---------- Health ----------
@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "status": "ok",
        "service": "TradingBot",
        "version": "2.1.0",
        "endpoints": ["/webhook", "/ping", "/healthz", "/selftest"],
    }), 200

@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"status": "ok"}), 200

@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True}), 200

# Quick server→Alpaca test (equity): POST /selftest?token=let_me_in
@app.route("/selftest", methods=["POST"])
def selftest():
    if request.args.get("token") != SELFTEST_TOKEN:
        return jsonify({"ok": False, "error": "forbidden"}), 403
    try:
        order = api.submit_order(symbol="AAPL", qty=1, side="buy", type="market", time_in_force="day")
        log("selftest_order", id=order.id, symbol="AAPL", side="buy", qty=1)
        return jsonify({"ok": True, "order_id": order.id}), 200
    except Exception as e:
        log("selftest_error", level="error", error=str(e))
        return jsonify({"ok": False, "error": str(e)}), 500

# ---------- Helpers ----------
def detect_crypto(symbol: str) -> bool:
    # Best: ask Alpaca what it is
    try:
        asset = api.get_asset(symbol)
        cls = getattr(asset, "class", None) or getattr(asset, "_raw", {}).get("class")
        return (cls or "").lower() == "crypto"
    except Exception:
        # Fallback: common crypto pairs
        return symbol.upper() in {"BTCUSD", "ETHUSD", "SOLUSD", "AVAXUSD", "LTCUSD"}

def parse_qty(raw_qty, crypto: bool):
    """
    Crypto: allow fractional, return string decimal for Alpaca
    Equity: require integer, return int
    """
    if raw_qty is None:
        raw_qty = "0.001" if crypto else "1"

    if crypto:
        try:
            q = Decimal(str(raw_qty))
        except InvalidOperation:
            raise ValueError("qty must be numeric for crypto (example: 0.001)")
        if q <= 0:
            raise ValueError("qty must be > 0")
        return str(q)  # IMPORTANT: keep fractional as string
    else:
        try:
            q = int(str(raw_qty))
        except Exception:
            raise ValueError("qty must be an integer for equities (example: 1)")
        if q <= 0:
            raise ValueError("qty must be > 0")
        return q

def get_json_body():
    # TradingView sometimes sends text/plain. Handle both.
    raw = request.get_data(as_text=True) or ""
    data = request.get_json(silent=True)
    if data is not None:
        return data, raw
    try:
        if raw.strip().startswith("{"):
            return json.loads(raw), raw
    except Exception:
        pass
    return {}, raw

# ---------- Webhook ----------
@app.route("/webhook", methods=["POST"])
def webhook():
    req_id = str(uuid.uuid4())[:8]
    data, raw = get_json_body()

    ua = request.headers.get("User-Agent", "unknown")
    src = "TradingView" if "tradingview" in ua.lower() else ("powershell" if "powershell" in ua.lower() else "unknown")

    # --- Auth: accept ?key=... OR JSON key/secret ---
    provided = (
        request.args.get("key")
        or request.args.get("token")
        or (data.get("key") if isinstance(data, dict) else None)
        or (data.get("secret") if isinstance(data, dict) else None)
        or ""
    )
    if WEBHOOK_KEY and not hmac.compare_digest(str(provided), str(WEBHOOK_KEY)):
        log("unauthorized", level="warning", req_id=req_id, source=src)
        return jsonify({"status": "error", "message": "unauthorized"}), 401

    # Log receipt (mask key)
    safe = {}
    if isinstance(data, dict):
        for k, v in data.items():
            safe[k] = "***" if str(k).lower() in {"key", "secret"} else v
    log("received", req_id=req_id, source=src, **safe)

    # --- Parse fields ---
    symbol = (data.get("symbol") or "TSLA").upper().strip() if isinstance(data, dict) else "TSLA"
    side   = (data.get("side") or data.get("action") or "buy").lower().strip() if isinstance(data, dict) else "buy"

    if side not in {"buy", "sell", "close"}:
        return jsonify({"status": "error", "message": "side/action must be buy/sell/close"}), 400

    crypto = detect_crypto(symbol)

    # time_in_force rules
    tif = "gtc" if crypto else (data.get("time_in_force") or "day").lower().strip()

    # qty rules
    qty = None
    raw_qty = (data.get("qty") if isinstance(data, dict) else None)

    # If TradingView sends {"side":"sell"} with NO qty for equities,
    # treat that as sell-to-close (close_position).
    sell_to_close_equity = (not crypto) and (side == "sell") and (raw_qty is None or str(raw_qty).strip() == "")

    if not sell_to_close_equity and side != "close":
        try:
            qty = parse_qty(raw_qty, crypto=crypto)
        except ValueError as e:
            return jsonify({"status": "error", "message": str(e)}), 400

    client_id = (
        (data.get("client_id") if isinstance(data, dict) else None)
        or f"{symbol}-{side}-{(data.get('qty', 'na') if isinstance(data, dict) else 'na')}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    )

    try:
        # Equity sell-to-close: allow TradingView to send {"side":"sell"} without qty
        if sell_to_close_equity:
            api.close_position(symbol)
            log("close_position", req_id=req_id, symbol=symbol, side=side, raw_qty=raw_qty, reason="sell_to_close_equity_no_qty")
            return jsonify({"status": "success", "action": "close", "symbol": symbol}), 200
    
        if side == "close":
            if crypto:
                return jsonify({"status": "error", "message": "close not supported for crypto in this tester; send sell with qty instead"}), 400
            api.close_position(symbol)
            log("close_position", req_id=req_id, symbol=symbol)
            return jsonify({"status": "success", "action": "close", "symbol": symbol}), 200
    
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type="market",
            time_in_force=tif,
            client_order_id=client_id,
        )

        log("order_submitted", req_id=req_id, id=order.id, symbol=symbol, side=side, qty=qty, tif=tif, asset=("crypto" if crypto else "equity"))
        return jsonify({"status": "success", "order_id": order.id, "client_id": client_id, "tif": tif}), 200

    except Exception as e:
        msg = str(e)
        log("order_error", level="error", req_id=req_id, error=msg, symbol=symbol, side=side, qty=qty, tif=tif)
        # Keep 200 so TradingView doesn't retry aggressively
        return jsonify({"status": "error", "message": msg}), 200
