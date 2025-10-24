# webhook_to_alpaca_price_action.py
# --------------------------------------------------------------------
# TradingBot webhook for Alpaca (Render + Flask, Python 3.11+)
#
# Expected JSON from TradingView (example):
# {
#   "symbol": "TSLA",         # default if omitted: "TSLA"
#   "side":   "buy",          # "buy" | "sell" | "close"
#   "qty":    1,              # required for buy/sell; ignored for close
#   "time_in_force": "day",   # optional; default "day"
#   "client_id": "optional-id", # optional idempotency key
#   "key": "<your WEBHOOK_KEY>" # optional if you set WEBHOOK_KEY in Render
# }
# --------------------------------------------------------------------

from flask import Flask, request, jsonify
import os
import uuid
import hmac
import logging
from datetime import datetime
import alpaca_trade_api as tradeapi

# ---------- Logging ----------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

def log(message: str, level: str = "info", **fields):
    """Human-friendly structured logger that masks sensitive values."""
    # mask obvious secret-ish keys
    masked = {}
    for k, v in fields.items():
        if k.lower() in {"key", "webhook_key", "apca_api_secret_key", "alpaca_secret_key"}:
            masked[k] = "***"
        else:
            masked[k] = v

    trailer = ""
    if masked:
        kv = ", ".join(f"{k}={v}" for k, v in masked.items())
        trailer = f" | {kv}"

    msg = f"{message}{trailer}"
    lvl = level.lower()
    getattr(logging, "warning" if lvl == "warning" else ("error" if lvl == "error" else "info"))(msg)

# ---------- Config (env vars on Render) ----------
ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_BASE_URL   = (
    os.getenv("ALPACA_BASE_URL")
    or os.getenv("APCA_API_BASE_URL")
    or "https://paper-api.alpaca.markets"
)

# Optional: simple auth for webhooks
WEBHOOK_KEY = os.getenv("WEBHOOK_KEY", "")

# ---------- Alpaca client ----------
api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)

app = Flask(__name__)

# ---------- Version / Ping ----------
@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({
        "status": "ok",
        "service": "TradingBot",
        "version": "1.0.0",
        "message": "Bot is alive and responding 🚀",
    }), 200

# ---------- Root health (browser-friendly) ----------
@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "TradingBot",
        "endpoints": ["/webhook", "/ping"],
    }), 200

# ---------- Webhook ----------
@app.route("/webhook", methods=["POST"])
def webhook():
    """
    Accepts TradingView JSON and places a market order or closes a position.
    Returns 200 with JSON on both success and handled errors (so TV doesn't retry forever).
    """
    req_id = str(uuid.uuid4())[:8]
    data = request.get_json(silent=True) or {}

    # Identify source for logs
    ua = request.headers.get("User-Agent", "unknown")
    src = "TradingView" if "tradingview" in ua.lower() else ("curl" if "curl" in ua.lower() else "unknown")

    # Log receipt (masking "key")
    safe = {k: ("***" if k.lower() == "key" else v) for k, v in data.items()}
    log("received", req_id=req_id, source=src, **safe)

    # --- simple auth (accept key via query (?key=/ ?token=) or JSON ("key"/"secret")) ---
    if WEBHOOK_KEY:
        provided = (
            request.args.get("key")
            or request.args.get("token")
            or (data.get("key") if isinstance(data, dict) else None)
            or (data.get("secret") if isinstance(data, dict) else None)
            or ""
    )
    if not hmac.compare_digest(str(provided), str(WEBHOOK_KEY)):
        log("unauthorized", level="warning", req_id=req_id)
        return jsonify({"status": "error", "message": "unauthorized"}), 401

    # --- validate payload ---
    symbol = (data.get("symbol") or "TSLA").upper().strip()
    side   = (data.get("side") or "buy").lower().strip()
    tif    = (data.get("time_in_force") or "day").lower().strip()

    if side not in {"buy", "sell", "close"}:
        return jsonify({"status": "error", "message": "side must be buy/sell/close"}), 400

    qty = None
    if side != "close":
        try:
            qty = int(data.get("qty", 1))
            if qty <= 0:
                return jsonify({"status": "error", "message": "qty must be > 0"}), 400
        except Exception:
            return jsonify({"status": "error", "message": "qty must be integer"}), 400
            
    # Quick server→Alpaca test: POST /selftest?token=let_me_in
    SELFTEST_TOKEN = os.getenv("SELFTEST_TOKEN", "let_me_in")
    
    @app.route("/selftest", methods=["POST"])
    def selftest():
        if request.args.get("token") != SELFTEST_TOKEN:
            return jsonify({"ok": False, "error": "forbidden"}), 403
        try:
            order = api.submit_order(
                symbol="AAPL", qty=1, side="buy", type="market", time_in_force="day"
            )
            log("selftest_order", id=order.id, symbol="AAPL", side="buy", qty=1)
            return jsonify({"ok": True, "order_id": order.id}), 200
        except Exception as e:
            log("selftest_error", level="error", error=str(e))
            return jsonify({"ok": False, "error": str(e)}), 500
           
    # --- idempotency (dedupe) ---
    client_id = (
        data.get("client_id")
        or f"{symbol}-{side}-{data.get('qty', 1)}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    )

    try:
        # Close = reduce-only (Alpaca closes open position entirely)
        if side == "close":
            api.close_position(symbol)
            log("close_position", req_id=req_id, symbol=symbol)
            return jsonify({"status": "success", "action": "close", "symbol": symbol}), 200

        # Place market order
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type="market",
            time_in_force=tif,
            client_order_id=client_id,
        )
        log("order_submitted", req_id=req_id, id=order.id, symbol=symbol, side=side, qty=qty)
        return jsonify({"status": "success", "order_id": order.id, "client_id": client_id}), 200

    except Exception as e:
        # Keep HTTP 200 so TradingView doesn't retry forever (switch to 400 if you want retries).
        msg = str(e)
        log("order_error", level="error", req_id=req_id, error=msg)
        return jsonify({"status": "error", "message": msg}), 200

# No app.run() here — Render starts this with Gunicorn:
# gunicorn -w 2 -k gthread -b 0.0.0.0:$PORT webhook_to_alpaca_price_action:app
