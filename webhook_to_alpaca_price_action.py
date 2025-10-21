from flask import Flask, request, jsonify
import os, uuid, hmac, logging
from datetime import datetime
import alpaca_trade_api as tradeapi

# ---------- Logging ----------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

def log(message, level="info", **fields):
    trailer = ""
    if fields:
        trailer = " | " + ", ".join(f"{k}={v}" for k, v in fields.items())
    msg = f"{message}{trailer}"
    lvl = level.lower()
    getattr(logging, "warning" if lvl == "warning" else "error" if lvl == "error" else "info")(msg)

# ---------- Config (env vars on Render) ----------
ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID")     or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_BASE_URL   = (
    os.getenv("ALPACA_BASE_URL")
    or os.getenv("APCA_API_BASE_URL")
    or "https://paper-api.alpaca.markets"
)

WEBHOOK_KEY = os.getenv("WEBHOOK_KEY", "")  # set in Render → Environment

# ---------- Alpaca client ----------
api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)

app = Flask(__name__)

# ---------- Health ----------
@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "TradingBot", "endpoints": ["/webhook"]})

# ---------- Version / Ping ----------
@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({
        "status": "ok",
        "service": "TradingBot",
        "version": "1.0.0",
        "message": "Bot is alive and responding 🚀"
    })

# ---------- Webhook ----------
@app.route("/webhook", methods=["POST"])
def webhook():
    req_id = str(uuid.uuid4())[:8]
    data = request.get_json(silent=True) or {}

    # Identify source (TradingView vs curl vs unknown)
    user_agent = request.headers.get("User-Agent", "unknown")
    ua = user_agent.lower()
    source = "TradingView" if "tradingview" in ua else ("curl" if "curl" in ua else "unknown")

    # Never log the secret key
    safe = {k: ("***" if k == "key" else v) for k, v in data.items()}
    log("received", req_id=req_id, source=source, **safe)

    # --- simple auth (optional) ---
    if WEBHOOK_KEY:
        provided = data.get("key", "")
        # timing-safe compare (and don't leak type errors)
        if not (isinstance(provided, str) and hmac.compare_digest(provided, WEBHOOK_KEY)):
            log("unauthorized", level="warning", req_id=req_id)
            return jsonify({"status": "error", "message": "unauthorized"}), 401

    # --- validate payload ---
    symbol = (data.get("symbol") or "SPY").upper().strip()
    side   = (data.get("side") or "buy").lower().strip()
    tif    = data.get("time_in_force", "day")

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

    # --- idempotency (dedupe) ---
    client_id = data.get("client_id") or f"{symbol}-{side}-{data.get('qty',1)}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    try:
        if side == "close":
            api.close_position(symbol)
            log("close_position", req_id=req_id, symbol=symbol)
            return jsonify({"status": "success", "action": "close", "symbol": symbol})

        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type="market",
            time_in_force=tif,
            client_order_id=client_id,
        )
        log("order_submitted", req_id=req_id, id=order.id, symbol=symbol, side=side, qty=qty)
        return jsonify({"status": "success", "order_id": order.id, "client_id": client_id})

    except Exception as e:
        # keep HTTP 200 so external senders don't retry forever; switch to 400 if you prefer retries
        msg = str(e)
        log("order_error", level="error", req_id=req_id, error=msg)
        return jsonify({"status": "error", "message": msg}), 200

# No app.run() here — Render starts it with Gunicorn
# Start command on Render:
# gunicorn -w 2 -k gthread -b 0.0.0.0:$PORT webhook_to_alpaca_price_action:app

