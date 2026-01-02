# webhook_to_alpaca_price_action.py
# --------------------------------------------------------------------
# TradingBot webhook for Alpaca (Render + Flask, Python 3.11+)
#
# Supports:
# - US equities (ex: TSLA): time_in_force defaults to "day"
# - Crypto (ex: BTCUSD): time_in_force forced to "gtc" (Alpaca crypto requirement)
#
# Expected JSON from TradingView (example):
# {
#   "symbol": "TSLA" or "BTCUSD",
#   "side":   "buy" | "sell" | "close",
#   "qty":    1            (equities: integer shares)
#   "qty":    0.001        (crypto: fractional ok)
#   "time_in_force": "day" (optional; ignored for crypto)
#   "client_id": "optional-id",
#   "key": "<optional key if not using ?key=...>"
# }
# --------------------------------------------------------------------

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
# Accept both naming styles
ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_BASE_URL   = os.getenv("ALPACA_BASE_URL") or os.getenv("APCA_API_BASE_URL") or "https://paper-api.alpaca.markets"

WEBHOOK_KEY   = os.getenv("WEBHOOK_KEY", "")
SELFTEST_TOKEN = os.getenv("SELFTEST_TOKEN", "let_me_in")

if not ALPACA_KEY_ID or not ALPACA_SECRET_KEY:
    log("missing_alpaca_keys", level="warning", hint="Set ALPACA_KEY_ID/ALPACA_SECRET_KEY (or APCA_*) in Render env vars")

api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)

app = Flask(__name__)

# ---------- Health / Ping ----------
@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "status": "ok",
        "service": "TradingBot",
        "version": "2.0.0",
        "endpoints": ["/webhook", "/ping", "/healthz", "/selftest"],
    }), 200

@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"status": "ok", "service": "TradingBot", "version": "2.0.0"}), 200

# Render Health Check Path in your settings is /healthz
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
def get_asset_class(symbol: str) -> str:
    """
    Returns: 'crypto' or 'us_equity' (or 'unknown')
    """
    try:
        asset = api.get_asset(symbol)
        # alpaca_trade_api Asset has .class (yes, named 'class')
        cls = getattr(asset, "class", None) or getattr(asset, "_raw", {}).get("class")
        return (cls or "unknown").lower()
    except Exception:
        return "unknown"

def is_crypto(symbol: str) -> bool:
    cls = get_asset_class(symbol)
    return cls == "crypto"

def parse_qty(raw_qty, crypto: bool):
    """
    equities: integer shares
    crypto: allow fractional (float)
    """
    if raw_qty is None:
        return 1.0 if crypto else 1

    if crypto:
        try:
            q = float(raw_qty)
            if q <= 0:
                raise ValueError()
            return q
        except Exception:
            raise ValueError("qty must be a positive number for crypto (example: 0.001)")
    else:
        try:
            q = int(raw_qty)
            if q <= 0:
                raise ValueError()
            return q
        except Exception:
            raise ValueError("qty must be a positive integer for equities (example: 1)")

# ---------- Webhook ----------
@app.route("/webhook", methods=["POST"])
def webhook():
    req_id = str(uuid.uuid4())[:8]
    import json

raw = request.get_data(as_text=True) or ""
data = request.get_json(silent=True)

if data is None:
    # TradingView often posts text/plain. If it looks like JSON, parse it.
    try:
        data = json.loads(raw) if raw.strip().startswith("{") else {}
    except Exception:
        data = {}

    ua = request.headers.get("User-Agent", "unknown")
    src = "TradingView" if "tradingview" in ua.lower() else ("powershell" if "powershell" in ua.lower() else "unknown")

    safe = {k: ("***" if k.lower() == "key" else v) for k, v in (data.items() if isinstance(data, dict) else [])}
    log("received", req_id=req_id, source=src, **safe)

    # --- Auth: accept ?key=... OR JSON key/secret ---
    provided = (
        request.args.get("key")
        or request.args.get("token")
        or (data.get("key") if isinstance(data, dict) else None)
        or (data.get("secret") if isinstance(data, dict) else None)
        or ""
    )
    if WEBHOOK_KEY and not hmac.compare_digest(str(provided), str(WEBHOOK_KEY)):
        log("unauthorized", level="warning", req_id=req_id)
        return jsonify({"status": "error", "message": "unauthorized"}), 401

    # --- Validate payload ---
    symbol = (data.get("symbol") or "TSLA").upper().strip()
    side   = (data.get("side") or "buy").lower().strip()

    if side not in {"buy", "sell", "close"}:
        return jsonify({"status": "error", "message": "side must be buy/sell/close"}), 400

    crypto = is_crypto(symbol)

    # IMPORTANT FIX:
    # Alpaca crypto does NOT accept time_in_force="day".
    # Force crypto TIF to gtc regardless of what TradingView sends.
    if crypto:
        tif = "gtc"
    else:
        tif = (data.get("time_in_force") or "day").lower().strip()

    # qty rules
    qty = None
    if side != "close":
        qty_raw = data.get("qty", 1)
        try:
            qty_dec = Decimal(str(qty_raw))
        except InvalidOperation:
            return jsonify({"status": "error", "message": "qty must be numeric"}), 400
    
        if qty_dec <= 0:
            return jsonify({"status": "error", "message": "qty must be > 0"}), 400

    qty = str(qty_dec)  # IMPORTANT: pass as string to Alpaca

    # idempotency
    client_id = (
        (data.get("client_id") if isinstance(data, dict) else None)
        or f"{symbol}-{side}-{data.get('qty', 1)}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    )

    try:
        # Close behavior:
        # - equities: close_position(symbol)
        # - crypto: keep it simple: do not auto-close (you can sell explicitly with qty)
        if side == "close":
            if crypto:
                return jsonify({"status": "error", "message": "close not supported for crypto in this simple tester; send a sell with qty instead"}), 400
            api.close_position(symbol)
            log("close_position", req_id=req_id, symbol=symbol)
            return jsonify({"status": "success", "action": "close", "symbol": symbol}), 200

        # Submit market order
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type="market",
            time_in_force=tif,
            client_order_id=client_id,
        )
        log("order_submitted", req_id=req_id, id=order.id, symbol=symbol, side=side, qty=qty, tif=tif, asset_class=("crypto" if crypto else "equity"))
        return jsonify({"status": "success", "order_id": order.id, "client_id": client_id, "tif": tif}), 200

    except Exception as e:
        msg = str(e)
        log("order_error", level="error", req_id=req_id, error=msg, symbol=symbol, side=side, qty=qty, tif=tif)
        # Keep 200 so TradingView doesn't hammer retries
        return jsonify({"status": "error", "message": msg}), 200
