import os
from flask import Flask, jsonify
import alpaca_trade_api as tradeapi

app = Flask(__name__)

# --- Alpaca connection ---
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")

api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")


@app.route("/")
def home():
    return "Alpaca Report Service Running"


@app.route("/report")
def report():
    """Minimal Phase 1 report: account + TSLA position"""

    # Get account
    acct = api.get_account()

    # Try to get TSLA position (may not exist if flat)
    position_data = None
    try:
        pos = api.get_position("TSLA")
        position_data = {
            "symbol": pos.symbol,
            "qty": float(pos.qty),
            "avg_entry": float(pos.avg_entry_price),
            "market_value": float(pos.market_value),
            "unrealized_pl": float(pos.unrealized_pl),
            "current_price": float(pos.current_price),
        }
    except Exception:
        position_data = None

    data = {
        "account": {
            "equity": float(acct.equity),
            "cash": float(acct.cash),
            "buying_power": float(acct.buying_power),
        },
        "position": position_data,
    }

    return jsonify(data)
