from flask import Flask, request, jsonify
import os
import alpaca_trade_api as tradeapi

# === Configuration ===
# Render: set these in the Render dashboard (Environment tab).
ALPACA_KEY_ID = os.getenv('ALPACA_KEY_ID')
ALPACA_SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')
ALPACA_BASE_URL = os.getenv('ALPACA_BASE_URL', 'https://paper-api.alpaca.markets')

# Init client (raises if keys missing at runtime)
api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)

app = Flask(__name__)

@app.route('/', methods=['GET'])
def health():
    """Basic health check for Render and for you to test in a browser."""
    return jsonify({'status': 'ok', 'service': 'TradingBot', 'endpoints': ['/webhook']})

@app.route('/webhook', methods=['POST'])
def webhook():
    """Receives TradingView webhook JSON and places a simple market order."""
    data = request.get_json(silent=True) or {}
    print("Received webhook:", data)

    symbol = data.get("symbol", "SPY")
    side = data.get("side", "buy")
    qty = int(data.get("qty", 1))

    try:
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type='market',
            time_in_force='day'
        )
        return jsonify({"status": "success", "symbol": symbol, "side": side, "qty": qty, "order_id": getattr(order, "id", None)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
