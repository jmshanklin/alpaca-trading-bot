import os
import time
import logging
from zoneinfo import ZoneInfo
import alpaca_trade_api as tradeapi

# ---- Logging in Central Time (CT) ----
class CTFormatter(logging.Formatter):
    converter = lambda self, ts: time.gmtime(ts)  # will be overridden below

def ct_time_converter(*args):
    # Convert "now" to America/Chicago for log timestamps
    return time.localtime()

# We'll set formatter with custom time format manually:
logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter(fmt="%(asctime)s CT [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logger.handlers = [handler]

CT = ZoneInfo("America/Chicago")

def ct_asctime(record, datefmt=None):
    dt = record.created
    # record.created is epoch seconds
    from datetime import datetime
    return datetime.fromtimestamp(dt, tz=CT).strftime(datefmt or "%Y-%m-%d %H:%M:%S")

# Monkey-patch formatter to use CT time
_old_formatTime = formatter.formatTime
def _formatTime(record, datefmt=None):
    return ct_asctime(record, datefmt)
formatter.formatTime = _formatTime


# ---- Env vars ----
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() in ("1", "true", "yes", "y", "on")
ORDER_QTY = int(os.getenv("ORDER_QTY", "1"))
DROP_PCT = float(os.getenv("DROP_PCT", "0.0015"))  # 0.15% default
POLL_SEC = float(os.getenv("POLL_SEC", "1"))

ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_BASE_URL   = os.getenv("ALPACA_BASE_URL") or os.getenv("APCA_API_BASE_URL") or "https://paper-api.alpaca.markets"

SYMBOL = os.getenv("ENGINE_SYMBOL", "TSLA").upper()

api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)

def get_live_price(symbol: str) -> float:
    """
    Uses latest trade price (not candles).
    """
    t = api.get_latest_trade(symbol)
    return float(t.price)

def main():
    logging.info(f"ENGINE_START drop_pct={DROP_PCT:.6f} dry_run={DRY_RUN} symbol={SYMBOL}")

    # Wait for market open
    while True:
        clock = api.get_clock()
        if clock.is_open:
            break
        logging.info("MARKET_CLOSED waiting...")
        time.sleep(30)

    # Step 1 Option A: Anchor initializes from live market price
    anchor_price = get_live_price(SYMBOL)
    next_trigger = anchor_price * (1.0 - DROP_PCT)

    logging.info(f"ANCHOR_INIT anchor={anchor_price:.2f} next_trigger={next_trigger:.2f}")

    while True:
        try:
            clock = api.get_clock()
            if not clock.is_open:
                logging.info("MARKET_CLOSED waiting...")
                time.sleep(30)
                continue

            price = get_live_price(SYMBOL)

            logging.info(f"PRICE {SYMBOL} last={price:.2f} anchor={anchor_price:.2f} next_trigger={next_trigger:.2f}")

            if price <= next_trigger:
                if DRY_RUN:
                    logging.info(f"SIGNAL WOULD_BUY drop_pct={DROP_PCT:.6f} at_price={price:.2f}")
                else:
                    logging.info(f"SIGNAL BUY (paper) qty={ORDER_QTY} submitting market order at_price={price:.2f}")
                    api.submit_order(
                        symbol=SYMBOL,
                        qty=ORDER_QTY,
                        side="buy",
                        type="market",
                        time_in_force="day",
                    )

                # After BUY trigger: anchor becomes the trigger price (step logic)
                anchor_price = next_trigger
                next_trigger = anchor_price * (1.0 - DROP_PCT)
                logging.info(f"ANCHOR_UPDATE anchor={anchor_price:.2f} next_trigger={next_trigger:.2f}")

            time.sleep(POLL_SEC)

        except Exception as e:
            logging.error(f"ENGINE_ERROR {e}", exc_info=True)
            time.sleep(5)

if __name__ == "__main__":
    main()
