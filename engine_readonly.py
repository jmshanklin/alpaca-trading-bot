import os
import time
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
import alpaca_trade_api as tradeapi

# =========================
# Logging in Central Time
# =========================
CT = ZoneInfo("America/Chicago")

class CTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=CT)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setFormatter(CTFormatter(fmt="%(asctime)s CT [%(levelname)s] %(message)s"))
logger.handlers = [handler]

# =========================
# Env vars
# =========================
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() in ("1", "true", "yes", "y", "on")
ORDER_QTY = int(os.getenv("ORDER_QTY", "1"))
DROP_PCT = float(os.getenv("DROP_PCT", "0.0015"))  # 0.15% default
POLL_SEC = float(os.getenv("POLL_SEC", "1"))
MAX_BUYS_PER_TICK = int(os.getenv("MAX_BUYS_PER_TICK", "3"))  # safety cap per poll

# Optional: log manual liquidation / position changes
LOG_POSITION_CHANGES = os.getenv("LOG_POSITION_CHANGES", "true").strip().lower() in ("1", "true", "yes", "y", "on")

ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_BASE_URL   = os.getenv("ALPACA_BASE_URL") or os.getenv("APCA_API_BASE_URL") or "https://paper-api.alpaca.markets"

SYMBOL = os.getenv("ENGINE_SYMBOL", "TSLA").upper()

api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)

def get_live_price(symbol: str) -> float:
    """Uses latest trade price (not candles)."""
    t = api.get_latest_trade(symbol)
    return float(t.price)

def get_position_qty(symbol: str) -> float:
    """
    Returns current position qty (0.0 if no position).
    Helps detect manual liquidation or any external position change.
    """
    try:
        pos = api.get_position(symbol)
        return float(pos.qty)
    except Exception:
        return 0.0

def main():
    logging.info(
        f"ENGINE_START drop_pct={DROP_PCT:.6f} dry_run={DRY_RUN} symbol={SYMBOL}"
    )
    logging.info(
        "ENGINE_CONFIG "
        f"symbol={SYMBOL} "
        f"drop_pct={DROP_PCT:.6f} "
        f"order_qty={ORDER_QTY} "
        f"poll_sec={POLL_SEC} "
        f"max_buys_per_tick={MAX_BUYS_PER_TICK} "
        f"dry_run={DRY_RUN} "
        f"log_position_changes={LOG_POSITION_CHANGES}"
    )

    # ---- State that persists while the worker runs ----
    anchor_price = None
    next_trigger = None
    buy_count = 0
    last_pos_qty = None

    # Wait for market open before initializing anchor
    while True:
        clock = api.get_clock()
        if clock.is_open:
            break
        logging.info("MARKET_CLOSED waiting...")
        time.sleep(30)

    # Initialize anchor/next_trigger from live price
    anchor_price = get_live_price(SYMBOL)
    next_trigger = anchor_price * (1.0 - DROP_PCT)

    logging.info(f"ANCHOR_INIT anchor={anchor_price:.2f} next_trigger={next_trigger:.2f}")

    # Initialize last_pos_qty
    if LOG_POSITION_CHANGES:
        last_pos_qty = get_position_qty(SYMBOL)
        logging.info(f"POSITION_INIT qty={last_pos_qty:.4f}")

    while True:
        try:
            clock = api.get_clock()
            if not clock.is_open:
                logging.info("MARKET_CLOSED waiting...")
                time.sleep(30)
                continue

            # Optional position-change logging (detect manual liquidation)
            if LOG_POSITION_CHANGES:
                pos_qty = get_position_qty(SYMBOL)
                if last_pos_qty is None:
                    last_pos_qty = pos_qty
                elif pos_qty != last_pos_qty:
                    logging.info(f"POSITION_CHANGE qty_from={last_pos_qty:.4f} qty_to={pos_qty:.4f}")
                    last_pos_qty = pos_qty

            last_price = get_live_price(SYMBOL)

            logging.info(
                f"PRICE {SYMBOL} "
                f"last={last_price:.2f} anchor={anchor_price:.2f} next_trigger={next_trigger:.2f}"
            )

            # ---- Ladder trigger check with catch-up ----
            buys_this_tick = 0

            while last_price <= next_trigger and buys_this_tick < MAX_BUYS_PER_TICK:
                buy_count += 1
                buys_this_tick += 1

                if DRY_RUN:
                    logging.info(
                        f"SIM_BUY trigger_hit #{buy_count} "
                        f"last={last_price:.2f} trigger={next_trigger:.2f} "
                        f"anchor_before={anchor_price:.2f} drop_pct={DROP_PCT:.6f} "
                        f"qty={ORDER_QTY} dry_run={DRY_RUN}"
                    )
                else:
                    logging.info(
                        f"BUY trigger_hit #{buy_count} "
                        f"last={last_price:.2f} trigger={next_trigger:.2f} "
                        f"anchor_before={anchor_price:.2f} drop_pct={DROP_PCT:.6f} "
                        f"qty={ORDER_QTY} submitting market order"
                    )
                    api.submit_order(
                        symbol=SYMBOL,
                        qty=ORDER_QTY,
                        side="buy",
                        type="market",
                        time_in_force="day",
                    )

                # After the BUY, anchor becomes the rung that fired
                anchor_price = next_trigger
                next_trigger = anchor_price * (1.0 - DROP_PCT)

                logging.info(
                    f"ANCHOR_ADVANCE anchor_now={anchor_price:.2f} next_trigger={next_trigger:.2f}"
                )

            if last_price <= next_trigger and buys_this_tick >= MAX_BUYS_PER_TICK:
                logging.warning(
                    f"BUY_LIMIT reached MAX_BUYS_PER_TICK={MAX_BUYS_PER_TICK} "
                    f"last={last_price:.2f} next_trigger={next_trigger:.2f}"
                )

            time.sleep(POLL_SEC)

        except Exception as e:
            logging.error(f"ENGINE_ERROR {e}", exc_info=True)
            time.sleep(5)

if __name__ == "__main__":
    main()
