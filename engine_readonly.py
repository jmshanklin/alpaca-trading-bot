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

FILL_TIMEOUT_SEC = float(os.getenv("FILL_TIMEOUT_SEC", "20"))
FILL_POLL_SEC    = float(os.getenv("FILL_POLL_SEC", "0.5"))

MAX_BUYS_PER_TICK = int(os.getenv("MAX_BUYS_PER_TICK", "3"))  # safety cap per price poll

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

def submit_market_buy(symbol: str, qty: int):
    """
    Submit a market buy and return the Alpaca Order object.
    """
    return api.submit_order(
        symbol=symbol,
        qty=qty,
        side="buy",
        type="market",
        time_in_force="day",
    )

def wait_for_fill(order_id: str, timeout_sec: float, poll_sec: float):
    """
    Poll the order until it is filled/canceled/rejected/expired, or until timeout.
    Returns the final Order object (or last seen).
    """
    start = time.time()
    last = None

    while True:
        o = api.get_order(order_id)
        last = o
        status = (o.status or "").lower()

        if status in ("filled", "canceled", "rejected", "expired"):
            return o

        if time.time() - start >= timeout_sec:
            return o  # timeout: return last known state

        time.sleep(poll_sec)
        
def classify_position_change(prev_qty: float, new_qty: float, now_ts: float, last_bot_ts: float, grace_sec: float) -> str:
    """
    Returns a label describing what likely happened.
    We can’t *prove* it was manual, but we can strongly infer when bot wasn’t acting.
    """
    if prev_qty == new_qty:
        return "NO_CHANGE"

    if new_qty == 0.0 and prev_qty > 0.0:
        # If the bot hasn’t just acted, this is very likely a manual liquidation / external close.
        if (now_ts - last_bot_ts) > grace_sec:
            return "MANUAL_LIQUIDATION_SUSPECTED"
        return "POSITION_WENT_FLAT_AFTER_BOT_ACTION"

    if new_qty < prev_qty:
        if (now_ts - last_bot_ts) > grace_sec:
            return "EXTERNAL_REDUCTION_SUSPECTED"
        return "REDUCTION_AFTER_BOT_ACTION"

    if new_qty > prev_qty:
        if (now_ts - last_bot_ts) > grace_sec:
            return "EXTERNAL_INCREASE_SUSPECTED"
        return "INCREASE_AFTER_BOT_ACTION"

def main():
    logging.info(f"ENGINE_START drop_pct={DROP_PCT:.6f} dry_run={DRY_RUN} symbol={SYMBOL}")
    logging.info(
        "ENGINE_CONFIG "
        f"symbol={SYMBOL} "
        f"drop_pct={DROP_PCT:.6f} "
        f"order_qty={ORDER_QTY} "
        f"poll_sec={POLL_SEC} "
        f"fill_timeout_sec={FILL_TIMEOUT_SEC} "
        f"fill_poll_sec={FILL_POLL_SEC} "
        f"max_buys_per_tick={MAX_BUYS_PER_TICK} "
        f"dry_run={DRY_RUN} "
        f"log_position_changes={LOG_POSITION_CHANGES}"
    )

    # ---- State that persists while the worker runs ----
    anchor_price = None
    next_trigger = None
    buy_count = 0
    last_pos_qty = None
    
    # NEW: helps classify position changes
    last_bot_action_ts = 0.0      # when the bot last submitted an order
    BOT_ACTION_GRACE_SEC = 10.0   # position change within this window is probably from bot
    
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
                    logging.info(f"POSITION_INIT qty={last_pos_qty:.4f}")
                elif pos_qty != last_pos_qty:
                    now_ts = time.time()
                    label = classify_position_change(last_pos_qty, pos_qty, now_ts, last_bot_action_ts, BOT_ACTION_GRACE_SEC)
            
                    logging.warning(
                        f"POSITION_CHANGE label={label} qty_from={last_pos_qty:.4f} qty_to={pos_qty:.4f}"
                    )
            
                    last_pos_qty = pos_qty

            last_price = get_live_price(SYMBOL)
            logging.info(f"PRICE {SYMBOL} last={last_price:.2f} anchor={anchor_price:.2f} next_trigger={next_trigger:.2f}")

            # ---- Ladder trigger check with catch-up ----
            buys_this_tick = 0

            while last_price <= next_trigger and buys_this_tick < MAX_BUYS_PER_TICK:
                buy_count += 1
                buys_this_tick += 1

                trigger_price = next_trigger  # capture the rung that fired

                if DRY_RUN:
                    logging.info(
                        f"SIM_BUY trigger_hit #{buy_count} "
                        f"last={last_price:.2f} trigger={trigger_price:.2f} "
                        f"anchor_before={anchor_price:.2f} drop_pct={DROP_PCT:.6f} "
                        f"qty={ORDER_QTY} dry_run={DRY_RUN}"
                    )
                else:
                    # 1) Submit
                    logging.info(
                        f"BUY_SUBMIT trigger_hit #{buy_count} "
                        f"last={last_price:.2f} trigger={trigger_price:.2f} "
                        f"anchor_before={anchor_price:.2f} drop_pct={DROP_PCT:.6f} "
                        f"qty={ORDER_QTY} type=market tif=day"
                    )
                    order = submit_market_buy(SYMBOL, ORDER_QTY)

                    logging.info(
                        f"BUY_SUBMITTED id={order.id} status={order.status} "
                        f"qty={getattr(order, 'qty', None)}"
                    )

                    # 2) Wait for fill (or terminal/timeout)
                    final_order = wait_for_fill(order.id, FILL_TIMEOUT_SEC, FILL_POLL_SEC)

                    status = (final_order.status or "").lower()
                    filled_qty = getattr(final_order, "filled_qty", None)
                    filled_avg_price = getattr(final_order, "filled_avg_price", None)

                    logging.info(
                        f"BUY_RESULT id={final_order.id} status={final_order.status} "
                        f"filled_qty={filled_qty} filled_avg_price={filled_avg_price}"
                    )

                    if status != "filled":
                        logging.warning(
                            f"BUY_NOT_FILLED id={final_order.id} status={final_order.status} "
                            f"(timeout={FILL_TIMEOUT_SEC}s poll={FILL_POLL_SEC}s)"
                        )

                # After the BUY, anchor becomes the rung that fired
                anchor_price = trigger_price
                next_trigger = anchor_price * (1.0 - DROP_PCT)

                logging.info(f"ANCHOR_ADVANCE anchor_now={anchor_price:.2f} next_trigger={next_trigger:.2f}")

                # Refresh price so "catch-up" can continue accurately
                last_price = get_live_price(SYMBOL)

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
