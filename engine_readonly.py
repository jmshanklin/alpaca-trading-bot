import os
import time
import json
import logging
from datetime import datetime, timezone
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
# Persistence helpers
# =========================
def resolve_state_path() -> str:
    """
    Choose a writable state path.
    Prefers Render disk mount (default /var/data) if writable, else falls back to /tmp.
    """
    state_dir = os.getenv("STATE_DIR", "/var/data")
    state_file = os.getenv("STATE_FILE", "engine_state.json")
    state_path = os.getenv("STATE_PATH", os.path.join(state_dir, state_file))

    try:
        os.makedirs(os.path.dirname(state_path), exist_ok=True)

        # quick write test to confirm this path is writable
        testfile = os.path.join(os.path.dirname(state_path), ".write_test")
        with open(testfile, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(testfile)

        return state_path
    except Exception as e:
        fallback_dir = "/tmp"
        os.makedirs(fallback_dir, exist_ok=True)
        fallback_path = os.path.join(fallback_dir, state_file)
        logging.warning(f"STATE_PATH not writable ({e}); falling back to {fallback_path}")
        return fallback_path


# =========================
# Env vars
# =========================
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() in ("1", "true", "yes", "y", "on")
ORDER_QTY = int(os.getenv("ORDER_QTY", "1"))

# Polling for new closed bars
POLL_SEC = float(os.getenv("POLL_SEC", "1.0"))

# Fill logging (used when DRY_RUN=false and we submit orders)
FILL_TIMEOUT_SEC = float(os.getenv("FILL_TIMEOUT_SEC", "20"))
FILL_POLL_SEC = float(os.getenv("FILL_POLL_SEC", "0.5"))

# Safety cap: prevents spamming buys in a single loop tick
MAX_BUYS_PER_TICK = int(os.getenv("MAX_BUYS_PER_TICK", "1"))

# Optional: log manual liquidation / position changes
LOG_POSITION_CHANGES = os.getenv("LOG_POSITION_CHANGES", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)

# Persistence
STATE_PATH = resolve_state_path()

# Alpaca connection
ALPACA_KEY_ID = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_BASE_URL = (
    os.getenv("ALPACA_BASE_URL")
    or os.getenv("APCA_API_BASE_URL")
    or "https://paper-api.alpaca.markets"
)

SYMBOL = os.getenv("ENGINE_SYMBOL", "TSLA").upper()

api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)


# =========================
# State I/O
# =========================
def load_state() -> dict:
    """Load persisted state from disk (if present)."""
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                s = json.load(f)
            return s or {}
    except Exception as e:
        logging.warning(f"STATE_LOAD failed: {e}")
    return {}


def save_state(state: dict) -> None:
    """Persist state to disk."""
    try:
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
    except Exception as e:
        logging.warning(f"STATE_SAVE failed: {e}")


# =========================
# Trading helpers
# =========================
def get_position_qty(symbol: str) -> float:
    """Returns current position qty (0.0 if no position)."""
    try:
        pos = api.get_position(symbol)
        return float(pos.qty)
    except Exception:
        return 0.0


def submit_market_buy(symbol: str, qty: int):
    """Submit a market buy and return the Alpaca Order object."""
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
            return o

        time.sleep(poll_sec)


def pick_latest_closed_bar(symbol: str, now_utc: datetime):
    """
    Fetch recent 1-min bars and choose the latest bar that is definitely closed.

    Rule: a bar is closed if its timestamp is < current minute floor (UTC).
    """
    bars = api.get_bars(symbol, tradeapi.TimeFrame.Minute, limit=5)
    if not bars:
        return None

    now_floor = now_utc.replace(second=0, microsecond=0)

    for b in reversed(bars):
        bt = b.t
        if bt.tzinfo is None:
            bt = bt.replace(tzinfo=timezone.utc)
        if bt < now_floor:
            return b

    return None


# =========================
# Main
# =========================
def main():
    logging.info(f"ENGINE_START mode=RED_CLOSE_OPTION_2 dry_run={DRY_RUN} symbol={SYMBOL}")
    logging.info(
        "ENGINE_CONFIG "
        f"symbol={SYMBOL} "
        f"order_qty={ORDER_QTY} "
        f"poll_sec={POLL_SEC} "
        f"fill_timeout_sec={FILL_TIMEOUT_SEC} "
        f"fill_poll_sec={FILL_POLL_SEC} "
        f"max_buys_per_tick={MAX_BUYS_PER_TICK} "
        f"log_position_changes={LOG_POSITION_CHANGES} "
        f"state_path={STATE_PATH} "
        f"dry_run={DRY_RUN}"
    )

    # ---- Load persisted state ----
    state = load_state()
    last_bar_ts_iso = state.get("last_bar_ts")  # ISO string
    last_red_buy_close = state.get("last_red_buy_close")  # float or None
    buy_count = int(state.get("buy_count", 0))

    # Convert last_bar_ts back to datetime (UTC)
    last_bar_ts = None
    if last_bar_ts_iso:
        try:
            last_bar_ts = datetime.fromisoformat(last_bar_ts_iso)
            if last_bar_ts.tzinfo is None:
                last_bar_ts = last_bar_ts.replace(tzinfo=timezone.utc)
        except Exception:
            last_bar_ts = None

    logging.info(
        f"STATE_LOADED last_bar_ts={last_bar_ts_iso} "
        f"last_red_buy_close={last_red_buy_close} buy_count={buy_count}"
    )

    # Position-change baseline
    last_pos_qty = None
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

            # Optional: detect manual liquidation (or any external position change)
            if LOG_POSITION_CHANGES:
                pos_qty = get_position_qty(SYMBOL)
                if last_pos_qty is None:
                    last_pos_qty = pos_qty
                elif pos_qty != last_pos_qty:
                    logging.info(
                        f"POSITION_CHANGE qty_from={last_pos_qty:.4f} qty_to={pos_qty:.4f}"
                    )
                    last_pos_qty = pos_qty

            # Use Alpaca clock timestamp as "now" (UTC)
            now_utc = clock.timestamp
            if now_utc.tzinfo is None:
                now_utc = now_utc.replace(tzinfo=timezone.utc)

            b = pick_latest_closed_bar(SYMBOL, now_utc)
            if b is None:
                time.sleep(POLL_SEC)
                continue

            bar_ts = b.t
            if bar_ts.tzinfo is None:
                bar_ts = bar_ts.replace(tzinfo=timezone.utc)

            # Skip if we've already processed this candle
            if last_bar_ts is not None and bar_ts <= last_bar_ts:
                time.sleep(POLL_SEC)
                continue

            o = float(b.o)
            c = float(b.c)
            is_red = c < o

            logging.info(
                f"BAR_CLOSE {SYMBOL} t={bar_ts.isoformat()} O={o:.2f} C={c:.2f} red={is_red}"
            )

            buys_this_tick = 0

            if is_red:
                if last_red_buy_close is None:
                    should_buy = True
                    reason = "FIRST_RED_BUY"
                else:
                    should_buy = c < float(last_red_buy_close)
                    reason = (
                        "LOWER_THAN_LAST_RED_BUY" if should_buy else "NOT_LOWER_THAN_LAST_RED_BUY"
                    )

                if should_buy:
                    if buys_this_tick >= MAX_BUYS_PER_TICK:
                        logging.warning(
                            f"BUY_LIMIT reached MAX_BUYS_PER_TICK={MAX_BUYS_PER_TICK} "
                            f"bar_ts={bar_ts.isoformat()} close={c:.2f}"
                        )
                    else:
                        buy_count += 1
                        buys_this_tick += 1

                        if DRY_RUN:
                            logging.info(
                                f"SIM_BUY #{buy_count} reason={reason} "
                                f"close={c:.2f} last_red_buy_close={last_red_buy_close} qty={ORDER_QTY}"
                            )
                        else:
                            logging.info(
                                f"BUY_SIGNAL #{buy_count} reason={reason} "
                                f"close={c:.2f} last_red_buy_close={last_red_buy_close} qty={ORDER_QTY}"
                            )
                            order = submit_market_buy(SYMBOL, ORDER_QTY)
                            logging.info(
                                f"ORDER_SUBMITTED id={order.id} qty={ORDER_QTY} type=market side=buy"
                            )

                            final = wait_for_fill(order.id, FILL_TIMEOUT_SEC, FILL_POLL_SEC)
                            status = (final.status or "").lower()

                            avg_fill = getattr(final, "filled_avg_price", None)
                            filled_qty = getattr(final, "filled_qty", None)

                            logging.info(
                                f"ORDER_FINAL id={order.id} status={status} "
                                f"filled_qty={filled_qty} avg_fill_price={avg_fill}"
                            )

                        # Update “memory” only when we actually buy
                        last_red_buy_close = float(c)
                        logging.info(
                            f"RED_BUY_MEMORY_UPDATE last_red_buy_close={last_red_buy_close:.2f}"
                        )
                else:
                    logging.info(
                        f"RED_SKIP reason={reason} close={c:.2f} last_red_buy_close={last_red_buy_close}"
                    )

            # Update last processed bar and persist
            last_bar_ts = bar_ts
            save_state(
                {
                    "last_bar_ts": last_bar_ts.isoformat(),
                    "last_red_buy_close": last_red_buy_close,
                    "buy_count": buy_count,
                    "symbol": SYMBOL,
                }
            )

            time.sleep(POLL_SEC)

        except Exception as e:
            logging.error(f"ENGINE_ERROR {e}", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
