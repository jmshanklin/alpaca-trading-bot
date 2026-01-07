# engine_readonly.py
import os
import time
import logging
from datetime import datetime, timezone

import alpaca_trade_api as tradeapi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_BASE_URL   = os.getenv("ALPACA_BASE_URL") or os.getenv("APCA_API_BASE_URL") or "https://paper-api.alpaca.markets"

SYMBOL = os.getenv("ENGINE_SYMBOL", "TSLA").upper()
TF_SEC = 60  # 1-minute bars
POLL_SEC = 2

api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)

def is_market_open_now() -> bool:
    """Simple + reliable: ask Alpaca clock."""
    try:
        clock = api.get_clock()
        return bool(getattr(clock, "is_open", False))
    except Exception as e:
        logging.warning(f"clock_error: {e}")
        return False

def get_latest_bar(symbol: str):
    """
    Alpaca v2 bars via alpaca_trade_api.
    Returns last bar as dict-like with o/h/l/c/t.
    """
    bars = api.get_bars(symbol, tradeapi.TimeFrame.Minute, limit=1)
    if not bars:
        return None
    b = bars[-1]
    # alpaca_trade_api returns Bar objects with attributes
    return {
        "t": getattr(b, "t", None),  # timestamp
        "o": float(b.o),
        "h": float(b.h),
        "l": float(b.l),
        "c": float(b.c),
        "v": float(b.v),
    }

def main():
    logging.info(f"ENGINE_START readonly symbol={SYMBOL} base_url={ALPACA_BASE_URL}")

    last_bar_time = None

    while True:
        try:
            if not is_market_open_now():
                logging.info("MARKET_CLOSED waiting...")
                time.sleep(15)
                continue

            bar = get_latest_bar(SYMBOL)
            if not bar or not bar["t"]:
                logging.info("NO_BAR waiting...")
                time.sleep(POLL_SEC)
                continue

            bar_time = bar["t"]
            # Only process once per new bar close
            if last_bar_time == bar_time:
                time.sleep(POLL_SEC)
                continue

            last_bar_time = bar_time

            o = bar["o"]
            c = bar["c"]
            is_red = c < o
            is_green = c > o

            logging.info(
                f"BAR_CLOSE {SYMBOL} t={bar_time} O={o:.2f} C={c:.2f} red={is_red} green={is_green}"
            )

            # === YOUR STRATEGY (phase 1): only log “would buy” on red closes ===
            if is_red:
                logging.info(f"SIGNAL WOULD_BUY {SYMBOL} reason=red_close qty=<your_qty_logic_here>")

            # Later we’ll add: ladder filters, groups, TP calc, and Alpaca position reconciliation

        except Exception as e:
            logging.error(f"ENGINE_LOOP_ERROR: {e}", exc_info=True)
            time.sleep(5)

if __name__ == "__main__":
    main()
