import os
import time
import logging
from datetime import datetime

import alpaca_trade_api as tradeapi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_BASE_URL   = os.getenv("ALPACA_BASE_URL") or os.getenv("APCA_API_BASE_URL") or "https://paper-api.alpaca.markets"

SYMBOL = os.getenv("ENGINE_SYMBOL", "TSLA").upper()

api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)

def main():
    logging.info(f"ENGINE_START readonly symbol={SYMBOL}")

    last_bar_time = None

    while True:
        try:
            clock = api.get_clock()
            if not clock.is_open:
                logging.info("MARKET_CLOSED waiting...")
                time.sleep(30)
                continue

            bars = api.get_bars(SYMBOL, tradeapi.TimeFrame.Minute, limit=1)
            if not bars:
                time.sleep(2)
                continue

            b = bars[-1]
            bar_time = b.t

            if last_bar_time == bar_time:
                time.sleep(2)
                continue

            last_bar_time = bar_time

            o = float(b.o)
            c = float(b.c)
            is_red = c < o
            is_green = c > o

            logging.info(
                f"BAR_CLOSE {SYMBOL} t={bar_time} O={o:.2f} C={c:.2f} red={is_red} green={is_green}"
            )

            if is_red:
                logging.info("SIGNAL WOULD_BUY (dry-run)")

        except Exception as e:
            logging.error(f"ENGINE_ERROR {e}", exc_info=True)
            time.sleep(5)

if __name__ == "__main__":
    main()
