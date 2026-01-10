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

# Fill polling
FILL_TIMEOUT_SEC = float(os.getenv("FILL_TIMEOUT_SEC", "20"))
FILL_POLL_SEC = float(os.getenv("FILL_POLL_SEC", "0.5"))

# Safety cap
MAX_BUYS_PER_TICK = int(os.getenv("MAX_BUYS_PER_TICK", "1"))

# Optional: log manual liquidation / position changes
LOG_POSITION_CHANGES = os.getenv("LOG_POSITION_CHANGES", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)

# State save throttle (0 = save every processed bar)
STATE_SAVE_SEC = float(os.getenv("STATE_SAVE_SEC", "0"))

# SELL target above anchor: e.g. 0.01 = +1%
SELL_PCT = float(os.getenv("SELL_PCT", "0.0"))
RESET_SIM_OWNED_ON_START = os.getenv(
    "RESET_SIM_OWNED_ON_START", "false"
).strip().lower() in ("1", "true", "yes", "y", "on")

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

if not ALPACA_KEY_ID or not ALPACA_SECRET_KEY:
    raise RuntimeError(
        "Missing Alpaca credentials: set ALPACA_KEY_ID/ALPACA_SECRET_KEY "
        "(or APCA_API_KEY_ID/APCA_API_SECRET_KEY)."
    )

api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)

# =========================
# State I/O
# =========================
def load_state() -> dict:
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                s = json.load(f)
            return s or {}
    except Exception as e:
        logging.warning(f"STATE_LOAD failed: {e}")
    return {}


def save_state(payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
    except Exception as e:
        logging.warning(f"STATE_SAVE failed: {e}")

def maybe_persist_state(state: dict, payload: dict) -> None:
    state.update(payload)

    if STATE_SAVE_SEC <= 0:
        save_state(state)
        state["_last_save_ts"] = time.time()
        return

    now_ts = time.time()
    last_ts = float(state.get("_last_save_ts", 0.0))
    if (now_ts - last_ts) >= STATE_SAVE_SEC:
        save_state(state)
        state["_last_save_ts"] = now_ts

# =========================
# Trading helpers
# =========================
def get_position(symbol: str):
    try:
        return api.get_position(symbol)
    except Exception:
        return None

def get_position_qty(symbol: str) -> float:
    pos = get_position(symbol)
    if not pos:
        return 0.0
    try:
        return float(pos.qty)
    except Exception:
        return 0.0

def get_position_avg_entry(symbol: str):
    pos = get_position(symbol)
    if not pos:
        return None
    avg = getattr(pos, "avg_entry_price", None)
    if avg is None:
        return None
    try:
        return float(avg)
    except Exception:
        return None

def submit_market_buy(symbol: str, qty: int):
    return api.submit_order(
        symbol=symbol,
        qty=qty,
        side="buy",
        type="market",
        time_in_force="day",
    )

def submit_market_sell(symbol: str, qty: int):
    return api.submit_order(
        symbol=symbol,
        qty=qty,
        side="sell",
        type="market",
        time_in_force="day",
    )


def wait_for_fill(order_id: str, timeout_sec: float, poll_sec: float):
    start = time.time()
    while True:
        o = api.get_order(order_id)
        status = (o.status or "").lower()
        if status in ("filled", "canceled", "rejected", "expired"):
            return o
        if time.time() - start >= timeout_sec:
            return o
        time.sleep(poll_sec)


def pick_latest_closed_bar(symbol: str, now_utc: datetime):
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


def reset_group_state(state: dict) -> None:
    state["group_anchor_close"] = None
    state["last_red_buy_close"] = None
    state["group_buy_count"] = 0


def get_owned_qty(state: dict) -> int:
    """
    Strategy-owned qty:
    - DRY_RUN uses sim_owned_qty
    - LIVE uses strategy_owned_qty
    """
    key = "sim_owned_qty" if DRY_RUN else "strategy_owned_qty"
    try:
        return int(state.get(key, 0))
    except Exception:
        return 0


def set_owned_qty(state: dict, new_qty: int) -> None:
    key = "sim_owned_qty" if DRY_RUN else "strategy_owned_qty"
    state[key] = max(0, int(new_qty))


# =========================
# Main
# =========================
def main():
    logging.info(
        f"ENGINE_START mode=RED_CLOSE_GROUP_SELL_ANCHOR_PCT dry_run={DRY_RUN} symbol={SYMBOL}"
    )
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
        f"state_save_sec={STATE_SAVE_SEC} "
        f"sell_pct={SELL_PCT} "
        f"dry_run={DRY_RUN} "
        f"alpaca_base_url={ALPACA_BASE_URL}"
    )

    state = load_state()

    # last processed bar
    last_bar_ts_iso = state.get("last_bar_ts")
    last_bar_ts = None
    if last_bar_ts_iso:
        try:
            last_bar_ts = datetime.fromisoformat(last_bar_ts_iso)
            if last_bar_ts.tzinfo is None:
                last_bar_ts = last_bar_ts.replace(tzinfo=timezone.utc)
        except Exception:
            last_bar_ts = None

    # group state
    group_anchor_close = state.get("group_anchor_close")
    last_red_buy_close = state.get("last_red_buy_close")
    buy_count_total = int(state.get("buy_count_total", 0))
    group_buy_count = int(state.get("group_buy_count", 0))

    # owned tracking (both can exist; we only use the appropriate one)
    if "strategy_owned_qty" not in state:
        state["strategy_owned_qty"] = 0
    if "sim_owned_qty" not in state:
        state["sim_owned_qty"] = 0
    
    # Optional: reset simulated ownership on startup (DRY_RUN only)
    if DRY_RUN and RESET_SIM_OWNED_ON_START:
        if state.get("sim_owned_qty", 0) != 0:
            logging.info(
                f"RESET_SIM_OWNED_ON_START enabled → sim_owned_qty "
                f"{state.get('sim_owned_qty')} → 0"
            )
        state["sim_owned_qty"] = 0

    logging.info(
        "STATE_LOADED "
        f"last_bar_ts={last_bar_ts_iso} "
        f"group_anchor_close={group_anchor_close} "
        f"last_red_buy_close={last_red_buy_close} "
        f"buy_count_total={buy_count_total} "
        f"group_buy_count={group_buy_count} "
        f"strategy_owned_qty={int(state.get('strategy_owned_qty', 0))} "
        f"sim_owned_qty={int(state.get('sim_owned_qty', 0))}"
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

            # Optional: detect manual position changes and keep owned qty sane
            if LOG_POSITION_CHANGES:
                pos_qty = get_position_qty(SYMBOL)
                if last_pos_qty is None:
                    last_pos_qty = pos_qty
                elif pos_qty != last_pos_qty:
                    logging.info(
                        f"POSITION_CHANGE qty_from={last_pos_qty:.4f} qty_to={pos_qty:.4f}"
                    )
                    last_pos_qty = pos_qty

                # Clamp owned qty so we never claim to own more than we hold
                owned = get_owned_qty(state)
                if int(pos_qty) < owned:
                    logging.warning(
                        f"OWNED_CLAMP position_qty={int(pos_qty)} owned_qty={owned} -> owned_qty={int(pos_qty)}"
                    )
                    set_owned_qty(state, int(pos_qty))

                # If externally liquidated to zero, reset group + owned (for current mode)
                if pos_qty == 0.0:
                    if get_owned_qty(state) != 0:
                        logging.info("LIQUIDATION_DETECTED setting owned_qty=0 for current mode")
                        set_owned_qty(state, 0)
                    if group_anchor_close is not None or last_red_buy_close is not None or group_buy_count != 0:
                        logging.info("LIQUIDATION_DETECTED resetting group state")
                        reset_group_state(state)
                        group_anchor_close = None
                        last_red_buy_close = None
                        group_buy_count = 0

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

            # Skip already processed candle
            if last_bar_ts is not None and bar_ts <= last_bar_ts:
                time.sleep(POLL_SEC)
                continue

            o = float(b.o)
            c = float(b.c)
            is_red = c < o

            pos_qty = get_position_qty(SYMBOL)
            avg_entry = get_position_avg_entry(SYMBOL) if pos_qty > 0 else None

            sell_target = None
            if group_anchor_close is not None:
                sell_target = float(group_anchor_close) * (1.0 + float(SELL_PCT))

            owned_qty = get_owned_qty(state)

            logging.info(
                f"BAR_CLOSE {SYMBOL} t={bar_ts.isoformat()} O={o:.2f} C={c:.2f} red={is_red} "
                f"group_anchor={group_anchor_close} sell_target={sell_target} "
                f"pos_qty={int(pos_qty)} avg_entry={avg_entry} owned_qty={owned_qty}"
            )

            if group_anchor_close is not None and avg_entry is not None:
                try:
                    anchor = float(group_anchor_close)
                    logging.info(
                        f"COMPARE close_to_anchor={(c - anchor):+.2f} close_to_avg_entry={(c - float(avg_entry)):+.2f} "
                        f"(close={c:.2f} anchor={anchor:.2f} avg_entry={float(avg_entry):.2f})"
                    )
                except Exception:
                    pass

            buys_this_tick = 0

            # =========================
            # SELL trigger (sell ONLY strategy-owned shares)
            # =========================
            if group_anchor_close is not None and sell_target is not None:
                if int(pos_qty) > 0 and owned_qty > 0 and c >= float(sell_target):
                    sell_qty = min(int(pos_qty), int(owned_qty))

                    if DRY_RUN:
                        logging.info(
                            f"SIM_SELL_OWNED trigger=CLOSE_AT_OR_ABOVE_TARGET "
                            f"close={c:.2f} target={float(sell_target):.2f} anchor={float(group_anchor_close):.2f} "
                            f"sell_pct={SELL_PCT} sell_qty={sell_qty} owned_qty={owned_qty} pos_qty={int(pos_qty)}"
                        )
                        # simulate position reduction for owned shares only
                        set_owned_qty(state, owned_qty - sell_qty)
                    else:
                        logging.info(
                            f"SELL_SIGNAL_OWNED trigger=CLOSE_AT_OR_ABOVE_TARGET "
                            f"close={c:.2f} target={float(sell_target):.2f} anchor={float(group_anchor_close):.2f} "
                            f"sell_pct={SELL_PCT} sell_qty={sell_qty} owned_qty={owned_qty} pos_qty={int(pos_qty)}"
                        )
                        order = submit_market_sell(SYMBOL, sell_qty)
                        logging.info(
                            f"ORDER_SUBMITTED id={order.id} qty={sell_qty} type=market side=sell"
                        )

                        final = wait_for_fill(order.id, FILL_TIMEOUT_SEC, FILL_POLL_SEC)
                        status = (final.status or "").lower()
                        filled_qty = getattr(final, "filled_qty", None)
                        avg_fill = getattr(final, "filled_avg_price", None)

                        logging.info(
                            f"ORDER_FINAL id={order.id} status={status} filled_qty={filled_qty} avg_fill_price={avg_fill}"
                        )

                        # Decrement owned by actual filled qty when possible
                        dec = 0
                        try:
                            dec = int(float(filled_qty)) if filled_qty is not None else sell_qty
                        except Exception:
                            dec = sell_qty
                        set_owned_qty(state, owned_qty - dec)

                    # Reset group after strategy-owned liquidation
                    logging.info("GROUP_RESET after owned sell")
                    reset_group_state(state)
                    group_anchor_close = None
                    last_red_buy_close = None
                    group_buy_count = 0

            # =========================
            # BUY trigger (red candle close)
            # =========================
            if is_red:
                if last_red_buy_close is None:
                    should_buy = True
                    reason = "FIRST_RED_BUY"
                else:
                    should_buy = c < float(last_red_buy_close)
                    reason = "LOWER_THAN_LAST_RED_BUY" if should_buy else "NOT_LOWER_THAN_LAST_RED_BUY"

                if should_buy:
                    if buys_this_tick >= MAX_BUYS_PER_TICK:
                        logging.warning(
                            f"BUY_LIMIT reached MAX_BUYS_PER_TICK={MAX_BUYS_PER_TICK} bar_ts={bar_ts.isoformat()} close={c:.2f}"
                        )
                    else:
                        buy_count_total += 1
                        group_buy_count += 1
                        buys_this_tick += 1

                        if group_anchor_close is None:
                            group_anchor_close = float(c)
                            logging.info(f"GROUP_ANCHOR_SET group_anchor_close={group_anchor_close:.2f}")

                        if DRY_RUN:
                            logging.info(
                                f"SIM_BUY total#{buy_count_total} group#{group_buy_count} reason={reason} "
                                f"close={c:.2f} qty={ORDER_QTY}"
                            )
                            # simulate owned qty increase
                            set_owned_qty(state, get_owned_qty(state) + ORDER_QTY)
                        else:
                            logging.info(
                                f"BUY_SIGNAL total#{buy_count_total} group#{group_buy_count} reason={reason} "
                                f"close={c:.2f} qty={ORDER_QTY}"
                            )
                            order = submit_market_buy(SYMBOL, ORDER_QTY)
                            logging.info(
                                f"ORDER_SUBMITTED id={order.id} qty={ORDER_QTY} type=market side=buy"
                            )

                            final = wait_for_fill(order.id, FILL_TIMEOUT_SEC, FILL_POLL_SEC)
                            status = (final.status or "").lower()
                            filled_qty = getattr(final, "filled_qty", None)
                            avg_fill = getattr(final, "filled_avg_price", None)

                            logging.info(
                                f"ORDER_FINAL id={order.id} status={status} filled_qty={filled_qty} avg_fill_price={avg_fill}"
                            )

                            inc = 0
                            try:
                                inc = int(float(filled_qty)) if filled_qty is not None else ORDER_QTY
                            except Exception:
                                inc = ORDER_QTY
                            set_owned_qty(state, get_owned_qty(state) + inc)

                        last_red_buy_close = float(c)
                        logging.info(f"RED_BUY_MEMORY_UPDATE last_red_buy_close={last_red_buy_close:.2f}")

                else:
                    logging.info(
                        f"RED_SKIP reason={reason} close={c:.2f} last_red_buy_close={last_red_buy_close}"
                    )

            # Persist
            last_bar_ts = bar_ts
            payload = {
                "last_bar_ts": last_bar_ts.isoformat(),
                "group_anchor_close": group_anchor_close,
                "last_red_buy_close": last_red_buy_close,
                "buy_count_total": buy_count_total,
                "group_buy_count": group_buy_count,
                "strategy_owned_qty": int(state.get("strategy_owned_qty", 0)),
                "sim_owned_qty": int(state.get("sim_owned_qty", 0)),
                "symbol": SYMBOL,
            }
            maybe_persist_state(state, payload)

            time.sleep(POLL_SEC)

        except Exception as e:
            logging.error(f"ENGINE_ERROR {e}", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
