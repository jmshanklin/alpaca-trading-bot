import os
import time
import json
import logging
import hashlib
import random
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Callable, TypeVar

import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame

# Postgres (for resilient v1 state + leader lock)
import psycopg2
from psycopg2.extras import Json


# =========================
# Logging in Central Time
# =========================
CT = ZoneInfo("America/Chicago")
ET = ZoneInfo("America/New_York")


class CTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=CT)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")


logger = logging.getLogger("engine")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(CTFormatter(fmt="%(asctime)s CT [%(levelname)s] %(message)s"))
logger.handlers = [handler]
logger.propagate = False


# =========================
# Alpaca retry helper
# =========================
T = TypeVar("T")


def alpaca_call_with_retry(
    fn: Callable[[], T],
    *,
    tries: int = 8,
    base_sleep: float = 0.5,
    max_sleep: float = 10.0,
    label: str = "alpaca_call",
) -> T:
    """
    Retries Alpaca calls on transient errors (500/502/503/504, timeouts, connection resets).
    Raises on fatal errors (401 Unauthorized, Forbidden).
    """
    for attempt in range(1, tries + 1):
        try:
            return fn()
        except Exception as e:
            msg = str(e).lower()

            transient = (
                "internal server error" in msg
                or "service unavailable" in msg
                or "bad gateway" in msg
                or "gateway timeout" in msg
                or "timed out" in msg
                or "timeout" in msg
                or "connection reset" in msg
                or "temporarily unavailable" in msg
            )

            fatal = ("unauthorized" in msg or "forbidden" in msg or "invalid api key" in msg)

            if fatal:
                logger.error(f"{label}: FATAL error (not retrying): {e}")
                raise

            # Unknown error: retry a couple times, then raise
            if (not transient) and attempt >= 3:
                logger.error(f"{label}: non-transient after {attempt} attempts: {e}")
                raise

            sleep_s = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
            sleep_s = sleep_s * (0.8 + 0.4 * random.random())  # jitter
            logger.warning(f"{label}: error attempt {attempt}/{tries}: {e} | sleeping {sleep_s:.2f}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"{label}: failed after {tries} attempts")


# =========================
# Persistence helpers (disk fallback)
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
        logger.warning(f"STATE_PATH not writable ({e}); falling back to {fallback_path}")
        return fallback_path


# =========================
# Env vars
# =========================
DRY_RUN = os.getenv("DRY_RUN", "true").strip().lower() in ("1", "true", "yes", "y", "on")
ORDER_QTY = int(os.getenv("ORDER_QTY", "1"))

POLL_SEC = float(os.getenv("POLL_SEC", "1.0"))

FILL_TIMEOUT_SEC = float(os.getenv("FILL_TIMEOUT_SEC", "20"))
FILL_POLL_SEC = float(os.getenv("FILL_POLL_SEC", "0.5"))

MAX_BUYS_PER_TICK = int(os.getenv("MAX_BUYS_PER_TICK", "1"))

LOG_POSITION_CHANGES = os.getenv("LOG_POSITION_CHANGES", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)

STATE_SAVE_SEC = float(os.getenv("STATE_SAVE_SEC", "0"))
SELL_PCT = float(os.getenv("SELL_PCT", "0.0"))

RESET_SIM_OWNED_ON_START = os.getenv("RESET_SIM_OWNED_ON_START", "false").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)

LIVE_TRADING_CONFIRM = os.getenv("LIVE_TRADING_CONFIRM", "").strip()
KILL_SWITCH = os.getenv("KILL_SWITCH", "false").strip().lower() in ("1", "true", "yes", "y", "on")

MAX_DOLLARS_PER_BUY = float(os.getenv("MAX_DOLLARS_PER_BUY", "0"))  # 0 disables
MAX_POSITION_QTY = int(os.getenv("MAX_POSITION_QTY", "0"))  # 0 disables
MAX_BUYS_PER_DAY = int(os.getenv("MAX_BUYS_PER_DAY", "0"))  # 0 disables

TRADE_START_ET = os.getenv("TRADE_START_ET", "").strip()
TRADE_END_ET = os.getenv("TRADE_END_ET", "").strip()

STATE_PATH = resolve_state_path()

ALPACA_KEY_ID = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
ALPACA_BASE_URL = (
    os.getenv("ALPACA_BASE_URL") or os.getenv("APCA_API_BASE_URL") or "https://paper-api.alpaca.markets"
)

SYMBOL = os.getenv("ENGINE_SYMBOL", "TSLA").upper()

if not ALPACA_KEY_ID or not ALPACA_SECRET_KEY:
    raise RuntimeError(
        "Missing Alpaca credentials: set ALPACA_KEY_ID/ALPACA_SECRET_KEY "
        "(or APCA_API_KEY_ID/APCA_API_SECRET_KEY)."
    )

api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
LEADER_LOCK_KEY = os.getenv("LEADER_LOCK_KEY", f"{SYMBOL}_ENGINE_V1").strip()
STANDBY_POLL_SEC = float(os.getenv("STANDBY_POLL_SEC", "2"))


def is_live_endpoint(url: str) -> bool:
    u = (url or "").lower()
    if "paper-api" in u:
        return False
    return "api.alpaca.markets" in u


def parse_hhmm(s: str):
    try:
        if not s:
            return None
        hh, mm = s.split(":")
        return int(hh), int(mm)
    except Exception:
        return None


def in_trade_window_et(now_utc: datetime) -> bool:
    start = parse_hhmm(TRADE_START_ET)
    end = parse_hhmm(TRADE_END_ET)
    if not start or not end:
        return True

    now_et = now_utc.astimezone(ET)
    mins = now_et.hour * 60 + now_et.minute
    start_m = start[0] * 60 + start[1]
    end_m = end[0] * 60 + end[1]
    return start_m <= mins <= end_m


def et_date_str(now_utc: datetime) -> str:
    return now_utc.astimezone(ET).date().isoformat()


def db_enabled() -> bool:
    return bool(DATABASE_URL)


def db_connect():
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    conn.autocommit = True
    return conn


def db_init(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS engine_state (
                id TEXT PRIMARY KEY,
                state JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )


def _lock_int64_from_key(key: str) -> int:
    h = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big", signed=False) % (2**63 - 1)


def try_acquire_leader_lock(conn, lock_key: str) -> bool:
    lock_id = _lock_int64_from_key(lock_key)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s);", (lock_id,))
        return bool(cur.fetchone()[0])


def load_state_db(conn, state_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT state FROM engine_state WHERE id=%s;", (state_id,))
        row = cur.fetchone()
        return (row[0] or {}) if row else {}


def save_state_db(conn, state_id: str, state: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO engine_state (id, state, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (id)
            DO UPDATE SET state = EXCLUDED.state, updated_at = now();
            """,
            (state_id, Json(state)),
        )


def load_state_disk() -> dict:
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception as e:
        logger.warning(f"STATE_LOAD failed: {e}")
    return {}


def save_state_disk(payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
    except Exception as e:
        logger.warning(f"STATE_SAVE failed: {e}")


def maybe_persist_state(state: dict, payload: dict, *, db_conn=None, state_id: str = "") -> None:
    state.update(payload)

    if STATE_SAVE_SEC <= 0:
        should_save = True
        state["_last_save_ts"] = time.time()
    else:
        now_ts = time.time()
        last_ts = float(state.get("_last_save_ts", 0.0))
        should_save = (now_ts - last_ts) >= STATE_SAVE_SEC
        if should_save:
            state["_last_save_ts"] = now_ts

    if not should_save:
        return

    if db_conn is not None and state_id:
        save_state_db(db_conn, state_id, state)
    else:
        save_state_disk(state)


def get_position(symbol: str):
    try:
        return alpaca_call_with_retry(lambda: api.get_position(symbol), label="get_position")
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
    return alpaca_call_with_retry(
        lambda: api.submit_order(symbol=symbol, qty=qty, side="buy", type="market", time_in_force="day"),
        label="submit_buy",
    )


def submit_market_sell(symbol: str, qty: int):
    return alpaca_call_with_retry(
        lambda: api.submit_order(symbol=symbol, qty=qty, side="sell", type="market", time_in_force="day"),
        label="submit_sell",
    )


def wait_for_fill(order_id: str, timeout_sec: float, poll_sec: float):
    start = time.time()
    while True:
        o = alpaca_call_with_retry(lambda: api.get_order(order_id), label="get_order")
        status = (o.status or "").lower()
        if status in ("filled", "canceled", "rejected", "expired"):
            return o
        if time.time() - start >= timeout_sec:
            return o
        time.sleep(poll_sec)


def pick_latest_closed_bar(symbol: str, now_utc: datetime):
    """
    Return the most recent *closed* 1-minute bar by requesting a short time range.
    """
    try:
        end = now_utc
        start = end - timedelta(minutes=10)

        def _fetch():
            FEED = os.getenv("ALPACA_DATA_FEED", "iex").strip().lower()
            logger.warning(f"DEBUG_FEED_SELECTED={FEED}")
            return api.get_bars(
                symbol,
                TimeFrame.Minute,
                start=start.isoformat(),
                end=end.isoformat(),
                limit=10,
                adjustment="raw",
                feed=FEED,   # <-- THIS is the fix SIP permission crash
            )

        bars = alpaca_call_with_retry(_fetch, label="get_bars_1m")
        if not bars:
            logger.warning("BARS_EMPTY (no data returned)")
            return None

        if isinstance(bars, dict):
            bars = bars.get(symbol, [])
        bars = list(bars)
        if not bars:
            return None

        now_floor = now_utc.replace(second=0, microsecond=0)

        for b in reversed(bars):
            bt = getattr(b, "t", None)
            if bt is None:
                continue
            if bt.tzinfo is None:
                bt = bt.replace(tzinfo=timezone.utc)
            if bt < now_floor:
                return b

        return None
    except Exception as e:
        logger.error(f"GET_BARS_FAILED {e}", exc_info=True)
        return None


def reset_group_state(state: dict) -> None:
    state["group_anchor_close"] = None
    state["last_red_buy_close"] = None
    state["group_buy_count"] = 0


def get_owned_qty(state: dict) -> int:
    key = "sim_owned_qty" if DRY_RUN else "strategy_owned_qty"
    try:
        return int(state.get(key, 0))
    except Exception:
        return 0


def set_owned_qty(state: dict, new_qty: int) -> None:
    key = "sim_owned_qty" if DRY_RUN else "strategy_owned_qty"
    state[key] = max(0, int(new_qty))


def main():
    live_endpoint = is_live_endpoint(ALPACA_BASE_URL)

    logger.info(f"ENGINE_START mode=RED_CLOSE_GROUP_SELL_ANCHOR_PCT dry_run={DRY_RUN} symbol={SYMBOL}")

    if (not DRY_RUN) and live_endpoint:
        if LIVE_TRADING_CONFIRM != "I_UNDERSTAND":
            raise RuntimeError("LIVE trading blocked: set LIVE_TRADING_CONFIRM=I_UNDERSTAND to enable live orders.")

    db_conn = None
    state_id = ""
    is_leader = True

    if db_enabled():
        db_conn = db_connect()
        db_init(db_conn)
        state_id = f"{SYMBOL}_state"
        is_leader = try_acquire_leader_lock(db_conn, LEADER_LOCK_KEY)
        logger.info("LEADER_LOCK acquired -> ACTIVE mode (orders allowed)" if is_leader else "LEADER_LOCK not acquired -> STANDBY mode (no orders)")
    else:
        logger.warning("DATABASE_URL not set -> using DISK state (single instance only)")

    state = load_state_db(db_conn, state_id) if db_conn is not None else load_state_disk()

    last_bar_ts_iso = state.get("last_bar_ts")
    last_bar_ts = None
    if last_bar_ts_iso:
        try:
            last_bar_ts = datetime.fromisoformat(last_bar_ts_iso)
            if last_bar_ts.tzinfo is None:
                last_bar_ts = last_bar_ts.replace(tzinfo=timezone.utc)
        except Exception:
            last_bar_ts = None

    group_anchor_close = state.get("group_anchor_close")
    last_red_buy_close = state.get("last_red_buy_close")
    buy_count_total = int(state.get("buy_count_total", 0))
    group_buy_count = int(state.get("group_buy_count", 0))

    state.setdefault("strategy_owned_qty", 0)
    state.setdefault("sim_owned_qty", 0)
    state.setdefault("buys_today_et", 0)
    state.setdefault("buys_today_date_et", None)

    if LOG_POSITION_CHANGES:
        last_pos_qty = get_position_qty(SYMBOL)
        logger.info(f"POSITION_INIT qty={last_pos_qty:.4f}")
    else:
        last_pos_qty = None

    while True:
        try:
            clock = alpaca_call_with_retry(lambda: api.get_clock(), label="get_clock")
            if not clock.is_open:
                logger.info("MARKET_CLOSED waiting...")
                time.sleep(30)
                continue

            if db_conn is not None and not is_leader:
                is_leader = try_acquire_leader_lock(db_conn, LEADER_LOCK_KEY)
                if not is_leader:
                    time.sleep(STANDBY_POLL_SEC)
                    continue
                logger.info("LEADER_LOCK acquired -> ACTIVE mode (orders allowed)")

            now_utc = clock.timestamp
            if now_utc.tzinfo is None:
                now_utc = now_utc.replace(tzinfo=timezone.utc)

            today_et = et_date_str(now_utc)
            if state.get("buys_today_date_et") != today_et:
                state["buys_today_date_et"] = today_et
                state["buys_today_et"] = 0
                logger.info(f"DAY_ROLLOVER_ET date={today_et} buys_today_et reset to 0")

            if LOG_POSITION_CHANGES:
                pos_qty_now = get_position_qty(SYMBOL)
                if last_pos_qty is None:
                    last_pos_qty = pos_qty_now
                elif pos_qty_now != last_pos_qty:
                    logger.info(f"POSITION_CHANGE qty_from={last_pos_qty:.4f} qty_to={pos_qty_now:.4f}")
                    last_pos_qty = pos_qty_now

            b = pick_latest_closed_bar(SYMBOL, now_utc)
            if b is None:
                time.sleep(POLL_SEC)
                continue

            bar_ts = b.t
            if bar_ts.tzinfo is None:
                bar_ts = bar_ts.replace(tzinfo=timezone.utc)

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

            logger.info(
                f"BAR_CLOSE {SYMBOL} t={bar_ts.isoformat()} O={o:.2f} C={c:.2f} red={is_red} "
                f"group_anchor={group_anchor_close} sell_target={sell_target} "
                f"pos_qty={int(pos_qty)} avg_entry={avg_entry} owned_qty={owned_qty} "
                f"buys_today_et={int(state.get('buys_today_et', 0))} is_leader={is_leader}"
            )

            buys_this_tick = 0

            # SELL trigger
            if group_anchor_close is not None and sell_target is not None:
                if int(pos_qty) > 0 and owned_qty > 0 and c >= float(sell_target):
                    sell_qty = min(int(pos_qty), int(owned_qty))

                    if DRY_RUN:
                        logger.info(f"SIM_SELL_OWNED close={c:.2f} target={float(sell_target):.2f} sell_qty={sell_qty}")
                        set_owned_qty(state, owned_qty - sell_qty)
                    else:
                        if db_conn is not None and not is_leader:
                            logger.warning("STANDBY_BLOCK: skipping SELL (no leader lock)")
                        else:
                            logger.info(f"SELL_SIGNAL_OWNED close={c:.2f} target={float(sell_target):.2f} sell_qty={sell_qty}")
                            order = submit_market_sell(SYMBOL, sell_qty)
                            final = wait_for_fill(order.id, FILL_TIMEOUT_SEC, FILL_POLL_SEC)
                            filled_qty = getattr(final, "filled_qty", None)
                            dec = sell_qty
                            try:
                                if filled_qty is not None:
                                    dec = int(float(filled_qty))
                            except Exception:
                                pass
                            set_owned_qty(state, owned_qty - dec)

                    logger.info("GROUP_RESET after owned sell")
                    reset_group_state(state)
                    group_anchor_close = None
                    last_red_buy_close = None
                    group_buy_count = 0

            # BUY trigger
            if is_red:
                if last_red_buy_close is None:
                    should_buy = True
                    reason = "FIRST_RED_BUY"
                else:
                    should_buy = c < float(last_red_buy_close)
                    reason = "LOWER_THAN_LAST_RED_BUY" if should_buy else "NOT_LOWER_THAN_LAST_RED_BUY"

                if should_buy and KILL_SWITCH:
                    logger.warning("BUY_BLOCKED KILL_SWITCH active (buys disabled; sells allowed).")
                    should_buy = False

                if should_buy and (not in_trade_window_et(now_utc)):
                    logger.info("BUY_BLOCKED outside trade window (ET).")
                    should_buy = False

                if should_buy and MAX_BUYS_PER_DAY > 0:
                    if int(state.get("buys_today_et", 0)) >= MAX_BUYS_PER_DAY:
                        logger.warning(f"BUY_BLOCKED max buys per ET day reached: {MAX_BUYS_PER_DAY}")
                        should_buy = False

                if should_buy and MAX_POSITION_QTY > 0:
                    current_pos = int(get_position_qty(SYMBOL))
                    if current_pos + int(ORDER_QTY) > MAX_POSITION_QTY:
                        logger.warning(f"BUY_BLOCKED would exceed MAX_POSITION_QTY={MAX_POSITION_QTY}")
                        should_buy = False

                if should_buy and MAX_DOLLARS_PER_BUY > 0:
                    est_cost = float(c) * int(ORDER_QTY)
                    if est_cost > MAX_DOLLARS_PER_BUY:
                        logger.warning(f"BUY_BLOCKED est_cost=${est_cost:.2f} exceeds MAX_DOLLARS_PER_BUY=${MAX_DOLLARS_PER_BUY:.2f}")
                        should_buy = False

                if should_buy:
                    if buys_this_tick >= MAX_BUYS_PER_TICK:
                        logger.warning(f"BUY_LIMIT reached MAX_BUYS_PER_TICK={MAX_BUYS_PER_TICK}")
                    else:
                        buy_count_total += 1
                        group_buy_count += 1
                        buys_this_tick += 1

                        if group_anchor_close is None:
                            group_anchor_close = float(c)
                            logger.info(f"GROUP_ANCHOR_SET group_anchor_close={group_anchor_close:.2f}")

                        if DRY_RUN:
                            logger.info(f"SIM_BUY total#{buy_count_total} group#{group_buy_count} reason={reason} close={c:.2f} qty={ORDER_QTY}")
                            set_owned_qty(state, get_owned_qty(state) + ORDER_QTY)
                            state["buys_today_et"] = int(state.get("buys_today_et", 0)) + 1
                        else:
                            if db_conn is not None and not is_leader:
                                logger.warning("STANDBY_BLOCK: skipping BUY (no leader lock)")
                            else:
                                logger.info(f"BUY_SIGNAL total#{buy_count_total} group#{group_buy_count} reason={reason} close={c:.2f} qty={ORDER_QTY}")
                                order = submit_market_buy(SYMBOL, ORDER_QTY)
                                final = wait_for_fill(order.id, FILL_TIMEOUT_SEC, FILL_POLL_SEC)
                                filled_qty = getattr(final, "filled_qty", None)
                                inc = ORDER_QTY
                                try:
                                    if filled_qty is not None:
                                        inc = int(float(filled_qty))
                                except Exception:
                                    pass
                                set_owned_qty(state, get_owned_qty(state) + inc)
                                state["buys_today_et"] = int(state.get("buys_today_et", 0)) + 1

                        last_red_buy_close = float(c)
                        logger.info(f"RED_BUY_MEMORY_UPDATE last_red_buy_close={last_red_buy_close:.2f}")

            last_bar_ts = bar_ts
            payload = {
                "last_bar_ts": last_bar_ts.isoformat(),
                "group_anchor_close": group_anchor_close,
                "last_red_buy_close": last_red_buy_close,
                "buy_count_total": buy_count_total,
                "group_buy_count": group_buy_count,
                "strategy_owned_qty": int(state.get("strategy_owned_qty", 0)),
                "sim_owned_qty": int(state.get("sim_owned_qty", 0)),
                "buys_today_date_et": state.get("buys_today_date_et"),
                "buys_today_et": int(state.get("buys_today_et", 0)),
                "symbol": SYMBOL,
            }
            maybe_persist_state(state, payload, db_conn=db_conn, state_id=state_id)

            time.sleep(POLL_SEC)

        except Exception as e:
            logger.error(f"ENGINE_ERROR {e}", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
