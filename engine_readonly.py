import os
import time
import json
import logging
import hashlib
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import alpaca_trade_api as tradeapi

# Postgres (for resilient v1 state + leader lock)
import psycopg2
from psycopg2.extras import Json

import random
from typing import Callable, TypeVar

T = TypeVar("T")

def alpaca_call_with_retry(fn: Callable[[], T], *, tries: int = 8, base_sleep: float = 0.5, max_sleep: float = 10.0, label: str = "alpaca_call") -> T:
    """
    Retries Alpaca calls on transient errors (500/502/503/504, timeouts, connection resets).
    Raises on non-transient errors (like 401 Unauthorized).
    """
    for attempt in range(1, tries + 1):
        try:
            return fn()
        except Exception as e:
            msg = str(e).lower()

            # --- Treat these as transient ---
            transient = (
                "internal server error" in msg or
                "service unavailable" in msg or
                "bad gateway" in msg or
                "gateway timeout" in msg or
                "timed out" in msg or
                "timeout" in msg or
                "connection reset" in msg or
                "temporarily unavailable" in msg
            )

            # --- Treat these as fatal (don’t retry) ---
            fatal = ("unauthorized" in msg or "forbidden" in msg or "invalid api key" in msg)

            if fatal:
                logger.error(f"{label}: FATAL error (not retrying): {e}")
                raise

            if not transient:
                # Unknown error: retry a couple times, then raise
                if attempt >= 3:
                    logger.error(f"{label}: non-transient after {attempt} attempts: {e}")
                    raise

            sleep_s = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
            sleep_s = sleep_s * (0.8 + 0.4 * random.random())  # jitter
            logger.warning(f"{label}: transient error attempt {attempt}/{tries}: {e} | sleeping {sleep_s:.2f}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"{label}: failed after {tries} attempts")

# =========================
# Logging in Central Time
# =========================
CT = ZoneInfo("America/Chicago")
ET = ZoneInfo("America/New_York")


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

# Safety cap per loop tick
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

# Reset simulated owned qty on startup (DRY_RUN only)
RESET_SIM_OWNED_ON_START = os.getenv(
    "RESET_SIM_OWNED_ON_START", "false"
).strip().lower() in ("1", "true", "yes", "y", "on")

# -------- LIVE v1 Safety Rails (Option B) --------
LIVE_TRADING_CONFIRM = os.getenv("LIVE_TRADING_CONFIRM", "").strip()
KILL_SWITCH = os.getenv("KILL_SWITCH", "false").strip().lower() in ("1", "true", "yes", "y", "on")

MAX_DOLLARS_PER_BUY = float(os.getenv("MAX_DOLLARS_PER_BUY", "0"))  # 0 disables
MAX_POSITION_QTY = int(os.getenv("MAX_POSITION_QTY", "0"))          # 0 disables
MAX_BUYS_PER_DAY = int(os.getenv("MAX_BUYS_PER_DAY", "0"))          # 0 disables

TRADE_START_ET = os.getenv("TRADE_START_ET", "").strip()  # e.g. "09:35" (blank disables)
TRADE_END_ET = os.getenv("TRADE_END_ET", "").strip()      # e.g. "15:55" (blank disables)

# Persistence (disk fallback)
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

# -------- Postgres state + leader lock (v1 resilient) --------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
LEADER_LOCK_KEY = os.getenv("LEADER_LOCK_KEY", f"{SYMBOL}_ENGINE_V1").strip()
STANDBY_POLL_SEC = float(os.getenv("STANDBY_POLL_SEC", "2"))


# =========================
# Live/paper detection + time helpers
# =========================
def is_live_endpoint(url: str) -> bool:
    """
    True for Alpaca LIVE endpoint, False for paper.
    We treat anything containing 'paper-api' as paper.
    """
    u = (url or "").lower()
    if "paper-api" in u:
        return False
    return "api.alpaca.markets" in u


def parse_hhmm(s: str):
    """Return (hour, minute) or None if blank/invalid."""
    try:
        if not s:
            return None
        hh, mm = s.split(":")
        return int(hh), int(mm)
    except Exception:
        return None


def in_trade_window_et(now_utc: datetime) -> bool:
    """
    If TRADE_START_ET/TRADE_END_ET are set, require now ET to be inside window.
    If either is blank/invalid, window is disabled (always True).
    """
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
    """YYYY-MM-DD in ET."""
    return now_utc.astimezone(ET).date().isoformat()


# =========================
# Postgres state + leader lock
# =========================
def db_enabled() -> bool:
    return bool(DATABASE_URL)


def db_connect():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL env var for Postgres.")
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
        if not row:
            return {}
        return row[0] or {}


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


# =========================
# Disk state (fallback)
# =========================
def load_state_disk() -> dict:
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                s = json.load(f)
            return s or {}
    except Exception as e:
        logging.warning(f"STATE_LOAD failed: {e}")
    return {}


def save_state_disk(payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
    except Exception as e:
        logging.warning(f"STATE_SAVE failed: {e}")


def maybe_persist_state(state: dict, payload: dict, *, db_conn=None, state_id: str = "") -> None:
    """
    Update in-memory state and persist with STATE_SAVE_SEC throttle.
    Saves to DB if db_conn + state_id are provided, else saves to disk.
    """
    state.update(payload)

    # Throttle logic
    should_save = False
    if STATE_SAVE_SEC <= 0:
        should_save = True
        state["_last_save_ts"] = time.time()
    else:
        now_ts = time.time()
        last_ts = float(state.get("_last_save_ts", 0.0))
        if (now_ts - last_ts) >= STATE_SAVE_SEC:
            should_save = True
            state["_last_save_ts"] = now_ts

    if not should_save:
        return

    if db_conn is not None and state_id:
        save_state_db(db_conn, state_id, state)
    else:
        save_state_disk(state)


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
    bars = alpaca_call_with_retry(lambda: api.get_bars(...), label="get_bars")
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
    - DRY_RUN=false uses strategy_owned_qty
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
    live_endpoint = is_live_endpoint(ALPACA_BASE_URL)

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
        f"reset_sim_owned_on_start={RESET_SIM_OWNED_ON_START} "
        f"kill_switch={KILL_SWITCH} "
        f"max_dollars_per_buy={MAX_DOLLARS_PER_BUY} "
        f"max_position_qty={MAX_POSITION_QTY} "
        f"max_buys_per_day={MAX_BUYS_PER_DAY} "
        f"trade_start_et={TRADE_START_ET} "
        f"trade_end_et={TRADE_END_ET} "
        f"dry_run={DRY_RUN} "
        f"alpaca_base_url={ALPACA_BASE_URL} "
        f"alpaca_is_live_endpoint={live_endpoint} "
        f"db_enabled={db_enabled()} "
        f"leader_lock_key={LEADER_LOCK_KEY if db_enabled() else ''}"
    )

    # Live trading confirmation gate (ONLY live endpoint + DRY_RUN=false)
    if (not DRY_RUN) and live_endpoint:
        if LIVE_TRADING_CONFIRM != "I_UNDERSTAND":
            raise RuntimeError(
                "LIVE trading blocked: set LIVE_TRADING_CONFIRM=I_UNDERSTAND to enable live orders."
            )

    # ---- Postgres + leader lock (optional) ----
    db_conn = None
    state_id = ""
    is_leader = True  # default if DB not enabled

    if db_enabled():
        db_conn = db_connect()
        db_init(db_conn)
        state_id = f"{SYMBOL}_state"

        is_leader = try_acquire_leader_lock(db_conn, LEADER_LOCK_KEY)
        if is_leader:
            logging.info("LEADER_LOCK acquired -> ACTIVE mode (orders allowed)")
        else:
            logging.warning("LEADER_LOCK not acquired -> STANDBY mode (no orders)")
    else:
        logging.warning("DATABASE_URL not set -> using DISK state (single instance only)")

    # ---- Load state ----
    if db_conn is not None:
        state = load_state_db(db_conn, state_id)
    else:
        state = load_state_disk()

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

    # owned tracking (both can exist)
    state.setdefault("strategy_owned_qty", 0)
    state.setdefault("sim_owned_qty", 0)

    # Daily buy limiter (ET day)
    state.setdefault("buys_today_et", 0)
    state.setdefault("buys_today_date_et", None)

    # Optional: reset simulated ownership on startup (DRY_RUN only)
    if DRY_RUN and RESET_SIM_OWNED_ON_START:
        old_sim = int(state.get("sim_owned_qty", 0))
        if old_sim != 0:
            logging.info(f"RESET_SIM_OWNED_ON_START enabled → sim_owned_qty {old_sim} → 0")
        else:
            logging.info("RESET_SIM_OWNED_ON_START enabled → sim_owned_qty already 0")
        state["sim_owned_qty"] = 0

    logging.info(
        "STATE_LOADED "
        f"last_bar_ts={last_bar_ts_iso} "
        f"group_anchor_close={group_anchor_close} "
        f"last_red_buy_close={last_red_buy_close} "
        f"buy_count_total={buy_count_total} "
        f"group_buy_count={group_buy_count} "
        f"strategy_owned_qty={int(state.get('strategy_owned_qty', 0))} "
        f"sim_owned_qty={int(state.get('sim_owned_qty', 0))} "
        f"buys_today_date_et={state.get('buys_today_date_et')} "
        f"buys_today_et={int(state.get('buys_today_et', 0))}"
    )

    # Position-change baseline
    last_pos_qty = None
    if LOG_POSITION_CHANGES:
        last_pos_qty = get_position_qty(SYMBOL)
        logging.info(f"POSITION_INIT qty={last_pos_qty:.4f}")

    while True:
        try:
            clock = alpaca_call_with_retry(lambda: api.get_clock(), label="get_clock")

            if not clock.is_open:
                logging.info("MARKET_CLOSED waiting...")
                time.sleep(30)
                continue

            # Standby: keep trying to become leader
            if db_conn is not None and not is_leader:
                is_leader = try_acquire_leader_lock(db_conn, LEADER_LOCK_KEY)
                if is_leader:
                    logging.info("LEADER_LOCK acquired -> ACTIVE mode (orders allowed)")
                else:
                    time.sleep(STANDBY_POLL_SEC)
                    continue

            # Use Alpaca clock timestamp as "now" (UTC)
            now_utc = clock.timestamp
            if now_utc.tzinfo is None:
                now_utc = now_utc.replace(tzinfo=timezone.utc)

            # ET day rollover (daily buy limiter)
            today_et = et_date_str(now_utc)
            if state.get("buys_today_date_et") != today_et:
                state["buys_today_date_et"] = today_et
                state["buys_today_et"] = 0
                logging.info(f"DAY_ROLLOVER_ET date={today_et} buys_today_et reset to 0")

            # Optional: detect manual position changes and keep owned qty sane
            if LOG_POSITION_CHANGES:
                pos_qty_now = get_position_qty(SYMBOL)
                if last_pos_qty is None:
                    last_pos_qty = pos_qty_now
                elif pos_qty_now != last_pos_qty:
                    logging.info(
                        f"POSITION_CHANGE qty_from={last_pos_qty:.4f} qty_to={pos_qty_now:.4f}"
                    )
                    last_pos_qty = pos_qty_now

                # Clamp owned qty so we never claim to own more than we hold
                owned_now = get_owned_qty(state)
                if int(pos_qty_now) < owned_now:
                    logging.warning(
                        f"OWNED_CLAMP position_qty={int(pos_qty_now)} owned_qty={owned_now} -> owned_qty={int(pos_qty_now)}"
                    )
                    set_owned_qty(state, int(pos_qty_now))

                # If externally liquidated to zero, reset group + owned (for current mode)
                if pos_qty_now == 0.0:
                    if get_owned_qty(state) != 0:
                        logging.info("LIQUIDATION_DETECTED setting owned_qty=0 for current mode")
                        set_owned_qty(state, 0)
                    if group_anchor_close is not None or last_red_buy_close is not None or group_buy_count != 0:
                        logging.info("LIQUIDATION_DETECTED resetting group state")
                        reset_group_state(state)
                        group_anchor_close = None
                        last_red_buy_close = None
                        group_buy_count = 0

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
                f"pos_qty={int(pos_qty)} avg_entry={avg_entry} owned_qty={owned_qty} "
                f"buys_today_et={int(state.get('buys_today_et', 0))} "
                f"is_leader={is_leader}"
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
            # NOTE: KILL_SWITCH does NOT block sells (sells reduce risk).
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
                        set_owned_qty(state, owned_qty - sell_qty)
                    else:
                        if db_conn is not None and not is_leader:
                            logging.warning("STANDBY_BLOCK: skipping SELL (no leader lock)")
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

                            dec = 0
                            try:
                                dec = int(float(filled_qty)) if filled_qty is not None else sell_qty
                            except Exception:
                                dec = sell_qty
                            set_owned_qty(state, owned_qty - dec)

                    # After sell (real or simulated), reset group
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

                # ---- Safety rails (BUY side) ----
                if should_buy and KILL_SWITCH:
                    logging.warning("BUY_BLOCKED KILL_SWITCH active (buys disabled; sells allowed).")
                    should_buy = False

                if should_buy and (not in_trade_window_et(now_utc)):
                    logging.info("BUY_BLOCKED outside trade window (ET).")
                    should_buy = False

                if should_buy and MAX_BUYS_PER_DAY > 0:
                    if int(state.get("buys_today_et", 0)) >= MAX_BUYS_PER_DAY:
                        logging.warning(f"BUY_BLOCKED max buys per ET day reached: {MAX_BUYS_PER_DAY}")
                        should_buy = False

                if should_buy and MAX_POSITION_QTY > 0:
                    current_pos = int(get_position_qty(SYMBOL))
                    if current_pos + int(ORDER_QTY) > MAX_POSITION_QTY:
                        logging.warning(
                            f"BUY_BLOCKED would exceed MAX_POSITION_QTY={MAX_POSITION_QTY} "
                            f"(current_pos={current_pos}, order_qty={ORDER_QTY})"
                        )
                        should_buy = False

                if should_buy and MAX_DOLLARS_PER_BUY > 0:
                    est_cost = float(c) * int(ORDER_QTY)  # close used as estimate
                    if est_cost > MAX_DOLLARS_PER_BUY:
                        logging.warning(
                            f"BUY_BLOCKED est_cost=${est_cost:.2f} exceeds MAX_DOLLARS_PER_BUY=${MAX_DOLLARS_PER_BUY:.2f}"
                        )
                        should_buy = False

                # ---- Execute buy decision ----
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
                            set_owned_qty(state, get_owned_qty(state) + ORDER_QTY)
                            state["buys_today_et"] = int(state.get("buys_today_et", 0)) + 1
                        else:
                            if db_conn is not None and not is_leader:
                                logging.warning("STANDBY_BLOCK: skipping BUY (no leader lock)")
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

                                # Count buy attempt toward daily limit (fills may vary)
                                state["buys_today_et"] = int(state.get("buys_today_et", 0)) + 1

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
                "buys_today_date_et": state.get("buys_today_date_et"),
                "buys_today_et": int(state.get("buys_today_et", 0)),
                "symbol": SYMBOL,
            }
            maybe_persist_state(state, payload, db_conn=db_conn, state_id=state_id)

            time.sleep(POLL_SEC)

        except Exception as e:
            logging.error(f"ENGINE_ERROR {e}", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
