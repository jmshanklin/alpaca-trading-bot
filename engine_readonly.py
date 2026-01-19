import os
import time
import json
import logging
import hashlib
import random
import re
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Callable, TypeVar, Optional

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
# Banners / Heartbeat
# =========================
HEARTBEAT_SEC = 300  # 5 minutes
_last_heartbeat_ts = 0


def print_startup_banner(*, live_endpoint: bool, is_leader: bool):
    mode = "SIMULATION (DRY_RUN)" if DRY_RUN else ("LIVE PAPER" if not live_endpoint else "LIVE REAL MONEY")
    orders = "ENABLED" if (is_leader and not KILL_SWITCH) else "BLOCKED"

    logger.warning("")
    logger.warning("==============================================")
    logger.warning("🚀 BOT STARTUP CONFIRMATION BANNER")
    logger.warning("----------------------------------------------")
    logger.warning(f"MODE:         {mode}")
    logger.warning(f"SYMBOL:       {SYMBOL}")
    logger.warning(f"SELL_PCT:     {SELL_PCT} ({SELL_PCT * 100:.3f}%)")
    logger.warning(f"LEADER:       {is_leader}")
    logger.warning(f"ORDERS:       {orders}")
    logger.warning(f"DRY_RUN:      {DRY_RUN}")
    logger.warning(f"KILL_SWITCH:  {KILL_SWITCH}")
    logger.warning(f"ENDPOINT:     {ALPACA_BASE_URL}")
    logger.warning("==============================================")
    logger.warning("")


def maybe_print_heartbeat(*, pos_qty, avg_entry, sell_target, is_leader):
    global _last_heartbeat_ts

    now = time.time()
    if now - _last_heartbeat_ts < HEARTBEAT_SEC:
        return

    _last_heartbeat_ts = now

    mode = "SIM" if DRY_RUN else "LIVE"
    target_str = f"{sell_target:.2f}" if sell_target is not None else "None"
    avg_str = f"{avg_entry:.2f}" if avg_entry is not None else "None"

    logger.warning("")
    logger.warning("💓 HEARTBEAT")
    logger.warning("----------------------------------------------")
    logger.warning(f"MODE:       {mode}")
    logger.warning(f"SYMBOL:     {SYMBOL}")
    logger.warning(f"POS_QTY:    {int(pos_qty)}")
    logger.warning(f"AVG_ENTRY:  {avg_str}")
    logger.warning(f"SELL_PCT:   {SELL_PCT * 100:.3f}%")
    logger.warning(f"SELL_TGT:   {target_str}")
    logger.warning(f"LEADER:     {is_leader}")
    logger.warning(f"KILL_SW:    {KILL_SWITCH}")
    logger.warning("----------------------------------------------")
    logger.warning("")


def print_profit_tracker_banner(
    *,
    symbol: str,
    pos_qty: float,
    avg_entry: Optional[float],
    current_price: Optional[float],
    unrealized_pl: Optional[float],
    unrealized_plpc: Optional[float],
    market_value: Optional[float],
    sell_pct: float,
    sell_target: Optional[float],
    is_leader: bool,
):
    logger.warning("")
    logger.warning("📊 LIVE PROFIT TRACKER (unrealized)")
    logger.warning("------------------------------------------------")
    logger.warning(f"SYMBOL:      {symbol}")
    logger.warning(f"LEADER:      {is_leader}")
    logger.warning(f"POS_QTY:     {int(float(pos_qty)) if pos_qty is not None else 0}")

    if avg_entry is not None:
        logger.warning(f"AVG_ENTRY:   {float(avg_entry):.2f}")
    else:
        logger.warning("AVG_ENTRY:   None")

    if current_price is not None:
        logger.warning(f"LAST_PRICE:  {float(current_price):.2f}")
    else:
        logger.warning("LAST_PRICE:  None")

    if market_value is not None:
        logger.warning(f"MKT_VALUE:   ${float(market_value):,.2f}")
    else:
        logger.warning("MKT_VALUE:   None")

    if unrealized_pl is not None:
        logger.warning(f"UNRLZD_P/L:  ${float(unrealized_pl):,.2f}")
    else:
        logger.warning("UNRLZD_P/L:  None")

    if unrealized_plpc is not None:
        logger.warning(f"UNRLZD_%:    {float(unrealized_plpc) * 100.0:.3f}%")
    else:
        logger.warning("UNRLZD_%:    None")

    logger.warning(f"SELL_PCT:    {float(sell_pct) * 100.0:.3f}%")

    if sell_target is not None:
        logger.warning(f"SELL_TGT:    {float(sell_target):.2f}")
    else:
        logger.warning("SELL_TGT:    None")

    logger.warning("------------------------------------------------")
    logger.warning("")

def print_daily_summary_banner(
    *,
    symbol: str,
    date_et: str,
    is_leader: bool,
    dry_run: bool,
    pos_qty: float,
    owned_qty: int,
    avg_entry: Optional[float],
    sell_pct: float,
    sell_target: Optional[float],
    buy_count_total: int,
    group_buy_count: int,
    buys_today_et: int,
    unrealized_pl: Optional[float],
    unrealized_plpc: Optional[float],
    market_value: Optional[float],
):
    mode = "SIMULATION (DRY_RUN)" if dry_run else "LIVE (Paper/Live)"

    logger.warning("")
    logger.warning("📅 DAILY SUMMARY (Market Close)")
    logger.warning("------------------------------------------------")
    logger.warning(f"DATE_ET:       {date_et}")
    logger.warning(f"MODE:          {mode}")
    logger.warning(f"SYMBOL:        {symbol}")
    logger.warning(f"LEADER:        {is_leader}")
    logger.warning(f"POS_QTY:       {int(pos_qty)}")
    logger.warning(f"OWNED_QTY:     {int(owned_qty)}")

    if avg_entry is not None:
        logger.warning(f"AVG_ENTRY:     {float(avg_entry):.2f}")
    else:
        logger.warning("AVG_ENTRY:     None")

    logger.warning(f"SELL_PCT:      {float(sell_pct) * 100.0:.3f}%")
    if sell_target is not None:
        logger.warning(f"SELL_TARGET:   {float(sell_target):.2f}")
    else:
        logger.warning("SELL_TARGET:   None")

    logger.warning(f"BUYS_TODAY_ET: {int(buys_today_et)}")
    logger.warning(f"BUY_COUNT_TTL: {int(buy_count_total)}")
    logger.warning(f"GROUP_BUY_CNT: {int(group_buy_count)}")

    # If live, include unrealized P/L snapshot (if available)
    if (not dry_run) and (unrealized_pl is not None):
        logger.warning(f"UNRLZD_P/L:    ${float(unrealized_pl):,.2f}")
    else:
        logger.warning("UNRLZD_P/L:    None")

    if (not dry_run) and (unrealized_plpc is not None):
        logger.warning(f"UNRLZD_%:      {float(unrealized_plpc) * 100.0:.3f}%")
    else:
        logger.warning("UNRLZD_%:      None")

    if (not dry_run) and (market_value is not None):
        logger.warning(f"MKT_VALUE:     ${float(market_value):,.2f}")
    else:
        logger.warning("MKT_VALUE:     None")

    logger.warning("------------------------------------------------")
    logger.warning("")


def maybe_print_daily_summary_banner(
    *,
    state: dict,
    now_utc: datetime,
    is_leader: bool,
    symbol: str,
    pos_qty: float,
    owned_qty: int,
    avg_entry: Optional[float],
    sell_pct: float,
    sell_target: Optional[float],
    buy_count_total: int,
    group_buy_count: int,
    buys_today_et: int,
    unrealized_pl: Optional[float],
    unrealized_plpc: Optional[float],
    market_value: Optional[float],
):
    if not DAILY_SUMMARY_BANNER:
        return

    # Convert to ET and compute "today" string
    now_et = now_utc.astimezone(ET)
    date_et = now_et.date().isoformat()

    # Only once per ET day
    if state.get("last_daily_summary_date_et") == date_et:
        return

    # Trigger time (defaults to 15:59 ET)
    hhmm = parse_hhmm(DAILY_SUMMARY_ET_TIME)
    if not hhmm:
        return
    target_h, target_m = hhmm

    # Fire within a small window so we don't miss it due to loop timing
    target_minutes = target_h * 60 + target_m
    now_minutes = now_et.hour * 60 + now_et.minute

    # 0..5 minute window starting at the target time
    if not (target_minutes <= now_minutes <= target_minutes + 5):
        return

    # Mark as printed BEFORE logging (so restarts don't spam)
    state["last_daily_summary_date_et"] = date_et

    print_daily_summary_banner(
        symbol=symbol,
        date_et=date_et,
        is_leader=is_leader,
        dry_run=bool(DRY_RUN),
        pos_qty=pos_qty,
        owned_qty=owned_qty,
        avg_entry=avg_entry,
        sell_pct=sell_pct,
        sell_target=sell_target,
        buy_count_total=buy_count_total,
        group_buy_count=group_buy_count,
        buys_today_et=buys_today_et,
        unrealized_pl=unrealized_pl,
        unrealized_plpc=unrealized_plpc,
        market_value=market_value,
    )

def maybe_print_profit_tracker_banner(
    *,
    state: dict,
    now_ts: float,
    symbol: str,
    pos_qty: float,
    avg_entry: Optional[float],
    current_price: Optional[float],
    unrealized_pl: Optional[float],
    unrealized_plpc: Optional[float],
    market_value: Optional[float],
    sell_pct: float,
    sell_target: Optional[float],
    is_leader: bool,
):
    # Only for real trading (paper/live) — not simulation
    if DRY_RUN:
        return

    every = float(PROFIT_TRACKER_EVERY_SEC)
    if every <= 0:
        return

    last_ts = float(state.get("last_profit_banner_ts", 0.0))
    if (now_ts - last_ts) < every:
        return

    state["last_profit_banner_ts"] = now_ts

    print_profit_tracker_banner(
        symbol=symbol,
        pos_qty=pos_qty,
        avg_entry=avg_entry,
        current_price=current_price,
        unrealized_pl=unrealized_pl,
        unrealized_plpc=unrealized_plpc,
        market_value=market_value,
        sell_pct=sell_pct,
        sell_target=sell_target,
        is_leader=is_leader,
    )


def print_first_buy_banner(
    *,
    live_endpoint: bool,
    is_leader: bool,
    symbol: str,
    close: float,
    qty: int,
    avg_entry,
    sell_pct: float,
    sell_target,
):
    mode = "SIMULATION (DRY_RUN)" if DRY_RUN else ("LIVE PAPER" if not live_endpoint else "LIVE REAL MONEY")
    orders = "ENABLED" if (is_leader and not KILL_SWITCH) else "BLOCKED"

    logger.warning("✅ FIRST BUY CONFIRMED")
    logger.warning("----------------------------------------------")
    logger.warning(f"MODE:     {mode}")
    logger.warning(f"ORDERS:   {orders}")
    logger.warning(f"SYMBOL:   {symbol}")
    logger.warning(f"BUY_CLS:  {close:.2f}")
    logger.warning(f"BUY_QTY:  {qty}")
    if avg_entry is not None:
        logger.warning(f"AVG_ENT:  {float(avg_entry):.2f}")
    logger.warning(f"SELL_PCT: {float(sell_pct) * 100:.3f}%")
    if sell_target is not None:
        logger.warning(f"SELL_TGT: {float(sell_target):.2f}")
    logger.warning(f"LEADER:   {is_leader}")
    logger.warning(f"KILL_SW:  {KILL_SWITCH}")
    logger.warning("----------------------------------------------")


def print_sell_arming_banner(
    *,
    symbol: str,
    close_price: float,
    avg_entry: Optional[float],
    sell_target: float,
    arm_price: float,
    leader: bool,
    dry_run: bool,
):
    logger.warning("⚠️  SELL ARMING")
    logger.warning("------------------------------------------------")
    logger.warning(f"SYMBOL:    {symbol}")
    logger.warning(f"CLOSE:     {close_price:.2f}")
    if avg_entry is not None:
        logger.warning(f"AVG_ENTRY: {avg_entry:.2f}")
    logger.warning(f"TARGET:    {sell_target:.2f}")
    logger.warning(f"ARM_AT:    {arm_price:.2f}")
    logger.warning(f"LEADER:    {leader}")
    logger.warning(f"DRY_RUN:   {dry_run}")
    logger.warning("------------------------------------------------")


def print_sell_banner(
    *,
    symbol: str,
    sell_qty: int,
    close_price: float,
    avg_entry: Optional[float],
    sell_target: Optional[float],
    pos_qty_before: float,
    leader: bool,
    dry_run: bool,
):
    logger.warning("✅ SELL CONFIRMED")
    logger.warning("----------------------------------------------")
    logger.warning(f"SYMBOL:      {symbol}")
    logger.warning(f"SELL_QTY:    {sell_qty}")
    logger.warning(f"CLOSE:       {close_price:.2f}")
    logger.warning(f"POS_BEFORE:  {pos_qty_before:.4f}")
    logger.warning(f"AVG_ENTRY:   {(f'{avg_entry:.2f}' if avg_entry is not None else 'None')}")
    logger.warning(f"SELL_TGT:    {(f'{sell_target:.2f}' if sell_target is not None else 'None')}")
    logger.warning(f"LEADER:      {leader}")
    logger.warning(f"DRY_RUN:     {dry_run}")
    logger.warning("----------------------------------------------")


# =========================
# Env parsing helpers
# =========================
_NUM_RE = re.compile(r"^\s*([+-]?\d+(?:\.\d+)?)")


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    m = _NUM_RE.match(str(raw))
    if not m:
        raise ValueError(f"Env {name} must start with a number. Got: {raw!r}")
    return float(m.group(1))


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    m = _NUM_RE.match(str(raw))
    if not m:
        raise ValueError(f"Env {name} must start with a number. Got: {raw!r}")
    return int(float(m.group(1)))


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def env_str(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


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

            if (not transient) and attempt >= 3:
                logger.error(f"{label}: non-transient after {attempt} attempts: {e}")
                raise

            sleep_s = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
            sleep_s = sleep_s * (0.8 + 0.4 * random.random())
            logger.warning(f"{label}: error attempt {attempt}/{tries}: {e} | sleeping {sleep_s:.2f}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"{label}: failed after {tries} attempts")


# =========================
# Persistence helpers (disk fallback)
# =========================
def resolve_state_path() -> str:
    state_dir = env_str("STATE_DIR", "/var/data")
    state_file = env_str("STATE_FILE", "engine_state.json")
    state_path = env_str("STATE_PATH", os.path.join(state_dir, state_file))

    try:
        os.makedirs(os.path.dirname(state_path), exist_ok=True)

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
DRY_RUN = env_bool("DRY_RUN", True)
ORDER_QTY = env_int("ORDER_QTY", 1)

POLL_SEC = env_float("POLL_SEC", 1.0)

FILL_TIMEOUT_SEC = env_float("FILL_TIMEOUT_SEC", 20.0)
FILL_POLL_SEC = env_float("FILL_POLL_SEC", 0.5)

MAX_BUYS_PER_TICK = env_int("MAX_BUYS_PER_TICK", 1)
LOG_POSITION_CHANGES = env_bool("LOG_POSITION_CHANGES", True)

STATE_SAVE_SEC = env_float("STATE_SAVE_SEC", 0.0)
SELL_PCT = env_float("SELL_PCT", 0.0)

SELL_ARM_BANNER = env_bool("SELL_ARM_BANNER", True)
SELL_ARM_PCT = env_float("SELL_ARM_PCT", 0.0005)  # 0.05% below target

RESET_SIM_OWNED_ON_START = env_bool("RESET_SIM_OWNED_ON_START", False)

LIVE_TRADING_CONFIRM = env_str("LIVE_TRADING_CONFIRM", "")
KILL_SWITCH = env_bool("KILL_SWITCH", False)

PROFIT_TRACKER_EVERY_SEC = env_float("PROFIT_TRACKER_EVERY_SEC", 300.0)  # 5 minutes
DAILY_SUMMARY_BANNER = env_bool("DAILY_SUMMARY_BANNER", True)
DAILY_SUMMARY_ET_TIME = env_str("DAILY_SUMMARY_ET_TIME", "15:59")  # 3:59pm ET

STANDBY_ONLY = env_bool("STANDBY_ONLY", False)

MAX_DOLLARS_PER_BUY = env_float("MAX_DOLLARS_PER_BUY", 0.0)
MAX_POSITION_QTY = env_int("MAX_POSITION_QTY", 0)
MAX_BUYS_PER_DAY = env_int("MAX_BUYS_PER_DAY", 0)

TRADE_START_ET = env_str("TRADE_START_ET", "")
TRADE_END_ET = env_str("TRADE_END_ET", "")

STATE_PATH = resolve_state_path()

ALPACA_KEY_ID = env_str("ALPACA_KEY_ID") or env_str("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = env_str("ALPACA_SECRET_KEY") or env_str("APCA_API_SECRET_KEY")
ALPACA_BASE_URL = env_str("ALPACA_BASE_URL") or env_str("APCA_API_BASE_URL") or "https://paper-api.alpaca.markets"

SYMBOL = env_str("ENGINE_SYMBOL", "TSLA").upper()

ALPACA_DATA_FEED = env_str("ALPACA_DATA_FEED", "iex").lower()
logger.info(f"CONFIG alpaca_data_feed={ALPACA_DATA_FEED}")

# =========================
# Self-test / Heartbeat mode (after-hours safe)
# =========================
SELF_TEST = env_bool("SELF_TEST", False)
SELF_TEST_EVERY_SEC = env_float("SELF_TEST_EVERY_SEC", 300.0)
SELF_TEST_LOOKBACK_MIN = env_int("SELF_TEST_LOOKBACK_MIN", 180)
SELF_TEST_MAX_AGE_MIN = env_int("SELF_TEST_MAX_AGE_MIN", 90)
SELF_TEST_NO_ORDERS = env_bool("SELF_TEST_NO_ORDERS", True)

SELF_TEST_DAILY_LOOKBACK_DAYS = env_int("SELF_TEST_DAILY_LOOKBACK_DAYS", 30)
SELF_TEST_DAILY_MAX_AGE_DAYS = env_int("SELF_TEST_DAILY_MAX_AGE_DAYS", 5)

if not ALPACA_KEY_ID or not ALPACA_SECRET_KEY:
    raise RuntimeError(
        "Missing Alpaca credentials: set ALPACA_KEY_ID/ALPACA_SECRET_KEY "
        "(or APCA_API_KEY_ID/APCA_API_SECRET_KEY)."
    )

api = tradeapi.REST(ALPACA_KEY_ID, ALPACA_SECRET_KEY, ALPACA_BASE_URL)

DATABASE_URL = env_str("DATABASE_URL", "")
LEADER_LOCK_KEY = env_str("LEADER_LOCK_KEY", f"{SYMBOL}_ENGINE_V1")
STANDBY_POLL_SEC = env_float("STANDBY_POLL_SEC", 2.0)


# =========================
# Live/paper detection + time helpers
# =========================
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


# =========================
# Self test (OPEN: minute bars, CLOSED: daily bars)
# =========================
def run_self_test(api_client, symbol: str, *, market_is_open: bool) -> bool:
    now_utc = datetime.now(timezone.utc)

    if market_is_open:
        start = now_utc - timedelta(minutes=SELF_TEST_LOOKBACK_MIN)
        tf = TimeFrame.Minute
        limit = min(10000, max(1, SELF_TEST_LOOKBACK_MIN))
        max_age_min = SELF_TEST_MAX_AGE_MIN

        logger.warning(
            f"SELF_TEST (OPEN) symbol={symbol} feed={ALPACA_DATA_FEED} lookback_min={SELF_TEST_LOOKBACK_MIN}"
        )

        try:
            bars = api_client.get_bars(
                symbol,
                tf,
                start=start.isoformat(),
                end=now_utc.isoformat(),
                limit=limit,
                adjustment="raw",
                feed=ALPACA_DATA_FEED,
            )
        except Exception as e:
            logger.error(f"SELF_TEST FAIL (OPEN): get_bars exception: {e}", exc_info=True)
            return False

        bars_list = list(bars) if bars else []
        if not bars_list:
            logger.error("SELF_TEST FAIL (OPEN): get_bars returned 0 bars")
            return False

        last = bars_list[-1]
        last_ts = getattr(last, "t", None)
        if last_ts is None:
            logger.error("SELF_TEST FAIL (OPEN): last bar missing timestamp 't'")
            return False
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)

        age_min = (now_utc - last_ts).total_seconds() / 60.0
        red_count = sum(1 for b in bars_list if float(getattr(b, "c", 0.0)) < float(getattr(b, "o", 0.0)))

        logger.warning(
            f"SELF_TEST (OPEN) bars={len(bars_list)} last_ts={last_ts.isoformat()} age_min={age_min:.1f} "
            f"last_o={float(last.o):.2f} last_c={float(last.c):.2f} red_count={red_count}"
        )

        if age_min > max_age_min:
            logger.error(f"SELF_TEST FAIL (OPEN): last bar too old (age_min={age_min:.1f} > {max_age_min})")
            return False

        logger.warning("SELF_TEST PASS ✅ (OPEN)")
        return True

    # CLOSED
    start = now_utc - timedelta(days=SELF_TEST_DAILY_LOOKBACK_DAYS)
    tf = TimeFrame.Day
    limit = max(5, min(365, SELF_TEST_DAILY_LOOKBACK_DAYS + 5))

    logger.warning(
        f"SELF_TEST (CLOSED) symbol={symbol} feed={ALPACA_DATA_FEED} lookback_days={SELF_TEST_DAILY_LOOKBACK_DAYS}"
    )

    try:
        bars = api_client.get_bars(
            symbol,
            tf,
            start=start.isoformat(),
            end=now_utc.isoformat(),
            limit=limit,
            adjustment="raw",
            feed=ALPACA_DATA_FEED,
        )
    except Exception as e:
        logger.error("SELF_TEST FAIL (CLOSED): get_bars exception: %s", e, exc_info=True)
        return False

    bars_list = list(bars) if bars else []
    if not bars_list:
        logger.error("SELF_TEST FAIL (CLOSED): get_bars returned 0 daily bars")
        return False

    last = bars_list[-1]
    last_ts = getattr(last, "t", None)
    if last_ts is None:
        logger.error("SELF_TEST FAIL (CLOSED): last daily bar missing timestamp 't'")
        return False
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=timezone.utc)

    age_days = (now_utc - last_ts).total_seconds() / (60.0 * 60.0 * 24.0)
    red_count = sum(1 for b in bars_list if float(getattr(b, "c", 0.0)) < float(getattr(b, "o", 0.0)))

    logger.warning(
        f"SELF_TEST (CLOSED) daily_bars={len(bars_list)} last_ts={last_ts.isoformat()} age_days={age_days:.2f} "
        f"last_o={float(last.o):.2f} last_c={float(last.c):.2f} red_days={red_count}"
    )

    if age_days > float(SELF_TEST_DAILY_MAX_AGE_DAYS):
        logger.error(
            f"SELF_TEST FAIL (CLOSED): last daily bar too old (age_days={age_days:.2f} > {SELF_TEST_DAILY_MAX_AGE_DAYS})"
        )
        return False

    logger.warning("SELF_TEST PASS ✅ (CLOSED)")
    return True


# =========================
# Postgres state + leader lock
# =========================
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


# =========================
# Disk state (fallback)
# =========================
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


# =========================
# Trading helpers
# =========================
def get_position(symbol: str):
    """
    Returns Alpaca position object or None if no position exists.

    HARDENED:
    - "position does not exist" => normal flat, return None
    - transient errors => retry using alpaca_call_with_retry
    - other unexpected errors => raise (so we don't incorrectly assume flat)
    """
    def _fetch():
        return api.get_position(symbol)

    try:
        return alpaca_call_with_retry(_fetch, label="get_position", tries=5, base_sleep=0.4, max_sleep=3.0)
    except Exception as e:
        msg = str(e).lower()
        if "position does not exist" in msg:
            return None
        # Anything else is NOT safe to treat as "flat"
        logger.error(f"get_position: unexpected error (NOT treating as flat): {e}", exc_info=True)
        raise

def get_position_qty(symbol: str) -> float:
    pos = get_position(symbol)
    if not pos:
        return 0.0
    try:
        return float(pos.qty)
    except Exception:
        return 0.0
        
def confirm_flat_position(symbol: str, *, checks: int = 2, delay_sec: float = 0.25) -> bool:
    """
    HARDENED flat check: only returns True if Alpaca is consistently flat.
    If any check errors, we do NOT reset (returns False).
    """
    for i in range(checks):
        try:
            pos = get_position(symbol)
        except Exception:
            # Unknown state; do NOT reset anything
            return False

        qty = 0.0
        if pos is not None:
            try:
                qty = float(getattr(pos, "qty", 0.0))
            except Exception:
                qty = 0.0

        if qty != 0.0:
            return False

        if i < checks - 1:
            time.sleep(delay_sec)

    return True

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
    try:
        end = now_utc
        start = end - timedelta(minutes=10)

        def _fetch():
            return api.get_bars(
                symbol,
                TimeFrame.Minute,
                start=start.isoformat(),
                end=end.isoformat(),
                limit=10,
                adjustment="raw",
                feed=ALPACA_DATA_FEED,
            )

        bars = alpaca_call_with_retry(_fetch, label="get_bars_1m")
        bars_list = list(bars) if bars else []
        if not bars_list:
            logger.warning("BARS_EMPTY (no data returned)")
            return None

        now_floor = now_utc.replace(second=0, microsecond=0)
        for b in reversed(bars_list):
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


# =========================
# Main
# =========================
def main():
    live_endpoint = is_live_endpoint(ALPACA_BASE_URL)

    logger.info(f"ENGINE_START mode=RED_CLOSE_GROUP_SELL_AVG_ENTRY_PCT dry_run={DRY_RUN} symbol={SYMBOL}")

    logger.info(
        "ENGINE_CONFIG "
        f"symbol={SYMBOL} order_qty={ORDER_QTY} poll_sec={POLL_SEC} "
        f"fill_timeout_sec={FILL_TIMEOUT_SEC} fill_poll_sec={FILL_POLL_SEC} "
        f"max_buys_per_tick={MAX_BUYS_PER_TICK} log_position_changes={LOG_POSITION_CHANGES} "
        f"state_path={STATE_PATH} state_save_sec={STATE_SAVE_SEC} sell_pct={SELL_PCT} "
        f"reset_sim_owned_on_start={RESET_SIM_OWNED_ON_START} kill_switch={KILL_SWITCH} "
        f"max_dollars_per_buy={MAX_DOLLARS_PER_BUY} max_position_qty={MAX_POSITION_QTY} "
        f"max_buys_per_day={MAX_BUYS_PER_DAY} trade_start_et={TRADE_START_ET} trade_end_et={TRADE_END_ET} "
        f"dry_run={DRY_RUN} alpaca_base_url={ALPACA_BASE_URL} alpaca_is_live_endpoint={live_endpoint} "
        f"db_enabled={db_enabled()} leader_lock_key={LEADER_LOCK_KEY if db_enabled() else ''} "
        f"standby_only={STANDBY_ONLY} standby_poll_sec={STANDBY_POLL_SEC} "
        f"self_test={SELF_TEST} self_test_every_sec={SELF_TEST_EVERY_SEC} self_test_no_orders={SELF_TEST_NO_ORDERS}"
    )

    # Live trading confirmation gate (ONLY live endpoint + DRY_RUN=false)
    if (not DRY_RUN) and live_endpoint:
        if LIVE_TRADING_CONFIRM != "I_UNDERSTAND":
            raise RuntimeError("LIVE trading blocked: set LIVE_TRADING_CONFIRM=I_UNDERSTAND to enable live orders.")

    # ---- Postgres + leader lock (optional) ----
    db_conn = None
    state_id = ""
    is_leader = True

    if db_enabled():
        db_conn = db_connect()
        db_init(db_conn)
        state_id = f"{SYMBOL}_state"

        if STANDBY_ONLY:
            is_leader = False
            logger.info("STANDBY_ONLY=true -> STANDBY mode (no leader lock attempt)")
        else:
            is_leader = try_acquire_leader_lock(db_conn, LEADER_LOCK_KEY)
            logger.info(
                "LEADER_LOCK acquired -> ACTIVE mode (orders allowed)"
                if is_leader
                else "LEADER_LOCK not acquired -> STANDBY mode (no orders)"
            )
    else:
        logger.warning("DATABASE_URL not set -> using DISK state (single instance only)")

    print_startup_banner(live_endpoint=live_endpoint, is_leader=is_leader)

    # ---- Load state ----
    state = load_state_db(db_conn, state_id) if db_conn is not None else load_state_disk()

    last_bar_ts_iso = state.get("last_bar_ts")
    last_bar_ts: Optional[datetime] = None
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

    state.setdefault("first_buy_banner_shown", False)
    state.setdefault("sell_banner_shown", False)
    state.setdefault("sell_arm_banner_shown", False)
    state.setdefault("last_profit_banner_ts", 0.0)  # ✅ required for profit tracker throttle
    state.setdefault("last_daily_summary_date_et", None)

    if DRY_RUN and RESET_SIM_OWNED_ON_START:
        old_sim = int(state.get("sim_owned_qty", 0))
        if old_sim != 0:
            logger.info(f"RESET_SIM_OWNED_ON_START enabled → sim_owned_qty {old_sim} → 0")
        state["sim_owned_qty"] = 0

    logger.info(
        "STATE_LOADED "
        f"last_bar_ts={last_bar_ts_iso} group_anchor_close={group_anchor_close} last_red_buy_close={last_red_buy_close} "
        f"buy_count_total={buy_count_total} group_buy_count={group_buy_count} "
        f"strategy_owned_qty={int(state.get('strategy_owned_qty', 0))} sim_owned_qty={int(state.get('sim_owned_qty', 0))} "
        f"buys_today_date_et={state.get('buys_today_date_et')} buys_today_et={int(state.get('buys_today_et', 0))}"
    )
    
    # ------------------------------------------------------------
    # BOOT-TIME RECONCILE (DB state -> Alpaca reality)
    # If Alpaca is flat, wipe any persisted "group/counter" memory
    # so the bot starts clean even after-hours.
    # ------------------------------------------------------------
    boot_pos_qty = get_position_qty(SYMBOL)

    if int(boot_pos_qty) == 0:
        logger.warning(
            "BOOT_RECONCILE: Alpaca position is 0 -> clearing persisted state counters + group memory"
        )

        # clear group memory
        reset_group_state(state)
        group_anchor_close = None
        last_red_buy_close = None
        group_buy_count = 0

        # clear counters (so logs match Alpaca reality)
        state["buy_count_total"] = 0
        state["buys_today_et"] = 0
        state["buys_today_date_et"] = et_date_str(datetime.now(timezone.utc))

        # clear owned qty trackers
        state["strategy_owned_qty"] = 0
        state["sim_owned_qty"] = 0

        # reset banners
        state["first_buy_banner_shown"] = False
        state["sell_banner_shown"] = False
        state["sell_arm_banner_shown"] = False

        # persist immediately so next startup is clean too
        payload = {
            "last_bar_ts": state.get("last_bar_ts"),
            "group_anchor_close": None,
            "last_red_buy_close": None,
            "buy_count_total": 0,
            "group_buy_count": 0,
            "strategy_owned_qty": 0,
            "sim_owned_qty": 0,
            "buys_today_date_et": state["buys_today_date_et"],
            "buys_today_et": 0,
            "symbol": SYMBOL,
        }
        maybe_persist_state(state, payload, db_conn=db_conn, state_id=state_id)

    # Position-change baseline
    last_pos_qty = None
    if LOG_POSITION_CHANGES:
        last_pos_qty = get_position_qty(SYMBOL)
        logger.info(f"POSITION_INIT qty={last_pos_qty:.4f}")

    while True:
        try:
            clock = alpaca_call_with_retry(lambda: api.get_clock(), label="get_clock")
            market_is_open = bool(clock.is_open)

            if not market_is_open:
                if SELF_TEST:
                    run_self_test(api, SYMBOL, market_is_open=False)
                    if SELF_TEST_NO_ORDERS:
                        logger.warning("SELF_TEST_NO_ORDERS is ON (trading disabled in self-test mode)")
                    time.sleep(SELF_TEST_EVERY_SEC)
                    continue

                logger.info("MARKET_CLOSED waiting...")
                time.sleep(30)
                continue

            if SELF_TEST:
                run_self_test(api, SYMBOL, market_is_open=True)
                if SELF_TEST_NO_ORDERS:
                    logger.warning("SELF_TEST_NO_ORDERS is ON (trading disabled in self-test mode)")
                time.sleep(SELF_TEST_EVERY_SEC)
                continue

            # Leader lock handling
            if db_conn is not None and not is_leader:
                if STANDBY_ONLY:
                    time.sleep(STANDBY_POLL_SEC)
                    continue

                is_leader = try_acquire_leader_lock(db_conn, LEADER_LOCK_KEY)
                if not is_leader:
                    time.sleep(STANDBY_POLL_SEC)
                    continue
                logger.info("LEADER_LOCK acquired -> ACTIVE mode (orders allowed)")

            now_utc = clock.timestamp
            if now_utc.tzinfo is None:
                now_utc = now_utc.replace(tzinfo=timezone.utc)

            # ET day rollover
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

            # Pull live P/L fields
            unrealized_pl = None
            unrealized_plpc = None
            market_value = None
            current_price = None

            pos_obj = get_position(SYMBOL)
            if pos_obj:
                try:
                    unrealized_pl = float(getattr(pos_obj, "unrealized_pl", None))
                except Exception:
                    pass
                try:
                    unrealized_plpc = float(getattr(pos_obj, "unrealized_plpc", None))
                except Exception:
                    pass
                try:
                    market_value = float(getattr(pos_obj, "market_value", None))
                except Exception:
                    pass
                try:
                    current_price = float(getattr(pos_obj, "current_price", None))
                except Exception:
                    pass

            # POSITION-AWARE RE-ARM
            is_flat_confirmed = confirm_flat_position(SYMBOL, checks=2, delay_sec=0.25)

            if is_flat_confirmed:
                if (group_anchor_close is not None) or (last_red_buy_close is not None) or int(state.get("strategy_owned_qty", 0)) != 0:
                    logger.warning(
                        "NO_POSITION (confirmed) -> resetting group memory so next red candle can start a new group "
                        f"(prev_anchor={group_anchor_close}, prev_last_red_buy_close={last_red_buy_close}, "
                        f"strategy_owned_qty={int(state.get('strategy_owned_qty', 0))})"
                    )
            
                reset_group_state(state)
                group_anchor_close = None
                last_red_buy_close = None
                group_buy_count = 0
            
                # Keep internal tracking aligned with Alpaca reality
                state["strategy_owned_qty"] = 0
                state["sim_owned_qty"] = 0
                state["buys_today_et"] = 0
            
                state["first_buy_banner_shown"] = False
                state["sell_banner_shown"] = False
                state["sell_arm_banner_shown"] = False
            else:
                # Optional: only log when pos_qty was 0 but we refused to reset due to uncertainty
                if int(pos_qty) == 0:
                    logger.warning("POS_QTY=0 but NOT confirmed flat (transient/unknown) -> NOT resetting state")

            # SELL target based on Alpaca avg entry
            sell_target = None
            if int(pos_qty) > 0 and avg_entry is not None and float(SELL_PCT) > 0:
                sell_target = float(avg_entry) * (1.0 + float(SELL_PCT))
                
            maybe_print_daily_summary_banner(
                state=state,
                now_utc=now_utc,
                is_leader=is_leader,
                symbol=SYMBOL,
                pos_qty=pos_qty,
                owned_qty=owned_qty,
                avg_entry=avg_entry,
                sell_pct=SELL_PCT,
                sell_target=sell_target,
                buy_count_total=buy_count_total,
                group_buy_count=group_buy_count,
                buys_today_et=int(state.get("buys_today_et", 0)),
                unrealized_pl=unrealized_pl,
                unrealized_plpc=unrealized_plpc,
                market_value=market_value,
            )

            # Profit tracker + heartbeat
            maybe_print_profit_tracker_banner(
                state=state,
                now_ts=time.time(),
                symbol=SYMBOL,
                pos_qty=pos_qty,
                avg_entry=avg_entry,
                current_price=current_price,
                unrealized_pl=unrealized_pl,
                unrealized_plpc=unrealized_plpc,
                market_value=market_value,
                sell_pct=SELL_PCT,
                sell_target=sell_target,
                is_leader=is_leader,
            )

            maybe_print_heartbeat(
                pos_qty=pos_qty,
                avg_entry=avg_entry,
                sell_target=sell_target,
                is_leader=is_leader,
            )

            logger.info(
                f"BAR_CLOSE {SYMBOL} t={bar_ts.isoformat()} O={o:.2f} C={c:.2f} red={is_red} "
                f"group_anchor={group_anchor_close} "
                f"sell_target={(f'{float(sell_target):.2f}' if sell_target is not None else None)} "
                f"pos_qty={int(pos_qty)} avg_entry={avg_entry} owned_qty={owned_qty} "
                f"buys_today_et={int(state.get('buys_today_et', 0))} is_leader={is_leader}"
            )

            buys_this_tick = 0

            # SELL ARMING banner (one-time when close approaches sell_target)
            if SELL_ARM_BANNER and sell_target is not None and int(pos_qty) > 0:
                arm_price = float(sell_target) * (1.0 - float(SELL_ARM_PCT))
                if (not state.get("sell_arm_banner_shown", False)) and float(c) >= arm_price and float(c) < float(sell_target):
                    print_sell_arming_banner(
                        symbol=SYMBOL,
                        close_price=float(c),
                        avg_entry=(float(avg_entry) if avg_entry is not None else None),
                        sell_target=float(sell_target),
                        arm_price=float(arm_price),
                        leader=bool(is_leader),
                        dry_run=bool(DRY_RUN),
                    )
                    state["sell_arm_banner_shown"] = True

            # =========================
            # SELL trigger (avg_entry-based)
            # =========================
            if sell_target is not None:
                if int(pos_qty) > 0 and (owned_qty > 0 or (not DRY_RUN)) and c >= float(sell_target):
                    sell_qty = int(pos_qty) if not DRY_RUN else min(int(pos_qty), int(owned_qty))

                    # SELL banner (only when SELL triggers)
                    if not state.get("sell_banner_shown", False):
                        print_sell_banner(
                            symbol=SYMBOL,
                            sell_qty=int(sell_qty),
                            close_price=float(c),
                            avg_entry=(float(avg_entry) if avg_entry is not None else None),
                            sell_target=(float(sell_target) if sell_target is not None else None),
                            pos_qty_before=float(pos_qty),
                            leader=bool(is_leader),
                            dry_run=bool(DRY_RUN),
                        )
                        state["sell_banner_shown"] = True

                    if DRY_RUN:
                        logger.info(
                            f"SIM_SELL_OWNED close={c:.2f} avg_entry={float(avg_entry):.2f} target={float(sell_target):.2f} "
                            f"sell_qty={sell_qty} owned_qty={owned_qty} pos_qty={int(pos_qty)}"
                        )
                        set_owned_qty(state, owned_qty - sell_qty)
                    else:
                        if db_conn is not None and not is_leader:
                            logger.warning("STANDBY_BLOCK: skipping SELL (no leader lock)")
                        else:
                            logger.info(
                                f"SELL_SIGNAL_OWNED close={c:.2f} avg_entry={float(avg_entry):.2f} "
                                f"target={float(sell_target):.2f} sell_qty={sell_qty}"
                            )
                            order = submit_market_sell(SYMBOL, sell_qty)
                            logger.info(f"ORDER_SUBMITTED id={order.id} qty={sell_qty} side=sell")
                            final = wait_for_fill(order.id, FILL_TIMEOUT_SEC, FILL_POLL_SEC)
                            logger.info(
                                f"ORDER_FINAL id={order.id} status={(final.status or '').lower()} "
                                f"filled_qty={getattr(final,'filled_qty',None)} avg_fill_price={getattr(final,'filled_avg_price',None)}"
                            )
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

                    state["sell_banner_shown"] = False
                    state["sell_arm_banner_shown"] = False
                    state["first_buy_banner_shown"] = False

            # =========================
            # BUY trigger
            # =========================
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
                        logger.warning(
                            f"BUY_BLOCKED would exceed MAX_POSITION_QTY={MAX_POSITION_QTY} "
                            f"(current_pos={current_pos}, order_qty={ORDER_QTY})"
                        )
                        should_buy = False

                if should_buy and MAX_DOLLARS_PER_BUY > 0:
                    est_cost = float(c) * int(ORDER_QTY)
                    if est_cost > MAX_DOLLARS_PER_BUY:
                        logger.warning(
                            f"BUY_BLOCKED est_cost=${est_cost:.2f} exceeds MAX_DOLLARS_PER_BUY=${MAX_DOLLARS_PER_BUY:.2f}"
                        )
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
                            logger.info(
                                f"SIM_BUY total#{buy_count_total} group#{group_buy_count} reason={reason} "
                                f"close={c:.2f} qty={ORDER_QTY}"
                            )
                            set_owned_qty(state, get_owned_qty(state) + ORDER_QTY)
                            state["buys_today_et"] = int(state.get("buys_today_et", 0)) + 1
                            avg_entry_after = avg_entry  # sim has no alpaca avg entry
                        else:
                            if db_conn is not None and not is_leader:
                                logger.warning("STANDBY_BLOCK: skipping BUY (no leader lock)")
                                avg_entry_after = avg_entry
                            else:
                                logger.info(
                                    f"BUY_SIGNAL total#{buy_count_total} group#{group_buy_count} reason={reason} "
                                    f"close={c:.2f} qty={ORDER_QTY}"
                                )
                                order = submit_market_buy(SYMBOL, ORDER_QTY)
                                logger.info(f"ORDER_SUBMITTED id={order.id} qty={ORDER_QTY} side=buy")
                                final = wait_for_fill(order.id, FILL_TIMEOUT_SEC, FILL_POLL_SEC)
                                logger.info(
                                    f"ORDER_FINAL id={order.id} status={(final.status or '').lower()} "
                                    f"filled_qty={getattr(final,'filled_qty',None)} avg_fill_price={getattr(final,'filled_avg_price',None)}"
                                )
                                filled_qty = getattr(final, "filled_qty", None)
                                inc = ORDER_QTY
                                try:
                                    if filled_qty is not None:
                                        inc = int(float(filled_qty))
                                except Exception:
                                    pass
                                set_owned_qty(state, get_owned_qty(state) + inc)
                                state["buys_today_et"] = int(state.get("buys_today_et", 0)) + 1
                                avg_entry_after = get_position_avg_entry(SYMBOL)

                        last_red_buy_close = float(c)
                        logger.info(f"RED_BUY_MEMORY_UPDATE last_red_buy_close={last_red_buy_close:.2f}")

                        # Recompute sell_target (after buy) for banner accuracy
                        sell_target_after = None
                        if (not DRY_RUN) and (avg_entry_after is not None) and float(SELL_PCT) > 0:
                            sell_target_after = float(avg_entry_after) * (1.0 + float(SELL_PCT))
                        elif DRY_RUN and (avg_entry_after is not None) and float(SELL_PCT) > 0:
                            sell_target_after = float(avg_entry_after) * (1.0 + float(SELL_PCT))

                        # FIRST BUY banner (one-time per group)
                        if group_buy_count == 1 and not state.get("first_buy_banner_shown", False):
                            print_first_buy_banner(
                                live_endpoint=live_endpoint,
                                is_leader=is_leader,
                                symbol=SYMBOL,
                                close=c,
                                qty=ORDER_QTY,
                                avg_entry=avg_entry_after,
                                sell_pct=SELL_PCT,
                                sell_target=sell_target_after,
                            )
                            state["first_buy_banner_shown"] = True
                else:
                    logger.info(f"RED_SKIP reason={reason} close={c:.2f} last_red_buy_close={last_red_buy_close}")

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
                "last_profit_banner_ts": float(state.get("last_profit_banner_ts", 0.0)),
                "first_buy_banner_shown": bool(state.get("first_buy_banner_shown", False)),
                "sell_banner_shown": bool(state.get("sell_banner_shown", False)),
                "sell_arm_banner_shown": bool(state.get("sell_arm_banner_shown", False)),
            }
            maybe_persist_state(state, payload, db_conn=db_conn, state_id=state_id)

            time.sleep(POLL_SEC)

        except Exception as e:
            logger.error(f"ENGINE_ERROR {e}", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
