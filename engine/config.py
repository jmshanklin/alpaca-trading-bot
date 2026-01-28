# engine/config.py
import os
import re
from dataclasses import dataclass
from typing import Optional, Tuple

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


def parse_hhmm(s: str) -> Optional[Tuple[int, int]]:
    try:
        if not s:
            return None
        hh, mm = s.split(":")
        return int(hh), int(mm)
    except Exception:
        return None


@dataclass(frozen=True)
class Config:
    # Alpaca
    symbol: str
    data_feed: str
    base_url: str
    key_id: str
    secret_key: str

    # Mode / safety
    dry_run: bool
    kill_switch: bool
    live_trading_confirm: str

    # Loop timing
    poll_sec: float
    fill_timeout_sec: float
    fill_poll_sec: float

    # Risk limits
    order_qty: int
    max_buys_per_tick: int
    max_buys_per_day: int
    max_dollars_per_buy: float
    max_position_qty: int

    # Time window
    trade_start_et: str
    trade_end_et: str

    # Grid strategy
    grid_step_start_usd: float
    grid_step_increment_usd: float
    grid_tier_size: int
    sell_rise_usd: float

    # Optional: DB/leader lock/state
    database_url: str
    leader_lock_key: str
    standby_only: bool
    standby_poll_sec: float
    state_path: str
    state_save_sec: float

    # Logging / banners
    log_position_changes: bool


def load_config() -> Config:
    # allow either APCA_* or ALPACA_* key names
    key_id = env_str("ALPACA_KEY_ID") or env_str("APCA_API_KEY_ID")
    secret = env_str("ALPACA_SECRET_KEY") or env_str("APCA_API_SECRET_KEY")
    base_url = env_str("ALPACA_BASE_URL") or env_str("APCA_API_BASE_URL") or "https://paper-api.alpaca.markets"

    if not key_id or not secret:
        raise RuntimeError("Missing Alpaca credentials (APCA_API_KEY_ID/APCA_API_SECRET_KEY).")

    symbol = env_str("ENGINE_SYMBOL", "TSLA").upper()

    # IMPORTANT: use ALPACA_DATA_FEED (keep default iex)
    data_feed = env_str("ALPACA_DATA_FEED", env_str("APCA_DATA_FEED", "iex")).lower()

    return Config(
        symbol=symbol,
        data_feed=data_feed,
        base_url=base_url,
        key_id=key_id,
        secret_key=secret,

        dry_run=env_bool("DRY_RUN", True),
        kill_switch=env_bool("KILL_SWITCH", False),
        live_trading_confirm=env_str("LIVE_TRADING_CONFIRM", ""),

        poll_sec=env_float("POLL_SEC", 1.0),
        fill_timeout_sec=env_float("FILL_TIMEOUT_SEC", 20.0),
        fill_poll_sec=env_float("FILL_POLL_SEC", 0.5),

        order_qty=env_int("ORDER_QTY", 1),
        max_buys_per_tick=env_int("MAX_BUYS_PER_TICK", 1),
        max_buys_per_day=env_int("MAX_BUYS_PER_DAY", 0),
        max_dollars_per_buy=env_float("MAX_DOLLARS_PER_BUY", 0.0),
        max_position_qty=env_int("MAX_POSITION_QTY", 0),

        trade_start_et=env_str("TRADE_START_ET", ""),
        trade_end_et=env_str("TRADE_END_ET", ""),

        grid_step_start_usd=env_float("GRID_STEP_START_USD", 1.0),
        grid_step_increment_usd=env_float("GRID_STEP_INCREMENT_USD", 1.0),
        grid_tier_size=env_int("GRID_TIER_SIZE", 5),
        sell_rise_usd=env_float("SELL_RISE_USD", 2.0),

        database_url=env_str("DATABASE_URL", ""),
        leader_lock_key=env_str("LEADER_LOCK_KEY", f"{symbol}_ENGINE_V1"),
        standby_only=env_bool("STANDBY_ONLY", False),
        standby_poll_sec=env_float("STANDBY_POLL_SEC", 2.0),
        state_path=env_str("STATE_PATH", "/var/data/engine_state.json"),
        state_save_sec=env_float("STATE_SAVE_SEC", 5.0),

        log_position_changes=env_bool("LOG_POSITION_CHANGES", True),
    )
