# engine/engine.py
# Engine v1.30.x — Render-safe startup, logger defined early, CT timestamp tagging in DB notes
import time
from datetime import datetime, timezone
from typing import Optional, Tuple
import uuid

import alpaca_trade_api as tradeapi

from logging_ct import build_logger
# ---- logger must exist before main() runs ----
logger = build_logger("engine")

from config import load_config
from risk import check_buy_allowed
from grid import (
    GridState,
    should_buy_now,
    should_sell_now,
    on_buy_filled,
    reset_group,
    current_step_usd,
)
from state import (
    db_enabled,
    db_connect,
    db_init,
    try_acquire_leader_lock,
    load_state_db,
    save_state_db,
    load_state_disk,
    save_state_disk,
)

# ----------------------------
# Logger must exist BEFORE any logger.* calls
# ----------------------------
logger = build_logger("engine")

# ----------------------------
# Boot signature (prints immediately even if logger breaks later)
# ----------------------------
BOOT_SIGNATURE = "ENGINE_1.30.0_BOOT_SIG_2026-01-30_C"
print(f"BOOT_SIGNATURE: {BOOT_SIGNATURE}", flush=True)
logger.warning(f"BOOT_SIGNATURE: {BOOT_SIGNATURE}")

# ----------------------------
# Time helpers
# ----------------------------
def ct_now_str(now_utc: Optional[datetime] = None) -> str:
    """
    Return a Central Time timestamp string.
    We keep DB ts_utc as UTC (best practice), but we TAG notes with CT so you never have to convert.
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    try:
        from zoneinfo import ZoneInfo  # py3.9+
        ct = now_utc.astimezone(ZoneInfo("America/Chicago"))
        return ct.strftime("%Y-%m-%d %H:%M:%S CT")
    except Exception:
        # Fallback if ZoneInfo missing for any reason
        return now_utc.strftime("%Y-%m-%d %I:%M:%S %p UTC")

def et_date_str(now_utc: datetime) -> str:
    """
    ET rollover key for "buys_today".
    """
    try:
        from zoneinfo import ZoneInfo  # py3.9+
        et = now_utc.astimezone(ZoneInfo("America/New_York"))
        return et.date().isoformat()
    except Exception:
        return now_utc.date().isoformat()

# ----------------------------
#          Helpers
# ----------------------------
def is_live_endpoint(url: str) -> bool:
    u = (url or "").lower()
    if "paper-api" in u:
        return False
    return "api.alpaca.markets" in u

def get_position_details(
    api: tradeapi.REST, symbol: str
) -> Tuple[Optional[int], Optional[float], Optional[float], Optional[float], bool]:
    """
    Returns: (qty, avg_entry, market_value, unrealized_pl, ok)

    ok=True  -> values are reliable
    ok=False -> position lookup failed; DO NOT treat as flat
    """
    try:
        pos = api.get_position(symbol)
        qty = int(float(pos.qty))

        def _f(attr: str) -> Optional[float]:
            v = getattr(pos, attr, None)
            return float(v) if v is not None else None

        avg_entry = _f("avg_entry_price")
        mv = _f("market_value")
        upl = _f("unrealized_pl")
        return qty, avg_entry, mv, upl, True

    except Exception as e:
        if "position does not exist" in str(e).lower():
            return 0, None, None, None, True  # flat is a valid state
        logger.warning(f"GET_POSITION_FAILED: {e}")
        return None, None, None, None, False

def submit_market_buy(api: tradeapi.REST, symbol: str, qty: int, client_order_id: str):
    return api.submit_order(
        symbol=symbol,
        qty=qty,
        side="buy",
        type="market",
        time_in_force="day",
        client_order_id=client_order_id,
    )


def submit_market_sell(api: tradeapi.REST, symbol: str, qty: int, client_order_id: str):
    return api.submit_order(
        symbol=symbol,
        qty=qty,
        side="sell",
        type="market",
        time_in_force="day",
        client_order_id=client_order_id,
    )


def get_last_price(api: tradeapi.REST, symbol: str) -> Optional[float]:
    """
    Uses latest trade; often works with IEX for equities during market hours.
    If latest trade isn't available, falls back to latest quote midpoint.
    """
    try:
        t = api.get_latest_trade(symbol)
        p = getattr(t, "price", None)
        if p is not None:
            return float(p)
    except Exception:
        pass

    try:
        q = api.get_latest_quote(symbol)
        bp = getattr(q, "bp", None)
        ap = getattr(q, "ap", None)
        if bp is not None and ap is not None:
            return (float(bp) + float(ap)) / 2.0
        if ap is not None:
            return float(ap)
        if bp is not None:
            return float(bp)
    except Exception:
        return None

    return None


def fmt_money(x: Optional[float]) -> str:
    if x is None:
        return "None"
    return f"{x:,.2f}"


def fmt_num(x: Optional[float]) -> str:
    if x is None:
        return "None"
    return f"{x:.2f}"


def journal_trade(
    *,
    conn,
    symbol: str,
    side: str,
    qty: int,
    est_price: Optional[float],
    is_dry_run: bool,
    is_leader: bool,
    group_id: Optional[str],
    gs: GridState,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
    note: str = "",
) -> None:
    """
    Writes one row to public.trade_journal.
    If conn is None, does nothing (disk-state mode).

    IMPORTANT:
    - DB column is ts_utc (UTC) — we keep it.
    - To avoid you ever converting times, we prefix every note with CT timestamp.
    """
    if conn is None:
        return

    # Normalize to match DB constraint: ('BUY','SELL')
    side_norm = (side or "").strip().upper()
    if side_norm not in ("BUY", "SELL"):
        logger.warning(f"public.TRADE_JOURNAL_BAD_SIDE: {side!r}")
        return

    ct_tag = ct_now_str(datetime.now(timezone.utc))
    note_tagged = f"[{ct_tag}] {note}".strip()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.trade_journal (
                  symbol, side, qty, est_price, order_id, client_order_id,
                  is_dry_run, is_leader,
                  group_id, anchor_price, last_buy_price, buys_in_group,
                  note
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                (
                    symbol,
                    side_norm,
                    int(qty),
                    float(est_price) if est_price is not None else None,
                    order_id,
                    client_order_id,
                    bool(is_dry_run),
                    bool(is_leader),
                    group_id,
                    float(gs.anchor_price) if gs.anchor_price is not None else None,
                    float(gs.last_buy_price) if gs.last_buy_price is not None else None,
                    int(gs.buy_count_in_group),
                    note_tagged,
                ),
            )
        try:
            conn.commit()
        except Exception:
            pass
    except Exception as e:
        logger.warning(f"public.TRADE_JOURNAL_WRITE_FAILED: {e}")


def heartbeat_banner(
    *,
    cfg,
    live_endpoint: bool,
    is_leader: bool,
    market_is_open: bool,
    price: Optional[float],
    pos_qty: int,
    avg_entry: Optional[float],
    market_value: Optional[float],
    unrealized_pl: Optional[float],
    gs: GridState,
) -> None:
    step_now = current_step_usd(
        step_start=cfg.grid_step_start_usd,
        step_increment=cfg.grid_step_increment_usd,
        tier_size=cfg.grid_tier_size,
        buy_count_in_group=gs.buy_count_in_group,
    )
    sell_target = (float(gs.anchor_price) + float(cfg.sell_rise_usd)) if gs.anchor_price is not None else None

    logger.warning("")
    logger.warning("💗 HEARTBEAT")
    logger.warning(f"TIME_CT:    {ct_now_str(datetime.now(timezone.utc))}")
    logger.warning("----------------------------------------------")
    logger.warning(f"MODE:        {'LIVE' if (not cfg.dry_run and live_endpoint) else 'PAPER/DRY'}")
    logger.warning(f"SYMBOL:      {cfg.symbol}")
    logger.warning(f"MKT_OPEN:    {market_is_open}")
    logger.warning(f"LEADER:      {is_leader}")
    logger.warning(f"KILL_SW:     {cfg.kill_switch}")
    logger.warning(f"LAST_PRICE:  {fmt_num(price)}")
    logger.warning(f"POS_QTY:     {pos_qty}")
    logger.warning(f"AVG_ENTRY:   {fmt_num(avg_entry)}")
    logger.warning(f"MKT_VALUE:   {fmt_money(market_value)}")
    logger.warning(f"UNRLD_P/L:   {fmt_money(unrealized_pl)}")
    logger.warning("----------------------------------------------")
    logger.warning("GRID STATE")
    logger.warning(f"ANCHOR:      {fmt_num(gs.anchor_price)}")
    logger.warning(f"LAST_BUY:    {fmt_num(gs.last_buy_price)}")
    logger.warning(f"BUYS_IN_GRP: {gs.buy_count_in_group}")
    logger.warning(
        f"STEP_NOW:    {fmt_num(step_now)}  (start={cfg.grid_step_start_usd} "
        f"inc={cfg.grid_step_increment_usd} tier={cfg.grid_tier_size})"
    )
    logger.warning(f"SELL_RISE:   {fmt_money(cfg.sell_rise_usd)}")
    logger.warning(f"SELL_TGT:    {fmt_num(sell_target)}")
    logger.warning("----------------------------------------------")
    logger.warning("")


def safe_get_clock(api: tradeapi.REST, retries: int = 3, sleep_sec: float = 1.0):
    last_err = None
    for _ in range(max(1, retries)):
        try:
            return api.get_clock()
        except Exception as e:
            last_err = e
            time.sleep(sleep_sec)
    raise last_err


# ----------------------------
# Main
# ----------------------------
def main():
    cfg = load_config()
    api = tradeapi.REST(cfg.key_id, cfg.secret_key, cfg.base_url)
    live_endpoint = is_live_endpoint(cfg.base_url)

    # Live-trading gate: only blocks real-money endpoint when DRY_RUN=false
    if (not cfg.dry_run) and live_endpoint:
        if cfg.live_trading_confirm != "I_UNDERSTAND":
            raise RuntimeError(
                "LIVE trading blocked: set LIVE_TRADING_CONFIRM=I_UNDERSTAND to enable live orders."
            )

    # Timing knobs (with safe fallbacks)
    poll_sec = float(getattr(cfg, "poll_sec", 5) or 5)
    standby_poll_sec = float(getattr(cfg, "standby_poll_sec", 10) or 10)
    market_closed_sleep_sec = float(getattr(cfg, "market_closed_sleep_sec", 30) or 30)

    # HEARTBEAT frequency (seconds)
    heartbeat_sec = float(getattr(cfg, "heartbeat_sec", 300) or 300)
    last_heartbeat_ts = 0.0

    # --- Leader lock + state ---
    conn = None
    state_id = f"{cfg.symbol}_state"
    is_leader = True
    prev_leader = is_leader  # track flips

    if db_enabled(cfg.database_url):
        conn = db_connect(cfg.database_url)
        db_init(conn)

        if cfg.standby_only:
            is_leader = False
            logger.info("STANDBY_ONLY=true -> STANDBY mode (no leader lock attempt)")
        else:
            is_leader = try_acquire_leader_lock(conn, cfg.leader_lock_key)
            logger.info("LEADER_LOCK acquired -> ACTIVE" if is_leader else "LEADER_LOCK not acquired -> STANDBY")

        prev_leader = is_leader
    else:
        logger.warning("DATABASE_URL not set -> using DISK state (single instance only)")

    # Load state
    state = load_state_db(conn, state_id) if conn else load_state_disk(cfg.state_path)

    # Group id (persisted) — ensure it exists so journals can tie trades together
    group_id = state.get("group_id")
    if not group_id:
        group_id = str(uuid.uuid4())
        state["group_id"] = group_id

    # Persisted counters
    buys_today = int(state.get("buys_today_et", 0) or 0)
    buys_today_date = state.get("buys_today_date_et")
    buy_count_total = int(state.get("buy_count_total", 0) or 0)

    # Roll over buys_today if date changed (ET)
    today_key = et_date_str(datetime.now(timezone.utc))
    if buys_today_date != today_key:
        buys_today_date = today_key
        buys_today = 0
        state["buys_today_date_et"] = buys_today_date
        state["buys_today_et"] = buys_today
        if conn:
            save_state_db(conn, state_id, state)
        else:
            save_state_disk(cfg.state_path, state)

    # Grid state
    gs = GridState(
        anchor_price=state.get("grid_anchor_price"),
        last_buy_price=state.get("grid_last_buy_price") or state.get("grid_last_trigger"),  # legacy fallback
        buy_count_in_group=int(state.get("grid_buy_count_in_group", 0) or state.get("grid_tier_buys_used", 0) or 0),
    )

    logger.warning("")
    logger.warning("==============================================")
    logger.warning("🚀 ENGINE START")
    logger.warning("----------------------------------------------")
    logger.warning(f"TIME_CT:       {ct_now_str(datetime.now(timezone.utc))}")
    logger.warning(f"SYMBOL:        {cfg.symbol}")
    logger.warning(f"DRY_RUN:       {cfg.dry_run}")
    logger.warning(f"KILL_SWITCH:   {cfg.kill_switch}")
    logger.warning(f"ENDPOINT:      {cfg.base_url} ({'LIVE' if live_endpoint else 'PAPER'})")
    logger.warning(f"LEADER_START:  {is_leader}")
    logger.warning(
        f"GRID:          start={cfg.grid_step_start_usd} inc={cfg.grid_step_increment_usd} "
        f"tier={cfg.grid_tier_size} sell_rise={cfg.sell_rise_usd}"
    )
    logger.warning(f"HEARTBEAT_SEC: {heartbeat_sec}")
    logger.warning("==============================================")
    logger.warning("")

    while True:
        try:
            # Re-acquire leader if standby (and not forced standby_only)
            if conn and (not cfg.standby_only):
                if not is_leader:
                    is_leader = try_acquire_leader_lock(conn, cfg.leader_lock_key)

                if is_leader != prev_leader:
                    logger.warning(f"LEADER_CHANGED: {prev_leader} -> {is_leader}")
                    prev_leader = is_leader

                if not is_leader:
                    time.sleep(standby_poll_sec)
                    continue

            # Clock / market
            clock = safe_get_clock(api, retries=3, sleep_sec=1.0)
            market_is_open = bool(clock.is_open)

            now_utc = clock.timestamp
            if now_utc.tzinfo is None:
                now_utc = now_utc.replace(tzinfo=timezone.utc)

            # Day rollover (ET) — reset per-day buy counter
            today_key = et_date_str(now_utc)
            if buys_today_date != today_key:
                buys_today_date = today_key
                buys_today = 0

            # Price
            price = get_last_price(api, cfg.symbol)
            if price is None:
                time.sleep(poll_sec)
                continue

            # Position details
            pos_qty, avg_entry, market_value, unrealized_pl, pos_ok = get_position_details(api, cfg.symbol)
            if not pos_ok:
                logger.warning("POSITION_UNAVAILABLE: skipping tick (won't reset group_id)")
                time.sleep(poll_sec)
                continue

            # If Alpaca is flat, grid memory should be empty
            if pos_qty <= 0 and (
                gs.anchor_price is not None or gs.last_buy_price is not None or gs.buy_count_in_group > 0
            ):
                reset_group(gs)
                group_id = str(uuid.uuid4())
                state["group_id"] = group_id

            # HEARTBEAT banner
            now_ts = time.time()
            if (now_ts - last_heartbeat_ts) >= heartbeat_sec:
                heartbeat_banner(
                    cfg=cfg,
                    live_endpoint=live_endpoint,
                    is_leader=is_leader,
                    market_is_open=market_is_open,
                    price=price,
                    pos_qty=pos_qty,
                    avg_entry=avg_entry,
                    market_value=market_value,
                    unrealized_pl=unrealized_pl,
                    gs=gs,
                )
                last_heartbeat_ts = now_ts

            # If market closed, just idle
            if not market_is_open:
                logger.info("MARKET_CLOSED waiting...")
                time.sleep(market_closed_sleep_sec)
                continue

            # --- SELL logic ---
            if pos_qty > 0 and should_sell_now(price=price, gs=gs, sell_rise_usd=cfg.sell_rise_usd):
                # Belt-and-suspenders: only meaningful when DB/leader mode is enabled
                if (not cfg.dry_run) and (conn is not None) and (not is_leader):
                    logger.warning("STANDBY_BLOCK: skipping SELL (no leader lock)")
                else:
                    sell_qty = int(pos_qty)
                    client_order_id = f"grid-sell-{cfg.symbol}-{uuid.uuid4().hex[:10]}"

                    logger.warning(
                        f"SELL_SIGNAL price={price:.2f} anchor={gs.anchor_price} qty={sell_qty} "
                        f"client_order_id={client_order_id}"
                    )

                    if cfg.dry_run:
                        journal_trade(
                            conn=conn,
                            symbol=cfg.symbol,
                            side="SELL",
                            qty=sell_qty,
                            est_price=price,
                            is_dry_run=True,
                            is_leader=is_leader,
                            group_id=group_id,
                            gs=gs,
                            order_id=None,
                            client_order_id=client_order_id,
                            note="DRY_RUN sell",
                        )
                        reset_group(gs)
                        pos_qty = 0
                    else:
                        order = submit_market_sell(api, cfg.symbol, sell_qty, client_order_id=client_order_id)
                        journal_trade(
                            conn=conn,
                            symbol=cfg.symbol,
                            side="SELL",
                            qty=sell_qty,
                            est_price=price,
                            is_dry_run=False,
                            is_leader=is_leader,
                            group_id=group_id,
                            gs=gs,
                            order_id=getattr(order, "id", None),
                            client_order_id=client_order_id,
                            note="ORDER_SUBMITTED sell",
                        )
                        reset_group(gs)
                        pos_qty = 0

                    # New group after a full exit
                    group_id = str(uuid.uuid4())
                    state["group_id"] = group_id

            # --- BUY logic ---
            buys_this_tick = 0

            while (not cfg.kill_switch) and buys_this_tick < cfg.max_buys_per_tick:
                step_now_dbg = current_step_usd(
                    step_start=cfg.grid_step_start_usd,
                    step_increment=cfg.grid_step_increment_usd,
                    tier_size=cfg.grid_tier_size,
                    buy_count_in_group=gs.buy_count_in_group,
                )
                next_buy_dbg = (gs.last_buy_price - step_now_dbg) if gs.last_buy_price is not None else None
                logger.info(
                    f"BUY_CHECK price={price:.2f} last_buy={gs.last_buy_price} step={step_now_dbg:.2f} next_buy={next_buy_dbg}"
                )

                # Grid gate
                if not should_buy_now(
                    price=price,
                    gs=gs,
                    step_start=cfg.grid_step_start_usd,
                    step_increment=cfg.grid_step_increment_usd,
                    tier_size=cfg.grid_tier_size,
                ):
                    break

                # Risk gate
                decision = check_buy_allowed(
                    kill_switch=cfg.kill_switch,
                    buys_this_tick=buys_this_tick,
                    max_buys_per_tick=cfg.max_buys_per_tick,
                    buys_today=buys_today,
                    max_buys_per_day=cfg.max_buys_per_day,
                    order_qty=cfg.order_qty,
                    est_price=price,
                    max_dollars_per_buy=cfg.max_dollars_per_buy,
                    current_pos_qty=max(0, int(pos_qty)),
                    max_position_qty=cfg.max_position_qty,
                    now_utc=now_utc,
                    trade_start_et=cfg.trade_start_et,
                    trade_end_et=cfg.trade_end_et,
                )

                if not decision.ok:
                    logger.info(f"BUY_BLOCKED reason={decision.reason}")

                    journal_trade(
                        conn=conn,
                        symbol=cfg.symbol,
                        side="BUY",
                        qty=int(cfg.order_qty),
                        est_price=price,
                        is_dry_run=bool(cfg.dry_run),
                        is_leader=bool(is_leader),
                        group_id=group_id,
                        gs=gs,
                        order_id=None,
                        client_order_id=None,
                        note=f"BUY_BLOCKED: {decision.reason}",
                    )
                    break

                # Standby protection (only relevant when DB/leader mode is enabled)
                if (not cfg.dry_run) and (conn is not None) and (not is_leader):
                    logger.warning("STANDBY_BLOCK: skipping BUY (no leader lock)")
                    break

                step_now = current_step_usd(
                    step_start=cfg.grid_step_start_usd,
                    step_increment=cfg.grid_step_increment_usd,
                    tier_size=cfg.grid_tier_size,
                    buy_count_in_group=gs.buy_count_in_group,
                )

                buy_count_total += 1
                buys_today += 1
                buys_this_tick += 1

                client_order_id = f"grid-buy-{cfg.symbol}-{uuid.uuid4().hex[:10]}"

                logger.info(
                    f"GRID_BUY #{buy_count_total} price={price:.2f} qty={cfg.order_qty} "
                    f"step_now={step_now:.2f} buys_in_group={gs.buy_count_in_group} "
                    f"client_order_id={client_order_id}"
                )

                if cfg.dry_run:
                    journal_trade(
                        conn=conn,
                        symbol=cfg.symbol,
                        side="BUY",
                        qty=int(cfg.order_qty),
                        est_price=price,
                        is_dry_run=True,
                        is_leader=is_leader,
                        group_id=group_id,
                        gs=gs,
                        order_id=None,
                        client_order_id=client_order_id,
                        note="DRY_RUN buy",
                    )
                    on_buy_filled(
                        fill_price=price,
                        gs=gs,
                        step_start=cfg.grid_step_start_usd,
                        step_increment=cfg.grid_step_increment_usd,
                        tier_size=cfg.grid_tier_size,
                    )
                    
                else:
                    order = submit_market_buy(api, cfg.symbol, cfg.order_qty, client_order_id=client_order_id)
                    journal_trade(
                        conn=conn,
                        symbol=cfg.symbol,
                        side="BUY",
                        qty=int(cfg.order_qty),
                        est_price=price,
                        is_dry_run=False,
                        is_leader=is_leader,
                        group_id=group_id,
                        gs=gs,
                        order_id=getattr(order, "id", None),
                        client_order_id=client_order_id,
                        note="ORDER_SUBMITTED buy",
                    )
                    on_buy_filled(
                        fill_price=price,
                        gs=gs,
                        step_start=cfg.grid_step_start_usd,
                        step_increment=cfg.grid_step_increment_usd,
                        tier_size=cfg.grid_tier_size,
                    )

            # --- Persist state ---
            state["buy_count_total"] = int(buy_count_total)
            state["buys_today_et"] = int(buys_today)
            state["buys_today_date_et"] = buys_today_date
            state["group_id"] = group_id

            # Grid persistence keys
            state["grid_anchor_price"] = gs.anchor_price
            state["grid_last_buy_price"] = gs.last_buy_price
            state["grid_buy_count_in_group"] = int(gs.buy_count_in_group)

            # Clear old keys (keeps state clean)
            state.pop("grid_in_group", None)
            state.pop("grid_last_trigger", None)
            state.pop("grid_next_trigger", None)
            state.pop("grid_step", None)
            state.pop("grid_tier_buys_used", None)

            if conn:
                save_state_db(conn, state_id, state)
            else:
                save_state_disk(cfg.state_path, state)

            time.sleep(poll_sec)

        except Exception as e:
            logger.error(f"ENGINE_ERROR {e}", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
