# engine/engine.py
import time
from datetime import datetime, timezone
from typing import Optional, Tuple

import alpaca_trade_api as tradeapi

from logging_ct import build_logger
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

logger = build_logger("engine")


# ----------------------------
# Helpers
# ----------------------------
def is_live_endpoint(url: str) -> bool:
    u = (url or "").lower()
    if "paper-api" in u:
        return False
    return "api.alpaca.markets" in u


def get_position_qty(api: tradeapi.REST, symbol: str) -> int:
    try:
        pos = api.get_position(symbol)
        return int(float(pos.qty))
    except Exception as e:
        if "position does not exist" in str(e).lower():
            return 0
        raise


def get_position_details(api: tradeapi.REST, symbol: str) -> Tuple[int, Optional[float], Optional[float], Optional[float]]:
    """
    Returns: (qty, avg_entry, market_value, unrealized_pl)
    All optional floats may be None if flat or unavailable.
    """
    try:
        pos = api.get_position(symbol)
        qty = int(float(pos.qty))
        avg_entry = float(getattr(pos, "avg_entry_price", None)) if getattr(pos, "avg_entry_price", None) is not None else None
        mv = float(getattr(pos, "market_value", None)) if getattr(pos, "market_value", None) is not None else None
        upl = float(getattr(pos, "unrealized_pl", None)) if getattr(pos, "unrealized_pl", None) is not None else None
        return qty, avg_entry, mv, upl
    except Exception as e:
        if "position does not exist" in str(e).lower():
            return 0, None, None, None
        return 0, None, None, None


def submit_market_buy(api: tradeapi.REST, symbol: str, qty: int):
    return api.submit_order(symbol=symbol, qty=qty, side="buy", type="market", time_in_force="day")


def submit_market_sell(api: tradeapi.REST, symbol: str, qty: int):
    return api.submit_order(symbol=symbol, qty=qty, side="sell", type="market", time_in_force="day")


def get_last_price(api: tradeapi.REST, symbol: str) -> Optional[float]:
    """
    Uses latest trade; generally works with IEX for equities during market hours.
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


def et_date_str(now_utc: datetime) -> str:
    """
    ET rollover key for "buys_today".
    """
    try:
        from zoneinfo import ZoneInfo  # py3.9+
        et = now_utc.astimezone(ZoneInfo("America/New_York"))
        return et.date().isoformat()
    except Exception:
        # fallback: stable, but not true ET
        return now_utc.date().isoformat()


def fmt_money(x: Optional[float]) -> str:
    if x is None:
        return "None"
    return f"{x:,.2f}"


def fmt_num(x: Optional[float]) -> str:
    if x is None:
        return "None"
    return f"{x:.2f}"


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
    logger.warning(f"STEP_NOW:    {fmt_num(step_now)}  (start={cfg.grid_step_start_usd} inc={cfg.grid_step_increment_usd} tier={cfg.grid_tier_size})")
    logger.warning(f"SELL_RISE:   {fmt_money(cfg.sell_rise_usd)}")
    logger.warning(f"SELL_TGT:    {fmt_num(sell_target)}")
    logger.warning("----------------------------------------------")
    logger.warning("")


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
            raise RuntimeError("LIVE trading blocked: set LIVE_TRADING_CONFIRM=I_UNDERSTAND to enable live orders.")

    # HEARTBEAT frequency (minutes -> seconds via env already; fallback if config doesn't have it)
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

    state = load_state_db(conn, state_id) if conn else load_state_disk(cfg.state_path)

    # persisted counters
    buys_today = int(state.get("buys_today_et", 0) or 0)
    buys_today_date = state.get("buys_today_date_et")
    buy_count_total = int(state.get("buy_count_total", 0) or 0)

    # roll over buys_today if date changed (ET)
    today_key = et_date_str(datetime.now(timezone.utc))
    if buys_today_date != today_key:
        buys_today_date = today_key
        buys_today = 0
        state["buys_today_date_et"] = buys_today_date
        state["buys_today_et"] = buys_today

    # grid state (simple GridState)
    gs = GridState(
        anchor_price=state.get("grid_anchor_price"),
        last_buy_price=state.get("grid_last_buy_price") or state.get("grid_last_trigger"),  # legacy fallback
        buy_count_in_group=int(state.get("grid_buy_count_in_group", 0) or state.get("grid_tier_buys_used", 0) or 0),
    )

    logger.warning("")
    logger.warning("==============================================")
    logger.warning("🚀 ENGINE START")
    logger.warning("----------------------------------------------")
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
            # re-acquire leader if standby (and not forced standby_only)
            if conn and (not cfg.standby_only):
                # attempt only if not leader
                if not is_leader:
                    is_leader = try_acquire_leader_lock(conn, cfg.leader_lock_key)

                # log only when it flips
                if is_leader != prev_leader:
                    logger.warning(f"LEADER_CHANGED: {prev_leader} -> {is_leader}")
                    prev_leader = is_leader

                # if still not leader, idle longer and skip loop work
                if not is_leader:
                    time.sleep(cfg.standby_poll_sec)
                    continue

            # clock / market
            clock = api.get_clock()
            market_is_open = bool(clock.is_open)

            now_utc = clock.timestamp
            if now_utc.tzinfo is None:
                now_utc = now_utc.replace(tzinfo=timezone.utc)

            # day rollover (ET)
            today_key = et_date_str(now_utc)
            if buys_today_date != today_key:
                buys_today_date = today_key
                buys_today = 0

            # price
            price = get_last_price(api, cfg.symbol)
            if price is None:
                time.sleep(cfg.poll_sec)
                continue

            # position details
            pos_qty, avg_entry, market_value, unrealized_pl = get_position_details(api, cfg.symbol)

            # If Alpaca is flat, grid memory should be empty
            if pos_qty <= 0 and (gs.anchor_price is not None or gs.last_buy_price is not None or gs.buy_count_in_group > 0):
                reset_group(gs)

            # HEARTBEAT banner (prints every X seconds)
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

            # If market closed, just idle (no trades)
            if not market_is_open:
                logger.info("MARKET_CLOSED waiting...")
                time.sleep(30)
                continue

            # --- SELL logic ---
            if pos_qty > 0 and should_sell_now(price=price, gs=gs, sell_rise_usd=cfg.sell_rise_usd):
                if (not cfg.dry_run) and (conn is not None) and (not is_leader):
                    logger.warning("STANDBY_BLOCK: skipping SELL (no leader lock)")
                else:
                    sell_qty = int(pos_qty)
                    logger.warning(f"SELL_SIGNAL price={price:.2f} anchor={gs.anchor_price} qty={sell_qty}")

                    if cfg.dry_run:
                        reset_group(gs)
                        pos_qty = 0
                    else:
                        submit_market_sell(api, cfg.symbol, sell_qty)
                        # reset immediately to prevent any accidental re-sell behavior if Alpaca lags
                        reset_group(gs)
                        pos_qty = 0

            # --- BUY logic ---
            buys_this_tick = 0

            while (not cfg.kill_switch) and buys_this_tick < cfg.max_buys_per_tick:
                # grid gate
                if not should_buy_now(
                    price=price,
                    gs=gs,
                    step_start=cfg.grid_step_start_usd,
                    step_increment=cfg.grid_step_increment_usd,
                    tier_size=cfg.grid_tier_size,
                ):
                    break

                # risk gate
                decision = check_buy_allowed(
                    kill_switch=cfg.kill_switch,
                    now_utc=now_utc,
                    trade_start_et=cfg.trade_start_et,
                    trade_end_et=cfg.trade_end_et,
                    buys_today=buys_today,
                    max_buys_per_day=cfg.max_buys_per_day,
                    current_pos_qty=max(0, int(pos_qty)),
                    order_qty=cfg.order_qty,
                    max_position_qty=cfg.max_position_qty,
                    est_price=price,
                    max_dollars_per_buy=cfg.max_dollars_per_buy,
                )
                if not decision.ok:
                    logger.info(f"BUY_BLOCKED reason={decision.reason}")
                    break

                # standby protection (belt-and-suspenders)
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

                logger.info(
                    f"GRID_BUY #{buy_count_total} price={price:.2f} qty={cfg.order_qty} "
                    f"step_now={step_now:.2f} buys_in_group={gs.buy_count_in_group}"
                )

                if cfg.dry_run:
                    on_buy_filled(fill_price=price, gs=gs)
                    pos_qty = int(pos_qty) + int(cfg.order_qty)
                else:
                    submit_market_buy(api, cfg.symbol, cfg.order_qty)
                    # optimistic fill at current price (simple); can be upgraded to actual fill price later
                    on_buy_filled(fill_price=price, gs=gs)
                    pos_qty = int(pos_qty) + int(cfg.order_qty)

            # --- persist state ---
            state["buy_count_total"] = int(buy_count_total)
            state["buys_today_et"] = int(buys_today)
            state["buys_today_date_et"] = buys_today_date

            # NEW grid persistence keys
            state["grid_anchor_price"] = gs.anchor_price
            state["grid_last_buy_price"] = gs.last_buy_price
            state["grid_buy_count_in_group"] = int(gs.buy_count_in_group)

            # clear old keys (keeps state clean)
            state.pop("grid_in_group", None)
            state.pop("grid_last_trigger", None)
            state.pop("grid_next_trigger", None)
            state.pop("grid_step", None)
            state.pop("grid_tier_buys_used", None)

            if conn:
                save_state_db(conn, state_id, state)
            else:
                save_state_disk(cfg.state_path, state)

            time.sleep(cfg.poll_sec)

        except Exception as e:
            logger.error(f"ENGINE_ERROR {e}", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
