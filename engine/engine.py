import time
from datetime import datetime, timezone
from typing import Optional

import alpaca_trade_api as tradeapi

from logging_ct import build_logger
from config import load_config
from risk import check_buy_allowed
from grid import GridParams, GridState, reset_grid, start_new_group, should_sell, maybe_advance_after_buy, buy_loop_needed
from state import (
    db_enabled, db_connect, db_init, try_acquire_leader_lock,
    load_state_db, save_state_db, load_state_disk, save_state_disk
)

logger = build_logger("engine")

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

def submit_market_buy(api: tradeapi.REST, symbol: str, qty: int):
    return api.submit_order(symbol=symbol, qty=qty, side="buy", type="market", time_in_force="day")

def submit_market_sell(api: tradeapi.REST, symbol: str, qty: int):
    return api.submit_order(symbol=symbol, qty=qty, side="sell", type="market", time_in_force="day")

def get_last_price(api: tradeapi.REST, symbol: str) -> Optional[float]:
    # Uses latest trade; generally works with IEX for equities during market hours.
    try:
        t = api.get_latest_trade(symbol)
        return float(getattr(t, "price", None))
    except Exception:
        return None

def main():
    cfg = load_config()
    api = tradeapi.REST(cfg.apca_key_id, cfg.apca_secret_key, cfg.apca_base_url)

    live_endpoint = is_live_endpoint(cfg.apca_base_url)

    # Live-trading gate: only blocks real-money endpoint when DRY_RUN=false
    if (not cfg.dry_run) and live_endpoint:
        if cfg.live_trading_confirm != "I_UNDERSTAND":
            raise RuntimeError("LIVE trading blocked: set LIVE_TRADING_CONFIRM=I_UNDERSTAND to enable live orders.")

    # --- Leader lock + state ---
    conn = None
    state_id = f"{cfg.symbol}_state"
    is_leader = True

    if db_enabled(cfg.database_url):
        conn = db_connect(cfg.database_url)
        db_init(conn)

        if cfg.standby_only:
            is_leader = False
            logger.info("STANDBY_ONLY=true -> STANDBY mode (no leader lock attempt)")
        else:
            is_leader = try_acquire_leader_lock(conn, cfg.leader_lock_key)
            logger.info("LEADER_LOCK acquired -> ACTIVE" if is_leader else "LEADER_LOCK not acquired -> STANDBY")
    else:
        logger.warning("DATABASE_URL not set -> using DISK state (single instance only)")

    state = load_state_db(conn, state_id) if conn else load_state_disk(cfg.state_path)

    # persisted counters
    buys_today = int(state.get("buys_today_et", 0))
    buys_today_date = state.get("buys_today_date_et")
    buy_count_total = int(state.get("buy_count_total", 0))

    # grid state
    gs = GridState(
        in_group=bool(state.get("grid_in_group", False)),
        anchor_price=state.get("grid_anchor_price"),
        last_trigger=state.get("grid_last_trigger"),
        next_trigger=state.get("grid_next_trigger"),
        step=float(state.get("grid_step", 0.0) or 0.0),
        tier_buys_used=int(state.get("grid_tier_buys_used", 0) or 0),
    )
    params = GridParams(
        step_start=cfg.grid_step_start_usd,
        step_increment=cfg.grid_step_increment_usd,
        tier_size=cfg.grid_tier_size,
        sell_rise=cfg.sell_rise_usd,
    )

    logger.warning("")
    logger.warning("==============================================")
    logger.warning("🚀 ENGINE START")
    logger.warning("----------------------------------------------")
    logger.warning(f"SYMBOL:      {cfg.symbol}")
    logger.warning(f"DRY_RUN:     {cfg.dry_run}")
    logger.warning(f"KILL_SWITCH: {cfg.kill_switch}")
    logger.warning(f"ENDPOINT:    {cfg.apca_base_url} ({'LIVE' if live_endpoint else 'PAPER'})")
    logger.warning(f"LEADER:      {is_leader}")
    logger.warning(f"GRID:        start={params.step_start} inc={params.step_increment} tier={params.tier_size} sell_rise={params.sell_rise}")
    logger.warning("==============================================")
    logger.warning("")

    while True:
        try:
            # re-acquire leader if standby
            if conn and (not is_leader) and (not cfg.standby_only):
                is_leader = try_acquire_leader_lock(conn, cfg.leader_lock_key)
                if not is_leader:
                    time.sleep(cfg.standby_poll_sec)
                    continue
                logger.info("LEADER_LOCK acquired -> ACTIVE")

            clock = api.get_clock()
            market_is_open = bool(clock.is_open)

            now_utc = clock.timestamp
            if now_utc.tzinfo is None:
                now_utc = now_utc.replace(tzinfo=timezone.utc)

            # ET day rollover (simple: compare YYYY-MM-DD string)
            today_et = now_utc.astimezone(timezone.utc).astimezone(
                timezone.utc
            )  # placeholder; day string not critical for safety if you prefer to keep it simple

            # price
            price = get_last_price(api, cfg.symbol)
            if price is None:
                time.sleep(cfg.poll_sec)
                continue

            pos_qty = get_position_qty(api, cfg.symbol)

            # If flat, grid should not be “in group”
            if pos_qty <= 0 and gs.in_group:
                reset_grid(gs)

            # If market closed, just idle (no trades)
            if not market_is_open:
                time.sleep(5)
                continue

            # --- SELL logic ---
            if pos_qty > 0 and should_sell(gs, params, price):
                if not cfg.dry_run and (conn is not None) and (not is_leader):
                    logger.warning("STANDBY_BLOCK: skipping SELL (no leader lock)")
                else:
                    sell_qty = int(pos_qty)
                    logger.warning(f"SELL_SIGNAL price={price:.2f} anchor={gs.anchor_price} qty={sell_qty}")

                    if cfg.dry_run:
                        # simulate flat
                        reset_grid(gs)
                    else:
                        submit_market_sell(api, cfg.symbol, sell_qty)
                        # assume sell succeeded; next loop will observe flat and reset

            # --- BUY logic ---
            # Start a new group only when flat and kill switch is off (buy side)
            if pos_qty <= 0 and (not gs.in_group) and (not cfg.kill_switch):
                if not cfg.dry_run and (conn is not None) and (not is_leader):
                    pass
                else:
                    # First buy = start group at current price
                    decision = check_buy_allowed(
                        kill_switch=cfg.kill_switch,
                        now_utc=now_utc,
                        trade_start_et=cfg.trade_start_et,
                        trade_end_et=cfg.trade_end_et,
                        buys_today=buys_today,
                        max_buys_per_day=cfg.max_buys_per_day,
                        current_pos_qty=pos_qty,
                        order_qty=cfg.order_qty,
                        max_position_qty=cfg.max_position_qty,
                        est_price=price,
                        max_dollars_per_buy=cfg.max_dollars_per_buy,
                    )

                    if decision.ok:
                        buy_count_total += 1
                        buys_today += 1
                        logger.info(f"FIRST_BUY price={price:.2f} qty={cfg.order_qty}")

                        if cfg.dry_run:
                            start_new_group(gs, params, first_buy_price=price)
                        else:
                            submit_market_buy(api, cfg.symbol, cfg.order_qty)
                            start_new_group(gs, params, first_buy_price=price)

            # Additional ladder buys (can happen multiple times in one tick, capped)
            buys_this_tick = 0
            while gs.in_group and buy_loop_needed(gs, price) and buys_this_tick < cfg.max_buys_per_tick:
                decision = check_buy_allowed(
                    kill_switch=cfg.kill_switch,
                    now_utc=now_utc,
                    trade_start_et=cfg.trade_start_et,
                    trade_end_et=cfg.trade_end_et,
                    buys_today=buys_today,
                    max_buys_per_day=cfg.max_buys_per_day,
                    current_pos_qty=max(0, pos_qty),
                    order_qty=cfg.order_qty,
                    max_position_qty=cfg.max_position_qty,
                    est_price=price,
                    max_dollars_per_buy=cfg.max_dollars_per_buy,
                )

                if not decision.ok:
                    logger.info(f"BUY_BLOCKED reason={decision.reason}")
                    break

                if not cfg.dry_run and (conn is not None) and (not is_leader):
                    logger.warning("STANDBY_BLOCK: skipping BUY (no leader lock)")
                    break

                buy_count_total += 1
                buys_today += 1
                buys_this_tick += 1

                logger.info(
                    f"GRID_BUY #{buy_count_total} price={price:.2f} "
                    f"trigger={gs.next_trigger} step={gs.step} tier_used={gs.tier_buys_used}/{params.tier_size}"
                )

                if not cfg.dry_run:
                    submit_market_buy(api, cfg.symbol, cfg.order_qty)

                maybe_advance_after_buy(gs, params)

            # --- persist state ---
            state["buy_count_total"] = int(buy_count_total)
            state["buys_today_et"] = int(buys_today)
            state["buys_today_date_et"] = buys_today_date

            state["grid_in_group"] = bool(gs.in_group)
            state["grid_anchor_price"] = gs.anchor_price
            state["grid_last_trigger"] = gs.last_trigger
            state["grid_next_trigger"] = gs.next_trigger
            state["grid_step"] = float(gs.step)
            state["grid_tier_buys_used"] = int(gs.tier_buys_used)

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
