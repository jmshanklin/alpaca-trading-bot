# engine/risk.py
from dataclasses import dataclass

@dataclass
class RiskDecision:
    ok: bool
    reason: str = "OK"

def check_buy_allowed(
    *,
    kill_switch: bool,
    buys_this_tick: int,
    max_buys_per_tick: int,
    buys_today: int,
    max_buys_per_day: int,
    order_qty: int,
    est_price: float,
    max_dollars_per_buy: float,
    current_pos_qty: int,
    max_position_qty: int,

    # Added to match engine call (currently not used by this function)
    now_utc=None,
    trade_start_et=None,
    trade_end_et=None,

    # Allows adding new fields in engine without crashing risk gate
    **kwargs,
) -> RiskDecision:

    if kill_switch:
        return RiskDecision(False, "KILL_SWITCH")

    if max_buys_per_tick > 0 and buys_this_tick >= max_buys_per_tick:
        return RiskDecision(False, "MAX_BUYS_PER_TICK")

    if max_buys_per_day > 0 and buys_today >= max_buys_per_day:
        return RiskDecision(False, "MAX_BUYS_PER_DAY")

    if max_position_qty > 0 and (current_pos_qty + order_qty) > max_position_qty:
        return RiskDecision(False, "MAX_POSITION_QTY")

    if max_dollars_per_buy > 0:
        est_cost = float(est_price) * int(order_qty)
        if est_cost > float(max_dollars_per_buy):
            return RiskDecision(False, "MAX_DOLLARS_PER_BUY")

    return RiskDecision(True, "OK")
