# engine/grid.py
from dataclasses import dataclass
from typing import Optional

@dataclass
class GridState:
    # anchor = first buy price of the current group
    anchor_price: Optional[float] = None

    # last executed buy fill (what Alpaca actually filled at)
    last_buy_price: Optional[float] = None

    # last grid rung/trigger reference (what the grid steps from)
    last_trigger_price: Optional[float] = None

    buy_count_in_group: int = 0

def current_step_usd(*, step_start: float, step_increment: float, tier_size: int, buy_count_in_group: int) -> float:
    """
    Tiered step:
      buys 1..tier_size      => step_start
      buys tier_size+1..2*tier_size => step_start + step_increment
      etc.
    """
    if tier_size <= 0:
        tier_size = 5
    tier_index = max(0, (max(0, buy_count_in_group) // tier_size))
    return float(step_start) + float(step_increment) * float(tier_index)

def should_buy_now(*, price: float, gs: GridState, step_start: float, step_increment: float, tier_size: int) -> bool:
    # First buy: always allowed (caller still applies risk + time window)
    if gs.last_trigger_price is None:
        return True

    step = current_step_usd(
        step_start=step_start,
        step_increment=step_increment,
        tier_size=tier_size,
        buy_count_in_group=gs.buy_count_in_group,
    )

    # Must be at least "step" BELOW the last trigger rung
    return float(price) <= float(gs.last_trigger_price) - float(step)

def on_buy_filled(
    *,
    fill_price: float,
    gs: GridState,
    step_start: float,
    step_increment: float,
    tier_size: int,
) -> None:
    fill_price = float(fill_price)

    # First buy sets the anchor and the first trigger rung
    if gs.anchor_price is None:
        gs.anchor_price = fill_price
        gs.last_trigger_price = fill_price
    else:
        # Advance the trigger rung by exactly one step (engine-matching)
        step = current_step_usd(
            step_start=step_start,
            step_increment=step_increment,
            tier_size=tier_size,
            buy_count_in_group=gs.buy_count_in_group,
        )
        gs.last_trigger_price = float(gs.last_trigger_price) - float(step)

    # Store actual fill separately (for display/P&L only)
    gs.last_buy_price = fill_price
    gs.buy_count_in_group += 1

def should_sell_now(*, price: float, gs: GridState, sell_rise_usd: float) -> bool:
    if gs.anchor_price is None:
        return False
    return float(price) >= float(gs.anchor_price) + float(sell_rise_usd)

def reset_group(gs: GridState) -> None:
    gs.anchor_price = None
    gs.last_buy_price = None
    gs.buy_count_in_group = 0
