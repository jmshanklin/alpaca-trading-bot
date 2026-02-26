# engine/grid.py
from dataclasses import dataclass
from typing import Optional


@dataclass
class GridState:
    """
    Grid state is split into:
      - anchor_price: first BUY fill of the current group (used for SELL target)
      - last_buy_price: last actual BUY fill price from Alpaca (display/analytics)
      - last_trigger_price: last grid rung reference (the deterministic ladder)
      - buy_count_in_group: number of completed buys in this group (1..N)
    """
    anchor_price: Optional[float] = None
    last_buy_price: Optional[float] = None
    last_trigger_price: Optional[float] = None
    buy_count_in_group: int = 0


def current_step_usd(
    *,
    step_start: float,
    step_increment: float,
    tier_size: int,
    buy_count_in_group: int,
) -> float:
    """
    Tiered step size (USD):
      buys 1..tier_size             => step_start
      buys tier_size+1..2*tier_size => step_start + step_increment
      buys 2*tier_size+1..3*tier_size => step_start + 2*step_increment
      etc.

    IMPORTANT:
      - buy_count_in_group is the count of COMPLETED buys so far.
      - The step used to *decide the next buy* should be based on the current count.
    """
    tier_size_i = int(tier_size) if int(tier_size) > 0 else 5
    count = max(0, int(buy_count_in_group))
    tier_index = count // tier_size_i
    return float(step_start) + float(step_increment) * float(tier_index)


def next_trigger_price(
    *,
    gs: GridState,
    step_start: float,
    step_increment: float,
    tier_size: int,
) -> Optional[float]:
    """
    Deterministic next rung price based on the last_trigger_price.

    If last_trigger_price is None (no buys yet), returns None.
    """
    if gs.last_trigger_price is None:
        return None

    step = current_step_usd(
        step_start=step_start,
        step_increment=step_increment,
        tier_size=tier_size,
        buy_count_in_group=gs.buy_count_in_group,
    )
    return float(gs.last_trigger_price) - float(step)


def should_buy_now(
    *,
    price: float,
    gs: GridState,
    step_start: float,
    step_increment: float,
    tier_size: int,
) -> bool:
    """
    Gate for BUY:
      - First buy is always allowed (risk/time window handled elsewhere).
      - Otherwise price must be <= (last_trigger_price - current_step).

    This makes the ladder "aim at predetermined rungs", independent of the
    prior *fill* price, while still naturally handling gap-downs.
    """
    p = float(price)

    # First buy: allow
    if gs.last_trigger_price is None:
        return True

    nxt = next_trigger_price(
        gs=gs,
        step_start=step_start,
        step_increment=step_increment,
        tier_size=tier_size,
    )
    if nxt is None:
        return True

    return p <= float(nxt)


def on_buy_filled(
    *,
    fill_price: float,
    gs: GridState,
    step_start: float,
    step_increment: float,
    tier_size: int,
) -> None:
    """
    Update state after a BUY is considered "filled" (or after submit in your current engine).

    Behavior:
      - First buy sets anchor_price and last_trigger_price to the fill price.
      - Subsequent buys advance last_trigger_price by EXACTLY ONE rung:
          last_trigger_price = last_trigger_price - current_step
        where current_step is based on buy_count_in_group BEFORE increment.

    Also records last_buy_price as the actual fill (or est) price for display.
    """
    fp = float(fill_price)

    if gs.anchor_price is None:
        gs.anchor_price = fp

    if gs.last_trigger_price is None:
        # First rung reference
        gs.last_trigger_price = fp
    else:
        # Advance exactly one rung deterministically
        step = current_step_usd(
            step_start=step_start,
            step_increment=step_increment,
            tier_size=tier_size,
            buy_count_in_group=gs.buy_count_in_group,
        )
        gs.last_trigger_price = float(gs.last_trigger_price) - float(step)

    gs.last_buy_price = fp
    gs.buy_count_in_group = int(gs.buy_count_in_group) + 1


def should_sell_now(*, price: float, gs: GridState, sell_rise_usd: float) -> bool:
    """
    SELL rule:
      price >= anchor_price + sell_rise_usd
    """
    if gs.anchor_price is None:
        return False
    return float(price) >= float(gs.anchor_price) + float(sell_rise_usd)


def reset_group(gs: GridState) -> None:
    """
    Reset grid state after a full exit.
    """
    gs.anchor_price = None
    gs.last_buy_price = None
    gs.last_trigger_price = None
    gs.buy_count_in_group = 0
