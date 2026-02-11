import os
from datetime import datetime, timedelta

import alpaca_trade_api as tradeapi
import pytz
from flask import Flask, Response, jsonify

app = Flask(__name__)

# --- Alpaca connection ---
API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")
BASE_URL = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version="v2")


# -------------------------
# Helpers
# -------------------------
def _get_attr(obj, name, default=None):
    """Works whether Alpaca returns an object or dict-like."""
    try:
        return getattr(obj, name)
    except Exception:
        pass
    try:
        return obj.get(name, default)
    except Exception:
        return default


def _parse_iso_time(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


# --- Timezone helper (UTC ➜ Central Time with AM/PM) ---
def to_central(ts):
    if not ts:
        return None
    try:
        utc = pytz.utc
        central = pytz.timezone("US/Central")

        # ensure timestamp is timezone-aware
        if ts.tzinfo is None:
            ts = utc.localize(ts)

        ct_time = ts.astimezone(central)
        return ct_time.strftime("%b %d, %Y %I:%M:%S %p CT")
    except Exception:
        return str(ts)


def fmt_ct_any(ts):
    """Accepts datetime OR ISO string."""
    if ts is None:
        return None
    if isinstance(ts, str):
        dt = _parse_iso_time(ts)
        return to_central(dt) if dt else ts
    return to_central(ts)


# --- Money formatting helper ---
def money(x):
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return str(x)


def money0(x):
    """Money formatter with 0 decimals (for BP numbers if desired)."""
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return str(x)


# -------------------------
# Aggregation + Cycles
# -------------------------
def aggregate_fills_by_order_id(activities, only_symbol="TSLA", only_side="buy"):
    """
    Turn Alpaca FILL activities into one row per order_id.
    Filters to TSLA buys by default.
    Returns rows sorted newest-first.
    """
    grouped = {}

    for act in activities:
        symbol = _get_attr(act, "symbol")
        side = _get_attr(act, "side")
        if only_symbol and symbol != only_symbol:
            continue
        if only_side and side != only_side:
            continue

        oid = _get_attr(act, "order_id")
        qty = float(_get_attr(act, "qty", 0) or 0)
        price = float(_get_attr(act, "price", 0) or 0)
        ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")

        if oid is None:
            oid = f"noid:{symbol}:{side}:{ts}:{price}:{qty}"

        g = grouped.setdefault(
            oid,
            {
                "order_id": oid,
                "time": ts,
                "side": side,
                "symbol": symbol,
                "filled_qty": 0.0,
                "pv": 0.0,
            },
        )

        g["filled_qty"] += qty
        g["pv"] += qty * price

        # keep earliest time if comparable
        if ts and g["time"] and ts < g["time"]:
            g["time"] = ts

    rows = []
    for _, g in grouped.items():
        if g["filled_qty"] <= 0:
            continue
        vwap = g["pv"] / g["filled_qty"]
        rows.append(
            {
                "time": g["time"].isoformat() if hasattr(g["time"], "isoformat") else str(g["time"]),
                "symbol": g["symbol"],
                "side": g["side"],
                "filled_qty": int(round(g["filled_qty"])),
                "vwap": round(float(vwap), 4),
                "total_dollars": round(float(g["pv"]), 2),
                "order_id": g["order_id"],
            }
        )

    rows.sort(key=lambda r: r["time"], reverse=True)  # newest-first
    return rows


def aggregate_fills_all_sides_by_order_id(activities, only_symbol="TSLA"):
    """
    Like aggregate_fills_by_order_id, but includes BOTH buys and sells.
    Returns rows sorted newest-first.
    """
    grouped = {}

    for act in activities:
        symbol = _get_attr(act, "symbol")
        if only_symbol and symbol != only_symbol:
            continue

        side = _get_attr(act, "side")
        oid = _get_attr(act, "order_id")
        qty = float(_get_attr(act, "qty", 0) or 0)
        price = float(_get_attr(act, "price", 0) or 0)
        ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")

        if oid is None:
            oid = f"noid:{symbol}:{side}:{ts}:{price}:{qty}"

        g = grouped.setdefault(
            oid,
            {
                "order_id": oid,
                "time": ts,
                "side": side,
                "symbol": symbol,
                "filled_qty": 0.0,
                "pv": 0.0,
            },
        )

        g["filled_qty"] += qty
        g["pv"] += qty * price

        # keep earliest time
        if ts and g["time"] and ts < g["time"]:
            g["time"] = ts

    rows = []
    for _, g in grouped.items():
        if g["filled_qty"] <= 0:
            continue
        vwap = g["pv"] / g["filled_qty"]
        rows.append(
            {
                "time": g["time"].isoformat() if hasattr(g["time"], "isoformat") else str(g["time"]),
                "symbol": g["symbol"],
                "side": g["side"],
                "filled_qty": int(round(g["filled_qty"])),
                "vwap": round(float(vwap), 4),
                "total_dollars": round(float(g["pv"]), 2),
                "order_id": g["order_id"],
            }
        )

    rows.sort(key=lambda r: r["time"], reverse=True)  # newest-first
    return rows


def build_trade_cycles_from_order_rows(order_rows):
    """
    Build closed trade cycles from aggregated order rows.
    Assumes your bot: multiple BUY orders -> one SELL order closes the group.
    Output: cycles newest-first.
    """
    ordered = list(reversed(order_rows))  # oldest-first

    cycles = []
    cur = None

    def start_cycle(buy_row):
        t = buy_row.get("time")
        return {
            "anchor_time_raw": t,
            "anchor_time": fmt_ct_any(t),
            "anchor_order_id": buy_row.get("order_id"),
            "anchor_vwap": float(buy_row.get("vwap") or 0),
            "buy_orders": 0,
            "shares": 0,
            "cost": 0.0,
            "last_buy_time_raw": t,
        }

    for r in ordered:
        side = r.get("side")
        qty = int(r.get("filled_qty") or 0)
        vwap = float(r.get("vwap") or 0)
        dollars = float(r.get("total_dollars") or 0)
        t = r.get("time")

        if side == "buy":
            if cur is None:
                cur = start_cycle(r)
            cur["buy_orders"] += 1
            cur["shares"] += qty
            cur["cost"] += dollars
            cur["last_buy_time_raw"] = t

        elif side == "sell":
            if cur is None or cur["shares"] <= 0:
                continue

            proceeds = dollars
            avg_entry = (cur["cost"] / cur["shares"]) if cur["shares"] else None
            avg_exit = vwap

            realized_pl = proceeds - cur["cost"]
            realized_pl_pct = (realized_pl / cur["cost"] * 100.0) if cur["cost"] else None

            cycles.append(
                {
                    "anchor_time": cur["anchor_time"],
                    "anchor_time_raw": cur["anchor_time_raw"],
                    "anchor_order_id": cur["anchor_order_id"],
                    "anchor_vwap": round(cur["anchor_vwap"], 4),
                    "shares": cur["shares"],
                    "buy_orders": cur["buy_orders"],
                    "avg_entry": round(avg_entry, 4) if avg_entry is not None else None,
                    "sell_time": fmt_ct_any(t),
                    "sell_time_raw": t,
                    "sell_order_id": r.get("order_id"),
                    "avg_exit": round(avg_exit, 4) if avg_exit is not None else None,
                    "proceeds": round(proceeds, 2),
                    "cost": round(cur["cost"], 2),
                    "realized_pl": round(realized_pl, 2),
                    "realized_pl_pct": round(realized_pl_pct, 3) if realized_pl_pct is not None else None,
                }
            )

            cur = None

    def _sell_sort_key(c):
        raw = c.get("sell_time_raw")
        if isinstance(raw, str):
            dt = _parse_iso_time(raw)
            return dt or datetime.min
        return raw or datetime.min

    cycles.sort(key=_sell_sort_key, reverse=True)  # newest-first
    return cycles


def compute_cycles(days=30, symbol="TSLA"):
    after = (datetime.utcnow() - timedelta(days=days)).isoformat() + "Z"
    acts = api.get_activities(activity_types="FILL", after=after)
    orders = aggregate_fills_all_sides_by_order_id(acts, only_symbol=symbol)
    return build_trade_cycles_from_order_rows(orders)


def find_last_tsla_sell_time(activities):
    """Return timestamp of most recent TSLA sell fill, else None."""
    last = None
    for act in activities:
        symbol = _get_attr(act, "symbol")
        side = _get_attr(act, "side")
        ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
        if symbol == "TSLA" and side == "sell" and ts:
            if last is None or ts > last:
                last = ts
    return last


# -------------------------
# Routes
# -------------------------
@app.route("/")
def home():
    return "Alpaca Report Service Running"


@app.route("/report")
def report():
    """Report: account + TSLA position + active group + ladder triggers. Always returns JSON."""
    try:
        acct = api.get_account()

        # TSLA position (may not exist)
        position_data = None
        try:
            pos = api.get_position("TSLA")
            position_data = {
                "symbol": pos.symbol,
                "qty": float(pos.qty),
                "avg_entry": float(pos.avg_entry_price),
                "market_value": float(pos.market_value),
                "unrealized_pl": float(pos.unrealized_pl),
                "current_price": float(pos.current_price),
            }
        except Exception:
            position_data = None

        data = {
            "ok": True,
            "account": {
                "equity": float(_get_attr(acct, "equity", 0) or 0),
                "cash": float(_get_attr(acct, "cash", 0) or 0),
                "buying_power": float(_get_attr(acct, "buying_power", 0) or 0),
                "regt_buying_power": float(_get_attr(acct, "regt_buying_power", 0) or 0),
                "daytrading_buying_power": float(_get_attr(acct, "daytrading_buying_power", 0) or 0),
                "effective_buying_power": float(_get_attr(acct, "effective_buying_power", 0) or 0),
                "non_marginable_buying_power": float(_get_attr(acct, "non_marginable_buying_power", 0) or 0),
                "long_market_value": float(_get_attr(acct, "long_market_value", 0) or 0),
                "initial_margin": float(_get_attr(acct, "initial_margin", 0) or 0),
                "maintenance_margin": float(_get_attr(acct, "maintenance_margin", 0) or 0),
            },
            "position": position_data,
        }

        # --- Recent TSLA BUY triggers (aggregated fills) ---
        try:
            after = (datetime.utcnow() - timedelta(days=5)).isoformat() + "Z"
            activities = api.get_activities(activity_types="FILL", after=after)
            buys = aggregate_fills_by_order_id(activities, only_symbol="TSLA", only_side="buy")
            data["recent_buy_triggers"] = buys[:50]
        except Exception as e:
            data["recent_buy_triggers_error"] = str(e)
            data["recent_buy_triggers"] = []

        # --- Active Group (computed from activities; one-group mode) ---
        try:
            after2 = (datetime.utcnow() - timedelta(days=10)).isoformat() + "Z"
            acts2 = api.get_activities(activity_types="FILL", after=after2)

            last_sell_ts = find_last_tsla_sell_time(acts2)

            # filter to TSLA buys AFTER last sell
            tsla_buys_after = []
            for act in acts2:
                symbol = _get_attr(act, "symbol")
                side = _get_attr(act, "side")
                ts = _get_attr(act, "transaction_time") or _get_attr(act, "time") or _get_attr(act, "timestamp")
                if symbol != "TSLA" or side != "buy" or not ts:
                    continue
                if last_sell_ts and ts <= last_sell_ts:
                    continue
                tsla_buys_after.append(act)

            group_buys = aggregate_fills_by_order_id(tsla_buys_after, only_symbol="TSLA", only_side="buy")
            # group_buys is newest-first

            active_group = None
            active_group_triggers = []

            if group_buys:
                # Anchor lock: FIRST buy trigger after last sell (oldest by time)
                def _row_time(r):
                    t = r.get("time")
                    if isinstance(t, str):
                        return _parse_iso_time(t) or datetime.max
                    return t or datetime.max

                anchor_row = min(group_buys, key=_row_time)
                anchor_price = float(anchor_row["vwap"])
                group_start_time = anchor_row.get("time")

                # strategy settings (hard-coded for now)
                BUY_QTY = 12
                SELL_OFFSET = 2.0
                FIRST_INCREMENT_COUNT = 5

                buys_count = len(group_buys)
                stage = (buys_count - 1) // FIRST_INCREMENT_COUNT + 1
                drop_increment = float(stage)

                buys_in_this_stage = (buys_count - 1) % FIRST_INCREMENT_COUNT + 1
                buys_remaining_before_increment_increases = FIRST_INCREMENT_COUNT - buys_in_this_stage

                # compute next buy price
                completed_drops = 0.0
                full_stages = (buys_count - 1) // FIRST_INCREMENT_COUNT
                for s in range(1, full_stages + 1):
                    completed_drops += s * FIRST_INCREMENT_COUNT
                partial = (buys_count - 1) % FIRST_INCREMENT_COUNT
                completed_drops += stage * partial
                next_buy_price = anchor_price - (completed_drops + stage)

                # current price
                current_price = None
                try:
                    if position_data and position_data.get("current_price") is not None:
                        current_price = float(position_data["current_price"])
                except Exception:
                    current_price = None

                sell_target = anchor_price + SELL_OFFSET

                distance_to_sell = None
                distance_to_next_buy = None
                if current_price is not None:
                    distance_to_sell = round(sell_target - current_price, 4)
                    distance_to_next_buy = round(current_price - next_buy_price, 4)

                active_group = {
                    "group_start_time": fmt_ct_any(group_start_time),
                    "anchor_vwap": round(anchor_price, 4),
                    "sell_target": round(sell_target, 4),
                    "buys_count": buys_count,
                    "drop_increment": drop_increment,
                    "buys_in_this_increment": buys_in_this_stage,
                    "buys_remaining_before_increment_increases": buys_remaining_before_increment_increases,
                    "next_buy_price": round(next_buy_price, 4),
                    "shares_expected": buys_count * BUY_QTY,
                    "current_price": round(current_price, 4) if current_price is not None else None,
                    "distance_to_sell": distance_to_sell,
                    "distance_to_next_buy": distance_to_next_buy,
                    "anchor_time": fmt_ct_any(anchor_row.get("time")),
                    "anchor_order_id": anchor_row.get("order_id"),
                }

                # Build chronological first (oldest-first) so actual_drop is correct.
                ladder_oldest_first = list(reversed(group_buys))  # oldest-first
                prev_vwap = None
                tmp_rows = []
                for i, row in enumerate(ladder_oldest_first, start=1):
                    vwap = float(row["vwap"])
                    intended_drop = ((i - 1) // 5) + 1 if i > 1 else None
                    actual_drop = round(prev_vwap - vwap, 4) if prev_vwap is not None else None

                    tmp_rows.append(
                        {
                            "trigger": i,
                            "time": fmt_ct_any(row.get("time")),
                            "shares": row.get("filled_qty"),
                            "avg_price": round(vwap, 4),
                            "total_dollars": row.get("total_dollars"),
                            "actual_drop": actual_drop,
                            "intended_drop": intended_drop,
                            "order_id": row.get("order_id"),
                        }
                    )
                    prev_vwap = vwap

                # Display newest-first (most recent fill first)
                active_group_triggers = list(reversed(tmp_rows))

            data["active_group"] = active_group
            data["active_group_triggers"] = active_group_triggers
            data["active_group_last_sell_time"] = (last_sell_ts.isoformat() if last_sell_ts else None)

        except Exception as e:
            data["active_group_error"] = str(e)
            data["active_group"] = None
            data["active_group_triggers"] = []

        return jsonify(data)

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/cycles")
def cycles():
    """Closed trade cycles (newest-first)."""
    try:
        cyc = compute_cycles(days=30, symbol="TSLA")
        return jsonify({"ok": True, "cycles": cyc})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/table")
def table_view():
    """
    HTML dashboard page:
    - Account (vertical list, same font as rest)
    - Active Group summary + Position
    - Ladder (WAITING row at TOP + newest fills below)
    - Cycles (newest-first)
    Auto-refreshes every 15 seconds by fetching /report and /cycles.
    """
    r = report()
    if isinstance(r, tuple):
        resp, _status = r
        data = resp.get_json() if hasattr(resp, "get_json") else {}
    else:
        data = r.get_json() if hasattr(r, "get_json") else {}

    ag = data.get("active_group") or {}
    rows = data.get("active_group_triggers") or []  # newest-first fills
    acct = data.get("account") or {}
    pos = data.get("position") or {}

    html = []
    html.append("<html><head><title>TSLA Ladder</title>")
    html.append(
        """
    <style>
      body { font-family: Arial, sans-serif; padding: 16px; }
      .box { padding: 12px; border: 1px solid #ccc; border-radius: 8px; margin-bottom: 16px; }
      .row { margin: 4px 0; }
      /* Account: two-column rows (same font as rest) */
      .acct-grid { max-width: 400px; margin: 0; }
      .acct-row {
        display: grid;
        grid-template-columns: 1fr auto;
        gap: 16px;
        padding: 2px 0;
      }
      .acct-label { text-align: left; }
      .acct-value { text-align: right; font-variant-numeric: tabular-nums; }
      .muted { color: #666; }
      table { border-collapse: collapse; width: 100%; margin-bottom: 24px; }
      th, td { border: 1px solid #ddd; padding: 8px; text-align: right; }
      th { background: #f5f5f5; text-align: right; }
      td:first-child, th:first-child { text-align: center; }
      td:nth-child(2), th:nth-child(2) { text-align: left; }
      /* BIG live TSLA price banner */
      .price-banner {
        font-size: 42px;
        font-weight: bold;
        text-align: center;
        margin-bottom: 10px;
      }
    
      .price-label {
        font-size: 16px;
        color: #666;
        text-align: center;
        margin-bottom: 4px;
      }

      /* Top row: Account (left) + Summary (right), centered as a pair */
      .top-row {
        display: flex;
        justify-content: center;
        gap: 24px;
        align-items: flex-start;
        margin-bottom: 16px;
      }

      /* Summary box width to match Account feel */
      .summary-grid {
        max-width: 400px;   /* match your acct-grid max-width */
        width: 100%;
      }

      /* 2-column rows for the Summary box (same idea as Account) */
      .sum-row {
        display: grid;
        grid-template-columns: 1fr auto;
        gap: 16px;
        padding: 2px 0;
      }
      .sum-label { text-align: left; }
      .sum-value { text-align: right; font-variant-numeric: tabular-nums; }

    </style>

    """
    )
    html.append("</head><body>")

    # Last updated timestamp (Central Time)
    now_ct = to_central(datetime.utcnow().replace(tzinfo=pytz.utc))

    html.append("<div class='top-row'>")

    # -------------------------
    # Top summary box (Active Group + Position)
    # -------------------------
    html.append('<div class="box">')
    html.append("<div class='price-label'>TSLA Price</div>")
    html.append(f"<div class='price-banner'>$<span id='tsla-price'>{money(pos.get('current_price')) if pos else ''}</span></div>")
    html.append(f"<div class='row'><b>Last updated:</b> <span id='last-updated'>{now_ct}</span></div>")
    html.append("<br>")

    html.append(
        "<div class='row'>"
        f"<b>Anchor:</b> <span id='ag-anchor-vwap'>{ag.get('anchor_vwap')}</span> "
        f"@ <span id='ag-anchor-time'>{ag.get('anchor_time')}</span>"
        "</div>"
    )
    html.append(
        "<div class='row'>"
        f"<b>Sell Target:</b> <span id='ag-sell-target'>{ag.get('sell_target')}</span>"
        f" &nbsp;&nbsp; <b>Distance:</b> <span id='ag-distance-sell'>{ag.get('distance_to_sell')}</span>"
        "</div>"
    )
    html.append(
        "<div class='row'>"
        f"<b>Next Buy:</b> <span id='ag-next-buy'>{ag.get('next_buy_price')}</span>"
        f" &nbsp;&nbsp; <b>Distance:</b> <span id='ag-distance-next'>{ag.get('distance_to_next_buy')}</span>"
        "</div>"
    )
    html.append(
        "<div class='row'>"
        f"<b>Buys:</b> <span id='ag-buys'>{ag.get('buys_count')}</span>"
        f" &nbsp;&nbsp; <b>Drop Increment:</b> <span id='ag-drop-inc'>{ag.get('drop_increment')}</span>"
        "</div>"
    )

    if pos:
        html.append("<br><div class='row'><b>TSLA Position:</b></div>")
        html.append(
            "<div class='row'>"
            f"Qty: <span id='pos-qty'>{pos.get('qty')}</span> &nbsp;&nbsp; "
            f"Avg: $<span id='pos-avg'>{money(pos.get('avg_entry'))}</span> &nbsp;&nbsp; "
            f"Mkt Val: $<span id='pos-mv'>{money(pos.get('market_value'))}</span> &nbsp;&nbsp; "
            f"uP/L: $<span id='pos-upl'>{money(pos.get('unrealized_pl'))}</span>"
            "</div>"
        )

    html.append("</div>")
    html.append("</div>")   # closes the top-row container

    # -------------------------
    # Account box (2-column grid, same font)
    # -------------------------
    html.append("<div class='box acct-grid'>")
    html.append("<div class='row'><b>Account</b></div>")
    html.append("<div class='row muted'>Updates every 15 seconds (time-based), independent of trade activity.</div>")
    html.append("<br>")
    
    def acct_row(label, key, fmt="money"):
        val = acct.get(key)
        sval = money0(val) if fmt == "money0" else money(val)
        return (
            "<div class='acct-row'>"
            f"<div class='acct-label'>{label}</div>"
            f"<div class='acct-value'>$<span id='acct-{key}'>{sval}</span></div>"
            "</div>"
        )
    
    html.append(acct_row("Equity", "equity"))
    html.append(acct_row("Cash", "cash"))
    html.append(acct_row("Buying Power", "buying_power", fmt="money0"))
    html.append(acct_row("RegT Buying Power", "regt_buying_power", fmt="money0"))
    html.append(acct_row("Day Trading Buying Power", "daytrading_buying_power", fmt="money0"))
    html.append(acct_row("Effective Buying Power", "effective_buying_power", fmt="money0"))
    html.append(acct_row("Non-Marginable Buying Power", "non_marginable_buying_power", fmt="money0"))
    
    html.append("<br>")
    
    html.append(acct_row("Long Market Value", "long_market_value"))
    html.append(acct_row("Initial Margin", "initial_margin"))
    html.append(acct_row("Maintenance Margin", "maintenance_margin"))
    
    html.append("</div>")

    # -------------------------
    # Ladder table (WAITING row at TOP)
    # -------------------------
    html.append("<h2>Active Group Ladder</h2>")
    html.append("<table>")
    html.append(
        "<thead><tr>"
        "<th>#</th>"
        "<th>Time (CT)</th>"
        "<th>Shares</th>"
        "<th>Total $</th>"
        "<th>Avg Price</th>"
        "<th>Intended Drop</th>"
        "<th>Actual Drop</th>"
        "</tr></thead>"
    )
    html.append("<tbody id='ladder-body'>")

    # WAITING row at TOP
    if ag and ag.get("next_buy_price") is not None and ag.get("buys_count") is not None:
        next_trigger = int(ag["buys_count"]) + 1
        intended_drop_next = ((next_trigger - 1) // 5) + 1
        html.append(
            "<tr>"
            f"<td><b>{next_trigger}</b></td>"
            f"<td><b>WAITING</b></td>"
            f"<td>12</td>"
            f"<td>—</td>"
            f"<td><b>{ag.get('next_buy_price')}</b></td>"
            f"<td><b>{intended_drop_next}</b></td>"
            f"<td>—</td>"
            "</tr>"
        )

    # Then newest-first filled rows
    for row in rows:
        html.append(
            "<tr>"
            f"<td>{row.get('trigger')}</td>"
            f"<td>{row.get('time')}</td>"
            f"<td>{row.get('shares')}</td>"
            f"<td>{row.get('total_dollars')}</td>"
            f"<td>{row.get('avg_price')}</td>"
            f"<td>{row.get('intended_drop')}</td>"
            f"<td>{row.get('actual_drop')}</td>"
            "</tr>"
        )

    html.append("</tbody></table>")

    # -------------------------
    # Cycles table
    # -------------------------
    html.append("<h2>Closed Trade Cycles</h2>")
    html.append(
        """
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Start</th>
          <th>End</th>
          <th>Buys</th>
          <th>Shares</th>
          <th>Avg Entry</th>
          <th>Avg Exit</th>
          <th>P/L $</th>
          <th>P/L %</th>
        </tr>
      </thead>
      <tbody id="cycles-body"></tbody>
    </table>
    """
    )

    # -------------------------
    # JS: refresh /report + /cycles every 15s
    #   - Ladder WAITING row is rendered FIRST (top) on every refresh
    # -------------------------
    html.append(
        """
    <script>
    const REFRESH_MS = 15000;

    function fmtMoney(x) {
      if (x == null || x === "") return "";
      const n = Number(x);
      if (Number.isNaN(n)) return String(x);
      return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function fmtMoney0(x) {
      if (x == null || x === "") return "";
      const n = Number(x);
      if (Number.isNaN(n)) return String(x);
      return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
    }

    async function refreshReport() {
      try {
        const res = await fetch("/report?t=" + Date.now(), { cache: "no-store" });
        const data = await res.json();
        if (!data.ok) return;

        // last updated (client-side)
        const lu = document.getElementById("last-updated");
        if (lu) lu.textContent = new Date().toLocaleString();

        // Active group fields
        const ag = data.active_group || {};
        const setText = (id, v) => {
          const el = document.getElementById(id);
          if (el) el.textContent = (v ?? "");
        };

        setText("ag-anchor-vwap", ag.anchor_vwap);
        setText("ag-anchor-time", ag.anchor_time);
        setText("ag-sell-target", ag.sell_target);
        setText("ag-distance-sell", ag.distance_to_sell);
        setText("ag-next-buy", ag.next_buy_price);
        setText("ag-distance-next", ag.distance_to_next_buy);
        setText("ag-buys", ag.buys_count);
        setText("ag-drop-inc", ag.drop_increment);

        // Position
        const pos = data.position || {};
        if (pos && Object.keys(pos).length) {
          setText("tsla-price", fmtMoney(pos.current_price));
          setText("pos-qty", pos.qty);
          setText("pos-avg", fmtMoney(pos.avg_entry));
          setText("pos-mv", fmtMoney(pos.market_value));
          setText("pos-upl", fmtMoney(pos.unrealized_pl));
        }

        // Account (same font as body; just update the numbers)
        const acct = data.account || {};
        const setAcct = (key, fmt0=false) => {
          const el = document.getElementById("acct-" + key);
          if (!el) return;
          el.textContent = fmt0 ? fmtMoney0(acct[key]) : fmtMoney(acct[key]);
        };

        setAcct("equity");
        setAcct("cash");
        setAcct("buying_power", true);
        setAcct("regt_buying_power", true);
        setAcct("daytrading_buying_power", true);
        setAcct("effective_buying_power", true);
        setAcct("non_marginable_buying_power", true);
        setAcct("long_market_value");
        setAcct("initial_margin");
        setAcct("maintenance_margin");

        // Ladder: WAITING row first, then newest-first fills
        const ladderBody = document.getElementById("ladder-body");
        if (ladderBody) {
          ladderBody.innerHTML = "";

          const rows = data.active_group_triggers || []; // newest-first fills

          // WAITING row at TOP
          if (ag && ag.next_buy_price != null && ag.buys_count != null) {
            const nextTrigger = Number(ag.buys_count) + 1;
            const intendedDropNext = Math.floor((nextTrigger - 1) / 5) + 1;

            const trW = document.createElement("tr");
            trW.innerHTML = `
              <td><b>${nextTrigger}</b></td>
              <td><b>WAITING</b></td>
              <td>12</td>
              <td>—</td>
              <td><b>${ag.next_buy_price}</b></td>
              <td><b>${intendedDropNext}</b></td>
              <td>—</td>
            `;
            ladderBody.appendChild(trW);
          }

          // Then newest-first filled rows
          rows.forEach((r) => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
              <td>${r.trigger ?? ""}</td>
              <td>${r.time ?? ""}</td>
              <td>${r.shares ?? ""}</td>
              <td>${r.total_dollars ?? ""}</td>
              <td>${r.avg_price ?? ""}</td>
              <td>${r.intended_drop ?? ""}</td>
              <td>${r.actual_drop ?? ""}</td>
            `;
            ladderBody.appendChild(tr);
          });
        }

      } catch (e) {
        console.error("refreshReport exception", e);
      }
    }

    async function loadCycles() {
      try {
        const res = await fetch("/cycles?t=" + Date.now(), { cache: "no-store" });
        const data = await res.json();
        if (!data.ok) return;

        const tbody = document.getElementById("cycles-body");
        if (!tbody) return;

        tbody.innerHTML = "";

        // cycles already newest-first
        data.cycles.forEach((c, i) => {
          const row = document.createElement("tr");
          row.innerHTML = `
            <td>${i + 1}</td>
            <td>${c.anchor_time ?? ""}</td>
            <td>${c.sell_time ?? ""}</td>
            <td>${c.buy_orders ?? ""}</td>
            <td>${c.shares ?? ""}</td>
            <td>${c.avg_entry != null ? "$" + c.avg_entry : ""}</td>
            <td>${c.avg_exit != null ? "$" + c.avg_exit : ""}</td>
            <td>${c.realized_pl != null ? "$" + c.realized_pl : ""}</td>
            <td>${c.realized_pl_pct != null ? c.realized_pl_pct + "%" : ""}</td>
          `;
          tbody.appendChild(row);
        });
      } catch (e) {
        console.error("loadCycles exception", e);
      }
    }

    // Initial load
    refreshReport();
    loadCycles();

    // Auto-refresh loop
    setInterval(() => {
      refreshReport();
      loadCycles();
    }, REFRESH_MS);
    </script>
    """
    )

    html.append("</body></html>")
    return Response("\n".join(html), mimetype="text/html")
