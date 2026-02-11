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

# -------------------------
# Aggregation + Cycles
# -------------------------
def aggregate_fills_by_order_id(activities, only_symbol="TSLA", only_side="buy"):
    """
    Turn Alpaca FILL activities into one row per order_id.
    Filters to TSLA buys by default.
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

    rows.sort(key=lambda r: r["time"], reverse=True)
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

    rows.sort(key=lambda r: r["time"], reverse=True)
    return rows


def build_trade_cycles_from_order_rows(order_rows):
    """
    Build closed trade cycles from aggregated order rows.
    Assumes your bot: multiple BUY orders -> one SELL order closes the group.
    Input: order_rows (newest-first or oldest-first OK)
    Output: cycles newest-first
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

    # robust sort using RAW sell time (not formatted CT string)
    def _sell_sort_key(c):
        raw = c.get("sell_time_raw")
        if isinstance(raw, str):
            dt = _parse_iso_time(raw)
            return dt or datetime.min
        return raw or datetime.min

    cycles.sort(key=_sell_sort_key, reverse=True)
    return cycles


def compute_cycles(days=30, symbol="TSLA"):
    """Shared cycles engine (so /cycles and other callers reuse identical logic)."""
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
        
            # Exposure / margin (these mirror what you see on Alpaca “Balance”)
            "long_market_value": float(_get_attr(acct, "long_market_value", 0) or 0),
            "initial_margin": float(_get_attr(acct, "initial_margin", 0) or 0),
            "maintenance_margin": float(_get_attr(acct, "maintenance_margin", 0) or 0),
        
            # Optional (only if present on your account type)
            "regt_buying_power": float(_get_attr(acct, "regt_buying_power", 0) or 0),
            "daytrading_buying_power": float(_get_attr(acct, "daytrading_buying_power", 0) or 0),
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

                # --- Active Group Triggers table (Excel-style) ---
                ladder = list(reversed(group_buys))  # oldest-first
                prev_vwap = None
                for i, row in enumerate(ladder, start=1):
                    vwap = float(row["vwap"])
                    intended_drop = ((i - 1) // 5) + 1 if i > 1 else None
                    actual_drop = round(prev_vwap - vwap, 4) if prev_vwap is not None else None

                    active_group_triggers.append(
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


@app.route("/active_group_triggers")
def active_group_triggers_only():
    """Clean endpoint for the ladder JSON."""
    try:
        r = report()
        if isinstance(r, tuple):
            resp, status = r
            data = resp.get_json() if hasattr(resp, "get_json") else {}
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": data.get("error", "Report failed"),
                        "active_group": None,
                        "active_group_triggers": [],
                    }
                ),
                status,
            )

        data = r.get_json() if hasattr(r, "get_json") else None
        if not isinstance(data, dict):
            return jsonify({"ok": False, "error": "Could not parse report output"}), 500

        return jsonify(
            {
                "ok": data.get("ok", False),
                "error": data.get("error"),
                "active_group_error": data.get("active_group_error"),
                "active_group": data.get("active_group", None),
                "active_group_triggers": data.get("active_group_triggers", []),
            }
        )

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/cycles")
def cycles():
    """
    Closed trade cycles (BUY group -> SELL closes group), derived from Alpaca FILL activities.
    Returns JSON only.
    """
    try:
        cyc = compute_cycles(days=30, symbol="TSLA")
        return jsonify({"ok": True, "cycles": cyc})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/table")
def table_view():
    """
    HTML dashboard page:
    - Ladder (server-rendered)
    - Cycles table (filled by JS fetch to /cycles)
    """
    # FIX: report() may return (resp, status) tuple on error
    r = report()
    if isinstance(r, tuple):
        resp, _status = r
        data = resp.get_json() if hasattr(resp, "get_json") else {}
    else:
        data = r.get_json() if hasattr(r, "get_json") else {}

    ag = data.get("active_group") or {}
    rows = data.get("active_group_triggers") or []
    acct = data.get("account") or {}
    pos = data.get("position") or {}

    html = []

    # FIX: remove meta refresh entirely (avoid any browser weirdness)
    html.append("<html><head><title>TSLA Ladder</title>")
    html.append(
        """
    <style>
      body { font-family: Arial, sans-serif; padding: 16px; }
      .box { padding: 12px; border: 1px solid #ccc; border-radius: 8px; margin-bottom: 16px; }
      table { border-collapse: collapse; width: 100%; margin-bottom: 24px; }
      th, td { border: 1px solid #ddd; padding: 8px; text-align: right; }
      th { background: #f5f5f5; text-align: right; }
      td:first-child, th:first-child { text-align: center; }
      td:nth-child(2), th:nth-child(2) { text-align: left; }
    </style>
    """
    )
    html.append("</head><body>")

    # Last updated timestamp (Central Time)
    now_ct = to_central(datetime.utcnow().replace(tzinfo=pytz.utc))

    # Header summary
    html.append('<div class="box">')
    html.append(f"<b>Last updated:</b> {now_ct}<br><br>")
    html.append(f"<b>Anchor:</b> {ag.get('anchor_vwap')} @ {ag.get('anchor_time')}<br>")
    html.append(
        f"<b>Sell Target:</b> {ag.get('sell_target')} &nbsp;&nbsp; <b>Distance:</b> {ag.get('distance_to_sell')}<br>"
    )
    html.append(
        f"<b>Next Buy:</b> {ag.get('next_buy_price')} &nbsp;&nbsp; <b>Distance:</b> {ag.get('distance_to_next_buy')}<br>"
    )
    html.append(f"<b>Buys:</b> {ag.get('buys_count')} &nbsp;&nbsp; <b>Drop Increment:</b> {ag.get('drop_increment')}")
    
    # --- TSLA Position ---
    if pos:
        html.append("<br><br><b>TSLA Position:</b><br>")
        html.append(
            f"Qty: {pos.get('qty')} &nbsp;&nbsp; "
            f"Avg: ${money(pos.get('avg_entry'))} &nbsp;&nbsp; "
            f"Mkt Val: ${money(pos.get('market_value'))} &nbsp;&nbsp; "
            f"uP/L: ${money(pos.get('unrealized_pl'))}"
        )
        
    html.append("</div>")
    html.append("<hr style='margin:10px 0;'>")
    html.append("<b>Account:</b><br>")
    html.append(f"Equity: ${acct.get('equity')} &nbsp;&nbsp; Cash: ${acct.get('cash')} &nbsp;&nbsp; Buying Power: ${acct.get('buying_power')}<br>")
    html.append(f"Long Mkt Value: ${acct.get('long_market_value')} &nbsp;&nbsp; Init Margin: ${acct.get('initial_margin')} &nbsp;&nbsp; Maint Margin: ${acct.get('maintenance_margin')}<br>")
    html.append(f"Reg-T BP: ${acct.get('regt_buying_power')} &nbsp;&nbsp; DT BP: ${acct.get('daytrading_buying_power')}<br>")

    # -------------------------
    # Ladder table (FIRST)
    # -------------------------
    html.append("<h2>Active Group Ladder</h2>")
    html.append("<table>")
    html.append(
        "<tr>"
        "<th>#</th>"
        "<th>Time (CT)</th>"
        "<th>Shares</th>"
        "<th>Total $</th>"
        "<th>Avg Price</th>"
        "<th>Intended Drop</th>"
        "<th>Actual Drop</th>"
        "</tr>"
    )

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

    # Add "next buy" ghost row
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

    html.append("</table>")

    # -------------------------
    # Cycles table (SECOND)
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
    # JS: load cycles every 15s (no full-page refresh)
    # -------------------------
    html.append(
        """
    <script>
    async function loadCycles() {
      try {
        const res = await fetch("/cycles", { cache: "no-store" });
        const data = await res.json();

        if (!data.ok) {
          console.error("Cycles load failed", data);
          return;
        }

        const tbody = document.getElementById("cycles-body");
        if (!tbody) return;

        tbody.innerHTML = "";

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
        console.error("Cycles exception", e);
      }
    }

    loadCycles();
    setInterval(loadCycles, 15000);
    </script>
    """
    )

    html.append("</body></html>")
    return Response("\n".join(html), mimetype="text/html")
