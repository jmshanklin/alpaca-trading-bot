const statusEl = document.getElementById("status");
const posGrid = document.getElementById("posGrid");
const anchorGrid = document.getElementById("anchorGrid");
const rawEl = document.getElementById("raw");
const refreshBtn = document.getElementById("refreshBtn");

function setStatus(t, ok = true) {
  statusEl.textContent = t;
  statusEl.className = ok ? "ok" : "bad";
}

function row(k, v) {
  return `<div class="k">${k}</div><div class="v">${v ?? "—"}</div>`;
}

function money(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n.toFixed(2) : "—";
}

function pct(x) {
  const n = Number(x);
  return Number.isFinite(n) ? (n * 100).toFixed(2) + "%" : "—";
}

async function refresh() {
  try {
    setStatus("loading…", true);

    const r = await fetch("/position", { cache: "no-store" });
    const p = await r.json();

    rawEl.textContent = JSON.stringify(p, null, 2);

    if (!p.ok) {
      setStatus("error (/position)", false);
      posGrid.innerHTML = row("error", p.error || "unknown");
      anchorGrid.innerHTML = "";
      return;
    }

    // Position block
    posGrid.innerHTML =
      row("symbol", p.symbol) +
      row("qty", p.qty) +
      row("avg_entry", money(p.avg_entry)) +
      row("market_price", money(p.market_price)) +
      row("unrealized_pl", money(p.unrealized_pl)) +
      row("unrealized_plpc", pct(p.unrealized_plpc));

    // Anchor block
    anchorGrid.innerHTML =
      row("anchor_price", money(p.anchor_price)) +
      row("anchor_time_utc", p.anchor_time_utc || "—") +
      row("sell_rise_usd", money(p.sell_rise_usd)) +
      row("sell_target", money(p.sell_target));

    // Friendly status
    if (!p.qty || Number(p.qty) <= 0) {
      setStatus("flat (no position)", true);
    } else if (p.sell_target == null || p.anchor_price == null) {
      setStatus("position open — anchor not found yet", false);
    } else {
      setStatus(`position open — sell target ${money(p.sell_target)}`, true);
    }
  } catch (e) {
    console.error(e);
    setStatus("exception", false);
  }
}

refreshBtn.onclick = refresh;

refresh();
setInterval(refresh, 5000);
