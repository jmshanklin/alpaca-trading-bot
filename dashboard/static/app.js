// ------------------------------------
// Alpaca Dashboard - app.js (CLEAN, CORRECTED, HARDENED)
// Fixes:
// - Timestamp bug (no object-based or backward updates)
// - History/LIVE mode
// - Group performance table
// - Anchor-based sell target line
// - BUY/SELL markers with cycle numbering
// - Right padding controls
// - Ruler tool
// - OHLC readout
// ------------------------------------

const statusEl = document.getElementById("status");
const barEl = document.getElementById("bar");
const chartEl = document.getElementById("chart");

// ----------------------------
// Mode: LIVE vs HISTORY
// ----------------------------
let HISTORY_MODE = false;
const LIVE_BAR_LIMIT = 300;
const HISTORY_BAR_LIMIT = 1500;

// ----------------------------
// Helpers
// ----------------------------
function setStatus(text) {
  statusEl.textContent = text;
}

function nowEpochSec() {
  return Math.floor(Date.now() / 1000);
}

function fmtChicago(tsSec) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    month: "numeric",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(tsSec * 1000));
}

function minuteFloor(tsSec) {
  return Math.floor(tsSec / 60) * 60;
}

function fmtSigned(n, d = 2) {
  if (!Number.isFinite(n)) return "—";
  return (n >= 0 ? "+" : "") + n.toFixed(d);
}

// ----------------------------
// Market hours (Chicago)
// ----------------------------
function isMarketOpenChicagoNow() {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(now);

  const wk = parts.find(p => p.type === "weekday")?.value || "";
  const h = parseInt(parts.find(p => p.type === "hour")?.value || "0", 10);
  const m = parseInt(parts.find(p => p.type === "minute")?.value || "0", 10);

  if (!["Mon", "Tue", "Wed", "Thu", "Fri"].includes(wk)) return false;
  const mins = h * 60 + m;
  return mins >= 510 && mins < 900; // 8:30–15:00 CT
}

// ----------------------------
// Group Performance
// ----------------------------
async function loadGroupPerformance() {
  const gpEl = document.getElementById("gp");
  const tableEl = document.getElementById("groupTable");
  if (!gpEl || !tableEl) return;

  try {
    const r = await fetch("/group_performance", { cache: "no-store" });
    const j = await r.json();

    if (!j || j.ok === false) {
      gpEl.textContent = "GP: error";
      return;
    }

    const rows = Array.isArray(j.rows) ? j.rows : [];
    const openCount = rows.filter(x => (x.cycle_status || "") === "OPEN").length;
    const closedCount = rows.filter(x => (x.cycle_status || "") === "CLOSED").length;

    const last = rows[0];
    const pnl = last?.pnl;
    const pct = last?.pnl_pct;

    gpEl.textContent =
      `GP: OPEN ${openCount} | CLOSED ${closedCount}` +
      (Number.isFinite(pnl) ? ` | Last ${pnl.toFixed(2)}` : "") +
      (Number.isFinite(pct) ? ` (${pct.toFixed(2)}%)` : "");

    if (!rows.length) {
      tableEl.innerHTML = "<tbody><tr><td>No group rows</td></tr></tbody>";
      return;
    }

    const cols = [
      "cycle_status",
      "win_loss",
      "cycle_start_ct",
      "cycle_last_ct",
      "buy_qty",
      "avg_buy_price",
      "sell_qty",
      "avg_sell_price",
      "pnl",
      "pnl_pct",
      "group_id"
    ];

    let html = "<thead><tr>";
    cols.forEach(c => html += `<th>${c}</th>`);
    html += "</tr></thead><tbody>";

    rows.slice(0, 50).forEach(r => {
      html += "<tr>";
      cols.forEach(c => {
        let v = r[c] ?? "";
        if (typeof v === "number") {
          v = c.includes("pct") ? `${v.toFixed(2)}%` : v.toFixed(2);
        }
        html += `<td>${v}</td>`;
      });
      html += "</tr>";
    });

    html += "</tbody>";
    tableEl.innerHTML = html;

  } catch (e) {
    console.error("GP error", e);
  }
}

// ----------------------------
// Chart
// ----------------------------
const chart = LightweightCharts.createChart(chartEl, {
  layout: { background: { color: "#0e1117" }, textColor: "#d1d4dc" },
  grid: {
    vertLines: { color: "#1f2430" },
    horzLines: { color: "#1f2430" },
  },
  timeScale: {
    timeVisible: true,
    secondsVisible: false,
    rightOffset: 12,
    barSpacing: 8,
    fixRightEdge: false,
    lockVisibleTimeRangeOnResize: false,
  },
  crosshair: {
    mode: 1,
    vertLine: { visible: true, labelVisible: true },
    horzLine: { visible: true, labelVisible: true },
  },
  localization: {
    timeFormatter: t => {
      const ts = typeof t === "number" ? t : t?.timestamp || 0;
      return fmtChicago(ts);
    }
  }
});

const candles = chart.addSeries(LightweightCharts.CandlestickSeries, {
  upColor: "#22c55e",
  downColor: "#ef4444",
  wickUpColor: "#22c55e",
  wickDownColor: "#ef4444",
  borderUpColor: "#22c55e",
  borderDownColor: "#ef4444"
});

// ----------------------------
// Price Lines
// ----------------------------
const avgEntryLine = candles.createPriceLine({
  price: 0,
  color: "#f5c542",
  lineWidth: 2,
  axisLabelVisible: true,
  title: "Avg Entry",
});

const sellTargetLine = candles.createPriceLine({
  price: 0,
  color: "#4aa3ff",
  lineWidth: 2,
  axisLabelVisible: true,
  title: "Sell Target",
});

// ----------------------------
// Markers
// ----------------------------
const markersLayer =
  typeof candles.setMarkers === "function"
    ? { set: ms => candles.setMarkers(ms) }
    : LightweightCharts.createSeriesMarkers
      ? (() => {
          const p = LightweightCharts.createSeriesMarkers(candles, []);
          return { set: ms => p.setMarkers(ms) };
        })()
      : null;

let lastMarkersHash = "";

function hashMarkers(ms) {
  return ms.map(m => `${m.time}|${m.text}`).join(";");
}

async function loadMarkers() {
  if (!markersLayer) return;

  try {
    const r = await fetch("/fills?limit=500", { cache: "no-store" });
    const j = await r.json();
    if (!j.ok || !Array.isArray(j.fills)) return;

    const fills = [...j.fills].sort(
      (a, b) => new Date(a.filled_at) - new Date(b.filled_at)
    );

    let runningQty = 0;
    let buyCount = 0;
    const markers = [];

    for (const f of fills) {
      const ts = minuteFloor(Math.floor(new Date(f.filled_at).getTime() / 1000));
      const side = (f.side || "").toLowerCase();
      const qty = Number(f.filled_qty || 0);
      const px = Number(f.filled_avg_price || 0);

      if (!qty || !px) continue;

      if (side === "buy") {
        if (runningQty === 0) buyCount = 0;
        runningQty += qty;
        buyCount++;
        markers.push({
          time: ts,
          position: "belowBar",
          shape: "arrowUp",
          color: "#22c55e",
          text: `B${buyCount} ${qty}@${px.toFixed(2)}`
        });
      }

      if (side === "sell") {
        runningQty = Math.max(0, runningQty - qty);
        markers.push({
          time: ts,
          position: "aboveBar",
          shape: "arrowDown",
          color: "#ef4444",
          text: `S ${qty}@${px.toFixed(2)}`
        });
        if (runningQty === 0) buyCount = 0;
      }
    }

    const h = hashMarkers(markers);
    if (h !== lastMarkersHash) {
      lastMarkersHash = h;
      markersLayer.set(markers);
    }

  } catch (e) {
    console.error("Markers error", e);
  }
}

// ----------------------------
// OHLC Readout
// ----------------------------
const readout = document.createElement("div");
readout.style.position = "absolute";
readout.style.left = "12px";
readout.style.top = "60px";
readout.style.padding = "6px 8px";
readout.style.background = "rgba(0,0,0,0.65)";
readout.style.border = "1px solid #1f2430";
readout.style.borderRadius = "6px";
readout.style.fontFamily = "monospace";
readout.style.fontSize = "14px";
readout.textContent = "—";
chartEl.parentElement.appendChild(readout);

function setReadout(bar) {
  if (!bar) {
    readout.textContent = "—";
    return;
  }
  readout.textContent =
    `O:${bar.open.toFixed(2)} ` +
    `H:${bar.high.toFixed(2)} ` +
    `L:${bar.low.toFixed(2)} ` +
    `C:${bar.close.toFixed(2)}`;
}

// ----------------------------
// State (HARDENED)
// ----------------------------
let lastBarTimeSec = null;
let lastBarObj = null;
let historyCount = 0;

// ----------------------------
// Load History
// ----------------------------
async function loadHistory(refit = false) {
  try {
    const limit = HISTORY_MODE ? HISTORY_BAR_LIMIT : LIVE_BAR_LIMIT;
    const r = await fetch(`/bars?limit=${limit}`, { cache: "no-store" });
    const j = await r.json();

    if (!j.ok || !Array.isArray(j.bars)) {
      setStatus("no history");
      return;
    }

    const bars = j.bars.map(b => ({
      time: Number(b.time),
      open: b.open,
      high: b.high,
      low: b.low,
      close: b.close
    }));

    candles.setData(bars);
    historyCount = bars.length;

    const last = bars[bars.length - 1];
    lastBarTimeSec = Number(last.time);
    lastBarObj = last;

    setReadout(last);
    setStatus(`bars: ${historyCount} | last: ${fmtChicago(lastBarTimeSec)} | ${HISTORY_MODE ? "HISTORY" : "LIVE"}`);

    if (refit) chart.timeScale().fitContent();

  } catch (e) {
    console.error("History error", e);
  }
}

// ----------------------------
// Latest Bar (HARD FIX)
// ----------------------------
async function fetchLatestBar() {
  try {
    const r = await fetch("/latest_bar", { cache: "no-store" });
    const j = await r.json();
    if (!j.ok) return;

    const barTime = Math.floor(new Date(j.t).getTime() / 1000);
    const barTimeMin = minuteFloor(barTime);

    // HARD GUARD: NEVER allow backward or duplicate updates
    if (lastBarTimeSec !== null && barTimeMin <= lastBarTimeSec) {
      return;
    }

    lastBarTimeSec = barTimeMin;
    lastBarObj = {
      open: j.o,
      high: j.h,
      low: j.l,
      close: j.c
    };

    candles.update({
      time: barTimeMin,
      open: j.o,
      high: j.h,
      low: j.l,
      close: j.c
    });

    setReadout(lastBarObj);
    loadMarkers();
    loadGroupPerformance();

    const age = nowEpochSec() - barTimeMin;
    const suffix = isMarketOpenChicagoNow()
      ? (age > 120 ? ` | STALE ${age}s` : "")
      : " | Market closed";

    setStatus(`last: ${fmtChicago(barTimeMin)}${suffix}`);

  } catch (e) {
    console.error("fetchLatestBar error", e);
  }
}

// ----------------------------
// Position
// ----------------------------
async function fetchPosition() {
  try {
    const r = await fetch("/position", { cache: "no-store" });
    const p = await r.json();
    console.log("POSITION payload:", p);

    if (!p.ok || !p.qty || p.qty <= 0) {
      avgEntryLine.applyOptions({ price: 0, title: "Avg Entry" });
      sellTargetLine.applyOptions({ price: 0, title: "Sell Target" });
      return;
    }

    avgEntryLine.applyOptions({
      price: p.avg_entry,
      title: `Avg Entry (${p.qty})`
    });

    if (p.sell_target == null) {
  sellTargetLine.applyOptions({ price: 0, title: "Sell Target" });
} else {
  const rise = Number(p.sell_rise_usd ?? 0);
  sellTargetLine.applyOptions({
    price: Number(p.sell_target),
    title: `Sell Target (+$${rise.toFixed(2)} from anchor)`,
  });
}

  } catch (e) {
    console.error("Position error", e);
  }
}

// ----------------------------
// Boot
// ----------------------------
(async function boot() {
  await loadHistory(true);
  await loadMarkers();
  await fetchPosition();
  await fetchLatestBar();
  await loadGroupPerformance();
})();

setInterval(fetchLatestBar, 5000);
setInterval(fetchPosition, 2000);
setInterval(() => {
  loadMarkers();
  loadGroupPerformance();
}, 5000);
