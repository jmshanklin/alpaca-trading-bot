// ------------------------------------
// Alpaca Dashboard - app.js (CLEAN, CORRECTED, UPGRADED)
// Chicago/Central time + crosshair time label + OHLC readout
// + Avg Entry line (gold) + Sell Target line (blue)
// + BUY/SELL markers (from /fills)
// + BUY numbering resets ONLY when position size returns to ZERO
// ------------------------------------

const statusEl = document.getElementById("status");
const barEl = document.getElementById("bar");
const chartEl = document.getElementById("chart");

// ----------------------------
// Mode: LIVE vs HISTORY
// ----------------------------
let HISTORY_MODE = false;

// Tune these:
const LIVE_BAR_LIMIT = 300;
const HISTORY_BAR_LIMIT = 1500; // try 1500 first; later 3000 if Render/browser can handle it

// ----------------------------
// Helpers
// ----------------------------
function setStatus(text) {
  statusEl.textContent = text;
}

function nowEpochSec() {
  return Math.floor(Date.now() / 1000);
}

// Status display: date + time in Chicago
function fmtChicago(tsSec) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    month: "numeric",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(tsSec * 1000));
}

// Market hours (Chicago): 8:30–15:00 CT, Mon–Fri
function isMarketOpenChicagoNow() {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(now);

  const wk = parts.find((p) => p.type === "weekday")?.value || "";
  const hour = parseInt(parts.find((p) => p.type === "hour")?.value || "0", 10);
  const minute = parseInt(parts.find((p) => p.type === "minute")?.value || "0", 10);

  const isWeekday = ["Mon", "Tue", "Wed", "Thu", "Fri"].includes(wk);
  if (!isWeekday) return false;

  const mins = hour * 60 + minute;
  const open = 8 * 60 + 30;  // 08:30
  const close = 15 * 60 + 0; // 15:00
  return mins >= open && mins < close;
}

function minuteFloor(tsSec) {
  return Math.floor(tsSec / 60) * 60;
}

function fmtNum(x) {
  if (x === null || x === undefined) return "";
  const n = Number(x);
  if (Number.isNaN(n)) return String(x);
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function fmtPct(x) {
  if (x === null || x === undefined) return "";
  const n = Number(x);
  if (Number.isNaN(n)) return String(x);
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 }) + "%";
}

async function loadGroupPerformance() {
  const gpEl = document.getElementById("gp");
  const tableEl = document.getElementById("groupTable");
  if (!gpEl || !tableEl) return;

  try {
    const r = await fetch("/group_performance");
    const j = await r.json();

    if (!j || j.ok === false) {
      gpEl.textContent = "GP: (error)";
      tableEl.textContent = JSON.stringify(j, null, 2);
      return;
    }

    // j.rows should be an array of cycles (one row per group_id)
    const rows = Array.isArray(j.rows) ? j.rows : [];

    // --- top-bar mini summary (OPEN/CLOSED counts + last pnl)
    const openCount = rows.filter(x => (x.cycle_status || "").toUpperCase() === "OPEN").length;
    const closedCount = rows.filter(x => (x.cycle_status || "").toUpperCase() === "CLOSED").length;

    // pick most recent row by cycle_last_ct if available, else first row
    const last = rows[0] || null;
    const lastPnl = last && typeof last.pnl === "number" ? last.pnl : null;
    const lastPct = last && typeof last.pnl_pct === "number" ? last.pnl_pct : null;

    gpEl.textContent =
      `GP: OPEN ${openCount} | CLOSED ${closedCount}` +
      (lastPnl !== null ? ` | Last PnL ${lastPnl.toFixed(2)}` : "") +
      (lastPct !== null ? ` (${lastPct.toFixed(2)}%)` : "");

    // --- table area: show a compact text table
    if (!rows.length) {
      tableEl.textContent = "No group rows yet.";
      return;
    }

    // Build monospace “table”
    const header = [
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

    const lines = [];
    lines.push(header.join(" | "));
    lines.push("-".repeat(140));

    for (const x of rows.slice(0, 25)) {
      const line = [
        (x.cycle_status ?? ""),
        (x.win_loss ?? ""),
        (x.cycle_start_ct ?? ""),
        (x.cycle_last_ct ?? ""),
        (x.buy_qty ?? ""),
        (x.avg_buy_price ?? ""),
        (x.sell_qty ?? ""),
        (x.avg_sell_price ?? ""),
        (typeof x.pnl === "number" ? x.pnl.toFixed(2) : (x.pnl ?? "")),
        (typeof x.pnl_pct === "number" ? x.pnl_pct.toFixed(2) : (x.pnl_pct ?? "")),
        (x.group_id ?? "")
      ].join(" | ");
      lines.push(line);
    }

    tableEl.textContent = lines.join("\n");
  } catch (e) {
    gpEl.textContent = "GP: (exception)";
    tableEl.textContent = String(e);
  }
}

// ----------------------------
// Chart
// ----------------------------
const chart = LightweightCharts.createChart(chartEl, {
  layout: {
    background: { color: "#0e1117" },
    textColor: "#d1d4dc",
    fontSize: 12,
  },
  grid: {
    vertLines: { color: "#1f2430" },
    horzLines: { color: "#1f2430" },
  },
  rightPriceScale: {
    borderColor: "#1f2430",
  },
  timeScale: {
    timeVisible: true,
    secondsVisible: false,
    rightOffset: 30,
    borderVisible: true,
    ticksVisible: true,
    fixRightEdge: true,
    lockVisibleTimeRangeOnResize: true,
  },
  crosshair: {
    mode: 1,
    vertLine: {
      visible: true,
      labelVisible: true,
      style: 2,
      width: 1,
      color: "#6b7280",
    },
    horzLine: {
      visible: true,
      labelVisible: true,
      style: 2,
      width: 1,
      color: "#6b7280",
    },
  },
  localization: {
    timeFormatter: (time) => {
      const tsSec = typeof time === "number" ? time : time?.timestamp;
      const t = typeof tsSec === "number" ? tsSec : 0;
      return new Intl.DateTimeFormat("en-US", {
        timeZone: "America/Chicago",
        month: "numeric",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
      }).format(new Date(t * 1000));
    },
  },
});

// ----------------------------
// UI: History Mode toggle button
// ----------------------------
const toggleBtn = document.createElement("button");
toggleBtn.textContent = "History: OFF";
toggleBtn.style.marginLeft = "12px";
toggleBtn.style.padding = "6px 10px";
toggleBtn.style.borderRadius = "8px";
toggleBtn.style.border = "1px solid #1f2430";
toggleBtn.style.background = "#0e1117";
toggleBtn.style.color = "#d1d4dc";
toggleBtn.style.cursor = "pointer";
toggleBtn.title = "Toggle History Mode (loads more 1-min bars)";

// Put it next to your status line
statusEl.parentElement.appendChild(toggleBtn);

toggleBtn.onclick = async () => {
  HISTORY_MODE = !HISTORY_MODE;
  toggleBtn.textContent = HISTORY_MODE ? "History: ON" : "History: OFF";

  // Re-load candles + markers for the selected mode
  await loadHistory(true);        // true = refit
  await loadMarkers();            // refresh markers after reload
  await fetchPosition();          // refresh lines
};

// ✅ Candles series
const candles = chart.addSeries(LightweightCharts.CandlestickSeries, {
  upColor: "#26a69a",
  downColor: "#ef5350",
  borderUpColor: "#26a69a",
  borderDownColor: "#ef5350",
  wickUpColor: "#26a69a",
  wickDownColor: "#ef5350",
});

// ✅ Markers support (v4 uses series.setMarkers, v5 uses createSeriesMarkers)
const markersLayer =
  typeof candles.setMarkers === "function"
    ? { set: (ms) => candles.setMarkers(ms) }
    : typeof LightweightCharts.createSeriesMarkers === "function"
      ? (() => {
          const p = LightweightCharts.createSeriesMarkers(candles, []);
          return { set: (ms) => p.setMarkers(ms) };
        })()
      : null;

// ✅ Price lines (define ONCE, right after candles exists)
const avgEntryLine = candles.createPriceLine({
  price: 0,
  color: "#f5c542", // gold
  lineWidth: 2,
  lineStyle: 2,
  axisLabelVisible: true,
  title: "Avg Entry",
});

const sellTargetLine = candles.createPriceLine({
  price: 0,
  color: "#4aa3ff", // blue
  lineWidth: 2,
  lineStyle: 2,
  axisLabelVisible: true,
  title: "Sell Target",
});

// ----------------------------
// BUY/SELL markers (from /fills)
// - BUYs numbered B1, B2, B3...
// - RESET numbering ONLY when position size returns to 0
// ----------------------------
let lastMarkersHash = "";

function hashMarkers(ms) {
  return ms.map((m) => `${m.time}|${m.position}|${m.text}`).join(";");
}

async function loadMarkers() {
  try {
    const r = await fetch("/fills?limit=500", { cache: "no-store" });
    const data = await r.json();
    if (!data.ok || !Array.isArray(data.fills)) return;
    if (!markersLayer) return;

    // Oldest → newest is REQUIRED for correct “running position” math
    const fills = [...data.fills].sort(
      (a, b) => new Date(a.filled_at) - new Date(b.filled_at)
    );

    let runningQty = 0;   // our reconstructed position size from fills
    let buyCount = 0;     // B1, B2, ... for the current open-position group

    const markers = [];

    for (const f of fills) {
      const ts = Math.floor(new Date(f.filled_at).getTime() / 1000);
      const t = minuteFloor(ts);

      const side = (f.side || "").toLowerCase();
      const isBuy = side === "buy";
      const isSell = side === "sell";

      const qty = Number(f.filled_qty || 0);
      const px = Number(f.filled_avg_price || 0);

      if (!(isBuy || isSell) || !Number.isFinite(qty) || qty <= 0) continue;

      if (isBuy) {
        // If we were flat (0) and a new buy comes in, this is a NEW group.
        if (runningQty === 0) buyCount = 0;

        runningQty += qty;
        buyCount += 1;

        markers.push({
          time: t,
          position: "belowBar",
          shape: "arrowUp",
          color: "#22c55e",
          text: `B${buyCount} ${qty}@${px.toFixed(2)}`,
        });
      }

      if (isSell) {
        // Apply sell to running position
        runningQty = Math.max(0, runningQty - qty);

        markers.push({
          time: t,
          position: "aboveBar",
          shape: "arrowDown",
          color: "#ef4444",
          text: `S ${qty}@${px.toFixed(2)}`,
        });

        // ✅ ONLY reset when the position is fully closed
        if (runningQty === 0) {
          buyCount = 0;
        }
      }
    }

    // Keep markers ordered (helps chart)
    markers.sort((a, b) => a.time - b.time);

    const h = hashMarkers(markers);
    if (h === lastMarkersHash) return;
    lastMarkersHash = h;

    markersLayer.set(markers);
  } catch (e) {
    console.error("loadMarkers failed", e);
  }
}

// ----------------------------
// Resize Observer (stabilizes layout + time scale)
// ----------------------------
function resizeChart() {
  chart.applyOptions({
    width: chartEl.clientWidth,
    height: chartEl.clientHeight,
  });
}

const ro = new ResizeObserver(() => resizeChart());
ro.observe(chartEl);

requestAnimationFrame(resizeChart);
setTimeout(resizeChart, 250);

// ----------------------------
// OHLC readout (top-left overlay)
// ----------------------------
const readout = document.createElement("div");
readout.style.position = "absolute";
readout.style.left = "12px";
readout.style.top = "60px";
readout.style.zIndex = "10";
readout.style.padding = "6px 8px";
readout.style.borderRadius = "6px";
readout.style.border = "1px solid #1f2430";
readout.style.background = "rgba(0,0,0,0.65)";
readout.style.backdropFilter = "blur(6px)";
readout.style.fontFamily =
  'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace';
readout.style.fontSize = "16px";
readout.style.fontWeight = "600";
readout.style.letterSpacing = "0.5px";
readout.style.pointerEvents = "none";
readout.textContent = "—";
chartEl.parentElement.appendChild(readout);

function setReadoutFromBar(tsSec, bar) {
  if (!tsSec || !bar) {
    readout.textContent = "—";
    return;
  }
  readout.textContent =
    `O:${bar.open.toFixed(2)} H:${bar.high.toFixed(2)} ` +
    `L:${bar.low.toFixed(2)} C:${bar.close.toFixed(2)}`;
}

// ----------------------------
// State + Debug
// ----------------------------
let lastBarTime = null; // epoch sec (snapped to minute)
let lastBarObj = null;  // {open,high,low,close}
let lastSymbol = "—";
let lastFeed = "—";
let historyCount = 0;

const debugState = {};

// ----------------------------
// Crosshair subscription
// ----------------------------
chart.subscribeCrosshairMove((param) => {
  if (!param || !param.time) {
    if (lastBarTime && lastBarObj) setReadoutFromBar(lastBarTime, lastBarObj);
    return;
  }

  const tsSec = typeof param.time === "number" ? param.time : param.time?.timestamp;
  const seriesData = param.seriesData.get(candles);
  if (!seriesData) return;

  setReadoutFromBar(tsSec, seriesData);
});

// ----------------------------
// Load History
// ----------------------------
async function loadHistory(refit = false) {
  setStatus("loading history…");

  try {
    const limit = HISTORY_MODE ? HISTORY_BAR_LIMIT : LIVE_BAR_LIMIT;

    const r = await fetch(`/bars?limit=${limit}`, { cache: "no-store" });
    const data = await r.json();

    debugState.history = {
      ok: data.ok,
      symbol: data.symbol,
      feed: data.feed,
      bars: data.bars?.length || 0,
      mode: HISTORY_MODE ? "HISTORY" : "LIVE",
      limit
    };
    barEl.textContent = JSON.stringify(debugState, null, 2);

    if (!data.ok || !data.bars?.length) {
      setStatus("no history");
      return;
    }

    lastSymbol = data.symbol;
    lastFeed = data.feed;
    historyCount = data.bars.length;

    candles.setData(data.bars);

    // IMPORTANT: only refit when you explicitly ask (toggle / first load)
    if (refit) {
      chart.timeScale().fitContent();
    }

    // last bar
    const last = data.bars[data.bars.length - 1];
    lastBarTime = last.time;
    lastBarObj = { open: last.open, high: last.high, low: last.low, close: last.close };
    setReadoutFromBar(lastBarTime, lastBarObj);

    resizeChart();

    setStatus(`${lastSymbol} ${lastFeed} | bars: ${historyCount} | last: ${fmtChicago(last.time)} | ${HISTORY_MODE ? "HISTORY" : "LIVE"}`);
  } catch (e) {
    console.error("loadHistory error:", e);
    setStatus("history error");
  }
}

// ----------------------------
// Position (avg entry + sell target)
// ----------------------------
async function fetchPosition() {
  try {
    const r = await fetch("/position", { cache: "no-store" });
    const p = await r.json();

    if (!p.ok || !p.qty || p.qty <= 0) {
      avgEntryLine.applyOptions({ price: 0, title: "Avg Entry" });
      sellTargetLine.applyOptions({ price: 0, title: "Sell Target" });
      return;
    }

    avgEntryLine.applyOptions({
      price: p.avg_entry,
      title: `Avg Entry (${p.qty})`,
    });

    sellTargetLine.applyOptions({
      price: p.sell_target,
      title: `Sell Target (+${(p.sell_pct * 100).toFixed(3)}%)`,
    });
  } catch (e) {
    console.error("fetchPosition failed", e);
  }
}

// ----------------------------
// Latest closed bar (snapped to minute)
// ----------------------------
async function fetchLatestBar() {
  try {
    const r = await fetch("/latest_bar", { cache: "no-store" });
    const data = await r.json();

    debugState.latest_bar = data;
    barEl.textContent = JSON.stringify(debugState, null, 2);

    if (!data.ok) return;

    lastSymbol = data.symbol || lastSymbol;
    lastFeed = data.feed || lastFeed;

    const barTime = Math.floor(new Date(data.t).getTime() / 1000);
    const barTimeMin = Math.floor(barTime / 60) * 60; // ✅ snap to minute

    if (barTimeMin !== lastBarTime) {
      lastBarTime = barTimeMin;
      lastBarObj = { open: data.o, high: data.h, low: data.l, close: data.c };

      candles.update({
        time: barTimeMin,
        open: data.o,
        high: data.h,
        low: data.l,
        close: data.c,
      });

      setReadoutFromBar(lastBarTime, lastBarObj);

      // Fills/markers often change right after a new bar closes (and after orders fill)
      // so refresh markers when a new bar arrives.
      loadMarkers().catch(() => {});
    }

    const age = nowEpochSec() - barTimeMin;
    const marketOpen = isMarketOpenChicagoNow();
    const suffix = marketOpen ? (age > 120 ? ` | STALE (${age}s)` : "") : " | Market closed";

    setStatus(`${lastSymbol} ${lastFeed} | bars: ${historyCount} | last: ${fmtChicago(barTimeMin)}${suffix}`);
  } catch (e) {
    console.error("fetchLatestBar error:", e);
  }
}

// ----------------------------
// Start
// ----------------------------
const LATEST_BAR_POLL_MS = 5000; // poll 5s so new candle appears quickly

(async function boot() {
  await loadHistory(true);   // fit once on boot
  await loadMarkers();
  await fetchPosition();
  await fetchLatestBar();
  await loadGroupPerformance();   // <-- RUN ON BOOT
})();

setInterval(fetchLatestBar, LATEST_BAR_POLL_MS);
setInterval(fetchPosition, 2000);

setInterval(() => {
  loadMarkers();
  loadGroupPerformance();   // <-- REFRESH EVERY 5s
}, 5000);

// IMPORTANT: do NOT keep reloading history every 60s
// setInterval(loadHistory, 60000);
