// ------------------------------------
// Alpaca Dashboard - app.js (CLEAN, CORRECTED, UPGRADED)
// Chicago/Central time + crosshair time label + OHLC readout
// + Avg Entry line (gold) + Sell Target line (blue)
// + BUY/SELL markers (from /fills)
// + BUY numbering resets ONLY when position size returns to ZERO
// + Right padding controls (Pad +/-) to pull candles away from price scale
// + Ruler tool (measure price/time between 2 points)
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

function fmtSigned(n, decimals = 2) {
  if (!Number.isFinite(n)) return "—";
  const s = n >= 0 ? "+" : "";
  return s + n.toFixed(decimals);
}

// ----------------------------
// Group Performance
// ----------------------------
async function loadGroupPerformance() {
  const gpEl = document.getElementById("gp");
  const tableEl = document.getElementById("groupTable");
  if (!gpEl || !tableEl) return;

  try {
    const r = await fetch("/group_performance?limit=50", { cache: "no-store" });
    const j = await r.json();

    if (!j || j.ok === false) {
      gpEl.textContent = "GP: (error)";
      tableEl.innerHTML = `<tbody><tr><td>${(j && j.error) ? j.error : "Unknown error"}</td></tr></tbody>`;
      return;
    }

    const rows = Array.isArray(j.rows) ? j.rows : [];

    const openCount = rows.filter(x => (x.cycle_status || "").toUpperCase() === "OPEN").length;
    const closedCount = rows.filter(x => (x.cycle_status || "").toUpperCase() === "CLOSED").length;

    // backend sorts DESC by cycle_last_utc -> rows[0] is most recent
    const lastClosed = rows.find(x => (x.cycle_status || "").toUpperCase() === "CLOSED") || null;
    const lastPnl = lastClosed && typeof lastClosed.pnl === "number" ? lastClosed.pnl : null;
    const lastPct = lastClosed && typeof lastClosed.pnl_pct === "number" ? lastClosed.pnl_pct : null;

    gpEl.textContent =
      `GP: OPEN ${openCount} | CLOSED ${closedCount}` +
      (lastPnl !== null ? ` | Last PnL ${lastPnl.toFixed(2)}` : "") +
      (lastPct !== null ? ` (${lastPct.toFixed(2)}%)` : "");

    if (!rows.length) {
      tableEl.innerHTML = "<tbody><tr><td>No group rows yet.</td></tr></tbody>";
      return;
    }

    const columns = [
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
    for (const col of columns) html += `<th>${col}</th>`;
    html += "</tr></thead><tbody>";

    for (const x of rows.slice(0, 50)) {
      html += "<tr>";
      for (const col of columns) {
        let val = x[col];

        if (val == null) val = "";

        if (typeof val === "number") {
          if (col === "pnl_pct") val = val.toFixed(2) + "%";
          else if (col.includes("price") || col === "pnl") val = val.toFixed(2);
          else val = String(val);
        }

        html += `<td>${val}</td>`;
      }
      html += "</tr>";
    }

    html += "</tbody>";
    tableEl.innerHTML = html;

  } catch (e) {
    gpEl.textContent = "GP: (exception)";
    tableEl.innerHTML = `<tbody><tr><td>${String(e)}</td></tr></tbody>`;
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
    rightOffset: 12,
    barSpacing: 8,
    borderVisible: true,
    ticksVisible: true,
    fixRightEdge: false,
    lockVisibleTimeRangeOnResize: false,
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

const candles = chart.addSeries(LightweightCharts.CandlestickSeries, {
  upColor: "#26a69a",
  downColor: "#ef5350",
  borderUpColor: "#26a69a",
  borderDownColor: "#ef5350",
  wickUpColor: "#26a69a",
  wickDownColor: "#ef5350",
});

// Markers support (v4 uses series.setMarkers, v5 uses createSeriesMarkers)
const markersLayer =
  typeof candles.setMarkers === "function"
    ? { set: (ms) => candles.setMarkers(ms) }
    : typeof LightweightCharts.createSeriesMarkers === "function"
      ? (() => {
          const p = LightweightCharts.createSeriesMarkers(candles, []);
          return { set: (ms) => p.setMarkers(ms) };
        })()
      : null;

// Price lines
const avgEntryLine = candles.createPriceLine({
  price: 0,
  color: "#f5c542",
  lineWidth: 2,
  lineStyle: 2,
  axisLabelVisible: true,
  title: "Avg Entry",
});

const sellTargetLine = candles.createPriceLine({
  price: 0,
  color: "#4aa3ff",
  lineWidth: 2,
  lineStyle: 2,
  axisLabelVisible: true,
  title: "Sell Target",
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
statusEl.parentElement.appendChild(toggleBtn);

// ----------------------------
// UI: Right-padding controls
// ----------------------------
let RIGHT_PAD = 12;

function applyRightPad() {
  chart.timeScale().applyOptions({ rightOffset: RIGHT_PAD });
}

const padMinusBtn = document.createElement("button");
padMinusBtn.textContent = "Pad −";
padMinusBtn.style.marginLeft = "8px";
padMinusBtn.style.padding = "6px 10px";
padMinusBtn.style.borderRadius = "8px";
padMinusBtn.style.border = "1px solid #1f2430";
padMinusBtn.style.background = "#0e1117";
padMinusBtn.style.color = "#d1d4dc";
padMinusBtn.style.cursor = "pointer";
padMinusBtn.title = "Decrease right padding";

const padPlusBtn = document.createElement("button");
padPlusBtn.textContent = "Pad +";
padPlusBtn.style.marginLeft = "6px";
padPlusBtn.style.padding = "6px 10px";
padPlusBtn.style.borderRadius = "8px";
padPlusBtn.style.border = "1px solid #1f2430";
padPlusBtn.style.background = "#0e1117";
padPlusBtn.style.color = "#d1d4dc";
padPlusBtn.style.cursor = "pointer";
padPlusBtn.title = "Increase right padding";

statusEl.parentElement.appendChild(padMinusBtn);
statusEl.parentElement.appendChild(padPlusBtn);

padMinusBtn.onclick = () => {
  RIGHT_PAD = Math.max(0, RIGHT_PAD - 2);
  applyRightPad();
};

padPlusBtn.onclick = () => {
  RIGHT_PAD = Math.min(80, RIGHT_PAD + 2);
  applyRightPad();
};

// Keyboard shortcuts: [ = less pad, ] = more pad
window.addEventListener("keydown", (e) => {
  if (e.key === "[") {
    RIGHT_PAD = Math.max(0, RIGHT_PAD - 2);
    applyRightPad();
  }
  if (e.key === "]") {
    RIGHT_PAD = Math.min(80, RIGHT_PAD + 2);
    applyRightPad();
  }
});

applyRightPad();

toggleBtn.onclick = async () => {
  HISTORY_MODE = !HISTORY_MODE;
  toggleBtn.textContent = HISTORY_MODE ? "History: ON" : "History: OFF";

  await loadHistory(true);
  await loadMarkers();
  await fetchPosition();
};

// ----------------------------
// RULER TOOL
// ----------------------------
let RULER_MODE = false;
let rulerA = null;          // { time, price }
let rulerB = null;          // { time, price }
let rulerLocked = false;

const rulerLine = chart.addSeries(LightweightCharts.LineSeries, {
  lineWidth: 2,
  priceLineVisible: false,
  lastValueVisible: false,
});

const rulerBox = document.createElement("div");
rulerBox.style.position = "absolute";
rulerBox.style.right = "12px";
rulerBox.style.top = "60px";
rulerBox.style.zIndex = "12";
rulerBox.style.padding = "6px 10px";
rulerBox.style.borderRadius = "8px";
rulerBox.style.border = "1px solid #1f2430";
rulerBox.style.background = "rgba(0,0,0,0.65)";
rulerBox.style.backdropFilter = "blur(6px)";
rulerBox.style.fontFamily =
  'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace';
rulerBox.style.fontSize = "12px";
rulerBox.style.whiteSpace = "nowrap";
rulerBox.style.display = "none";
rulerBox.textContent = "Ruler: —";
chartEl.parentElement.appendChild(rulerBox);

const rulerBtn = document.createElement("button");
rulerBtn.textContent = "Ruler: OFF";
rulerBtn.style.marginLeft = "8px";
rulerBtn.style.padding = "6px 10px";
rulerBtn.style.borderRadius = "8px";
rulerBtn.style.border = "1px solid #1f2430";
rulerBtn.style.background = "#0e1117";
rulerBtn.style.color = "#d1d4dc";
rulerBtn.style.cursor = "pointer";
rulerBtn.title = "Toggle ruler tool (click 2 points to measure)";
statusEl.parentElement.appendChild(rulerBtn);

function clearRuler() {
  rulerA = null;
  rulerB = null;
  rulerLocked = false;
  rulerLine.setData([]);
  if (RULER_MODE) rulerBox.textContent = "Ruler: click point A…";
}

rulerBtn.onclick = () => {
  RULER_MODE = !RULER_MODE;

  rulerBtn.textContent = RULER_MODE ? "Ruler: ON" : "Ruler: OFF";
  rulerBox.style.display = RULER_MODE ? "block" : "none";

  if (!RULER_MODE) {
    clearRuler();
    rulerBox.textContent = "Ruler: —";
  } else {
    clearRuler();
    rulerBox.textContent = "Ruler: click point A…";
  }
};

window.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && RULER_MODE) {
    clearRuler();
  }
});

function pointFromParam(param) {
  if (!param || !param.time) return null;

  const tsSec = typeof param.time === "number" ? param.time : param.time?.timestamp;
  if (!tsSec) return null;

  const sd = param.seriesData?.get?.(candles);
  if (!sd) return null;

  const price = Number(sd.close);
  if (!Number.isFinite(price)) return null;

  return { time: tsSec, price };
}

function updateRulerVisuals() {
  if (!RULER_MODE || !rulerA || !rulerB) return;

  rulerLine.setData([
    { time: rulerA.time, value: rulerA.price },
    { time: rulerB.time, value: rulerB.price },
  ]);

  const dp = rulerB.price - rulerA.price;
  const pct = (dp / rulerA.price) * 100;

  const dtSec = Math.abs(rulerB.time - rulerA.time);
  const dtMin = dtSec / 60;
  const bars = Math.round(dtMin);

  rulerBox.textContent =
    `ΔPrice ${fmtSigned(dp, 2)}  |  Δ% ${fmtSigned(pct, 2)}%  |  ` +
    `ΔTime ${dtMin.toFixed(1)}m  |  Bars ${bars}`;
}

// ----------------------------
// BUY/SELL markers
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

    const fills = [...data.fills].sort(
      (a, b) => new Date(a.filled_at) - new Date(b.filled_at)
    );

    let runningQty = 0;
    let buyCount = 0;

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
        runningQty = Math.max(0, runningQty - qty);

        markers.push({
          time: t,
          position: "aboveBar",
          shape: "arrowDown",
          color: "#ef4444",
          text: `S ${qty}@${px.toFixed(2)}`,
        });

        if (runningQty === 0) buyCount = 0;
      }
    }

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
// Resize Observer
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
let lastBarTime = null;
let lastBarObj = null;
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
  const seriesData = param.seriesData?.get?.(candles);
  if (!seriesData) return;

  setReadoutFromBar(tsSec, seriesData);

  if (RULER_MODE && rulerA && !rulerLocked) {
    const pt = pointFromParam(param);
    if (pt) {
      rulerB = pt;
      updateRulerVisuals();
    }
  }
});

// ----------------------------
// Ruler clicks
// ----------------------------
chart.subscribeClick((param) => {
  if (!RULER_MODE) return;

  const pt = pointFromParam(param);
  if (!pt) return;

  if (rulerA && rulerB && rulerLocked) {
    clearRuler();
    rulerA = pt;
    rulerBox.textContent = "Ruler: move to preview B, click to lock…";
    return;
  }

  if (!rulerA) {
    rulerA = pt;
    rulerLocked = false;
    rulerBox.textContent = "Ruler: move to preview B, click to lock…";
    return;
  }

  rulerB = pt;
  rulerLocked = true;
  updateRulerVisuals();
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
      limit,
    };
    barEl.textContent = JSON.stringify(debugState, null, 2);

    if (!data.ok || !data.bars?.length) {
      setStatus("no history");
      return;
    }

    lastSymbol = data.symbol;
    lastFeed = data.feed;
    historyCount = data.bars.length;

    // Normalize bars to epoch seconds in `time`
    const bars = (data.bars || []).map((b) => {
      let t = b.time;

      if (t && typeof t === "object" && typeof t.timestamp === "number") {
        t = t.timestamp;
      }

      if (typeof t === "string") {
        const parsed = Math.floor(new Date(t).getTime() / 1000);
        if (Number.isFinite(parsed)) t = parsed;
      }

      if (typeof t === "number" && t > 2_000_000_000_000) {
        t = Math.floor(t / 1000);
      }

      if (typeof t === "number") {
        t = Math.floor(t / 60) * 60;
      }

      return {
        time: t,
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
      };
    });

    candles.setData(bars);

    if (refit) {
      chart.timeScale().fitContent();
      applyRightPad();
    }

    const last = bars[bars.length - 1];
    lastBarTime = last.time;
    lastBarObj = { open: last.open, high: last.high, low: last.low, close: last.close };
    setReadoutFromBar(lastBarTime, lastBarObj);

    resizeChart();

    setStatus(
      `${lastSymbol} ${lastFeed} | bars: ${historyCount} | last: ${fmtChicago(last.time)} | ${HISTORY_MODE ? "HISTORY" : "LIVE"}`
    );
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
      price: Number(p.avg_entry),
      title: `Avg Entry (${p.qty})`,
    });

    // Sell target shown as anchor + SELL_RISE_USD
    const rise = Number(p.sell_rise_usd ?? 0);

    if (p.sell_target == null) {
      sellTargetLine.applyOptions({ price: 0, title: "Sell Target" });
    } else {
      sellTargetLine.applyOptions({
        price: Number(p.sell_target),
        title: `Sell Target (+$${rise.toFixed(2)} from anchor)`,
      });
    }
  } catch (e) {
    console.error("fetchPosition failed", e);
  }
}

// ----------------------------
// Latest closed bar
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

    let barTimeSec;

    // Normalize backend time into epoch seconds
    if (typeof data.t === "number") {
      barTimeSec = data.t;
    } else if (typeof data.t === "string") {
      barTimeSec = Math.floor(new Date(data.t).getTime() / 1000);
    } else if (data.t && typeof data.t.timestamp === "number") {
      barTimeSec = data.t.timestamp;
    } else {
      return; // can't use this bar
    }
    
    const barTimeMin = Math.floor(barTimeSec / 60) * 60;

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

      loadMarkers().catch(() => {});
      loadGroupPerformance().catch(() => {});
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
const LATEST_BAR_POLL_MS = 5000;

(async function boot() {
  await loadHistory(true);
  await loadMarkers();
  await fetchPosition();
  await fetchLatestBar();
  await loadGroupPerformance();
})();

setInterval(fetchLatestBar, LATEST_BAR_POLL_MS);
setInterval(fetchPosition, 2000);

setInterval(() => {
  loadMarkers();
  loadGroupPerformance();
}, 5000);
