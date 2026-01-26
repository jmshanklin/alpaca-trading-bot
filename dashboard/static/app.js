// ------------------------------------
// Alpaca Dashboard - app.js (Complete)
// Chicago/Central time + crosshair time label + OHLC readout
// ------------------------------------

const statusEl = document.getElementById("status");
const barEl = document.getElementById("bar");
const chartEl = document.getElementById("chart");

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

// Crosshair display: time in Chicago (include seconds if you want)
function fmtChicagoTime(tsSec) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
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

  const wk = parts.find(p => p.type === "weekday")?.value || "";
  const hour = parseInt(parts.find(p => p.type === "hour")?.value || "0", 10);
  const minute = parseInt(parts.find(p => p.type === "minute")?.value || "0", 10);

  const isWeekday = ["Mon","Tue","Wed","Thu","Fri"].includes(wk);
  if (!isWeekday) return false;

  const mins = hour * 60 + minute;
  const open = 8 * 60 + 30;   // 08:30
  const close = 15 * 60 + 0;  // 15:00

  return mins >= open && mins < close;
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

  // ✅ This is the key: make sure the time scale is drawn and has room
  timeScale: {
    timeVisible: true,
    secondsVisible: false,
    rightOffset: 10,
    borderVisible: true,
    ticksVisible: true,
  },

  // ✅ This is the key: bottom label on crosshair must be enabled
  crosshair: {
    mode: 1,
    vertLine: {
      visible: true,
      labelVisible: true,  // <-- this is the bottom time label
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

  // ✅ Chicago formatting, but do it via "localization" (works for axis labels)
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

// ----------------------------
// ResizeObserver (stabilizes layout + time scale)
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
// OHLC + Time readout (top-left overlay)
// This restores the “timestamp while strafing” feel.
// ----------------------------

const readout = document.createElement("div");
readout.style.position = "absolute";
readout.style.left = "12px";
readout.style.top = "60px"; // below top bar
readout.style.zIndex = "10";
readout.style.padding = "6px 8px";
readout.style.borderRadius = "6px";
readout.style.border = "1px solid #1f2430";
readout.style.background = "rgba(0,0,0,0.65)";
readout.style.backdropFilter = "blur(6px)";
readout.style.fontFamily = 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace';
readout.style.fontSize = "16px";        // bigger text
readout.style.fontWeight = "600";      // slightly bolder
readout.style.letterSpacing = "0.5px"; // improves readability
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
// Avg Entry Line (only when position exists)
// ----------------------------

let avgEntryLine = null;

function ensureAvgEntryLine() {
  if (avgEntryLine) return avgEntryLine;
  avgEntryLine = candles.createPriceLine({
    price: 0,
    color: "#f5c542",
    lineWidth: 2,
    lineStyle: 2,
    axisLabelVisible: true,
    title: "Avg Entry",
  });
  return avgEntryLine;
}

function removeAvgEntryLine() {
  if (!avgEntryLine) return;
  candles.removePriceLine(avgEntryLine);
  avgEntryLine = null;
}

// Sell target line (Avg Entry * (1 + SELL_PCT))
const sellTargetLine = candles.createPriceLine({
  price: 0,
  color: "#4da3ff",
  lineWidth: 2,
  lineStyle: 2,
  axisLabelVisible: true,
  title: "Sell Target"
});

// ----------------------------
// State + Debug
// ----------------------------

let lastBarTime = null;   // epoch sec
let lastBarObj = null;    // {open,high,low,close}
let lastSymbol = "—";
let lastFeed = "—";
let historyCount = 0;

const debugState = {};

// ----------------------------
// Crosshair subscription (THIS is the key part)
// ----------------------------

chart.subscribeCrosshairMove((param) => {
  // If mouse is off the chart, revert readout to last known bar
  if (!param || !param.time) {
    if (lastBarTime && lastBarObj) setReadoutFromBar(lastBarTime, lastBarObj);
    return;
  }

  // param.time is epoch seconds for intraday data
  const tsSec = typeof param.time === "number" ? param.time : param.time?.timestamp;

  // Get candle bar at crosshair time
  const seriesData = param.seriesData.get(candles);
  if (!seriesData) return;

  // seriesData has {open, high, low, close}
  setReadoutFromBar(tsSec, seriesData);
});

// ----------------------------
// Load History
// ----------------------------

async function loadHistory() {
  setStatus("loading history…");

  try {
    const r = await fetch("/bars?limit=300", { cache: "no-store" });
    const data = await r.json();

    debugState.history = {
      ok: data.ok,
      symbol: data.symbol,
      feed: data.feed,
      bars: data.bars?.length || 0,
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
    chart.timeScale().fitContent();

    const last = data.bars[data.bars.length - 1];
    lastBarTime = last.time;
    lastBarObj = { open: last.open, high: last.high, low: last.low, close: last.close };
    setReadoutFromBar(lastBarTime, lastBarObj);

    resizeChart();

    setStatus(`${lastSymbol} ${lastFeed} | bars: ${historyCount} | last: ${fmtChicago(last.time)}`);
  } catch (e) {
    console.error("loadHistory error:", e);
    setStatus("history error");
  }
}

// ----------------------------
// Position
// ----------------------------

async function fetchPosition() {
  try {
    const r = await fetch("/position", { cache: "no-store" });
    const p = await r.json();

    if (!p.ok || !p.qty || p.qty <= 0) {
      avgEntryLine.applyOptions({ price: 0, title: "Avg Entry (flat)" });
      sellTargetLine.applyOptions({ price: 0, title: "Sell Target (flat)" });
      return;
    }

    avgEntryLine.applyOptions({
      price: p.avg_entry,
      title: `Avg Entry (${p.qty})`
    });

    // Sell target = avg entry * (1 + sell_pct)
    sellTargetLine.applyOptions({
      price: p.sell_target,
      title: `Sell Target (+${(p.sell_pct * 100).toFixed(2)}%)`
    });

  } catch {}
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

    const barTime = Math.floor(new Date(data.t).getTime() / 1000);

    // Update series only when new closed bar arrives
    if (barTime !== lastBarTime) {
      lastBarTime = barTime;
      lastBarObj = { open: data.o, high: data.h, low: data.l, close: data.c };

      candles.update({
        time: barTime,
        open: data.o,
        high: data.h,
        low: data.l,
        close: data.c,
      });

      // If user isn't hovering, keep readout on latest bar
      setReadoutFromBar(lastBarTime, lastBarObj);
    }

    const age = nowEpochSec() - barTime;
    const marketOpen = isMarketOpenChicagoNow();

    let suffix = marketOpen ? (age > 120 ? ` | STALE (${age}s)` : "") : " | Market closed";

    setStatus(`${lastSymbol} ${lastFeed} | bars: ${historyCount} | last: ${fmtChicago(barTime)}${suffix}`);
  } catch (e) {
    console.error("fetchLatestBar error:", e);
  }
}

// ----------------------------
// Start
// ----------------------------
const LATEST_BAR_POLL_MS = 15000; // 15 seconds (1-minute candles don't need 1-second polling)

loadHistory();
fetchPosition();
fetchLatestBar();

setInterval(fetchLatestBar, LATEST_BAR_POLL_MS);
setInterval(fetchPosition, 2000);
setInterval(loadHistory, 60000);
