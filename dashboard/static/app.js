// ------------------------------------
// Alpaca Dashboard - app.js (Complete)
// Chicago/Central time + resize fix
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

// For status text (date + time in Chicago)
function fmtChicago(tsSec) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    month: "numeric",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(tsSec * 1000));
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
  rightPriceScale: {
    borderColor: "#1f2430",
  },
  timeScale: {
    timeVisible: true,
    secondsVisible: false,
    rightOffset: 10, // space so last candle isn't glued to the price scale
  },

  // Force axis labels to Chicago time
  localization: {
    timeFormatter: (time) => {
      // Lightweight Charts passes epoch seconds for intraday bars
      const tsSec = typeof time === "number" ? time : time?.timestamp;
      const t = typeof tsSec === "number" ? tsSec : 0;

      return new Intl.DateTimeFormat("en-US", {
        timeZone: "America/Chicago",
        hour: "numeric",
        minute: "2-digit",
      }).format(new Date(t * 1000));
    },
  },
});

// Candlestick series
const candles = chart.addSeries(LightweightCharts.CandlestickSeries, {
  upColor: "#26a69a",
  downColor: "#ef5350",
  borderUpColor: "#26a69a",
  borderDownColor: "#ef5350",
  wickUpColor: "#26a69a",
  wickDownColor: "#ef5350",
});

// ----------------------------
// IMPORTANT: ResizeObserver
// Fixes "no time scale at bottom" issues
// ----------------------------

function resizeChart() {
  chart.applyOptions({
    width: chartEl.clientWidth,
    height: chartEl.clientHeight,
  });
}

const ro = new ResizeObserver(() => resizeChart());
ro.observe(chartEl);

// Initial sizing after layout settles
requestAnimationFrame(() => resizeChart());
setTimeout(() => resizeChart(), 250);

// ----------------------------
// Avg Entry Price Line (only if position exists)
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

// ----------------------------
// State
// ----------------------------

let lastBarTime = null;        // epoch sec of last closed bar applied to chart
let lastSymbol = "—";
let lastFeed = "—";
let historyCount = 0;

// Debug panel content as an object (so we don't JSON.parse arbitrary text)
const debugState = {};

// ----------------------------
// Load History
// ----------------------------

async function loadHistory() {
  setStatus("loading history…");

  try {
    const r = await fetch("/bars?limit=300", { cache: "no-store" });
    const data = await r.json();

    debugState.history_meta = { ok: data.ok, symbol: data.symbol, feed: data.feed, bars: data.bars?.length };
    barEl.textContent = JSON.stringify(debugState, null, 2);

    if (!data.ok || !data.bars || data.bars.length === 0) {
      setStatus("no history");
      return;
    }

    lastSymbol = data.symbol;
    lastFeed = data.feed;
    historyCount = data.bars.length;

    candles.setData(data.bars);
    chart.timeScale().fitContent();

    const last = data.bars[data.bars.length - 1];
    lastBarTime = last?.time ?? lastBarTime;

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

    debugState.position = p;
    barEl.textContent = JSON.stringify(debugState, null, 2);

    if (!p.ok || !p.qty || p.qty <= 0) {
      removeAvgEntryLine();
      return;
    }

    const line = ensureAvgEntryLine();
    line.applyOptions({
      price: p.avg_entry,
      title: `Avg Entry (${p.qty})`,
    });
  } catch (e) {
    console.error("fetchPosition error:", e);
  }
}

// ----------------------------
// Latest closed bar (polling)
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

    // Always update status (even if bar hasn't changed)
    const age = nowEpochSec() - barTime;
    const staleTag = age > 120 ? ` | STALE (${age}s)` : "";

    // If it's a new closed bar, update the series
    if (barTime !== lastBarTime) {
      lastBarTime = barTime;

      candles.update({
        time: barTime,
        open: data.o,
        high: data.h,
        low: data.l,
        close: data.c,
      });
    }

    setStatus(`${lastSymbol} ${lastFeed} | bars: ${historyCount} | last: ${fmtChicago(barTime)}${staleTag}`);
  } catch (e) {
    console.error("fetchLatestBar error:", e);
  }
}

// ----------------------------
// Start
// ----------------------------

loadHistory();
fetchPosition();
fetchLatestBar();

// Polling intervals
setInterval(fetchLatestBar, 1000);
setInterval(fetchPosition, 2000);

// History refresh (heavy) - keep it slower
setInterval(loadHistory, 60000);
