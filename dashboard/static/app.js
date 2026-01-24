// ------------------------------------
// Alpaca Dashboard - app.js (Complete)
// Fix time axis + Chicago time + sane status
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
  timeScale: {
    timeVisible: true,
    secondsVisible: false,
    rightOffset: 10,
    borderVisible: true,
    ticksVisible: true, // <-- IMPORTANT: forces bottom tick labels
  },
  localization: {
    timeFormatter: (time) => {
      // For intraday, LightweightCharts gives epoch seconds
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

const candles = chart.addSeries(LightweightCharts.CandlestickSeries, {
  upColor: "#26a69a",
  downColor: "#ef5350",
  borderUpColor: "#26a69a",
  borderDownColor: "#ef5350",
  wickUpColor: "#26a69a",
  wickDownColor: "#ef5350",
});

// Add a tiny bottom margin so the time scale never gets “squeezed”
candles.applyOptions({
  priceScaleId: "right",
});

// ----------------------------
// Resize handling (stronger)
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

// ----------------------------
// State + Debug
// ----------------------------

let lastBarTime = null;
let lastSymbol = "—";
let lastFeed = "—";
let historyCount = 0;

const debugState = {};

// ----------------------------
// Load History
// ----------------------------

async function loadHistory() {
  setStatus("loading history…");

  try {
    const r = await fetch("/bars?limit=300", { cache: "no-store" });
    const data = await r.json();

    debugState.history = { ok: data.ok, symbol: data.symbol, feed: data.feed, bars: data.bars?.length || 0 };
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
    lastBarTime = last?.time ?? lastBarTime;

    // Force a resize after data is set (this often restores the bottom axis)
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
      candles.update({
        time: barTime,
        open: data.o,
        high: data.h,
        low: data.l,
        close: data.c,
      });
    }

    // Status: only call it STALE during market hours
    const age = nowEpochSec() - barTime;
    const marketOpen = isMarketOpenChicagoNow();

    let suffix = "";
    if (marketOpen && age > 120) suffix = ` | STALE (${age}s)`;
    if (!marketOpen) suffix = ` | Market closed`;

    setStatus(`${lastSymbol} ${lastFeed} | bars: ${historyCount} | last: ${fmtChicago(barTime)}${suffix}`);
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

setInterval(fetchLatestBar, 1000);
setInterval(fetchPosition, 2000);
setInterval(loadHistory, 60000);
