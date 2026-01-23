// ----------------------------
// Alpaca Dashboard - app.js
// Complete version (Central Time)
// ----------------------------

const statusEl = document.getElementById("status");
const barEl = document.getElementById("bar");

// ----------------------------
// Helpers
// ----------------------------

function setStatus(text) {
  statusEl.textContent = text;
}

function fmtTimeChicagoFromEpochSec(tsSec) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    hour: "numeric",
    minute: "2-digit",
    second: undefined,
  }).format(new Date(tsSec * 1000));
}

function nowEpochSec() {
  return Math.floor(Date.now() / 1000);
}

// ----------------------------
// Create chart
// ----------------------------

const chartEl = document.getElementById("chart");

const chart = LightweightCharts.createChart(chartEl, {
  layout: { background: { color: "#0e1117" }, textColor: "#d1d4dc" },
  grid: {
    vertLines: { color: "#1f2430" },
    horzLines: { color: "#1f2430" },
  },
  timeScale: {
    timeVisible: true,
    secondsVisible: false,
    rightOffset: 10, // breathing room from price scale
  },
  rightPriceScale: {
    borderColor: "#1f2430",
  },

  // Force time labels to Central Time
  localization: {
    timeFormatter: (time) => {
      // Lightweight Charts provides epoch seconds for business-day/time scale
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

// Candles series
const candles = chart.addSeries(LightweightCharts.CandlestickSeries, {
  upColor: "#26a69a",
  downColor: "#ef5350",
  borderUpColor: "#26a69a",
  borderDownColor: "#ef5350",
  wickUpColor: "#26a69a",
  wickDownColor: "#ef5350",
});

// ----------------------------
// Resize handling (fixes missing bottom time axis)
// ----------------------------

function resizeChart() {
  chart.applyOptions({
    width: chartEl.clientWidth,
    height: chartEl.clientHeight,
  });
}

window.addEventListener("resize", resizeChart);
setTimeout(resizeChart, 0);

// ----------------------------
// Avg Entry Line (only if position exists)
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

let lastBarTime = null; // epoch seconds of last closed bar we displayed
let lastSymbol = "—";
let lastFeed = "—";
let lastHistoryCount = 0;

// ----------------------------
// Load History
// ----------------------------

async function loadHistory() {
  setStatus("loading history…");

  try {
    const r = await fetch("/bars?limit=300", { cache: "no-store" });
    const data = await r.json();

    if (!data.ok) {
      setStatus("no data");
      return;
    }

    lastSymbol = data.symbol;
    lastFeed = data.feed;
    lastHistoryCount = data.bars?.length || 0;

    candles.setData(data.bars);
    chart.timeScale().fitContent();

    const last = data.bars[data.bars.length - 1];
    lastBarTime = last?.time ?? lastBarTime;

    setStatus(
      `${lastSymbol} ${lastFeed} | bars: ${lastHistoryCount} | last: ${fmtTimeChicagoFromEpochSec(
        last.time
      )}`
    );
  } catch (e) {
    console.error("loadHistory error:", e);
    setStatus("history error");
  }
}

// ----------------------------
// Position Line
// ----------------------------

async function fetchPosition() {
  try {
    const r = await fetch("/position", { cache: "no-store" });
    const p = await r.json();

    // Put position JSON into debug panel (append-friendly)
    // We'll show both bar + position in the panel.
    const existing = barEl.textContent ? JSON.parse(barEl.textContent) : {};
    const merged = { ...existing, position: p };
    barEl.textContent = JSON.stringify(merged, null, 2);

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
    // Don't remove line on transient errors
    console.error("fetchPosition error:", e);
  }
}

// ----------------------------
// Live Bar Updates (poll latest closed bar)
// ----------------------------

async function fetchLatestBar() {
  try {
    const r = await fetch("/latest_bar", { cache: "no-store" });
    const data = await r.json();

    if (!data.ok) return;

    lastSymbol = data.symbol || lastSymbol;
    lastFeed = data.feed || lastFeed;

    const barTime = Math.floor(new Date(data.t).getTime() / 1000);

    // Debug panel: latest bar
    const existing = barEl.textContent ? JSON.parse(barEl.textContent) : {};
    const merged = { ...existing, latest_bar: data };
    barEl.textContent = JSON.stringify(merged, null, 2);

    // Only update when a new closed bar arrives
    if (barTime === lastBarTime) {
      // Still update status with staleness
      const age = nowEpochSec() - barTime;
      const staleTag = age > 120 ? ` | STALE (${age}s)` : "";
      setStatus(
        `${lastSymbol} ${lastFeed} | bars: ${lastHistoryCount} | last: ${fmtTimeChicagoFromEpochSec(
          barTime
        )}${staleTag}`
      );
      return;
    }

    lastBarTime = barTime;

    candles.update({
      time: barTime,
      open: data.o,
      high: data.h,
      low: data.l,
      close: data.c,
    });

    const age = nowEpochSec() - barTime;
    const staleTag = age > 120 ? ` | STALE (${age}s)` : "";

    setStatus(
      `${lastSymbol} ${lastFeed} | bars: ${lastHistoryCount} | last: ${fmtTimeChicagoFromEpochSec(
        barTime
      )}${staleTag}`
    );
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

// Polling intervals (safe defaults)
setInterval(fetchLatestBar, 1000);
setInterval(fetchPosition, 2000);

// History refresh less often (heavy call)
setInterval(loadHistory, 60000);
