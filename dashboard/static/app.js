// ------------------------------------
// Alpaca Dashboard - app.js (CLEAN, UPDATED)
// Chicago/Central time + crosshair time label + OHLC readout
// + Avg Entry line (gold) + Sell Target line (blue)
// + BUY/SELL markers (from /fills)
// ------------------------------------

const statusEl = document.getElementById("status");
const barEl = document.getElementById("bar");
const chartEl = document.getElementById("chart");

// ----------------------------
//  Helpers
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
    fixRightEdge: true,   // keeps the last candle anchored nicely
    lockVisibleTimeRangeOnResize: true // reduces weird jumps on resize
  },
  crosshair: {
    mode: 1,
    vertLine: {
      visible: true,
      labelVisible: true, // bottom time label
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
const markersLayer = (typeof candles.setMarkers === "function")
  ? { set: (ms) => candles.setMarkers(ms) }
  : (typeof LightweightCharts.createSeriesMarkers === "function")
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
// ----------------------------
let lastMarkersHash = "";

function minuteFloor(tsSec) {
  return Math.floor(tsSec / 60) * 60;
}

function hashMarkers(ms) {
  // stable enough to avoid constant redraws
  return ms.map((m) => `${m.time}|${m.position}|${m.text}`).join(";");
}

async function loadMarkers() {
  try {
    const r = await fetch("/fills?limit=500", { cache: "no-store" });
    const data = await r.json();
    if (!data.ok || !Array.isArray(data.fills)) return;

    let buyCount = 0; // resets after every SELL

    // IMPORTANT: process oldest → newest
    const fills = [...data.fills].sort(
      (a, b) => new Date(a.filled_at) - new Date(b.filled_at)
    );

    const markers = fills.map((f) => {
      const ts = Math.floor(new Date(f.filled_at).getTime() / 1000);
      const t = minuteFloor(ts);

      const side = (f.side || "").toLowerCase();
      const isBuy = side === "buy";

      const qty = Number(f.filled_qty || 0);
      const px = Number(f.filled_avg_price || 0);

      // 🔁 Reset group on SELL
      if (!isBuy) {
        buyCount = 0;
      }

      if (isBuy) {
        buyCount++;
      }

      return {
        time: t,
        position: isBuy ? "belowBar" : "aboveBar",
        shape: isBuy ? "arrowUp" : "arrowDown",
        color: isBuy ? "#22c55e" : "#ef4444",
        text: isBuy
          ? `B${buyCount} ${qty}@${px.toFixed(2)}`
          : `S ${qty}@${px.toFixed(2)}`,
      };
    });

    const h = hashMarkers(markers);
    if (h === lastMarkersHash) return;
    lastMarkersHash = h;

    if (!markersLayer) return;
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
let lastBarTime = null; // epoch sec
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
    await loadMarkers(); // markers after data exists
    
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
    const barTimeMin = Math.floor(barTime / 60) * 60; // ✅ snap to minute
    
    // Update series only when new closed bar arrives
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
const LATEST_BAR_POLL_MS = 5000; // poll 5s so the new candle appears quickly after the minute closes

let didFitOnce = false;

async function loadHistoryOnce() {
  await loadHistory();              // your existing loadHistory()
  if (!didFitOnce) {
    chart.timeScale().fitContent(); // do it once only
    didFitOnce = true;
  }
}

loadHistoryOnce();
fetchPosition();
fetchLatestBar();
fetchMarkers();

setInterval(fetchLatestBar, LATEST_BAR_POLL_MS);
setInterval(fetchPosition, 2000);
setInterval(fetchMarkers, 5000);

// IMPORTANT: do NOT keep reloading history every 60s
// setInterval(loadHistory, 60000);

