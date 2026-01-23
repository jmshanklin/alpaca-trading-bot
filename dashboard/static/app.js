const statusEl = document.getElementById("status");
const chartEl = document.getElementById("chart");

// Create chart
const chart = LightweightCharts.createChart(chartEl, {
  layout: { background: { color: "#0e1117" }, textColor: "#d1d4dc" },
  grid: { vertLines: { color: "#1f2430" }, horzLines: { color: "#1f2430" } },
  timeScale: { timeVisible: true, secondsVisible: false },
  rightPriceScale: { borderVisible: false },
});

// ✅ NEW API (matches your current build)
const candles = chart.addSeries(LightweightCharts.CandlestickSeries, {
  upColor: "#26a69a",
  downColor: "#ef5350",
  borderUpColor: "#26a69a",
  borderDownColor: "#ef5350",
  wickUpColor: "#26a69a",
  wickDownColor: "#ef5350",
});

// Resize to fill container
function resizeChart() {
  const rect = chartEl.getBoundingClientRect();
  chart.resize(Math.floor(rect.width), Math.floor(rect.height));
}
window.addEventListener("resize", resizeChart);
resizeChart();

async function loadHistory() {
  try {
    statusEl.textContent = "loading…";

    const r = await fetch("/bars?limit=300", { cache: "no-store" });
    const data = await r.json();

    const bars = data?.bars || [];

    if (!data?.ok || bars.length === 0) {
      statusEl.textContent = `no bars (ok=${data?.ok}) ${data?.error ? "| " + data.error : ""}`;
      candles.setData([]);
      return;
    }

    // bars must be: [{time:<unix seconds>, open, high, low, close}, ...]
    candles.setData(bars);
    chart.timeScale().fitContent();

    const last = bars[bars.length - 1];
    statusEl.textContent =
      `${data.symbol} ${data.feed} | bars: ${bars.length} | last: ${new Date(last.time * 1000).toLocaleTimeString()}`;
  } catch (e) {
    statusEl.textContent = `loadHistory error: ${e}`;
    console.error(e);
  }
}

loadHistory();
setInterval(loadHistory, 10000);
