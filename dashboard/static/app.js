const statusEl = document.getElementById("status");
const chartEl = document.getElementById("chart");

// Create chart
const chart = LightweightCharts.createChart(chartEl, {
  layout: { background: { color: "#0e1117" }, textColor: "#d1d4dc" },
  grid: { vertLines: { color: "#1f2430" }, horzLines: { color: "#1f2430" } },
  timeScale: { timeVisible: true, secondsVisible: false },
  rightPriceScale: { borderVisible: false },
});

// ✅ Most compatible series creation:
const candles = chart.addCandlestickSeries();

// Keep chart sized to the container
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
    if (!r.ok) {
      const txt = await r.text();
      statusEl.textContent = `bars HTTP ${r.status}: ${txt.slice(0, 120)}`;
      return;
    }

    const data = await r.json();

    // IMPORTANT: ok:true doesn't guarantee bars exist
    const bars = (data && data.bars) ? data.bars : [];
    if (!data.ok || bars.length === 0) {
      statusEl.textContent = `no bars (ok=${data?.ok}) ${data?.error ? "| " + data.error : ""}`;
      candles.setData([]);
      return;
    }

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
