const statusEl = document.getElementById("status");

// Create chart
const chart = LightweightCharts.createChart(document.getElementById("chart"), {
  layout: { background: { color: "#0e1117" }, textColor: "#d1d4dc" },
  grid: { vertLines: { color: "#1f2430" }, horzLines: { color: "#1f2430" } },
  timeScale: { timeVisible: true, secondsVisible: false },
});

const candles = chart.addSeries(LightweightCharts.CandlestickSeries);

// Load initial history
async function loadHistory() {
  statusEl.textContent = "loading…";

  const r = await fetch("/bars?limit=300", { cache: "no-store" });
  const data = await r.json();

  if (!data.ok) {
    statusEl.textContent = `no data (${data.error || "unknown"})`;
    return;
  }

  candles.setData(data.bars);
  chart.timeScale().fitContent();

  const last = data.bars[data.bars.length - 1];
  statusEl.textContent = `${data.symbol} ${data.feed} | last bar: ${new Date(
    last.time * 1000
  ).toLocaleTimeString()}`;
}

loadHistory();

// Refresh every 10 seconds (fine for now; we’ll go realtime later)
setInterval(loadHistory, 10000);
