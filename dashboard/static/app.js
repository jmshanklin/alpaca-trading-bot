const statusEl = document.getElementById("status");

// Create chart
const chart = LightweightCharts.createChart(document.getElementById("chart"), {
  layout: { background: { color: "#0e1117" }, textColor: "#d1d4dc" },
  grid: { vertLines: { color: "#1f2430" }, horzLines: { color: "#1f2430" } },
  timeScale: { timeVisible: true, secondsVisible: false },
});

const candles = chart.addSeries(LightweightCharts.CandlestickSeries);

// Avg entry line (updates when position changes)
const avgEntryLine = candles.createPriceLine({
  price: 0,
  color: "#f5c542",
  lineWidth: 2,
  lineStyle: 2, // dashed
  axisLabelVisible: true,
  title: "Avg Entry",
});

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

async function fetchPosition() {
  try {
    const r = await fetch("/position", { cache: "no-store" });
    const p = await r.json();

    if (!p.ok) return;

    // If no position, just change the label (don’t force price to 0)
    if (!p.qty || p.qty <= 0 || !p.avg_entry) {
      avgEntryLine.applyOptions({ title: "Avg Entry (flat)" });
      return;
    }

    avgEntryLine.applyOptions({
      price: Number(p.avg_entry),
      title: `Avg Entry (${p.qty})`,
    });
  } catch (e) {
    // ignore for now
  }
}

// Kick off
loadHistory();
fetchPosition();

// Polling
setInterval(fetchPosition, 2000);
setInterval(loadHistory, 10000);
