const chartEl = document.getElementById("chart");
const barEl = document.getElementById("bar");

// Create chart
const chart = LightweightCharts.createChart(chartEl, {
  layout: {
    background: { color: "#0e1117" },
    textColor: "#d1d4dc",
  },
  grid: {
    vertLines: { color: "#1f2933" },
    horzLines: { color: "#1f2933" },
  },
  timeScale: {
    timeVisible: true,
    secondsVisible: false,
  },
});

const candleSeries = chart.addCandlestickSeries();

// Keep track of last bar time so we update instead of duplicating
let lastBarTime = null;

async function fetchLatestBar() {
  try {
    const r = await fetch("/latest_bar", { cache: "no-store" });
    const data = await r.json();

    if (!data.ok) {
      barEl.textContent = `latest_bar: not ok → ${JSON.stringify(data)}`;
      return;
    }

    const bar = {
      time: Math.floor(new Date(data.t).getTime() / 1000),
      open: data.o,
      high: data.h,
      low: data.l,
      close: data.c,
    };

    if (lastBarTime === bar.time) {
      candleSeries.update(bar);
    } else {
      candleSeries.update(bar);
      lastBarTime = bar.time;
    }

    barEl.textContent =
      `Latest 1m bar (${data.symbol})\n` +
      `t: ${data.t}\n` +
      `O: ${data.o}  H: ${data.h}  L: ${data.l}  C: ${data.c}  V: ${data.v}\n` +
      `feed: ${data.feed}`;
  } catch (e) {
    barEl.textContent = `latest_bar: error → ${e}`;
  }
}

// Poll every second
fetchLatestBar();
setInterval(fetchLatestBar, 1000);
