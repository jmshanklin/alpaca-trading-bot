const statusEl = document.getElementById("status");

// Create chart
const chart = LightweightCharts.createChart(document.getElementById("chart"), {
  layout: {
    background: { color: "#0e1117" },
    textColor: "#d1d4dc"
  },
  grid: {
    vertLines: { color: "#1f2430" },
    horzLines: { color: "#1f2430" }
  },
  timeScale: {
    timeVisible: true,
    secondsVisible: false
  }
});

// Add candlestick series
const candles = chart.addCandlestickSeries({
  upColor: "#26a69a",
  downColor: "#ef5350",
  borderUpColor: "#26a69a",
  borderDownColor: "#ef5350",
  wickUpColor: "#26a69a",
  wickDownColor: "#ef5350"
});

let lastBarTime = null;

async function fetchLatestBar() {
  try {
    const r = await fetch("/latest_bar", { cache: "no-store" });
    const data = await r.json();

    if (!data.ok) {
      statusEl.textContent = "no data";
      return;
    }

    statusEl.textContent = `TSLA ${data.feed} @ ${new Date(data.t).toLocaleTimeString()}`;

    const barTime = Math.floor(new Date(data.t).getTime() / 1000);

    // Prevent duplicate candles
    if (barTime === lastBarTime) return;
    lastBarTime = barTime;

    candles.update({
      time: barTime,
      open: data.o,
      high: data.h,
      low: data.l,
      close: data.c
    });

  } catch (err) {
    statusEl.textContent = "connection error";
    console.error(err);
  }
}

// Poll every second
fetchLatestBar();
setInterval(fetchLatestBar, 1000);
