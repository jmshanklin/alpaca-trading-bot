async function fetchLatestBar() {
  const el = document.getElementById("bar");

  try {
    const r = await fetch("/latest_bar", { cache: "no-store" });
    const data = await r.json();

    if (!data.ok) {
      el.textContent = `latest_bar: not ok → ${JSON.stringify(data)}`;
      return;
    }

    el.textContent =
      `Latest 1m bar (${data.symbol})\n` +
      `t: ${data.t}\n` +
      `O: ${data.o}  H: ${data.h}  L: ${data.l}  C: ${data.c}  V: ${data.v}\n` +
      `feed: ${data.feed}`;
  } catch (e) {
    el.textContent = `latest_bar: error → ${e}`;
  }
}

// Poll every 1 second (fine for now)
fetchLatestBar();
setInterval(fetchLatestBar, 1000);
