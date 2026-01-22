(async function () {
  const statusEl = document.getElementById("status");

  // 1) Prove JS is running
  statusEl.textContent = "JS loaded…";

  // 2) Prove we can call the backend
  try {
    const res = await fetch("/health", { cache: "no-store" });
    const data = await res.json();
    statusEl.textContent = data.ok ? "Backend: OK ✅" : "Backend: NOT OK ⚠️";
  } catch (err) {
    statusEl.textContent = "Backend: ERROR ❌";
    console.error(err);
  }

  // 3) Fill the page so it’s obvious something rendered
  const chart = document.getElementById("chart");
  chart.style.padding = "12px";
  chart.innerHTML = `
    <div style="font-size:14px;">
      <div><b>Next:</b> we’ll render 1-min candles here.</div>
      <div style="margin-top:8px; font-family:monospace;">
        Time: ${new Date().toLocaleString()}
      </div>
    </div>
  `;
})();

