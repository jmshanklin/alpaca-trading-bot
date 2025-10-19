# TradingBot (Render Minimal)

This is the *minimum working* Flask app that receives webhooks (e.g., from TradingView)
and submits market orders to Alpaca. Follow this **top-to-bottom** checklist.

---
## 1) Local Folder Layout

```text
TradingBot/
├─ webhook_to_alpaca_price_action.py
├─ requirements.txt
├─ Procfile                # no file extension
├─ Procfile.gunicorn      # optional alternative
├─ .gitignore             # optional but recommended
├─ examples/
│  ├─ tradingview_payload.json
│  └─ curl_test.http
└─ docs/
   └─ RENDER_SETUP.md
```

In Windows File Explorer: **View → Show → File name extensions** (checked).
Confirm that `Procfile` has **no** `.txt` extension.

---
## 2) File Contents (copy/paste)

- `requirements.txt`: minimal libs plus optional `gunicorn` (if you use the gunicorn Procfile).
- `Procfile`: `web: python webhook_to_alpaca_price_action.py`
- `Procfile.gunicorn` (optional): `web: gunicorn -w 2 -k gthread -b 0.0.0.0:$PORT webhook_to_alpaca_price_action:app`
- `.gitignore`: keeps `.env`, `*.json`, caches out of Git.
- `webhook_to_alpaca_price_action.py`: Flask app with `/` health and `/webhook` endpoints.

---
## 3) GitHub (one-time)

1. Create repo, e.g. **alpaca-trading-bot**.
2. Upload all files from `TradingBot/` (or push via Git).
3. Ensure the top level of the repo contains `Procfile`, `requirements.txt`, and the `.py` file.

---
## 4) Render → New Web Service

- Connect to your GitHub repo.
- Runtime: Python.
- Build command: *(leave empty for Python; Render installs from `requirements.txt`)*
- Start command: automatically reads `Procfile`.
- **Environment Variables** (Render → *Environment* tab):
    - `ALPACA_KEY_ID = <your key>`
    - `ALPACA_SECRET_KEY = <your secret>`
    - `ALPACA_BASE_URL = https://paper-api.alpaca.markets` *(default; omit if you like)*
- Deploy.

---
## 5) Quick Tests (after deploy)

### 5.1 Health check (GET)
Open in browser:
```
https://<your-render-subdomain>.onrender.com/
```

### 5.2 Webhook test (POST)
Use the provided `examples/curl_test.http` (or raw curl):
```bash
curl -X POST https://<your-render-subdomain>.onrender.com/webhook          -H "Content-Type: application/json"          -d '{"symbol":"SPY","side":"buy","qty":1}'
```

Expected response:
```json
{"status":"success","symbol":"SPY","side":"buy","qty":1,"order_id":"..."} 
```
If you get `error`, the message will include the reason (e.g., invalid keys).

---
## 6) TradingView Alert Setup

- In TradingView: **Alerts → Webhook URL** = `https://<your-render-subdomain>.onrender.com/webhook`
- Alert message (JSON):
```json
{"symbol":"SPY","side":"buy","qty":1}
```
You can add fields in the future (e.g., stop/limit), but this is the minimum.

---
## 7) Common Gotchas

- **Wrong Procfile extension**: must be exactly `Procfile` (no `.txt`).
- **Missing env vars**: set all required keys in Render before testing `/webhook`.
- **Out of funds or permissions** on Alpaca: Alpaca returns an error message—check response.
- **Paper vs Live**: default base URL is paper. Switch only when ready.

---
## 8) Optional Improvements (later)

- Switch `Procfile` to `gunicorn` for production robustness.
- Add request validation/signing for webhook security.
- Add logging to Google Sheets or a database.
- Add symbol whitelists, position sizing rules, or risk checks.

---
## 9) One-Page Render Setup Guide

See `docs/RENDER_SETUP.md` for a step-by-step with screenshots checklist you can follow each time.
