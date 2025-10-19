# Render Setup (Step-by-Step)

1. **Repo ready**: confirm `requirements.txt`, `Procfile`, and `webhook_to_alpaca_price_action.py` exist at the repo root.
2. **Create Service**: Render → New → Web Service → Connect GitHub → pick repo → Deploy.
3. **Environment**: Add `ALPACA_KEY_ID`, `ALPACA_SECRET_KEY`, optional `ALPACA_BASE_URL` if different from paper.
4. **Logs**: Check the *Logs* tab; ensure `Trading bot is running` appears when hitting `/`.
5. **Test**: POST to `/webhook` using curl or TradingView alert. Confirm `order_id`.
6. **Iterate**: If using gunicorn, update Start Command by just keeping `Procfile` (Render reads it automatically).
