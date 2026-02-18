# engine/state.py
import json
import os
import time
import hashlib
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.extras import Json


# ----------------------------
# DB enable + connect
# ----------------------------
def db_enabled(database_url: str) -> bool:
    return bool(database_url and str(database_url).strip())


def db_connect(database_url: str):
    conn = psycopg2.connect(database_url, connect_timeout=10)
    conn.autocommit = True
    return conn

def db_init(conn) -> None:
    with conn.cursor() as cur:
        # Engine state table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.engine_state (
                id TEXT PRIMARY KEY,
                state JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )

        # Trade journal table (used by engine.py journal_trade + report_app ladder)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS public.trade_journal (
                id BIGSERIAL PRIMARY KEY,
                ts_utc TIMESTAMPTZ NOT NULL DEFAULT now(),

                symbol TEXT NOT NULL,
                side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
                qty INTEGER NOT NULL CHECK (qty > 0),
                est_price DOUBLE PRECISION NULL,

                order_id TEXT NULL,
                client_order_id TEXT NULL,

                is_dry_run BOOLEAN NOT NULL DEFAULT TRUE,
                is_leader BOOLEAN NOT NULL DEFAULT FALSE,

                group_id TEXT NULL,
                anchor_price DOUBLE PRECISION NULL,
                last_buy_price DOUBLE PRECISION NULL,
                buys_in_group INTEGER NULL,

                note TEXT NULL
            );
            """
        )

        # Helpful indexes for report queries
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_journal_ts ON public.trade_journal (ts_utc DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_journal_symbol_ts ON public.trade_journal (symbol, ts_utc DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_journal_group ON public.trade_journal (group_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_journal_client_order_id ON public.trade_journal (client_order_id);")

# ----------------------------
# Leader Lock (Postgres advisory lock)
# ----------------------------
def _lock_int64_from_key(key: str) -> int:
    """
    Convert a string key into a signed 64-bit int for pg_try_advisory_lock.
    """
    h = hashlib.sha256(key.encode("utf-8")).digest()
    # Use first 8 bytes, then force into signed 63-bit range (portable)
    val = int.from_bytes(h[:8], "big", signed=False) % (2**63 - 1)
    return int(val)


def try_acquire_leader_lock(conn, lock_key: str) -> bool:
    lock_id = _lock_int64_from_key(lock_key)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s);", (lock_id,))
        return bool(cur.fetchone()[0])


# ----------------------------
# DB state read/write
# ----------------------------
def load_state_db(conn, state_id: str) -> Dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT state FROM engine_state WHERE id=%s;", (state_id,))
        row = cur.fetchone()
        return (row[0] or {}) if row else {}


def save_state_db(conn, state_id: str, state: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO engine_state (id, state, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (id)
            DO UPDATE SET state = EXCLUDED.state, updated_at = now();
            """,
            (state_id, Json(state)),
        )


# ----------------------------
# Disk fallback state read/write
# ----------------------------
def load_state_disk(state_path: str) -> Dict[str, Any]:
    try:
        if state_path and os.path.exists(state_path):
            with open(state_path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def save_state_disk(state_path: str, state: Dict[str, Any]) -> None:
    try:
        if not state_path:
            return
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
    except Exception:
        pass

def journal_event(
    conn,
    *,
    symbol: str,
    side: str,
    qty: int,
    est_price,
    order_id: str | None = None,
    client_order_id: str | None = None,
    is_dry_run: bool = True,
    is_leader: bool = False,
    group_id: str | None = None,
    anchor_price=None,
    last_buy_price=None,
    buys_in_group=None,
    note: str | None = None,
) -> None:
    """
    Append one row to trade_journal. Safe no-op if conn is None.
    """
    if conn is None:
        return

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO trade_journal
            (symbol, side, qty, est_price, order_id, client_order_id,
             is_dry_run, is_leader, group_id, anchor_price, last_buy_price,
             buys_in_group, note)
        VALUES
            (%s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s,
             %s, %s)
        """,
        (
            symbol,
            side,
            int(qty),
            est_price,
            order_id,
            client_order_id,
            bool(is_dry_run),
            bool(is_leader),
            group_id,
            anchor_price,
            last_buy_price,
            buys_in_group,
            note,
        ),
    )
    conn.commit()
    cur.close()

