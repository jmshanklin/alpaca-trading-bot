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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS engine_state (
                id TEXT PRIMARY KEY,
                state JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )


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
