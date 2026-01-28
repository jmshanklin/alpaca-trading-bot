# engine/state.py
import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Optional

import psycopg2
from psycopg2.extras import Json

from grid import GridState

@dataclass
class Persisted:
    # day counters
    buys_today_date_et: Optional[str] = None
    buys_today_et: int = 0

    # grid state
    grid_anchor_price: Optional[float] = None
    grid_last_buy_price: Optional[float] = None
    grid_buy_count_in_group: int = 0

    # misc
    last_save_ts: float = 0.0

def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def load_disk(path: str) -> Persisted:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
            return Persisted(**obj)
    except Exception:
        pass
    return Persisted()

def save_disk(path: str, p: Persisted) -> None:
    _ensure_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(asdict(p), f, indent=2, sort_keys=True)

def db_connect(database_url: str):
    conn = psycopg2.connect(database_url, connect_timeout=10)
    conn.autocommit = True
    return conn

def db_init(conn):
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

def load_db(conn, state_id: str) -> Persisted:
    with conn.cursor() as cur:
        cur.execute("SELECT state FROM engine_state WHERE id=%s;", (state_id,))
        row = cur.fetchone()
        data = (row[0] or {}) if row else {}
        try:
            return Persisted(**data)
        except Exception:
            return Persisted()

def save_db(conn, state_id: str, p: Persisted) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO engine_state (id, state, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (id)
            DO UPDATE SET state = EXCLUDED.state, updated_at = now();
            """,
            (state_id, Json(asdict(p))),
        )

def hydrate_grid(p: Persisted) -> GridState:
    return GridState(
        anchor_price=p.grid_anchor_price,
        last_buy_price=p.grid_last_buy_price,
        buy_count_in_group=int(p.grid_buy_count_in_group or 0),
    )

def dehydrate_grid(p: Persisted, gs: GridState) -> None:
    p.grid_anchor_price = gs.anchor_price
    p.grid_last_buy_price = gs.last_buy_price
    p.grid_buy_count_in_group = int(gs.buy_count_in_group or 0)

def should_save(p: Persisted, every_sec: float) -> bool:
    if every_sec <= 0:
        return True
    return (time.time() - float(p.last_save_ts or 0.0)) >= float(every_sec)

def mark_saved(p: Persisted) -> None:
    p.last_save_ts = time.time()
