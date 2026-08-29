"""
Database layer.

SQLite, single file, no ORM. Two tables:

- raw_events: append-only durable log of everything that hit /ingest,
  regardless of what happened to it afterwards. This is the source of
  truth we can always replay from. Nothing is ever deleted or mutated
  here except its `status` and `error` fields, which just narrate what
  happened to that submission.

- processed_events: the canonical, normalized, deduplicated dataset.
  A row only lands here once, ever, per unique event fingerprint
  (idempotency_hash has a UNIQUE constraint). This table is what the
  aggregation API reads from.

Keeping these separate means "did we receive it" and "did we count it"
are two different questions with two different answers, which is the
crux of the failure-handling requirements in the brief.
"""

import sqlite3
import json
from contextlib import contextmanager

DB_PATH = "pipeline.db"


def init_db():
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS raw_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at TEXT NOT NULL,
                client_hint TEXT,
                raw_payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'received',
                    -- received | processed | duplicate | rejected | failed
                error TEXT,
                idempotency_hash TEXT
            );

            CREATE TABLE IF NOT EXISTS processed_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                idempotency_hash TEXT NOT NULL UNIQUE,
                client_id TEXT NOT NULL,
                metric TEXT NOT NULL,
                amount REAL,
                timestamp TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                raw_event_id INTEGER NOT NULL,
                flags TEXT,
                FOREIGN KEY (raw_event_id) REFERENCES raw_events(id)
            );

            CREATE INDEX IF NOT EXISTS idx_processed_client ON processed_events(client_id);
            CREATE INDEX IF NOT EXISTS idx_processed_ts ON processed_events(timestamp);
            """
        )


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def row_to_dict(row):
    d = dict(row)
    for key in ("raw_payload", "flags"):
        if key in d and d[key]:
            try:
                d[key] = json.loads(d[key])
            except (TypeError, json.JSONDecodeError):
                pass
    return d
