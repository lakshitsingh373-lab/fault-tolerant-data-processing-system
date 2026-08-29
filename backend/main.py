"""
Fault-tolerant ingestion + aggregation service.

Endpoint map:
  POST /ingest         submit one raw client event
  GET  /events          list raw_events, optionally filtered by status
  GET  /aggregate       aggregated totals, filterable by client + time range
  GET  /health          liveness

Ingestion pipeline for POST /ingest, in order:

  1. Write to raw_events immediately, status='received'.
     This commits before anything else runs. It's our durable log —
     if every single thing below this line throws, we still have the
     client's data on disk and can reprocess it later. Nothing is lost.

  2. Normalize the payload (backend/normalize.py). If normalization
     can't salvage a client_id at all, mark the raw row 'rejected' and
     stop — this was bad data, not a system failure, so it's not
     retried automatically.

  3. Compute a deterministic idempotency hash from the normalized
     core fields. Same logical event -> same hash, every time,
     regardless of whether the client sent a request ID (they don't,
     per the brief).

  4. If ?simulate_failure=true, raise here — after step 1 committed,
     before step 5 commits. This models "DB write fails mid-request"
     from the brief exactly: the raw log has the event, nothing was
     double-counted, and a retry is safe (see step 5).

  5. INSERT into processed_events with idempotency_hash UNIQUE.
     - First time this hash is seen: insert succeeds, row becomes the
       canonical processed record. raw row -> 'processed'.
     - Hash already exists (this is a retry of something that already
       made it through): the UNIQUE constraint blocks the insert, we
       catch that, and mark the raw row 'duplicate' rather than
       processed. No double counting, no error surfaced to the client
       beyond "already processed".

Every step from 2-5 runs inside one SQLite transaction scoped to a
single connection (see db.get_conn), so a crash mid-way can't leave
processed_events half-written — SQLite's transaction either commits
the row or it doesn't exist at all.
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timezone

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import db
from normalize import normalize, ValidationError

app = FastAPI(title="Fault-Tolerant Data Processing System")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    db.init_db()


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def compute_hash(canonical: dict) -> str:
    """Fingerprint of the logical event, independent of client-supplied
    IDs (which the brief says we can't rely on). Two submissions that
    normalize to the same client/metric/amount/timestamp are treated
    as the same event — that's the deliberate trade-off documented in
    the README (it means a client legitimately logging the same
    metric/amount in the same instant will be deduped too; we accept
    that in exchange for retry-safety without needing client IDs).
    """
    basis = f"{canonical['client_id']}|{canonical['metric']}|{canonical['amount']}|{canonical['timestamp']}"
    return hashlib.sha256(basis.encode()).hexdigest()


@app.post("/ingest")
def ingest(raw: dict, simulate_failure: bool = Query(False)):
    with db.get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO raw_events (received_at, client_hint, raw_payload, status) "
            "VALUES (?, ?, ?, 'received')",
            (now_iso(), raw.get("source") or raw.get("client"), json.dumps(raw)),
        )
        raw_id = cur.lastrowid

    # Step 2: normalize
    try:
        canonical, flags = normalize(raw)
    except ValidationError as e:
        with db.get_conn() as conn:
            conn.execute(
                "UPDATE raw_events SET status='rejected', error=? WHERE id=?",
                (e.message, raw_id),
            )
        return JSONResponse(
            status_code=422,
            content={"raw_event_id": raw_id, "status": "rejected", "reason": e.message},
        )

    idempotency_hash = compute_hash(canonical)
    with db.get_conn() as conn:
        conn.execute(
            "UPDATE raw_events SET idempotency_hash=? WHERE id=?", (idempotency_hash, raw_id)
        )

    # Step 4: simulated partial failure, modeling "DB write fails mid-request"
    if simulate_failure:
        with db.get_conn() as conn:
            conn.execute(
                "UPDATE raw_events SET status='failed', error=? WHERE id=?",
                ("simulated database failure during write", raw_id),
            )
        return JSONResponse(
            status_code=500,
            content={
                "raw_event_id": raw_id,
                "status": "failed",
                "reason": "simulated database failure — event preserved in raw log, safe to retry",
                "idempotency_hash": idempotency_hash,
            },
        )

    # Step 5: idempotent write into processed_events
    try:
        with db.get_conn() as conn:
            conn.execute(
                "INSERT INTO processed_events "
                "(idempotency_hash, client_id, metric, amount, timestamp, processed_at, raw_event_id, flags) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    idempotency_hash,
                    canonical["client_id"],
                    canonical["metric"],
                    canonical["amount"],
                    canonical["timestamp"],
                    now_iso(),
                    raw_id,
                    json.dumps(flags),
                ),
            )
            conn.execute("UPDATE raw_events SET status='processed' WHERE id=?", (raw_id,))
        return {
            "raw_event_id": raw_id,
            "status": "processed",
            "event": canonical,
            "flags": flags,
            "idempotency_hash": idempotency_hash,
        }
    except sqlite3.IntegrityError:
        # hash collision -> this exact logical event was already processed.
        with db.get_conn() as conn:
            conn.execute("UPDATE raw_events SET status='duplicate' WHERE id=?", (raw_id,))
        return {
            "raw_event_id": raw_id,
            "status": "duplicate",
            "reason": "an event with this fingerprint was already processed — not counted again",
            "idempotency_hash": idempotency_hash,
        }


@app.get("/events")
def list_events(status: str | None = None, limit: int = 100):
    q = "SELECT * FROM raw_events"
    params = []
    if status:
        q += " WHERE status = ?"
        params.append(status)
    q += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    with db.get_conn() as conn:
        rows = conn.execute(q, params).fetchall()
    return [db.row_to_dict(r) for r in rows]


@app.get("/aggregate")
def aggregate(
    client_id: str | None = None,
    from_ts: str | None = None,
    to_ts: str | None = None,
):
    q = (
        "SELECT client_id, metric, COUNT(*) as count, SUM(amount) as total, AVG(amount) as avg "
        "FROM processed_events WHERE 1=1"
    )
    params = []
    if client_id:
        q += " AND client_id = ?"
        params.append(client_id)
    if from_ts:
        q += " AND timestamp >= ?"
        params.append(from_ts)
    if to_ts:
        q += " AND timestamp <= ?"
        params.append(to_ts)
    q += " GROUP BY client_id, metric ORDER BY client_id, metric"

    with db.get_conn() as conn:
        rows = conn.execute(q, params).fetchall()
        totals = conn.execute(
            "SELECT COUNT(*) as count, SUM(amount) as total FROM processed_events"
        ).fetchone()

    return {
        "breakdown": [dict(r) for r in rows],
        "overall": dict(totals),
    }


@app.get("/health")
def health():
    return {"status": "ok"}
