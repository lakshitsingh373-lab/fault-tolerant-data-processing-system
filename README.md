# Fault-Tolerant Data Processing System

A minimal ingestion → normalization → dedup → aggregation pipeline for
unreliable multi-client event data. Python (FastAPI + SQLite) backend,
single-page vanilla-JS frontend. No auth, no Docker, no microservices,
per the brief.

## Running it

```bash
cd backend
pip install fastapi uvicorn python-dateutil
python3 -m uvicorn main:app --reload --port 8000
```

Then open `frontend/index.html` directly in a browser (it talks to
`http://localhost:8000`).

## Architecture in one paragraph

Every submission is written to an append-only `raw_events` log **first**,
before any normalization or business logic runs. That log is the
durable source of truth. Normalization then produces a canonical event
and a deterministic content hash (fingerprint), and only the hash is
what gets a uniqueness guarantee in the `processed_events` table — not
a client-supplied ID, since the brief says those aren't reliable. The
aggregation API reads only from `processed_events`, so it's automatically
consistent regardless of how many times a client retried.

See `backend/main.py` and `backend/normalize.py` docstrings for the
step-by-step reasoning inline with the code.

---

## What assumptions did I make?

- **No reliable client-supplied event ID exists**, so I couldn't dedupe
  on one. Instead I fingerprint the *normalized* event
  (`client_id + metric + amount + timestamp`) with SHA-256. This means
  two genuinely different events that happen to normalize to identical
  values in the same instant would collide and get deduped — I accepted
  that trade-off deliberately in exchange for not needing any client
  cooperation to be retry-safe. In a real system I'd ask clients to add
  even a weak idempotency key to reduce this collision surface, and
  treat this hash as a fallback rather than the only mechanism.
- **A missing/unparseable timestamp is better replaced with the server's
  receipt time (flagged) than left null or rejected**, since aggregation
  by time range needs *some* value, and the whole point is graceful
  degradation. The flag (`timestamp_inferred`) keeps this honest — it's
  never presented as if the client actually reported it.
- **`client_id` is the one field I don't default.** Every other field
  can degrade to a placeholder; a data point with no attributable source
  is meaningless to aggregate at all, so those get rejected outright
  rather than silently miscounted somewhere.
- **Field-name variants are handled through a small alias list per
  canonical field**, not per-client branches, per the "don't hardcode
  client-specific logic" constraint. Adding a client whose schema uses
  new synonyms means adding a string to a list.

## How does the system prevent double counting?

The dedup key is a hash of the *normalized* event, and it's enforced as
a `UNIQUE` column in `processed_events` at the database level — not in
application code. The insert either succeeds once, ever, per fingerprint,
or SQLite throws an integrity error that the ingestion handler catches
and reports as `duplicate`. Because the constraint lives in the database
and the insert is a single atomic statement, this holds even under
concurrent retries — there's no read-then-write race window where two
requests could both "see no existing row" and both insert.

## What happens if the database fails mid-request?

The raw event is already durably logged *before* normalization or the
processed-table write is attempted, so a failure at that later point
never loses the client's data — it's sitting in `raw_events` with
`status='failed'` and the original payload intact, ready to be retried
either by the client or a background sweep.

Because the processed-table write is a single atomic insert protected
by the unique-hash constraint, there's no possibility of a half-written
row: SQLite's transaction either commits the full processed row or it
commits nothing. If the client retries after a failure, the retry
recomputes the same fingerprint and takes one of two paths — the first
successful attempt inserts and is counted, or if the earlier attempt had
in fact partially completed (which SQLite's atomicity here prevents),
the retry would just be caught as a duplicate. Either way, the event
ends up processed exactly once, never zero times, never twice.

The `simulate_failure` toggle in the UI/API exercises exactly this path
so it's demonstrable, not just asserted.

## What would break first at scale?

**SQLite itself**, specifically its single-writer model — every insert
into `raw_events` / `processed_events` takes a write lock on the whole
file, so as concurrent ingestion volume grows, writers start queuing and
latency climbs before anything else in this design gives out. That's
the first ceiling, and the fix is a straightforward swap to a proper
server (Postgres) with the same schema and the same unique-constraint
dedup strategy — the logic doesn't need to change, just the engine.

Right behind that: **the aggregation endpoint computes `GROUP BY` sums
on read, over the whole `processed_events` table**, with no
pre-aggregation or caching. That's fine at the current scale and it
keeps aggregation trivially consistent with whatever's been ingested,
but it's an O(n) scan per query — it'd need to move to incremental/
materialized rollups (updated at write time, keyed by the same
client × metric × time-bucket grouping) once the table grows past what
a live scan can serve within a reasonable response time.

Third: the **idempotency hash space and normalization heuristics** are
tuned for the shapes of messiness described in the brief, not open-ended
adversarial input — a client sending genuinely chaotic or hostile
payloads at scale would need real schema-drift monitoring and alerting
on flag rates, rather than the alias-list approach silently absorbing
everything indefinitely.
