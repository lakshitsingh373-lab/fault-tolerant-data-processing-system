"""
Normalization layer.

Design decisions (see README for the full rationale):

- Field-name resolution is config-driven (ALIAS_MAP below), not
  hardcoded per client. Adding a new client that calls the amount field
  "sum" instead of "amount" means adding one string to a list, not
  writing an if-branch keyed on client name.

- This layer never raises on unexpected extra fields — anything not
  recognized is ignored, not rejected. New fields appearing is treated
  as expected, not exceptional.

- It DOES raise (ValidationError) when a field we can't safely default
  is missing: right now that's just client_id, since attributing data
  to no client at all makes the record meaningless. Everything else
  (metric, amount, timestamp) degrades gracefully with a flag rather
  than a rejection.

- The output is always a fresh dict — the raw input is never mutated
  or passed through. Callers keep the raw payload separately (in
  raw_events) and only ever store this normalized shape downstream.
"""

from datetime import datetime, timezone
from dateutil import parser as dateparser

ALIAS_MAP = {
    "client_id": ["client_id", "client", "source", "src"],
    "metric": ["metric", "metric_name", "name", "type", "event_type"],
    "amount": ["amount", "value", "val", "amt", "count"],
    "timestamp": ["timestamp", "time", "date", "ts", "occurred_at"],
}


class ValidationError(Exception):
    def __init__(self, message, flags=None):
        super().__init__(message)
        self.message = message
        self.flags = flags or []


def _flatten(raw: dict) -> dict:
    """Client envelopes commonly nest the real fields under 'payload'.
    Merge payload keys up to top level (payload wins on collision) so
    the alias lookup below doesn't care whether a client nests or not.
    """
    flat = dict(raw)
    payload = raw.get("payload")
    if isinstance(payload, dict):
        flat.update(payload)
    return flat


def _find_field(flat: dict, keys: list):
    for k in keys:
        if k in flat and flat[k] is not None and flat[k] != "":
            return flat[k]
    return None


def _coerce_amount(value):
    if value is None:
        return None, True
    if isinstance(value, (int, float)):
        return float(value), False
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "").replace("$", "")
        try:
            return float(cleaned), False
        except ValueError:
            return None, True
    return None, True


def _coerce_timestamp(value):
    """Returns (iso_string, was_inferred). Falls back to 'now' (UTC)
    if the value is missing or unparseable, flagged so consumers know
    it's not authoritative.
    """
    if value:
        try:
            dt = dateparser.parse(str(value))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"), False
        except (dateparser.ParserError, ValueError, OverflowError):
            pass
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), True


def normalize(raw: dict) -> tuple[dict, list]:
    """Returns (canonical_event, flags). Raises ValidationError if the
    event can't be salvaged at all.
    """
    flat = _flatten(raw)
    flags = []

    client_id = _find_field(flat, ALIAS_MAP["client_id"])
    if not client_id:
        raise ValidationError(
            "no recognizable client identifier field", flags=["missing_client_id"]
        )

    metric = _find_field(flat, ALIAS_MAP["metric"])
    if not metric:
        metric = "unknown"
        flags.append("missing_metric")

    raw_amount = _find_field(flat, ALIAS_MAP["amount"])
    amount, amount_was_bad = _coerce_amount(raw_amount)
    if amount_was_bad:
        flags.append("missing_or_invalid_amount")

    raw_ts = _find_field(flat, ALIAS_MAP["timestamp"])
    timestamp, ts_inferred = _coerce_timestamp(raw_ts)
    if ts_inferred:
        flags.append("timestamp_inferred")

    canonical = {
        "client_id": str(client_id),
        "metric": str(metric),
        "amount": amount,
        "timestamp": timestamp,
    }
    return canonical, flags
