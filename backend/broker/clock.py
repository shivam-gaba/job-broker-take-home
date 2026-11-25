"""Simulated clock. Time is integer seconds since the Unix epoch (UTC).

Nothing in the service sleeps: time only moves when the event loop or an API
call advances it, so a week of trace replays in seconds.
"""
from __future__ import annotations

from datetime import datetime, timezone


class SimClock:
    def __init__(self, now: int) -> None:
        self.now = int(now)


def to_iso(ts: int | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(value: str) -> int:
    """Parse ISO-8601; naive timestamps are taken as UTC, offsets are honoured."""
    dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())
