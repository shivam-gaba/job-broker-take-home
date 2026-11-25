"""Load and clean the submission trace.

Problems found in 01a_trace.csv and what we do about them:
  * mixed timezone offsets (+01:00, -07:00) -> converted to UTC (the brief says
    times are UTC "unless stated"; an explicit offset is a statement)
  * rows out of time order -> sorted by normalised submit time
  * exact duplicate rows (same time after UTC normalisation, team, gpus,
    estimate, actual, priority, outcome) -> treated as a client retry of the
    same submission and kept once, exactly what an idempotency key would do
  * GPU counts outside {1,2,4,8} (3 and 16) -> rejected, same as the API
  * anything unparsable / non-positive -> rejected
Every dropped row is listed in the returned report.
"""
from __future__ import annotations

import csv
from collections import Counter
from dataclasses import dataclass

from .clock import parse_iso
from .models import PRIORITIES, TERMINAL, VALID_GPUS


@dataclass(frozen=True)
class TraceRow:
    submitted_at: int
    team: str
    gpus: int
    estimated_minutes: int
    actual_minutes: int
    priority: str
    outcome: str
    line: int


def load_trace(path: str) -> tuple[list[TraceRow], dict]:
    rows: list[TraceRow] = []
    rejected: list[dict] = []
    tz_converted = 0
    out_of_order = 0
    last_ts = None
    with open(path, newline="") as f:
        for line_no, rec in enumerate(csv.DictReader(f), start=2):
            try:
                raw_ts = rec["submitted_at"].strip()
                ts = parse_iso(raw_ts)
                if not raw_ts.endswith("Z") and ("+" in raw_ts[10:] or "-" in raw_ts[10:]):
                    tz_converted += 1
                row = TraceRow(
                    submitted_at=ts,
                    team=rec["team"].strip().lower(),
                    gpus=int(rec["gpus"]),
                    estimated_minutes=int(rec["estimated_minutes"]),
                    actual_minutes=int(rec["actual_minutes"]),
                    priority=rec["priority"].strip().lower(),
                    outcome=rec["outcome"].strip().lower(),
                    line=line_no,
                )
            except (KeyError, ValueError, AttributeError) as e:
                rejected.append({"line": line_no, "reason": f"unparsable: {e}"})
                continue
            problem = _validate(row)
            if problem:
                rejected.append({"line": line_no, "reason": problem})
                continue
            if last_ts is not None and ts < last_ts:
                out_of_order += 1
            last_ts = ts
            rows.append(row)

    seen: set = set()
    unique: list[TraceRow] = []
    duplicates: list[int] = []
    for r in rows:
        key = (r.submitted_at, r.team, r.gpus, r.estimated_minutes, r.actual_minutes,
               r.priority, r.outcome)
        if key in seen:
            duplicates.append(r.line)
            continue
        seen.add(key)
        unique.append(r)
    unique.sort(key=lambda r: (r.submitted_at, r.line))

    report = {
        "rows_read": len(rows) + len(rejected),
        "rows_used": len(unique),
        "rejected": rejected,
        "duplicate_lines_dropped": duplicates,
        "timezone_offsets_converted": tz_converted,
        "out_of_order_rows": out_of_order,
        "per_team": dict(Counter(r.team for r in unique)),
    }
    return unique, report


def _validate(r: TraceRow) -> str | None:
    if not r.team:
        return "missing team"
    if r.gpus not in VALID_GPUS:
        return f"gpus={r.gpus} not in {VALID_GPUS} (no node can run it / not a valid request)"
    if r.estimated_minutes <= 0:
        return "estimated_minutes must be > 0"
    if r.actual_minutes < 0:
        return "actual_minutes must be >= 0"
    if r.priority not in PRIORITIES:
        return f"unknown priority {r.priority}"
    if r.outcome not in TERMINAL:
        return f"unknown outcome {r.outcome}"
    return None
