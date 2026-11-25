from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

PRIORITIES = {"low": 0, "normal": 1, "high": 2}
VALID_GPUS = (1, 2, 4, 8)
TERMINAL = ("succeeded", "failed", "cancelled")
OVERHEAD_TEAM = "_overhead"  # failed launches: provider's fault, owned by the platform


@dataclass
class Job:
    id: str
    team: str
    gpus: int
    estimated_minutes: int
    priority: str
    submitted_at: int
    status: str = "queued"
    idempotency_key: Optional[str] = None
    # Simulation-only: how long the job really runs and how it ends. The
    # scheduler never reads these; only the event loop does.
    run_seconds: Optional[int] = None
    final_outcome: str = "succeeded"
    started_at: Optional[int] = None
    ended_at: Optional[int] = None
    node_id: Optional[str] = None
    attempts: int = 0
    reason: Optional[str] = None

    @property
    def wait_seconds(self) -> Optional[int]:
        if self.started_at is None:
            return None
        return self.started_at - self.submitted_at

    @property
    def expected_end(self) -> Optional[int]:
        """When the scheduler *believes* the job ends (estimate only)."""
        if self.started_at is None:
            return None
        return self.started_at + self.estimated_minutes * 60


@dataclass
class Node:
    id: str
    type: str
    gpus: int
    price_per_hour: float
    launched_at: int
    team: str  # team currently accountable for the node's cost
    state: str = "booting"  # booting | ready | terminated | failed
    ready_at: Optional[int] = None
    ended_at: Optional[int] = None
    job_ids: list[str] = field(default_factory=list)
    used_gpus: int = 0
    idle_since: Optional[int] = None
    draining: bool = False
    drain_for: Optional[str] = None
    # currently open cost segment
    seg_kind: str = "boot"
    seg_start: int = 0

    @property
    def alive(self) -> bool:
        return self.state in ("booting", "ready")

    @property
    def free_gpus(self) -> int:
        return self.gpus - self.used_gpus


@dataclass
class Segment:
    """A slice of a node's billed life attributed to one team.

    kind: boot | busy | idle | failed_launch | minimum
    For kind == "minimum" (10-minute minimum top-up) start == end and the
    extra billed seconds are in `extra_seconds`.
    """
    node_id: str
    team: str
    kind: str
    start: int
    end: int
    price_per_hour: float
    extra_seconds: int = 0

    def cost_in(self, lo: int, hi: int) -> float:
        if self.kind == "minimum":
            return self.extra_seconds * self.price_per_hour / 3600 if lo <= self.end < hi else 0.0
        overlap = min(self.end, hi) - max(self.start, lo)
        return max(0, overlap) * self.price_per_hour / 3600
