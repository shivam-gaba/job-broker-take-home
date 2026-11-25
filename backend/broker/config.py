from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Optional


@dataclass
class Config:
    # Idle reaper: terminate a node with no running jobs after this long.
    # The first 10 minutes of a node are paid for anyway (billing minimum), so
    # a node is never reaped before launched_at + 10 min.
    reaper_idle_minutes: float = 10.0
    max_nodes: int = 10
    # "fair": priority (with aging) first, then the team holding the fewest
    #         GPUs, then FIFO; plus node draining so one team cannot keep all
    #         nodes while another waits.
    # "fifo": strict priority then submit time; no draining.
    ordering: str = "fair"
    aging_minutes: float = 60.0  # waiting this long bumps a job one priority level
    drain: bool = True  # only used with ordering == "fair"
    # Security. Default: teams share nodes. A team listed in `isolated_teams`
    # never shares a node with another team at the same time (an empty node may
    # be reused; nodes are assumed wiped between tenants). isolation=True
    # isolates every team.
    isolation: bool = False
    isolated_teams: tuple = ()
    # Node slots only base-priority-high jobs may launch into or use, so a high
    # job never waits for the node cap. 0 disables.
    high_reserved_slots: int = 1
    # Low-priority jobs run on interruptible nodes (35% price). If reclaimed,
    # their jobs are re-queued and rerun on on-demand nodes. A low job that has
    # aged to a higher effective priority also moves to on-demand.
    interruptible_for_low: bool = True
    # ...but only if the estimate is at most this long (None = no limit). A
    # reclaim restarts the job from scratch, so long jobs are a bad bet.
    interruptible_max_estimate_minutes: Optional[float] = 120
    # "demand": size a new node for the team's queued demand (pack small jobs)
    # "smallest": smallest node type that fits the job
    sizing: str = "demand"
    # Don't put a job on an empty node bigger than the one we would launch for
    # it, unless launching is impossible right now. Boot (4-8 min) still fits
    # the 10-minute start target; an oversized node bills its full price.
    right_size: bool = True
    # At the node cap, replace a team's small nodes with gpu-l when its backlog
    # would fill one (more GPUs per scarce node slot).
    upgrade: bool = True
    # A healthy node is ready within 8 minutes; past this we assume it is one of
    # the ~3% that never come up, terminate it and launch a replacement instead
    # of waiting for the provider's 15-minute failure report.
    boot_timeout_minutes: float = 9.0
    # Exponential backoff per node type after CapacityUnavailable.
    backoff_base_seconds: int = 60
    backoff_max_seconds: int = 16 * 60

    def to_dict(self) -> dict:
        return asdict(self)
