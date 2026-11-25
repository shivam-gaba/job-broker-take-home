"""Fake cloud provider.

Behaves like the provider described in the brief:
  * node types gpu-s / gpu-m / gpu-l (1 / 4 / 8 GPUs)
  * launch can raise CapacityUnavailable, and failures come in bursts
    (an "outage" for one node type lasts 10-60 simulated minutes)
  * a node is ready 4-8 minutes after launch; ~3% never become ready and are
    reported failed 15 minutes after launch
  * billed per second from launch, with a 10 minute minimum per node
  * interruptible variants at 35% of the price; the provider reclaims them
    with 2 minutes' notice. Reclaims are a Poisson process with a hazard of
    5% per node-hour, i.e. roughly 5% of interruptible node-hours are lost.

The provider is treated as an external system: it persists its own state to
its own file (atomic JSON write) independently of the broker database, so
after a broker restart its nodes are "still alive and billing".
"""
from __future__ import annotations

import json
import os
import random
from dataclasses import asdict, dataclass
from typing import Optional

from .clock import SimClock


class CapacityUnavailable(Exception):
    """The provider has no capacity of the requested type right now."""


@dataclass(frozen=True)
class NodeType:
    name: str
    gpus: int
    price_per_hour: float


NODE_TYPES: dict[str, NodeType] = {
    "gpu-s": NodeType("gpu-s", 1, 1.00),
    "gpu-m": NodeType("gpu-m", 4, 4.50),
    "gpu-l": NodeType("gpu-l", 8, 8.50),
}

MIN_BILLED_SECONDS = 10 * 60
NEVER_READY_REPORT_SECONDS = 15 * 60
INTERRUPTIBLE_PRICE_FACTOR = 0.35
RECLAIM_NOTICE_SECONDS = 2 * 60


def node_price(node_type: str, interruptible: bool) -> float:
    base = NODE_TYPES[node_type].price_per_hour
    return base * INTERRUPTIBLE_PRICE_FACTOR if interruptible else base


@dataclass
class ProviderNode:
    id: str
    type: str
    launched_at: int
    ready_at: int  # when it becomes ready (meaningless if doomed)
    doomed: bool  # never becomes ready
    terminated_at: Optional[int] = None
    interruptible: bool = False
    reclaim_at: Optional[int] = None  # interruptible only

    @property
    def failed_at(self) -> int:
        return self.launched_at + NEVER_READY_REPORT_SECONDS

    @property
    def price_per_hour(self) -> float:
        return node_price(self.type, self.interruptible)

    def state(self, now: int) -> str:
        if self.terminated_at is not None and self.terminated_at <= now:
            return "terminated"
        if self.reclaim_at is not None and now >= self.reclaim_at:
            return "reclaimed"
        if self.doomed:
            return "failed" if now >= self.failed_at else "booting"
        return "ready" if now >= self.ready_at else "booting"

    def billing_end(self, now: int) -> Optional[int]:
        """When billing stopped, or None while still billing."""
        ends = [t for t in (self.terminated_at, self.reclaim_at,
                            self.failed_at if self.doomed else None)
                if t is not None and t <= now]
        return min(ends) if ends else None

    def billed_cost(self, now: int) -> float:
        end = self.billing_end(now)
        if end is None:
            seconds = now - self.launched_at
        else:
            seconds = max(end - self.launched_at, MIN_BILLED_SECONDS)
        return seconds * self.price_per_hour / 3600


class FakeProvider:
    def __init__(
        self,
        clock: SimClock,
        seed: int = 0,
        path: Optional[str] = None,
        never_ready_rate: float = 0.03,
        outage_start_prob: float = 0.03,
        outage_fail_prob: float = 0.9,
        outage_minutes: tuple[int, int] = (10, 60),
        reclaim_rate_per_hour: float = 0.05,
    ) -> None:
        self.clock = clock
        self.path = path
        self.never_ready_rate = never_ready_rate
        self.outage_start_prob = outage_start_prob
        self.outage_fail_prob = outage_fail_prob
        self.outage_minutes = outage_minutes
        self.reclaim_rate_per_hour = reclaim_rate_per_hour
        self.rng = random.Random(seed)
        self.nodes: dict[str, ProviderNode] = {}
        self.outage_until: dict[str, int] = {}
        self.forced_outage_until: dict[str, int] = {}
        self.counter = 0
        self.launch_calls = 0
        self.capacity_errors = 0
        if path and os.path.exists(path):
            self._load()

    # ---- provider API -------------------------------------------------
    def launch_node(self, node_type: str, interruptible: bool = False) -> str:
        if node_type not in NODE_TYPES:
            raise ValueError(f"unknown node type {node_type}")
        now = self.clock.now
        self.launch_calls += 1
        market = capacity_key(node_type, interruptible)  # outages are per market
        try:
            if now < self.forced_outage_until.get(market, 0):
                raise CapacityUnavailable(market)
            if now < self.outage_until.get(market, 0):
                if self.rng.random() < self.outage_fail_prob:
                    raise CapacityUnavailable(market)
            elif self.rng.random() < self.outage_start_prob:
                lo, hi = self.outage_minutes
                self.outage_until[market] = now + self.rng.randint(lo, hi) * 60
                raise CapacityUnavailable(market)
        except CapacityUnavailable:
            self.capacity_errors += 1
            self._save()
            raise
        self.counter += 1
        node = ProviderNode(
            id=f"n-{self.counter:05d}",
            type=node_type,
            launched_at=now,
            ready_at=now + self.rng.randint(4 * 60, 8 * 60),
            doomed=self.rng.random() < self.never_ready_rate,
            interruptible=interruptible,
        )
        if interruptible and self.reclaim_rate_per_hour > 0:
            node.reclaim_at = now + max(1, int(self.rng.expovariate(self.reclaim_rate_per_hour / 3600)))
        self.nodes[node.id] = node
        self._save()
        return node.id

    def terminate_node(self, node_id: str) -> None:
        node = self.nodes[node_id]
        now = self.clock.now
        if node.terminated_at is None:
            # billing already stopped if the provider failed or reclaimed it
            node.terminated_at = node.billing_end(now) or now
            self._save()

    def describe_node(self, node_id: str) -> dict:
        node = self.nodes[node_id]
        now = self.clock.now
        return {"id": node.id, "type": node.type, "state": node.state(now),
                "ready_at": node.ready_at, "launched_at": node.launched_at,
                "interruptible": node.interruptible, "reclaim_at": node.reclaim_at,
                "reclaim_notice": node.reclaim_at is not None
                and now >= node.reclaim_at - RECLAIM_NOTICE_SECONDS}

    def list_alive(self) -> list[str]:
        now = self.clock.now
        return [n.id for n in self.nodes.values() if n.state(now) in ("booting", "ready")]

    def next_event_time(self) -> Optional[int]:
        """Simulation hook: next time a node changes state on its own.

        A real broker would learn this from polling or a webhook; the event
        loop uses it so simulated time can jump straight to the next change.
        """
        now = self.clock.now
        times = []
        for n in self.nodes.values():
            st = n.state(now)
            if st == "booting":
                times.append(n.failed_at if n.doomed else n.ready_at)
            if st in ("booting", "ready") and n.reclaim_at is not None:
                times += [n.reclaim_at - RECLAIM_NOTICE_SECONDS, n.reclaim_at]
        future = [t for t in times if t > now]
        return min(future) if future else None

    def invoice(self, now: Optional[int] = None) -> float:
        now = self.clock.now if now is None else now
        return sum(n.billed_cost(now) for n in self.nodes.values())

    # ---- test helpers -------------------------------------------------
    def force_outage(self, node_type: str, until: int, interruptible: bool = False) -> None:
        """Every launch of node_type fails until `until` (deterministic)."""
        self.forced_outage_until[capacity_key(node_type, interruptible)] = until

    def force_reclaim(self, node_id: str, at: int) -> None:
        self.nodes[node_id].reclaim_at = at

    # ---- persistence --------------------------------------------------
    def _save(self) -> None:
        if not self.path:
            return
        state = {
            "nodes": [asdict(n) for n in self.nodes.values()],
            "outage_until": self.outage_until,
            "counter": self.counter,
            "launch_calls": self.launch_calls,
            "capacity_errors": self.capacity_errors,
            "rng": _rng_to_json(self.rng.getstate()),
        }
        tmp = f"{self.path}.tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)

    def _load(self) -> None:
        with open(self.path) as f:
            state = json.load(f)
        self.nodes = {d["id"]: ProviderNode(**d) for d in state["nodes"]}
        self.outage_until = state["outage_until"]
        self.counter = state["counter"]
        self.launch_calls = state.get("launch_calls", 0)
        self.capacity_errors = state.get("capacity_errors", 0)
        self.rng.setstate(_rng_from_json(state["rng"]))


def capacity_key(node_type: str, interruptible: bool) -> str:
    return f"{node_type}/spot" if interruptible else node_type


def _rng_to_json(state):
    version, internal, gauss = state
    return [version, list(internal), gauss]


def _rng_from_json(state):
    version, internal, gauss = state
    return (version, tuple(internal), gauss)
