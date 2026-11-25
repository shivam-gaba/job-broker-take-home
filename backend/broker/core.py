"""The Job Broker: queue, placement, node lifecycle, idle reaper, cost ledger.

Everything runs on a simulated clock. `advance_to(t)` is a small
discrete-event loop: it jumps to the next thing that can change state (a job
ending, a node becoming ready, a boot timeout, a reaper deadline, a backoff
expiring), processes it, re-runs the scheduler, and repeats.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Iterator, Optional, Union

from .clock import SimClock
from .config import Config
from .models import OVERHEAD_TEAM, PRIORITIES, TERMINAL, VALID_GPUS, Job, Node, Segment
from .provider import NODE_TYPES, CapacityUnavailable, FakeProvider

MIN_BILLED_SECONDS = 600
TYPE_ORDER = ("gpu-s", "gpu-m", "gpu-l")


class BrokerError(Exception):
    status = 400


class InvalidRequest(BrokerError):
    status = 422


class NotFound(BrokerError):
    status = 404


class Conflict(BrokerError):
    status = 409


class Broker:
    def __init__(self, provider: FakeProvider, clock: SimClock,
                 config: Optional[Config] = None, store=None) -> None:
        self.provider = provider
        self.clock = clock
        self.config = config or Config()
        self.store = store
        self.jobs: dict[str, Job] = {}
        self.nodes: dict[str, Node] = {}
        self.segments: list[Segment] = []
        self.idem: dict[str, str] = {}
        self.job_counter = 0
        self.backoff_until: dict[str, int] = {}
        self.backoff_fails: dict[str, int] = {}
        self.stats: Counter = Counter()
        # indexes (rebuilt on load)
        self.queue: dict[str, Job] = {}
        self.running: dict[str, Job] = {}
        self.alive: dict[str, Node] = {}
        self._waiting_min_gpus: dict[str, int] = {}
        if store is not None:
            store.load_into(self)
            self.reconcile()
            self._commit()

    @property
    def now(self) -> int:
        return self.clock.now

    # ================================================================ API
    def submit(self, team: str, gpus: int, estimated_minutes: float,
               priority: str = "normal", idempotency_key: Optional[str] = None,
               run_minutes: Optional[float] = None,
               outcome: str = "succeeded") -> tuple[Job, bool]:
        """Returns (job, created). Re-submitting a known key returns the
        original job with created=False; a different payload is a conflict."""
        team = (team or "").strip()
        if not team or team.startswith("_"):
            raise InvalidRequest("team is required (and may not start with '_')")
        if gpus not in VALID_GPUS:
            raise InvalidRequest(f"gpus must be one of {VALID_GPUS}")
        if not estimated_minutes or estimated_minutes <= 0:
            raise InvalidRequest("estimated_minutes must be > 0")
        if priority not in PRIORITIES:
            raise InvalidRequest(f"priority must be one of {list(PRIORITIES)}")
        if outcome not in TERMINAL:
            raise InvalidRequest(f"outcome must be one of {TERMINAL}")
        if run_minutes is not None and run_minutes < 0:
            raise InvalidRequest("run_minutes must be >= 0")

        if idempotency_key:
            existing_id = self.idem.get(idempotency_key)
            if existing_id:
                job = self.jobs[existing_id]
                same = (job.team, job.gpus, job.estimated_minutes, job.priority) == (
                    team, gpus, int(estimated_minutes), priority)
                if not same:
                    raise Conflict("idempotency key already used with a different payload")
                return job, False

        self.job_counter += 1
        job = Job(
            id=f"j-{self.job_counter:06d}", team=team, gpus=gpus,
            estimated_minutes=int(estimated_minutes), priority=priority,
            submitted_at=self.now, idempotency_key=idempotency_key,
            run_seconds=round((estimated_minutes if run_minutes is None else run_minutes) * 60),
            final_outcome=outcome,
        )
        self.jobs[job.id] = job
        self.queue[job.id] = job
        if idempotency_key:
            self.idem[idempotency_key] = job.id
        self.schedule()
        self._commit()
        return job, True

    def get(self, job_id: str) -> Job:
        job = self.jobs.get(job_id)
        if job is None:
            raise NotFound(f"job {job_id} not found")
        return job

    def cancel(self, job_id: str) -> Job:
        job = self.get(job_id)
        if job.status == "queued":
            del self.queue[job.id]
            job.status, job.ended_at, job.reason = "cancelled", self.now, "cancelled while queued"
        elif job.status == "running":
            self._finish(job, "cancelled", self.now)
            job.reason = "cancelled while running"
        else:
            raise Conflict(f"job is already {job.status}")
        self.schedule()
        self._commit()
        return job

    def complete(self, job_id: str, outcome: str = "succeeded") -> Job:
        """A job reports its own completion (instead of the simulated clock)."""
        if outcome not in ("succeeded", "failed"):
            raise InvalidRequest("outcome must be succeeded or failed")
        job = self.get(job_id)
        if job.status != "running":
            raise Conflict(f"job is {job.status}, not running")
        self._finish(job, outcome, self.now)
        self.schedule()
        self._commit()
        return job

    def advance(self, seconds: int) -> None:
        if seconds < 0:
            raise InvalidRequest("cannot move time backwards")
        self.advance_to(self.now + int(seconds))

    def advance_to(self, target: int, commit: bool = True) -> None:
        if target < self.now:
            raise InvalidRequest("cannot move time backwards")
        while True:
            t = self._next_event_time()
            if t is None or t > target:
                break
            self.clock.now = t
            self._process_due()
            self.schedule()
        self.clock.now = target
        self._process_due()
        self.schedule()
        if commit:
            self._commit()

    def run_until_idle(self, limit_seconds: int = 30 * 24 * 3600) -> None:
        """Advance until no work is left and every node has been reaped."""
        deadline = self.now + limit_seconds
        while self.now < deadline:
            t = self._next_event_time()
            if t is None:
                break
            self.advance_to(t, commit=False)
        self._commit()

    # ============================================================ events
    def _next_event_time(self) -> Optional[int]:
        now = self.now
        times: list[int] = [j.started_at + j.run_seconds for j in self.running.values()]
        pe = self.provider.next_event_time()
        if pe is not None:
            times.append(pe)
        for n in self.alive.values():
            if n.state == "booting":
                times.append(n.launched_at + int(self.config.boot_timeout_minutes * 60))
            elif not n.job_ids:
                times.append(self._reap_deadline(n))
        if self.queue:
            times.extend(self.backoff_until.values())
        future = [t for t in times if t > now]
        return min(future) if future else None

    def _reap_deadline(self, n: Node) -> int:
        return max(n.idle_since + int(self.config.reaper_idle_minutes * 60),
                   n.launched_at + MIN_BILLED_SECONDS)

    def _process_due(self) -> None:
        now = self.now
        for job in sorted(self.running.values(), key=lambda j: j.started_at + j.run_seconds):
            end = job.started_at + job.run_seconds
            if end <= now:
                self._finish(job, job.final_outcome, end)

        for node in [n for n in self.alive.values() if n.state == "booting"]:
            info = self.provider.describe_node(node.id)
            if info["state"] == "ready":
                self._node_ready(node, min(info["ready_at"], now))
            elif info["state"] in ("failed", "terminated"):
                self.stats["never_ready"] += 1
                self.provider.terminate_node(node.id)
                end = self.provider.nodes[node.id].terminated_at or now
                self._end_node(node, end, "failed", overhead=True)
            elif now >= node.launched_at + self.config.boot_timeout_minutes * 60:
                self.stats["boot_timeouts"] += 1
                self.provider.terminate_node(node.id)
                self._end_node(node, now, "failed", overhead=True)

        for node in [n for n in self.alive.values() if n.state == "ready" and not n.job_ids]:
            if now >= self._reap_deadline(node):
                self.stats["reaped"] += 1
                self._terminate(node)

    # ========================================================== scheduler
    def schedule(self) -> None:
        if not self.queue:
            return
        cfg = self.config
        alloc: dict[str, int] = defaultdict(int)  # GPUs held per team (fairness)
        for j in self.running.values():
            alloc[j.team] += j.gpus
        # unclaimed capacity on nodes that are still booting
        planned = {n.id: n.gpus for n in self.alive.values() if n.state == "booting"}
        pending: dict[str, int] = defaultdict(int)  # not-yet-handled demand
        for j in self.queue.values():
            pending[j.team if cfg.isolation else "*"] += j.gpus

        # smallest queued request per team: an empty node launched for team X is
        # not handed to team Y while X still has queued work that fits on it
        self._waiting_min_gpus: dict[str, int] = {}
        for j in self.queue.values():
            cur = self._waiting_min_gpus.get(j.team)
            self._waiting_min_gpus[j.team] = j.gpus if cur is None else min(cur, j.gpus)

        cap_blocked = False
        blocked_teams: list[str] = []
        for job in self._ordered(alloc):
            demand = pending[job.team if cfg.isolation else "*"]
            pending[job.team if cfg.isolation else "*"] -= job.gpus

            max_gpus = (NODE_TYPES[self._type_candidates(job.gpus, demand)[0]].gpus
                        if cfg.right_size else None)
            node = self._find_ready_node(job, max_empty_gpus=max_gpus)
            if node is not None:
                self._start(job, node)
                alloc[job.team] += job.gpus
                continue
            booting = self._find_booting_node(job, planned)
            if booting is not None:
                planned[booting.id] -= job.gpus
                alloc[job.team] += job.gpus
                continue
            result = "cap" if cap_blocked else self._launch_for(job, demand)
            if isinstance(result, Node):
                planned[result.id] = result.gpus - job.gpus
                alloc[job.team] += job.gpus
                continue
            # cannot launch: an oversized empty node now beats waiting
            node = self._find_ready_node(job) if cfg.right_size else None
            if node is not None:
                self._start(job, node)
                alloc[job.team] += job.gpus
                continue
            if result == "cap":
                # don't let a lower-ranked job take a node slot a higher one needs
                cap_blocked = True
                blocked_teams.append(job.team)

        if cfg.ordering == "fair" and cfg.drain:
            for team in dict.fromkeys(blocked_teams):
                self._maybe_drain(team)
        if cfg.upgrade and cap_blocked:
            backlog: dict[str, int] = defaultdict(int)
            for j in self.queue.values():
                backlog[j.team if cfg.isolation else "*"] += j.gpus
            for team in dict.fromkeys(blocked_teams):
                self._maybe_upgrade(team if cfg.isolation else None,
                                    backlog[team if cfg.isolation else "*"])

    def effective_priority(self, job: Job) -> int:
        base = PRIORITIES[job.priority]
        if self.config.ordering != "fair" or self.config.aging_minutes <= 0:
            return base
        waited = (self.now - job.submitted_at) / 60
        return min(PRIORITIES["high"], base + int(waited // self.config.aging_minutes))

    def _ordered(self, alloc: dict[str, int]) -> Iterator[Job]:
        """Yield queued jobs in the order the scheduler considers them.

        fair: highest effective priority first; ties go to the team currently
        holding the fewest GPUs (re-evaluated after every placement); then FIFO.
        """
        if self.config.ordering == "fifo":
            yield from sorted(self.queue.values(),
                              key=lambda j: (-PRIORITIES[j.priority], j.submitted_at, j.id))
            return
        eff = {j.id: self.effective_priority(j) for j in self.queue.values()}
        per_team: dict[str, list[Job]] = defaultdict(list)
        for j in self.queue.values():
            per_team[j.team].append(j)
        for lst in per_team.values():
            lst.sort(key=lambda j: (-eff[j.id], j.submitted_at, j.id), reverse=True)
        while per_team:
            team = min(per_team, key=lambda t: (-eff[per_team[t][-1].id], alloc[t],
                                                per_team[t][-1].submitted_at, per_team[t][-1].id))
            job = per_team[team].pop()
            if not per_team[team]:
                del per_team[team]
            yield job

    def queue_order(self) -> list[Job]:
        alloc: dict[str, int] = defaultdict(int)
        for j in self.running.values():
            alloc[j.team] += j.gpus
        return list(self._ordered(alloc))

    def _can_share(self, node: Node, team: str) -> bool:
        return not self.config.isolation or not node.job_ids or node.team == team

    def _find_ready_node(self, job: Job, max_empty_gpus: Optional[int] = None) -> Optional[Node]:
        """Best ready node for `job`. Empty nodes larger than `max_empty_gpus`
        are skipped (right-sizing: a 1-GPU job should not hold an 8-GPU node
        when a right-sized one is a few minutes' boot away)."""
        best, best_key = None, None
        for n in self.alive.values():
            if n.state != "ready" or n.draining or n.free_gpus < job.gpus:
                continue
            if not self._can_share(n, job.team):
                continue
            if (not n.job_ids and n.team != job.team
                    and self._waiting_min_gpus.get(n.team, 99) <= n.gpus):
                continue  # reserved for the team that launched it
            if not n.job_ids and max_empty_gpus is not None and n.gpus > max_empty_gpus:
                continue
            # prefer packing onto a busy node (best fit), then the smallest empty node
            key = (0, n.free_gpus - job.gpus) if n.job_ids else (1, n.gpus)
            if best_key is None or key < best_key:
                best, best_key = n, key
        return best

    def _find_booting_node(self, job: Job, planned: dict[str, int]) -> Optional[Node]:
        best = None
        for n in self.alive.values():
            free = planned.get(n.id)
            if n.state != "booting" or free is None or free < job.gpus:
                continue
            # a booting node is promised to the team that launched it, unless
            # nobody from that team has claimed any of it in this pass
            if self.config.isolation and n.team != job.team and free < n.gpus:
                continue
            if best is None or free < planned[best.id]:
                best = n
        return best

    def _type_candidates(self, gpus: int, demand: int) -> list[str]:
        fitting = [t for t in TYPE_ORDER if NODE_TYPES[t].gpus >= gpus]
        want = gpus if self.config.sizing == "smallest" else max(gpus, min(demand, 8))
        preferred = next(t for t in fitting if NODE_TYPES[t].gpus >= want)
        i = fitting.index(preferred)
        return [preferred] + fitting[i + 1:] + list(reversed(fitting[:i]))

    def _launch_for(self, job: Job, demand: int) -> Union[Node, str]:
        now = self.now
        types = [t for t in self._type_candidates(job.gpus, demand)
                 if self.backoff_until.get(t, 0) <= now]
        if not types:
            return "backoff"
        if len(self.alive) >= self.config.max_nodes:
            victim = self._eviction_candidate()
            if victim is None:
                return "cap"
            self.stats["evictions"] += 1
            self._terminate(victim)
        for t in types:
            try:
                node_id = self.provider.launch_node(t)
            except CapacityUnavailable:
                self.stats["capacity_errors"] += 1
                fails = self.backoff_fails.get(t, 0) + 1
                self.backoff_fails[t] = fails
                delay = min(self.config.backoff_base_seconds * 2 ** (fails - 1),
                            self.config.backoff_max_seconds)
                self.backoff_until[t] = now + delay
                continue
            self.backoff_fails[t] = 0
            nt = NODE_TYPES[t]
            node = Node(id=node_id, type=t, gpus=nt.gpus, price_per_hour=nt.price_per_hour,
                        launched_at=now, team=job.team, seg_kind="boot", seg_start=now)
            self.nodes[node.id] = node
            self.alive[node.id] = node
            self.stats["launches"] += 1
            return node
        return "provider"

    def _eviction_candidate(self) -> Optional[Node]:
        """An empty ready node that didn't suit the job we need a slot for."""
        idle = [n for n in self.alive.values() if n.state == "ready" and not n.job_ids]
        return min(idle, key=lambda n: n.idle_since) if idle else None

    def _maybe_drain(self, team: str) -> None:
        """Under isolation a busy node is refilled by its own team forever, so a
        team that grabbed every node can starve the rest. If `team` is blocked
        by the node cap and another team holds clearly more nodes, stop placing
        new work on that team's soonest-finishing node; once empty it can be
        reused or replaced for `team`."""
        if any(n.draining and n.drain_for == team for n in self.alive.values()):
            return
        counts = Counter(n.team for n in self.alive.values())
        mine = counts.get(team, 0)
        others = [(c, t) for t, c in counts.items() if t != team]
        if not others:
            return
        count, rich = max(others)
        if count <= mine + 1:
            return
        cands = [n for n in self.alive.values()
                 if n.team == rich and n.state == "ready" and n.job_ids and not n.draining]
        if not cands:
            return
        node = min(cands, key=lambda n: max(self.jobs[j].expected_end for j in n.job_ids))
        node.draining, node.drain_for = True, team
        self.stats["drains"] += 1

    def _maybe_upgrade(self, team: Optional[str], backlog: int) -> None:
        """Node slots are the scarce thing at the cap. If a team's backlog would
        fill a whole gpu-l, stop refilling one of its smaller nodes; when it
        empties it is terminated and the slot is relaunched as a gpu-l."""
        largest = NODE_TYPES[TYPE_ORDER[-1]].gpus
        if backlog < largest:
            return
        mine = [n for n in self.alive.values() if team is None or n.team == team]
        if any(n.retire for n in mine):
            return
        cands = [n for n in mine if n.state == "ready" and n.gpus < largest
                 and not n.draining and n.job_ids]
        if not cands:
            return
        node = min(cands, key=lambda n: (n.gpus, max(self.jobs[j].expected_end
                                                     for j in n.job_ids)))
        node.draining = node.retire = True
        self.stats["upgrades"] += 1

    # ================================================== state transitions
    def _start(self, job: Job, node: Node) -> None:
        now = self.now
        if not node.job_ids:
            self._phase(node, "busy", job.team, now)
            node.idle_since = None
        node.job_ids.append(job.id)
        node.used_gpus += job.gpus
        job.status, job.started_at, job.node_id = "running", now, node.id
        job.attempts += 1
        del self.queue[job.id]
        self.running[job.id] = job

    def _finish(self, job: Job, outcome: str, at: int) -> None:
        job.status, job.ended_at = outcome, at
        del self.running[job.id]
        node = self.nodes[job.node_id]
        node.job_ids.remove(job.id)
        node.used_gpus -= job.gpus
        if not node.job_ids:
            self._phase(node, "idle", node.team, at)
            node.idle_since = at
            node.draining, node.drain_for = False, None
            if node.retire:
                self.stats["retired"] += 1
                self._terminate(node)

    def _node_ready(self, node: Node, at: int) -> None:
        node.state, node.ready_at, node.idle_since = "ready", at, at
        self._phase(node, "idle", node.team, at)

    def _terminate(self, node: Node) -> None:
        self.provider.terminate_node(node.id)
        self._end_node(node, self.now, "terminated")

    def _phase(self, node: Node, kind: str, team: str, at: int,
               close_team: Optional[str] = None, close_kind: Optional[str] = None) -> None:
        """Close the node's open cost segment at `at` and open a new one."""
        if at > node.seg_start:
            self.segments.append(Segment(node.id, close_team or node.team, close_kind or node.seg_kind,
                                         node.seg_start, at, node.price_per_hour))
        node.seg_kind, node.team, node.seg_start = kind, team, at

    def _end_node(self, node: Node, at: int, state: str, overhead: bool = False) -> None:
        if overhead:
            self._phase(node, "ended", OVERHEAD_TEAM, at, close_team=OVERHEAD_TEAM,
                        close_kind="failed_launch")
        else:
            self._phase(node, "ended", node.team, at)
        lifetime = at - node.launched_at
        if lifetime < MIN_BILLED_SECONDS:
            self.segments.append(Segment(node.id, node.team, "minimum", at, at,
                                         node.price_per_hour, MIN_BILLED_SECONDS - lifetime))
        node.state, node.ended_at = state, at
        node.draining = False
        self.alive.pop(node.id, None)

    # ============================================================ restart
    def reconcile(self) -> None:
        """After a restart, make the broker's view agree with the provider's.

        * provider nodes the broker never recorded (crash between launch and
          commit) are terminated and their cost booked as overhead;
        * broker nodes the provider no longer has are marked lost and their
          running jobs go back to the queue.
        """
        now = self.now
        for pid in self.provider.list_alive():
            if pid in self.nodes:
                continue
            p = self.provider.nodes[pid]
            nt = NODE_TYPES[p.type]
            node = Node(id=pid, type=p.type, gpus=nt.gpus, price_per_hour=nt.price_per_hour,
                        launched_at=p.launched_at, team=OVERHEAD_TEAM, seg_start=p.launched_at)
            self.nodes[pid] = node
            self.alive[pid] = node
            self.stats["orphans_terminated"] += 1
            self.provider.terminate_node(pid)
            self._end_node(node, now, "terminated", overhead=True)
        for node in list(self.alive.values()):
            p = self.provider.nodes.get(node.id)
            if p is not None and p.state(now) in ("booting", "ready", "failed"):
                continue  # failed ones are handled by the normal event loop
            for jid in list(node.job_ids):
                job = self.jobs[jid]
                self._finish(job, "queued", now)
                job.status, job.started_at, job.ended_at, job.node_id = "queued", None, None, None
                job.reason = "requeued: node lost"
                self.queue[job.id] = job
            self._end_node(node, now, "terminated")
        self.schedule()

    def _commit(self) -> None:
        if self.store is not None:
            self.store.save(self)

    # ============================================================ reports
    def cost_report(self, start: Optional[int] = None, end: Optional[int] = None) -> dict:
        lo = 0 if start is None else start
        hi = self.now if end is None else min(end, self.now)
        segs = list(self.segments) + [
            Segment(n.id, n.team, n.seg_kind, n.seg_start, self.now, n.price_per_hour)
            for n in self.alive.values()]
        rows: dict[str, dict] = defaultdict(lambda: {
            "gpu_hours": 0.0, "busy_cost": 0.0, "idle_cost": 0.0,
            "breakdown": defaultdict(float), "jobs": 0})
        for s in segs:
            c = s.cost_in(lo, hi)
            if c <= 0:
                continue
            r = rows[s.team]
            r["busy_cost" if s.kind == "busy" else "idle_cost"] += c
            r["breakdown"][s.kind] += c
        for j in self.jobs.values():
            if j.started_at is None:
                continue
            j_end = j.ended_at if j.ended_at is not None else self.now
            overlap = min(j_end, hi) - max(j.started_at, lo)
            if overlap > 0 or (lo <= j.started_at < hi):
                rows[j.team]["jobs"] += 1
            if overlap > 0:
                rows[j.team]["gpu_hours"] += j.gpus * overlap / 3600
        teams = []
        for team, r in sorted(rows.items()):
            total = r["busy_cost"] + r["idle_cost"]
            teams.append({
                "team": team,
                "gpu_hours": round(r["gpu_hours"], 3),
                "busy_cost": round(r["busy_cost"], 2),
                "idle_cost": round(r["idle_cost"], 2),
                "total_cost": round(total, 2),
                "idle_share": round(r["idle_cost"] / total, 3) if total else 0.0,
                "breakdown": {k: round(v, 2) for k, v in sorted(r["breakdown"].items())},
                "jobs": r["jobs"],
            })
        total_cost = sum(r["busy_cost"] + r["idle_cost"] for r in rows.values())
        idle_cost = sum(r["idle_cost"] for r in rows.values())
        return {
            "start": lo, "end": hi,
            "teams": teams,
            "total_cost": round(total_cost, 2),
            "total_idle_cost": round(idle_cost, 2),
            "total_gpu_hours": round(sum(r["gpu_hours"] for r in rows.values()), 3),
        }
