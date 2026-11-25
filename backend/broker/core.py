"""The Job Broker: queue, placement, node lifecycle, idle reaper, cost ledger.

Everything runs on a simulated clock. `advance_to(t)` is a small
discrete-event loop: it jumps to the next thing that can change state (a job
ending, a node becoming ready, a reclaim notice, a boot timeout, a reaper
deadline, a backoff expiring), processes it, re-runs the scheduler, repeats.

Vocabulary used below:
  market   on-demand or interruptible ("spot"). Low-priority jobs use spot.
  tenancy  "*" for teams that share nodes, or the team name for a team that
           opted into isolation (it never shares a node with another team).
  pool     (spot?, tenancy): jobs in the same pool may share a node.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Iterator, Optional, Union

from .clock import SimClock
from .config import Config
from .models import OVERHEAD_TEAM, PRIORITIES, TERMINAL, VALID_GPUS, Job, Node, Segment
from .provider import NODE_TYPES, CapacityUnavailable, FakeProvider, capacity_key, node_price

MIN_BILLED_SECONDS = 600
TYPE_ORDER = ("gpu-s", "gpu-m", "gpu-l")

Pool = tuple[bool, str]


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

    # ===================================================== pools & rules
    def _isolated(self, team: str) -> bool:
        return self.config.isolation or team in self.config.isolated_teams

    def _tenancy(self, team: str) -> str:
        return team if self._isolated(team) else "*"

    def wants_interruptible(self, job: Job) -> bool:
        return (self.config.interruptible_for_low and job.priority == "low"
                and not job.no_interruptible and self.effective_priority(job) == 0)

    def _pool(self, job: Job) -> Pool:
        return (self.wants_interruptible(job), self._tenancy(job.team))

    def _node_pool(self, n: Node) -> Pool:
        return (n.interruptible, self._tenancy(n.team))

    def _teams_on(self, n: Node) -> set[str]:
        return {self.jobs[j].team for j in n.job_ids}

    def _dominant_team(self, n: Node) -> str:
        gpus: Counter = Counter()
        for j in n.job_ids:
            gpus[self.jobs[j].team] += self.jobs[j].gpus
        return gpus.most_common(1)[0][0] if gpus else n.team

    def _reserved_slots(self) -> int:
        return max(0, min(self.config.high_reserved_slots, self.config.max_nodes - 1))

    def _can_place(self, n: Node, job: Job, spot: bool) -> bool:
        if n.interruptible != spot:
            return False
        if n.reserved and job.priority != "high":
            return False
        if n.draining and n.drain_for != job.team:
            return False
        teams = self._teams_on(n)
        if not teams or teams == {job.team}:
            return True
        if self._isolated(job.team):
            return False
        return not any(self._isolated(t) for t in teams)

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

        for node in [n for n in self.alive.values() if n.interruptible]:
            info = self.provider.describe_node(node.id)
            if info["state"] == "reclaimed":
                at = info["reclaim_at"]
                self.stats["reclaims"] += 1
                node.retire = False  # it is ending anyway; don't terminate twice
                for jid in list(node.job_ids):
                    self._requeue(self.jobs[jid], at, "requeued: interruptible node reclaimed")
                self.provider.terminate_node(node.id)
                self._end_node(node, at, "reclaimed")
            elif info["reclaim_notice"] and not node.reclaim_notice:
                # 2 minutes' notice: too short to save work without checkpoints,
                # but stop placing anything new on it
                node.reclaim_notice = node.draining = True
                node.drain_for = None

        for node in [n for n in self.alive.values() if n.state == "booting"]:
            info = self.provider.describe_node(node.id)
            if info["state"] == "ready":
                self._node_ready(node, min(info["ready_at"], now))
            elif info["state"] in ("failed", "terminated"):
                self.stats["never_ready"] += 1
                self.provider.terminate_node(node.id)
                end = self.provider.nodes[node.id].terminated_at or now
                self._end_node(node, end, "failed", failed_launch=True)
            elif now >= node.launched_at + self.config.boot_timeout_minutes * 60:
                self.stats["boot_timeouts"] += 1
                self.provider.terminate_node(node.id)
                self._end_node(node, now, "failed", failed_launch=True)

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
        # unclaimed capacity on nodes still booting, and which pool claimed it
        planned = {n.id: n.gpus for n in self.alive.values() if n.state == "booting"}
        planned_pool = {n.id: self._node_pool(n) for n in self.alive.values()
                        if n.state == "booting"}
        pools = {j.id: self._pool(j) for j in self.queue.values()}
        pending: dict[Pool, int] = defaultdict(int)  # not-yet-handled demand per pool
        for j in self.queue.values():
            pending[pools[j.id]] += j.gpus

        # smallest queued request per team: an empty node launched for team X is
        # not handed to another team while X has queued work that fits on it
        self._waiting_min_gpus = {}
        for j in self.queue.values():
            cur = self._waiting_min_gpus.get(j.team)
            self._waiting_min_gpus[j.team] = j.gpus if cur is None else min(cur, j.gpus)

        hard_blocked = False  # a high job hit the full cap: nobody launches
        soft_blocked = False  # a non-high job hit cap minus reserved slots
        blocked: list[Job] = []
        for job in self._ordered(alloc):
            pool = pools[job.id]
            spot = pool[0]
            demand = pending[pool]
            pending[pool] -= job.gpus

            max_gpus = (NODE_TYPES[self._type_candidates(job.gpus, demand)[0]].gpus
                        if cfg.right_size else None)
            node = self._find_ready_node(job, spot, max_empty_gpus=max_gpus)
            if node is not None:
                self._start(job, node)
                alloc[job.team] += job.gpus
                continue
            booting = self._find_booting_node(job, pool, planned, planned_pool)
            if booting is not None:
                planned[booting.id] -= job.gpus
                planned_pool[booting.id] = pool
                alloc[job.team] += job.gpus
                continue
            is_high = job.priority == "high"
            if hard_blocked or (soft_blocked and not is_high):
                result: Union[Node, str] = "cap"
            else:
                result = self._launch_for(job, demand, spot)
            if isinstance(result, Node):
                planned[result.id] = result.gpus - job.gpus
                planned_pool[result.id] = pool
                alloc[job.team] += job.gpus
                continue
            # cannot launch: an oversized empty node now beats waiting
            node = self._find_ready_node(job, spot) if cfg.right_size else None
            if node is not None:
                self._start(job, node)
                alloc[job.team] += job.gpus
                continue
            if result == "cap":
                # don't let a lower-ranked job take a node slot a higher one needs
                if is_high:
                    hard_blocked = True
                else:
                    soft_blocked = True
                blocked.append(job)

        first_blocked = list({j.team: j for j in reversed(blocked)}.values())[::-1]
        if cfg.ordering == "fair" and cfg.drain:
            for job in first_blocked:
                self._maybe_drain(job, pools[job.id][0])
        if cfg.upgrade and blocked:
            backlog: dict[Pool, int] = defaultdict(int)
            for j in self.queue.values():
                backlog[pools.get(j.id, self._pool(j))] += j.gpus
            for pool in dict.fromkeys(pools[j.id] for j in first_blocked):
                self._maybe_upgrade(pool, backlog[pool])

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

    def _find_ready_node(self, job: Job, spot: bool,
                         max_empty_gpus: Optional[int] = None) -> Optional[Node]:
        """Best ready node for `job`. Empty nodes larger than `max_empty_gpus`
        are skipped (right-sizing: a 1-GPU job should not hold an 8-GPU node
        when a right-sized one is a few minutes' boot away)."""
        best, best_key = None, None
        for n in self.alive.values():
            if n.state != "ready" or n.free_gpus < job.gpus:
                continue
            if not self._can_place(n, job, spot):
                continue
            if not n.job_ids:
                if n.team != job.team and self._waiting_min_gpus.get(n.team, 99) <= n.gpus:
                    continue  # reserved for the team that launched it
                if max_empty_gpus is not None and n.gpus > max_empty_gpus:
                    continue
            # prefer packing onto a busy node (best fit), then the smallest empty node
            key = (0, n.free_gpus - job.gpus) if n.job_ids else (1, n.gpus)
            if best_key is None or key < best_key:
                best, best_key = n, key
        return best

    def _find_booting_node(self, job: Job, pool: Pool, planned: dict[str, int],
                           planned_pool: dict[str, Pool]) -> Optional[Node]:
        best = None
        for n in self.alive.values():
            free = planned.get(n.id)
            if n.state != "booting" or free is None or free < job.gpus:
                continue
            if n.interruptible != pool[0] or (n.reserved and job.priority != "high"):
                continue
            # capacity promised to another pool is off limits unless nobody has
            # claimed any of the node yet in this pass
            if planned_pool[n.id] != pool and free < n.gpus:
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

    def _launch_for(self, job: Job, demand: int, spot: bool) -> Union[Node, str]:
        now = self.now
        is_high = job.priority == "high"
        limit = self.config.max_nodes - (0 if is_high else self._reserved_slots())
        # a high job launching into the reserved headroom gets a reserved node,
        # sized for that job alone (nobody else may use it)
        reserved = is_high and len(self.alive) >= self.config.max_nodes - self._reserved_slots()
        types = [t for t in self._type_candidates(job.gpus, job.gpus if reserved else demand)
                 if self.backoff_until.get(capacity_key(t, spot), 0) <= now]
        if not types:
            return "backoff"
        if len(self.alive) >= limit:
            victim = self._eviction_candidate()
            if victim is None or len(self.alive) > limit:
                return "cap"
            self.stats["evictions"] += 1
            self._terminate(victim)
        for t in types:
            key = capacity_key(t, spot)
            try:
                node_id = self.provider.launch_node(t, interruptible=spot)
            except CapacityUnavailable:
                self.stats["capacity_errors"] += 1
                fails = self.backoff_fails.get(key, 0) + 1
                self.backoff_fails[key] = fails
                delay = min(self.config.backoff_base_seconds * 2 ** (fails - 1),
                            self.config.backoff_max_seconds)
                self.backoff_until[key] = now + delay
                continue
            self.backoff_fails[key] = 0
            node = Node(id=node_id, type=t, gpus=NODE_TYPES[t].gpus,
                        price_per_hour=node_price(t, spot), launched_at=now, team=job.team,
                        seg_kind="boot", seg_start=now, interruptible=spot, reserved=reserved)
            self.nodes[node.id] = node
            self.alive[node.id] = node
            self.stats["launches"] += 1
            if spot:
                self.stats["interruptible_launches"] += 1
            return node
        return "provider"

    def _eviction_candidate(self) -> Optional[Node]:
        """An empty ready node that didn't suit the job we need a slot for."""
        idle = [n for n in self.alive.values() if n.state == "ready" and not n.job_ids]
        return min(idle, key=lambda n: n.idle_since) if idle else None

    def _maybe_drain(self, job: Job, spot: bool) -> None:
        """A busy node keeps being refilled by whoever fits, so a team that
        holds most nodes (or a stream of small jobs) can starve a team that
        needs room. If `job`'s team is blocked by the node cap and another team
        dominates clearly more nodes, stop placing new work on that team's
        soonest-finishing node; once empty it goes to the blocked team."""
        team = job.team
        if any(n.draining and n.drain_for == team for n in self.alive.values()):
            return
        counts = Counter(self._dominant_team(n) for n in self.alive.values())
        mine = counts.get(team, 0)
        others = [(c, t) for t, c in counts.items() if t != team]
        if not others:
            return
        count, rich = max(others)
        if count <= mine + 1:
            return
        cands = [n for n in self.alive.values()
                 if n.state == "ready" and n.job_ids and not n.draining and not n.reserved
                 and n.interruptible == spot and self._dominant_team(n) == rich]
        if not cands:
            return
        node = min(cands, key=lambda n: max(self.jobs[j].expected_end for j in n.job_ids))
        node.draining, node.drain_for = True, team
        self.stats["drains"] += 1

    def _maybe_upgrade(self, pool: Pool, backlog: int) -> None:
        """Node slots are the scarce thing at the cap. If a pool's backlog would
        fill a whole gpu-l, stop refilling one of its smaller nodes; when it
        empties it is terminated and the slot is relaunched as a gpu-l."""
        largest = NODE_TYPES[TYPE_ORDER[-1]].gpus
        if backlog < largest:
            return
        mine = [n for n in self.alive.values() if self._node_pool(n) == pool]
        if any(n.retire for n in mine):
            return
        cands = [n for n in mine if n.state == "ready" and n.gpus < largest
                 and not n.draining and n.job_ids]
        if not cands:
            return
        node = min(cands, key=lambda n: (n.gpus, max(self.jobs[j].expected_end
                                                     for j in n.job_ids)))
        node.draining = node.retire = True
        node.drain_for = None
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
        if job.first_started_at is None:
            job.first_started_at = now
        job.attempts += 1
        del self.queue[job.id]
        self.running[job.id] = job

    def _release(self, job: Job, at: int) -> None:
        """Take a job off its node and record the run interval."""
        del self.running[job.id]
        node = self.nodes[job.node_id]
        job.runs.append([job.started_at, at, node.id])
        node.job_ids.remove(job.id)
        node.used_gpus -= job.gpus
        if not node.job_ids:
            self._phase(node, "idle", node.team, at)
            node.idle_since = at
            if not node.reclaim_notice:
                node.draining, node.drain_for = False, None
            if node.retire and node.alive:
                self.stats["retired"] += 1
                self._terminate(node)

    def _finish(self, job: Job, outcome: str, at: int) -> None:
        self._release(job, at)
        job.status, job.ended_at = outcome, at

    def _requeue(self, job: Job, at: int, reason: str) -> None:
        """Infrastructure lost the job's node: run it again from scratch,
        on-demand this time. Submit time is kept, so it keeps its place."""
        self.stats["requeued"] += 1
        self.stats["lost_gpu_seconds"] += job.gpus * (at - job.started_at)
        self._release(job, at)
        job.status, job.started_at, job.node_id, job.reason = "queued", None, None, reason
        job.interruptions += 1
        job.no_interruptible = True
        self.queue[job.id] = job

    def _node_ready(self, node: Node, at: int) -> None:
        node.state, node.ready_at, node.idle_since = "ready", at, at
        self._phase(node, "idle", node.team, at)

    def _terminate(self, node: Node) -> None:
        self.provider.terminate_node(node.id)
        self._end_node(node, self.now, "terminated")

    def _phase(self, node: Node, kind: str, team: str, at: int,
               close_kind: Optional[str] = None) -> None:
        """Close the node's open cost segment at `at` and open a new one."""
        if at > node.seg_start:
            self.segments.append(Segment(node.id, node.team, close_kind or node.seg_kind,
                                         node.seg_start, at, node.price_per_hour))
        node.seg_kind, node.team, node.seg_start = kind, team, at

    def _end_node(self, node: Node, at: int, state: str, failed_launch: bool = False) -> None:
        self._phase(node, "ended", node.team, at,
                    close_kind="failed_launch" if failed_launch else None)
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
          commit) are terminated; their cost lands in the shared idle pool;
        * broker nodes the provider no longer has are marked lost and their
          running jobs go back to the queue.
        """
        now = self.now
        for pid in self.provider.list_alive():
            if pid in self.nodes:
                continue
            p = self.provider.nodes[pid]
            node = Node(id=pid, type=p.type, gpus=NODE_TYPES[p.type].gpus,
                        price_per_hour=p.price_per_hour, launched_at=p.launched_at,
                        team=OVERHEAD_TEAM, seg_start=p.launched_at,
                        interruptible=p.interruptible)
            self.nodes[pid] = node
            self.alive[pid] = node
            self.stats["orphans_terminated"] += 1
            self.provider.terminate_node(pid)
            self._end_node(node, now, "terminated", failed_launch=True)
        for node in list(self.alive.values()):
            p = self.provider.nodes.get(node.id)
            if p is not None and p.state(now) in ("booting", "ready", "failed", "reclaimed"):
                continue  # the normal event loop handles failed / reclaimed
            node.retire = False
            for jid in list(node.job_ids):
                self._requeue(self.jobs[jid], now, "requeued: node lost")
            self._end_node(node, now, "terminated")
        self.schedule()

    def _commit(self) -> None:
        if self.store is not None:
            self.store.save(self)

    # ============================================================ reports
    def cost_report(self, start: Optional[int] = None, end: Optional[int] = None) -> dict:
        """GPU-hours and cost per team for [start, end).

        busy_cost: each job pays for the GPUs it used, at its node's per-GPU
        price, for the time it ran. Everything else the provider bills (boot,
        idle, unused GPUs on busy nodes, the 10-minute minimum, failed
        launches) is a shared idle pool, split across teams pro-rata by their
        GPU-hours in the window.
        """
        if start is None:
            first = [n.launched_at for n in self.nodes.values()] + [
                j.submitted_at for j in self.jobs.values()]
            start = min(first, default=self.now)
        lo = start
        hi = self.now if end is None else min(end, self.now)

        segs = list(self.segments) + [
            Segment(n.id, n.team, n.seg_kind, n.seg_start, self.now, n.price_per_hour)
            for n in self.alive.values()]
        billed = 0.0
        busy_segments = 0.0
        pool_breakdown: dict[str, float] = defaultdict(float)
        for s in segs:
            c = s.cost_in(lo, hi)
            if c <= 0:
                continue
            billed += c
            if s.kind == "busy":
                busy_segments += c
            else:
                pool_breakdown[s.kind] += c

        rows: dict[str, dict] = defaultdict(lambda: {"gpu_hours": 0.0, "busy_cost": 0.0, "jobs": 0})
        for j in self.jobs.values():
            intervals = list(j.runs)
            if j.status == "running":
                intervals.append([j.started_at, self.now, j.node_id])
            counted = False
            for st, en, nid in intervals:
                overlap = min(en, hi) - max(st, lo)
                if overlap <= 0:
                    continue
                n = self.nodes[nid]
                rows[j.team]["gpu_hours"] += j.gpus * overlap / 3600
                rows[j.team]["busy_cost"] += overlap * j.gpus * n.price_per_hour / n.gpus / 3600
                counted = True
            if counted:
                rows[j.team]["jobs"] += 1

        job_busy = sum(r["busy_cost"] for r in rows.values())
        pool_breakdown["unused_gpus"] = max(0.0, busy_segments - job_busy)
        idle_pool = max(0.0, billed - job_busy)
        total_gh = sum(r["gpu_hours"] for r in rows.values())
        if total_gh <= 0 and idle_pool > 0:
            rows["_unallocated"]  # nobody used a GPU in the window
        teams = []
        for team, r in sorted(rows.items()):
            if total_gh > 0:
                idle = idle_pool * r["gpu_hours"] / total_gh
            else:
                idle = idle_pool if team == "_unallocated" else 0.0
            total = r["busy_cost"] + idle
            teams.append({
                "team": team,
                "gpu_hours": round(r["gpu_hours"], 3),
                "busy_cost": round(r["busy_cost"], 2),
                "idle_cost": round(idle, 2),
                "total_cost": round(total, 2),
                "idle_share": round(idle / total, 3) if total else 0.0,
                "jobs": r["jobs"],
            })
        return {
            "start": lo, "end": hi,
            "teams": teams,
            "total_cost": round(billed, 2),
            "total_idle_cost": round(idle_pool, 2),
            "idle_pool_breakdown": {k: round(v, 2) for k, v in sorted(pool_breakdown.items())
                                    if v > 0.005},
            "total_gpu_hours": round(total_gh, 3),
        }
