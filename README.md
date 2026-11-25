# Northwind Job Broker

A shared GPU job broker. It takes jobs from teams, places them on rented nodes, starts and stops nodes to control cost, and reports cost per team. It has a Python back end (FastAPI, SQLite, simulated clock), an Angular dashboard, and a trace replay that compares scheduling policies.

```
backend/broker/
  provider.py   fake cloud provider (bursty CapacityUnavailable, 4-8 min boot, 3% never ready,
                interruptible nodes with 2-min reclaim notice, billing)
  core.py       the broker: queue, placement, launch/backoff, reaper, lifecycle, fairness, cost ledger
  store.py      SQLite persistence (restart safety)
  api.py        HTTP API
  trace.py      trace loading + cleaning
  replay.py     trace replay + policy comparison
backend/tests/  pytest (core, API, restart, trace)
frontend/       Angular 19 dashboard
docs/API.md             HTTP API reference
docs/replay_results.md  generated replay output (all tables)
```

## Running it

Requirements: Python 3.10+ and Node 18+ (Node is only needed for the UI).

```bash
# back end + tests
cd backend
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest -q                                   # 38 tests, a few seconds

# trace replay (8 policy variants x 5 provider seeds, ~10 s)
python -m broker.replay ../data/01a_trace.csv --seeds 5 --out ../docs/replay_results.md

# API on :8000 (state in backend/var/, survives restarts)
uvicorn broker.api:app --port 8000
```

```bash
# UI, option A: dev server on :4200, proxies /api to :8000
cd frontend && npm ci && npm start
# UI, option B: build once; the API then serves it at http://localhost:8000/
cd frontend && npm ci && npm run build
```

Simulated time only moves when you click **+1m / +5m / +15m / +60m** in the UI or call `POST /api/clock/advance`. Nothing sleeps. To start from an empty state, delete `backend/var/`. Optional environment variables: `BROKER_REAPER_MINUTES` (default 10), `BROKER_ISOLATED_TEAMS` (comma-separated teams that must never share a node), `BROKER_ISOLATE_ALL=1`, `BROKER_START` (default `2025-03-03T00:00:00Z`) and `BROKER_VAR_DIR`.

## Design decisions

### How I weighed the stakeholders

| Voice | Weight | What I did |
|---|---|---|
| **Security**: no two teams on one machine at the same time | **A requirement for teams that need it, not for everyone.** By default teams share nodes; a team with sensitive data opts in. | `isolated_teams` (or `BROKER_ISOLATED_TEAMS`): an isolated team never shares a node with any other team at the same time. Once empty, a node can be reused; I assume nodes are wiped between tenants. `isolation=True` isolates every team. |
| **ML engineers**: start within 10 min | Strong goal, not a guarantee. A cold node takes 4-8 minutes to boot, so 10 minutes is just reachable. | Packing onto running nodes, right-sized launches, a boot timeout for nodes that never come up, fair ordering, and a node slot reserved for high-priority work. Reported as "% started within 10 min". |
| **Finance**: idle time, accountability | Strong. | Idle reaper. Each job pays for its own GPUs; all other spend is a visible idle pool shared by usage. Interruptible nodes for low-priority work. The ledger reconciles to the provider invoice. |
| **Platform**: don't page us weekly | Strong. It shapes the operations design. | Self-healing: per-type backoff, boot timeout, automatic re-queue on reclaim, provider reconciliation on restart. One process plus SQLite, no other moving parts. |

### Scheduling (requirement 2)
- **Pools.** Jobs that may share a node form a *pool*: same market (on-demand, or interruptible for low priority) and compatible tenancy (all sharing teams together; each isolated team alone).
- **Placement.** A job goes, in order of preference:
  1. onto a busy node it may share that has enough free GPUs (best fit);
  2. onto an empty node, but only if that node is no bigger than the one we would launch for it;
  3. onto free capacity on a node already booting for its pool;
  4. onto a newly launched node.

  An empty node launched for one team is not handed to another team while the launching team still has queued work that fits on it.
- **Sizing.** A new node is sized for the pool's queued demand. Demand of 1 GPU gets a gpu-s, 2-4 a gpu-m, 5 or more a gpu-l. This packs bursts of small jobs onto fewer nodes.
- **Right-sizing.** An idle oversized node (for example a gpu-l) is not handed to a 1-GPU job when a gpu-s is a few minutes of boot away. We fall back to the oversized node only when no launch is possible.
- **Node cap (10).** Booting nodes count toward the cap. One slot is reserved for high priority (see below). At the cap, an empty node that doesn't suit the job is terminated to free its slot. Once a higher-ranked job is blocked by the cap, lower-ranked jobs may still use existing capacity but may not take a new slot.
- **Slot upgrade.** At the cap, if a pool's backlog would fill a whole gpu-l (8 GPUs or more), one of its smaller nodes stops taking new jobs and is terminated when it empties. The slot is then relaunched as a gpu-l. Without this, the slots fill up with gpu-s/gpu-m nodes that are refilled forever during a burst.
- **Provider failures.**
  - `CapacityUnavailable` puts that node type and market into exponential backoff (1, 2, 4 … 16 min, reset on success). We try the other node types that fit right away. Jobs stay queued and nothing crashes.
  - A node that isn't ready after **9 minutes** (healthy nodes take 4-8) is assumed to be one of the 3% that never boot. We terminate it and launch a replacement instead of waiting for the provider's 15-minute failure report.
- **Idle reaper (requirement 3).** A node with no jobs for N minutes is terminated (N defaults to 10, configurable). The first 10 minutes of a node are paid for anyway (billing minimum), so a node is never reaped before `launched_at + 10 min`.

### Interruptible nodes
- **Who uses them.** Low-priority jobs run on interruptible nodes at 35% of the price. Interruptible and on-demand jobs never share a node.
- **Reclaim notice.** On the 2-minute notice the node stops taking new jobs. Without checkpointing, 2 minutes isn't enough to save any work.
- **On reclaim.** Running jobs go back to the queue with their original submit time, so they keep their place. They rerun from scratch, **on on-demand nodes**. A job is never interrupted twice.
- **Aging.** A low job that has waited long enough to age past low also moves to on-demand. So when spot capacity is short, low-priority work still gets done.
- **Model.** The provider reclaims nodes as a Poisson process with a 5% per node-hour hazard, so roughly 5% of interruptible node-hours are lost.
- **Reporting.** Wait time is measured to a job's *first* start. Lost runs still count toward GPU-hours and cost.

### Priority and fairness (requirement 5)
- **Order.** Jobs are ordered by effective priority, then by the team currently holding the fewest GPUs, then by submit time.
- **High priority** means three things:
  - it goes first in the queue;
  - it can use **one reserved node slot**. Other jobs may not launch into that slot or use a node in it, so a high job never waits for the cap;
  - it does **not** preempt running jobs. There is no checkpointing, so killing a running job throws away work done by someone who did nothing wrong.

  The reserved slot counts the job's *submitted* priority; aging does not grant access to it.
- **Aging.** Every 60 minutes of waiting raises a job one priority level, up to high, so low-priority jobs cannot starve.
- **Draining.** A busy node keeps being refilled by whoever fits. So a team flooding the cluster with small jobs can keep every node, and an 8-GPU job from another team never finds an empty one. When a team is blocked by the cap, and another team dominates at least two more nodes, that team's soonest-finishing node is put into draining mode. It takes new jobs only from the blocked team, and once it empties the blocked team gets it.
- **Why this policy.** It is simple to explain ("priority first, then whoever has the least, then first come first served"). It needs no per-team quotas to configure, and teams that only occasionally submit aren't stuck behind another team's sweep.

### Cost attribution (requirement 6)
- **Busy cost.** Each job pays for the GPUs it used, at its node's per-GPU price, for the time it ran. For example, 2 GPUs on a gpu-m for 1 hour cost 2 × 4.50/4 = 2.25. Interruptible nodes give a 35% per-GPU price.
- **Idle pool.** Everything else the provider bills goes into a shared pool:
  - boot time;
  - idle time before the reaper acts;
  - **unused GPUs on busy nodes**;
  - 10-minute minimum top-ups;
  - launches that never became ready.

  The pool is split across teams **pro-rata by their GPU-hours** in the window. The report shows its breakdown so Finance can see where idle money goes.
- **Why pro-rata.** With shared nodes, most idle cost has no single cause: a node booted for one team serves three. Splitting by usage is simple, predictable and hard to game. The trade-off is a weaker incentive for any one team, which the visible pool breakdown makes up for.
- **No GPU use in the window.** If idle cost exists but no GPUs were used in the window, it shows as `_unallocated`.
- **GPU-hours** are the requested GPUs multiplied by the hours run, as the brief defines.
- **Report windows.** The report takes a `[start, end)` window and clips everything to it. The ledger total equals the provider's invoice: tests check this, and so does every replay seed.

### Lifecycle (requirement 4)
- States: `queued → running → succeeded | failed | cancelled`. Cancel works while queued or running; cancelling a finished job returns 409.
- A job ends in one of two ways. It can report completion (`POST /jobs/{id}/complete`). Or the simulated clock ends it after its simulated runtime, which defaults to the estimate; the trace replay supplies `actual_minutes`.
- The scheduler only ever reads the estimate.

### Idempotency (requirement 1)
- `Idempotency-Key` header (or `idempotency_key` in the body).
- Same key with the same payload: returns the original job, with **200** instead of 201.
- Same key with a different payload: **409**.
- The UI generates one key per click.

### Restart safety (requirement 7)
- **Atomic saves.** Every mutating call ends with one SQLite transaction. It writes changed jobs and nodes, new cost segments, the clock, the backoff state and counters, and only rows whose JSON changed. A crash rolls back to the last consistent snapshot, including simulated time.
- **Provider state.** The fake provider is treated as an external system with its own state file (atomic write). After a restart its nodes are still alive and billing.
- **Reconciliation on startup.**
  - Provider nodes the broker never recorded (a crash between `launch_node` and the commit) are terminated, and their cost goes into the idle pool.
  - Broker nodes the provider no longer has are marked lost, and their jobs are re-queued.
- Tests cover a stop/start in the middle of work: same clock, jobs, nodes and cost report, idempotency keys still honoured, and an orphaned node cleaned up.

## Trace replay (requirement 8)

Full tables: [docs/replay_results.md](docs/replay_results.md). These are means over 5 provider seeds, with wait times pooled.

**Data problems and what I did about them** (in `trace.py`):
- 15 timestamps carry `+01:00` or `-07:00` offsets. They were converted to UTC (an explicit offset counts as "stated"), which puts every one of them in sequence with its neighbours.
- 15 rows were out of order; I sorted them.
- 14 exact duplicate rows were dropped as client retries. This is the same thing an idempotency key does. One pair only becomes a duplicate after timezone normalisation (`18:01Z` and `19:01+01:00`). A few duplicates fall inside the vision burst and could be genuine identical jobs. Without a client key there's no way to tell, so I chose the idempotent reading.
- I rejected `gpus=3` (not a valid request) and `gpus=16` (no node can run it), the same as the API does.
- Long outliers, for example 2,880 min (exactly 48 h) on a 60-minute estimate, are kept. They look like hung jobs hitting a timeout; see next steps.

**Results: chosen policy (fair) vs FIFO alternative** (5-seed means)

| | fair (chosen) | fifo |
|---|---|---|
| Total cost | 2,875 (seed range 2,736-3,086) | **2,788** (2,745-2,831) |
| p50 / p90 / p99 wait, all jobs | 6.6 / 98 / 148 min | 6.5 / **80 / 100** min |
| platform p90 / p99 | **7.8 / 70 min** | 39 / 83 min |
| speech p90 / p99 | **20.5** / 58.6 min | 22.8 / 57.8 min |
| vision p50 / p90 | 38.7 / 123 min | **34.2 / 87** min |
| nlp p99 | 62.5 min | **16.0** min |
| high-priority p90 | 7.7 min | 7.5 min |

Other variants (full table in `docs/replay_results.md`):

| variant | cost | p90 wait | started ≤10 min | note |
|---|---|---|---|---|
| fair (chosen) | 2,875 | 98 | 61% | |
| every team isolated | 2,833 | 124 | 53% | about 20% more launches |
| no interruptible nodes | 3,011 | 105 | 63% | spot saves about 4.5% |
| no reserved high slot | 2,819 | 75 | 64% | high p90 7.6 vs 7.7 |
| reaper N=0 / N=30 | 2,770 / 3,012 | 90 / 106 | 59% / 60% | |
| no right-sizing | 2,923 | 75 | 65% | |

**Analysis**
- **Fair-share is a trade-off, not a free win, now that teams share nodes.**
  - What it buys: platform's p90 drops from 39 to 8 minutes, because occasional submitters no longer queue behind vision's sweep. Draining specifically improves speech's p99 (69 → 59 vs fair without draining) and platform's p90 (16 → 8).
  - Who pays: vision's burst. Its p90 rises from 87 to 123 minutes, and vision is more than half of all jobs, so the all-jobs p90 looks worse than FIFO.
  - Unexplained: nlp's p99 is also worse under fair. I suspect aged vision jobs competing at "high" effective priority, but I haven't confirmed it.
  - I'd still choose fair. Worst-case waits for small teams matter more than the median of one team's batch sweep. A sweep that size should be submitted at low priority.
- **The vision tail is a capacity problem.** On 03-05 at 06:10, vision submitted about 230 one-GPU, 30-minute jobs in 5 minutes, about 119 GPU-hours. The 10-node cap gives at most 80 GPUs, so it takes at least 90 minutes under any policy. Excluding vision, 84-94% of each team's jobs start within 10 minutes.
- **The reserved high slot doesn't pay off on this trace.** High-priority p90 is 7.7 minutes with it and 7.6 without. High jobs (nlp, forecast) arrive in the afternoon, when the cluster is rarely full, while the slot costs vision's burst one of its 10 nodes (overall p90 98 vs 75). It's insurance against a high job arriving into a full cluster; on this trace it never paid out. Turning it off is `high_reserved_slots=0`.
- **Interruptible nodes save about 4.5%** (3,011 → 2,875), with about 6 reclaims a week and about 40 GPU-hours of lost work. They also widen the cost spread between seeds. The losses come from long low-priority jobs that badly overran their estimates, for example platform's 2,783-minute job estimated at 240: reclaiming one of those means rerunning hours of work. Next step: only send low jobs with an estimate under about 2 hours to spot.
- **Isolating everyone is cheaper but slower.** Making every team isolated costs about the same (2,833) but cuts on-time starts from 61% to 53%. Opt-in isolation keeps that cost to the teams that need it.
- **Where the idle money goes.** The idle pool is about 28% of the bill. The largest part is **unused GPUs on busy nodes** (450-760 per week, depending on seed), not idle nodes: boot is about 85 and idle-before-reap about 125. Under single-team attribution this waste was hidden inside each team's busy cost. Now Finance can see it, and it points at node sizing and packing as the lever, more than the reaper.
- **Reaper N** stays at 10: N=0 is about 3.5% cheaper but drops on-time starts; N=30 is 5% more expensive with no latency benefit.

## Assumptions
- `actual_minutes` for failed and cancelled rows is time spent *running*. The job then ends in that state.
- Nodes are wiped between tenants, so an isolated team's empty node can be reused by another team. Only concurrent sharing is forbidden.
- An isolated team may leave GPUs unused on its node. Those unused GPUs go into the shared pool like anyone else's; charging isolated teams a premium would be a fair refinement.
- On reclaim, interrupted jobs restart from scratch (no checkpointing).
- Idempotency keys are global and kept forever. In production they would expire after about 24 hours.
- The provider's capacity outages are modelled per node type: each launch has a 3% chance of starting a 10-60 minute outage, during which 90% of launches fail. The 3% of nodes that never boot are reported failed at 15 minutes.
- Time is integer seconds since the epoch, UTC. The API speaks ISO-8601.

## What I cut, and what I'd do next
- **Smarter interruptible use.** Only send short low jobs to spot (estimate under about 2 hours). Let jobs opt in or out. Use checkpoints, once jobs support them, to make reclaims cheap.
- **Isolation premium.** Charge isolated teams for the unused GPUs on their nodes instead of pooling them.
- **Runtime watchdog.** Kill or flag jobs running more than about 4× their estimate. The 48-hour speech job cost about 408 on its own, roughly 14% of the week's bill.
- **Preemption for high priority**, once jobs can checkpoint.
- **Per-team quotas and budgets.** Hard caps, alerts at x% of monthly budget.
- **Alerting for Platform.** Page only on sustained symptoms: p90 queue wait above 30 minutes for 30 minutes; every node type in backoff for more than 30 minutes; ledger ≠ invoice. Everything else self-heals.
- **Real-time clock mode.** The service runs only on the explicitly advanced simulated clock. A wall-clock ticker would be a few lines.
- **Auth.** Team identity is trusted from the request body. In production it would come from the caller's credentials.
- **UI tests.** The Angular app has no unit or e2e tests; the API it calls is covered. Also: pagination and a historical (terminated) nodes view.
- **Persistence scale.** The full snapshot diff is O(jobs) per request. That's fine at this scale; beyond it, track dirty rows explicitly and archive old jobs.

## AI usage and time
- **Tools:** Claude Code (Anthropic Claude). It was used to design and write the code, tests and documentation together with me. I reviewed the decisions; the replay-driven fixes (slot upgrade, right-sizing) came from inspecting simulation output together.
- **Time:** _started: …, finished: …, roughly … hours of active work, including time with AI tools._ (fill in)
