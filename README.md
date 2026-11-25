# Northwind Job Broker

A shared GPU job broker. It takes jobs from teams, places them on rented nodes, starts and stops nodes to control cost, and reports cost per team. It has a Python back end (FastAPI, SQLite, simulated clock), an Angular dashboard, and a trace replay that compares scheduling policies.

```
backend/broker/
  provider.py   fake cloud provider (bursty CapacityUnavailable, 4-8 min boot, 3% never ready, billing)
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
pytest -q                                   # 33 tests, a few seconds

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

Simulated time only moves when you click **+1m / +5m / +15m / +60m** in the UI or call `POST /api/clock/advance`. Nothing sleeps. To start from an empty state, delete `backend/var/`. Optional environment variables: `BROKER_REAPER_MINUTES` (default 10), `BROKER_START` (default `2025-03-03T00:00:00Z`) and `BROKER_VAR_DIR`.

## Design decisions

### How I weighed the stakeholders

| Voice | Weight | What I did |
|---|---|---|
| **Security**: no two teams on one machine at the same time | **Hard constraint.** A data leak can't be undone, and the replay shows the rule is cheap (see below). | A node runs jobs from **one team at a time**. Once a node is empty, another team can reuse it. I assume nodes are wiped between tenants. |
| **ML engineers**: start within 10 min | Strong goal, not a guarantee. A cold node takes 4-8 minutes to boot, so 10 minutes is just reachable. | Packing onto already-running nodes, right-sized launches, a boot timeout for nodes that never come up, fair ordering. Reported as "% started within 10 min". |
| **Finance**: idle time, accountability | Strong. | Idle reaper. Every second of node billing is charged to a team, with idle time shown separately. The ledger reconciles to the provider invoice. |
| **Platform**: don't page us weekly | Strong. It shapes the operations design. | Self-healing: per-type backoff, boot timeout, provider reconciliation on restart. One process plus SQLite, no other moving parts. |

### Scheduling (requirement 2)
- **Placement.** A job first goes onto a ready node of its own team that has enough free GPUs (best fit). Failing that, it goes onto an empty node, but only if that node is no bigger than the node we would launch for it. Failing that, it claims free capacity on a node its team already has booting. Only then do we launch a new node.
- **Sizing.** A new node is sized for the team's queued demand. Demand of 1 GPU gets a gpu-s, 2-4 a gpu-m, 5 or more a gpu-l. This packs bursts of small jobs onto fewer nodes. `sizing="smallest"` is the alternative: the replay shows it wastes node slots and blows up the tail wait (p90 315 min).
- **Right-sizing.** An idle oversized node (for example a gpu-l) is not handed to a 1-GPU job when a gpu-s is a few minutes of boot away. We fall back to the oversized node only when no launch is possible. This cut total cost by 12% in the replay (3403 → 2987) for +0.8 min median wait.
- **Node cap (10).** Booting nodes count toward the cap. At the cap, an empty node that doesn't suit the job is terminated to free its slot. Once a higher-ranked job is blocked by the cap, lower-ranked jobs may still use existing capacity but may not take a new slot.
- **Slot upgrade.** At the cap, if a team's backlog would fill a whole gpu-l (8 GPUs or more), one of its smaller nodes stops taking new jobs and is terminated when it empties. The slot is then relaunched as a gpu-l. Without this, a team keeps refilling its gpu-s nodes forever during a burst: 10 slots holding 1-4 GPUs each.
- **Provider failures.**
  - `CapacityUnavailable` puts that node type into exponential backoff (1, 2, 4 … 16 min, reset on success). We try the other node types that fit right away. Jobs stay queued and nothing crashes.
  - A node that isn't ready after **9 minutes** (healthy nodes take 4-8) is assumed to be one of the 3% that never boot. We terminate it and launch a replacement instead of waiting for the provider's 15-minute failure report. This saves about 6 minutes of wait, and 5 of the 15 billed minutes, per bad node.
- **Idle reaper (requirement 3).** A node with no jobs for N minutes is terminated (N defaults to 10, configurable). The first 10 minutes of a node are paid for anyway (billing minimum), so a node is never reaped before `launched_at + 10 min`.

### Priority and fairness (requirement 5)
- **Order.** Jobs are ordered by effective priority, then by the team currently holding the fewest GPUs, then by submit time.
- **High priority** affects ordering only. It does **not** preempt running jobs: there is no checkpointing, so killing a running job throws away its work, and the cost of that is borne by someone who did nothing wrong.
- **Aging.** Every 60 minutes of waiting raises a job one priority level, up to high, so low-priority jobs cannot starve.
- **Draining.** Isolation creates a fairness problem: a busy node is only ever refilled by its own team, so a team that grabbed every node keeps them. When a team is blocked by the cap and another team holds at least two more nodes than it does, the richer team's soonest-finishing node is put into draining mode. It takes no new jobs, and once it empties it is reused or replaced for the blocked team.
- **Why this policy.** It is simple to explain ("priority first, then whoever has the least, then first come first served"). It needs no per-team quotas to configure, and teams that only occasionally submit aren't stuck behind another team's sweep.

### Cost attribution (requirement 6)
- **Segments.** Every second a node is billed falls into a segment with a team and a kind: `boot`, `busy`, `idle`, `minimum` (the 10-minute top-up) or `failed_launch`.
- **Who pays.** Because nodes are single-tenant, there is always exactly one accountable team:
  - **boot** is charged to the team whose demand launched the node;
  - **busy** is charged to the team running on it (its whole node price, since nobody else can use the node);
  - **idle** after work is charged to the last team that ran on it, until the node is reaped or another team reuses it.

  This answers Finance's question directly: teams whose jobs leave nodes idle see that cost on their own line. It also creates the right incentive to submit accurate, batched work.
- **Failed launches** (nodes that never boot) go to an `_overhead` line. That is the provider's fault, not the team's, and the platform should own it.
- **GPU-hours** are the requested GPUs multiplied by the hours run, as the brief defines.
- **Report windows.** The report takes a `[start, end)` window and clips segments to it. The ledger total equals the provider's invoice: tests check this, and so does every replay seed.

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
  - Provider nodes the broker never recorded (a crash between `launch_node` and the commit) are terminated, and their cost goes to `_overhead`.
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

**Results: chosen policy (fair) vs FIFO alternative**

| | fair (chosen) | fifo |
|---|---|---|
| Total cost | **2,987** (idle 239, 8%) | 2,990 |
| p50 / p90 / p99 wait, all jobs | 6.8 / 101 / 142 min | 6.9 / 108 / 142 min |
| platform p90 wait | **7.9 min** | 92.6 min |
| speech p99 wait | **81.9 min** | 95.8 min |
| vision p50 wait | 41.8 min | 46.3 min |
| forecast / nlp p90 | 7.6 / 8.9 min | 7.8 / 8.6 min |

Per-team p50 / p90 / p99 for both policies are in `docs/replay_results.md`.

**Analysis**
- **Fair-share is free and protects small teams.** Total cost is the same as FIFO. With FIFO, platform's jobs that arrive during vision's burst wait behind about 230 vision jobs (p90 93 min). With fair ordering plus draining they start within about 8 minutes.
- **The vision tail is a capacity problem, not a scheduling one.** On 03-05 at 06:10, vision submitted about 230 one-GPU, 30-minute jobs in 5 minutes. That is about 119 GPU-hours. The 10-node cap gives at most 80 GPUs, so draining that burst takes at least 90 minutes under any policy. Hence vision's p90 of about 2 hours, and only 61% of all jobs starting within 10 minutes. Excluding vision, 83-93% of each team's jobs start within 10 minutes. Remedies are organisational: raise the node cap, or have sweeps submitted at low priority with a team quota.
- **Isolation costs almost nothing.** Without isolation (`fair-shared`) the total cost is 2,979, just 0.3% less. The median wait improves by 1 minute, and there are fewer launches. That's a cheap price for the Security requirement, so I kept it as a hard rule.
- **Reaper N is a cost-vs-latency knob.**
  - N=0 (but never before the paid 10 minutes): -1% cost, 2 percentage points fewer jobs starting within 10 minutes, about 2× the capacity errors from relaunching.
  - N=30: +6.5% cost, and idle rises to 14% of the bill.
  - I chose 10 minutes.
- **Idle is 8% of the bill under this broker,** against the "about a third" Finance saw. Most of the remaining idle is unavoidable boot time (4-8 minutes per node), which is attributed to the launching team.
- **Ablations:**
  - right-sizing saves 12%;
  - slot upgrade cuts the overall p90 from 114 to 101 minutes;
  - demand sizing vs smallest-fit: p90 101 vs 315 minutes.

## Assumptions
- `actual_minutes` for failed and cancelled rows is time spent *running*. The job then ends in that state.
- Nodes are wiped between tenants, so sequential reuse by another team is allowed. Only concurrent sharing is forbidden.
- A team pays the full node price while running, even if it uses 1 of 8 GPUs. Isolation means nobody else could use the rest.
- Idempotency keys are global and kept forever. In production they would expire after about 24 hours.
- The provider's capacity outages are modelled per node type: each launch has a 3% chance of starting a 10-60 minute outage, during which 90% of launches fail. The 3% of nodes that never boot are reported failed at 15 minutes.
- Time is integer seconds since the epoch, UTC. The API speaks ISO-8601.

## What I cut, and what I'd do next
- **Interruptible nodes: not used.** They are 65% cheaper, but jobs here have no checkpoint/resume semantics, so an interruption wastes the work done so far, and they add a page-worthy failure mode. Next step: an opt-in `interruptible_ok` flag for low-priority jobs. Run those on interruptible nodes (never mixed with on-demand ones); on the 2-minute notice, re-queue the jobs with their wait time preserved and launch on-demand if the job has already been interrupted once.
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
