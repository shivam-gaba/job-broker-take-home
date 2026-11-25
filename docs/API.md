# Job Broker HTTP API

Base path: `/api`. JSON in, JSON out. Times are ISO-8601 UTC (`2025-03-03T06:10:00Z`).
Errors: `{"detail": "..."}` with 404 (unknown id), 409 (conflict), 422 (validation).
Interactive docs (OpenAPI) are served by FastAPI at `/docs`.

## Jobs

### `POST /api/jobs` — submit
Header `Idempotency-Key: <string>` (optional; or `idempotency_key` in the body).

```json
{ "team": "vision", "gpus": 2, "estimated_minutes": 30, "priority": "normal",
  "simulated_runtime_minutes": 42, "simulated_outcome": "succeeded" }
```
* `gpus` in {1, 2, 4, 8}; `priority` in low | normal | high (default normal)
* `simulated_*` are simulation-only (how long the fake job really runs and how it ends);
  default: runs for its estimate and succeeds. The scheduler never sees them.

Responses: **201** new job; **200** same key + same payload (the original job, no new job);
**409** same key, different payload; **422** invalid.

Job object:
```json
{ "id": "j-000001", "team": "vision", "gpus": 2, "estimated_minutes": 30,
  "priority": "normal", "effective_priority": 1, "status": "queued",
  "idempotency_key": "abc", "submitted_at": "...", "started_at": null, "ended_at": null,
  "node_id": null, "wait_minutes": 3.0, "attempts": 0, "reason": null,
  "first_started_at": null, "interruptions": 0, "runs_on_interruptible": false }
```
`effective_priority` (0 low .. 2 high) includes aging; only set while queued.
`wait_minutes` is submit to *first* start. `interruptions` counts reclaims of interruptible
nodes under the job (it was re-queued and rerun on-demand). `runs_on_interruptible`: for a
queued job, whether it will be placed on an interruptible node (low priority, estimate <= 120 min, not yet aged,
never interrupted); for a started job, whether its node is interruptible.

### `GET /api/jobs?status=&team=&limit=200` — list (newest first)
### `GET /api/jobs/{id}` — get
### `POST /api/jobs/{id}/cancel` — cancel a queued or running job (409 if already finished)
### `POST /api/jobs/{id}/complete` — a running job reports completion
Body `{"outcome": "succeeded" | "failed"}`. 409 if not running.

## Queue and nodes

### `GET /api/queue`
Queued jobs **in the order the scheduler will consider them** (priority with aging, then the
team holding the fewest GPUs, then submit time).

### `GET /api/nodes?include_ended=false`
```json
{ "id": "n-00003", "type": "gpu-l", "gpus": 8, "used_gpus": 6, "price_per_hour": 8.5,
  "state": "ready", "team": "vision", "job_ids": ["j-000004"], "launched_at": "...",
  "ready_at": "...", "ended_at": null, "idle_since": null, "draining": false,
  "retiring": false, "interruptible": false, "reserved_for_high": false,
  "reclaim_notice": false, "teams": ["vision", "nlp"], "cost_so_far": 1.23 }
```
`state`: booting | ready | terminated | failed | reclaimed. `teams`: teams with jobs on it now
(several unless one of them is isolated). `team`: team it was launched for / first served.
`draining`: takes new jobs only from the team it is being freed for (or none after a reclaim
notice); `retiring`: will be replaced by a larger node once empty; `reserved_for_high`: sits in
the reserved slot, only base-priority-high jobs may use it.

## Cost

### `GET /api/cost?start=<iso>&end=<iso>`
Window `[start, end)`; defaults to first activity .. now.
```json
{ "start": "...", "end": "...", "total_cost": 21.05, "total_idle_cost": 7.13,
  "total_gpu_hours": 13.06, "provider_invoice_to_date": 21.05,
  "idle_pool_breakdown": { "boot": 6.86, "minimum": 0.01, "unused_gpus": 0.25 },
  "teams": [ { "team": "nlp", "jobs": 1, "gpu_hours": 0.22, "busy_cost": 0.25,
               "idle_cost": 0.12, "total_cost": 0.37, "idle_share": 0.33 } ] }
```
`busy_cost`: the GPUs the team's jobs used × their node's per-GPU price × time run.
`idle_cost`: the team's share of the idle pool, pro-rata by GPU-hours in the window.
`idle_pool_breakdown` kinds: boot, idle, unused_gpus (free GPUs on busy nodes), minimum
(10-minute billing minimum top-up), failed_launch. If idle cost exists but nobody used a GPU
in the window it appears as team `_unallocated`. Sum of team totals = `total_cost`.

## Clock and status

* `GET /api/clock` → `{"now": "..."}`
* `POST /api/clock/advance` body `{"minutes": 15}` → runs the simulation forward, returns new `now`
* `GET /api/summary` → counts, GPUs in use, node-type backoffs, counters, active config (for the dashboard)
* `GET /api/health`

## Example
```bash
curl -XPOST localhost:8000/api/jobs -H 'content-type: application/json' \
     -H 'Idempotency-Key: run-42' -d '{"team":"nlp","gpus":4,"estimated_minutes":60,"priority":"high"}'
curl -XPOST localhost:8000/api/clock/advance -H 'content-type: application/json' -d '{"minutes":10}'
curl 'localhost:8000/api/cost?start=2025-03-03T00:00:00Z&end=2025-03-04T00:00:00Z'
```
