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
  "node_id": null, "wait_minutes": 3.0, "attempts": 0, "reason": null }
```
`effective_priority` (0 low .. 2 high) includes aging; only set while queued.

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
  "retiring": false, "cost_so_far": 1.23 }
```
`state`: booting | ready | terminated | failed. `team`: the team the node is serving / charged to.
`draining`: takes no new jobs (being freed for another team); `retiring`: will be replaced by a
larger node once empty.

## Cost

### `GET /api/cost?start=<iso>&end=<iso>`
Window `[start, end)`; defaults to first activity .. now.
```json
{ "start": "...", "end": "...", "total_cost": 45.26, "total_idle_cost": 9.58,
  "total_gpu_hours": 32.76, "provider_invoice_to_date": 45.26,
  "teams": [ { "team": "nlp", "jobs": 1, "gpu_hours": 2.98, "busy_cost": 3.35,
               "idle_cost": 0.33, "total_cost": 3.68, "idle_share": 0.09,
               "breakdown": { "boot": 0.33, "busy": 3.35 } } ] }
```
`breakdown` kinds: busy, boot, idle, minimum (10-minute billing minimum top-up),
failed_launch. Team `_overhead` holds launches that never became ready.

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
