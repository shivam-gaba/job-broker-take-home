"""HTTP API (FastAPI). See docs/API.md for the contract.

Run:  uvicorn broker.api:app --port 8000      (from backend/)

One process, one broker, one lock: every request is serialised, and every
mutating request commits to SQLite before it returns.
"""
from __future__ import annotations

import os
import threading
from typing import Literal, Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .clock import SimClock, parse_iso, to_iso
from .config import Config
from .core import Broker, BrokerError
from .models import Job, Node
from .provider import FakeProvider
from .store import Store

DEFAULT_START = "2025-03-03T00:00:00Z"


class SubmitJob(BaseModel):
    team: str = Field(..., min_length=1, max_length=64)
    gpus: int
    estimated_minutes: float = Field(..., gt=0)
    priority: Literal["low", "normal", "high"] = "normal"
    idempotency_key: Optional[str] = Field(None, max_length=128)
    # simulation only: how long the job really runs / how it ends
    simulated_runtime_minutes: Optional[float] = Field(None, ge=0)
    simulated_outcome: Literal["succeeded", "failed", "cancelled"] = "succeeded"


class CompleteJob(BaseModel):
    outcome: Literal["succeeded", "failed"] = "succeeded"


class Advance(BaseModel):
    minutes: float = Field(..., gt=0, le=60 * 24 * 14)


def job_dict(b: Broker, j: Job) -> dict:
    return {
        "id": j.id, "team": j.team, "gpus": j.gpus,
        "estimated_minutes": j.estimated_minutes, "priority": j.priority,
        "effective_priority": b.effective_priority(j) if j.status == "queued" else None,
        "status": j.status, "idempotency_key": j.idempotency_key,
        "submitted_at": to_iso(j.submitted_at), "started_at": to_iso(j.started_at),
        "ended_at": to_iso(j.ended_at), "node_id": j.node_id,
        "wait_minutes": round(j.wait_seconds / 60, 1) if j.wait_seconds is not None else
        round((b.now - j.submitted_at) / 60, 1) if j.status == "queued" else None,
        "attempts": j.attempts, "reason": j.reason,
        "first_started_at": to_iso(j.first_started_at), "interruptions": j.interruptions,
        "runs_on_interruptible": b.wants_interruptible(j) if j.status == "queued" else
        (b.nodes[j.node_id].interruptible if j.node_id else None),
    }


def node_dict(b: Broker, n: Node) -> dict:
    return {
        "id": n.id, "type": n.type, "gpus": n.gpus, "used_gpus": n.used_gpus,
        "price_per_hour": n.price_per_hour, "state": n.state, "team": n.team,
        "job_ids": list(n.job_ids), "launched_at": to_iso(n.launched_at),
        "ready_at": to_iso(n.ready_at), "ended_at": to_iso(n.ended_at),
        "idle_since": to_iso(n.idle_since) if n.state == "ready" and not n.job_ids else None,
        "draining": n.draining, "retiring": n.retire,
        "interruptible": n.interruptible, "reserved_for_high": n.reserved,
        "reclaim_notice": n.reclaim_notice,
        "teams": sorted({b.jobs[j].team for j in n.job_ids}),
        "cost_so_far": round(((n.ended_at or b.now) - n.launched_at) * n.price_per_hour / 3600, 2),
    }


def _time_param(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return parse_iso(value)
    except ValueError:
        raise HTTPException(422, f"bad timestamp {value!r}, expected ISO-8601")


def create_app(db_path: Optional[str] = None, provider_path: Optional[str] = None,
               config: Optional[Config] = None, start: str = DEFAULT_START,
               seed: int = 7, static_dir: Optional[str] = None) -> FastAPI:
    clock = SimClock(parse_iso(start))
    provider = FakeProvider(clock, seed=seed, path=provider_path)
    store = Store(db_path) if db_path else None
    broker = Broker(provider, clock, config or Config(), store=store)
    lock = threading.Lock()

    app = FastAPI(title="Northwind Job Broker", version="1.0")
    app.state.broker = broker
    app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:4200"],
                       allow_methods=["*"], allow_headers=["*"])

    @app.exception_handler(BrokerError)
    def broker_error(_req: Request, exc: BrokerError):
        return JSONResponse(status_code=exc.status, content={"detail": str(exc)})

    @app.get("/api/health")
    def health():
        return {"ok": True, "now": to_iso(broker.now)}

    @app.post("/api/jobs", status_code=201)
    def submit(body: SubmitJob, idempotency_key: Optional[str] = Header(None)):
        key = idempotency_key or body.idempotency_key
        with lock:
            job, created = broker.submit(
                body.team, body.gpus, body.estimated_minutes, body.priority,
                idempotency_key=key, run_minutes=body.simulated_runtime_minutes,
                outcome=body.simulated_outcome)
            data = job_dict(broker, job)
        return data if created else JSONResponse(status_code=200, content=data)

    @app.get("/api/jobs")
    def list_jobs(status: Optional[str] = None, team: Optional[str] = None,
                  limit: int = Query(200, ge=1, le=5000)):
        with lock:
            jobs = [j for j in broker.jobs.values()
                    if (status is None or j.status == status) and (team is None or j.team == team)]
            jobs = sorted(jobs, key=lambda j: j.submitted_at, reverse=True)[:limit]
            return [job_dict(broker, j) for j in jobs]

    @app.get("/api/jobs/{job_id}")
    def get_job(job_id: str):
        with lock:
            return job_dict(broker, broker.get(job_id))

    @app.post("/api/jobs/{job_id}/cancel")
    def cancel(job_id: str):
        with lock:
            return job_dict(broker, broker.cancel(job_id))

    @app.post("/api/jobs/{job_id}/complete")
    def complete(job_id: str, body: CompleteJob):
        with lock:
            return job_dict(broker, broker.complete(job_id, body.outcome))

    @app.get("/api/queue")
    def queue():
        """Queued jobs in the order the scheduler will consider them."""
        with lock:
            return [job_dict(broker, j) for j in broker.queue_order()]

    @app.get("/api/nodes")
    def nodes(include_ended: bool = False):
        with lock:
            ns = broker.nodes.values() if include_ended else broker.alive.values()
            return [node_dict(broker, n) for n in ns]

    @app.get("/api/cost")
    def cost(start: Optional[str] = None, end: Optional[str] = None):
        with lock:
            rep = broker.cost_report(_time_param(start), _time_param(end))
            rep["start"], rep["end"] = to_iso(rep["start"]), to_iso(rep["end"])
            rep["provider_invoice_to_date"] = round(broker.provider.invoice(), 2)
            return rep

    @app.get("/api/clock")
    def get_clock():
        with lock:
            return {"now": to_iso(broker.now)}

    @app.post("/api/clock/advance")
    def advance(body: Advance):
        with lock:
            broker.advance(int(body.minutes * 60))
            return {"now": to_iso(broker.now)}

    @app.get("/api/summary")
    def summary():
        """Everything the dashboard needs in one call."""
        with lock:
            alive = list(broker.alive.values())
            return {
                "now": to_iso(broker.now),
                "config": broker.config.to_dict(),
                "queued": len(broker.queue), "running": len(broker.running),
                "nodes_alive": len(alive), "max_nodes": broker.config.max_nodes,
                "gpus_total": sum(n.gpus for n in alive if n.state == "ready"),
                "gpus_used": sum(n.used_gpus for n in alive),
                "backoff_until": {t: to_iso(u) for t, u in broker.backoff_until.items()
                                  if u > broker.now},
                "stats": dict(broker.stats),
            }

    if static_dir and os.path.isdir(static_dir):
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="ui")
    return app


def _app_from_env() -> FastAPI:
    here = os.path.dirname(os.path.abspath(__file__))
    var = os.environ.get("BROKER_VAR_DIR", os.path.join(here, "..", "var"))
    os.makedirs(var, exist_ok=True)
    isolated = tuple(t.strip() for t in os.environ.get("BROKER_ISOLATED_TEAMS", "").split(",")
                     if t.strip())
    cfg = Config(reaper_idle_minutes=float(os.environ.get("BROKER_REAPER_MINUTES", "10")),
                 isolation=os.environ.get("BROKER_ISOLATE_ALL", "0") == "1",
                 isolated_teams=isolated)
    ui = os.environ.get("BROKER_UI_DIR",
                        os.path.join(here, "..", "..", "frontend", "dist", "frontend", "browser"))
    return create_app(db_path=os.path.join(var, "broker.db"),
                      provider_path=os.path.join(var, "provider_state.json"),
                      config=cfg, start=os.environ.get("BROKER_START", DEFAULT_START),
                      static_dir=ui)


app = _app_from_env() if os.environ.get("BROKER_NO_AUTOAPP") != "1" else None
