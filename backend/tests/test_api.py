from fastapi.testclient import TestClient

from broker.api import create_app


def client(tmp_path=None):
    kw = {}
    if tmp_path is not None:
        kw = dict(db_path=str(tmp_path / "b.db"), provider_path=str(tmp_path / "p.json"))
    app = create_app(seed=3, **kw)
    app.state.broker.provider.never_ready_rate = 0.0
    app.state.broker.provider.outage_start_prob = 0.0
    return TestClient(app)


def test_submit_get_and_idempotency_header():
    c = client()
    body = {"team": "vision", "gpus": 2, "estimated_minutes": 30}
    r1 = c.post("/api/jobs", json=body, headers={"Idempotency-Key": "abc"})
    r2 = c.post("/api/jobs", json=body, headers={"Idempotency-Key": "abc"})
    assert r1.status_code == 201 and r2.status_code == 200
    assert r1.json()["id"] == r2.json()["id"]
    r3 = c.post("/api/jobs", json={**body, "gpus": 4}, headers={"Idempotency-Key": "abc"})
    assert r3.status_code == 409
    assert c.get(f"/api/jobs/{r1.json()['id']}").json()["status"] == "queued"
    assert c.get("/api/jobs/nope").status_code == 404


def test_validation_errors():
    c = client()
    assert c.post("/api/jobs", json={"team": "v", "gpus": 3, "estimated_minutes": 30}).status_code == 422
    assert c.post("/api/jobs", json={"team": "v", "gpus": 1, "estimated_minutes": 30,
                                     "priority": "urgent"}).status_code == 422
    assert c.get("/api/cost?start=yesterday").status_code == 422


def test_lifecycle_queue_nodes_cost_via_http():
    c = client()
    job = c.post("/api/jobs", json={"team": "nlp", "gpus": 4, "estimated_minutes": 60}).json()
    other = c.post("/api/jobs", json={"team": "nlp", "gpus": 1, "estimated_minutes": 60,
                                      "priority": "low"}).json()
    assert [j["id"] for j in c.get("/api/queue").json()] == [job["id"], other["id"]]
    assert len(c.get("/api/nodes").json()) >= 1
    c.post("/api/clock/advance", json={"minutes": 10})
    assert c.get(f"/api/jobs/{job['id']}").json()["status"] == "running"
    assert c.post(f"/api/jobs/{other['id']}/cancel").json()["status"] == "cancelled"
    assert c.post(f"/api/jobs/{job['id']}/complete", json={"outcome": "failed"}).json()["status"] == "failed"
    assert c.post(f"/api/jobs/{job['id']}/cancel").status_code == 409
    c.post("/api/clock/advance", json={"minutes": 60})
    cost = c.get("/api/cost").json()
    nlp = next(t for t in cost["teams"] if t["team"] == "nlp")
    assert nlp["gpu_hours"] > 0 and nlp["idle_cost"] > 0
    assert abs(cost["total_cost"] - cost["provider_invoice_to_date"]) < 0.05
    s = c.get("/api/summary").json()
    assert s["queued"] == 0 and s["nodes_alive"] == 0


def test_restart_via_api(tmp_path):
    c = client(tmp_path)
    job = c.post("/api/jobs", json={"team": "nlp", "gpus": 1, "estimated_minutes": 60}).json()
    c.post("/api/clock/advance", json={"minutes": 15})
    before = c.get(f"/api/jobs/{job['id']}").json(), c.get("/api/clock").json(), c.get("/api/cost").json()
    c.app.state.broker.store.close()
    c2 = client(tmp_path)
    after = c2.get(f"/api/jobs/{job['id']}").json(), c2.get("/api/clock").json(), c2.get("/api/cost").json()
    assert before == after
