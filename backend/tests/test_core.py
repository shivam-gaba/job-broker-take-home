import pytest

from broker.clock import SimClock
from broker.config import Config
from broker.core import Broker, Conflict, InvalidRequest
from broker.models import OVERHEAD_TEAM
from broker.provider import FakeProvider
from broker.store import Store

T0 = 1_740_960_000  # 2025-03-03T00:00:00Z
MIN = 60


def make(config=None, store=None, provider_path=None, **provider_kw):
    clock = SimClock(T0)
    kw = dict(never_ready_rate=0.0, outage_start_prob=0.0)
    kw.update(provider_kw)
    provider = FakeProvider(clock, seed=1, path=provider_path, **kw)
    return Broker(provider, clock, config or Config(), store=store)


def boot(b):
    """Advance past the maximum boot time (8 min)."""
    b.advance(8 * MIN)


# ---------------------------------------------------------------- submit
def test_submit_returns_job_and_launches_node():
    b = make()
    job, created = b.submit("vision", 1, 30)
    assert created and job.status == "queued"
    assert len(b.alive) == 1 and next(iter(b.alive.values())).type == "gpu-s"
    boot(b)
    assert job.status == "running"
    assert 4 * MIN <= job.wait_seconds <= 8 * MIN


def test_idempotency_key_dedupes_and_conflicts_on_different_payload():
    b = make()
    j1, c1 = b.submit("vision", 2, 30, idempotency_key="k1")
    j2, c2 = b.submit("vision", 2, 30, idempotency_key="k1")
    assert c1 and not c2 and j1.id == j2.id and len(b.jobs) == 1
    with pytest.raises(Conflict):
        b.submit("vision", 4, 30, idempotency_key="k1")


@pytest.mark.parametrize("kw", [dict(gpus=3), dict(gpus=16), dict(estimated_minutes=0),
                                dict(priority="urgent"), dict(team="")])
def test_submit_validation(kw):
    args = dict(team="vision", gpus=1, estimated_minutes=30, priority="normal")
    args.update(kw)
    with pytest.raises(InvalidRequest):
        make().submit(**args)


# ------------------------------------------------------------ scheduling
def test_jobs_share_a_node_of_the_same_team():
    b = make()
    b.submit("vision", 2, 30)
    b.submit("vision", 2, 30)  # demand = 4 -> one gpu-m, not two nodes
    assert [n.type for n in b.alive.values()] == ["gpu-m"]
    boot(b)
    assert len(b.running) == 2


def test_isolation_never_mixes_teams_on_a_node():
    b = make()
    b.submit("vision", 2, 60)
    boot(b)  # vision has a gpu-m with 2 free GPUs
    b.submit("nlp", 1, 60)
    boot(b)
    for n in b.alive.values():
        assert len({b.jobs[j].team for j in n.job_ids}) <= 1
    assert len(b.alive) == 2


def test_without_isolation_teams_share():
    b = make(Config(isolation=False))
    b.submit("vision", 2, 60)
    boot(b)
    b.submit("nlp", 1, 60)
    assert len(b.alive) == 1 and len(b.running) == 2


def test_empty_node_is_reused_by_another_team():
    b = make(Config(reaper_idle_minutes=30))
    b.submit("vision", 1, 10)
    boot(b)
    b.advance(10 * MIN)  # vision job done, node idle
    job, _ = b.submit("nlp", 1, 10)
    assert job.status == "running" and len(b.nodes) == 1


def test_node_cap_is_respected():
    b = make()
    for _ in range(15):
        b.submit("vision", 8, 60)
    assert len(b.alive) == 10
    boot(b)
    assert len(b.running) == 10 and len(b.queue) == 5
    b.run_until_idle()
    assert all(j.status == "succeeded" for j in b.jobs.values())


def test_provider_outage_backs_off_and_falls_back_to_another_type():
    b = make()
    b.provider.force_outage("gpu-s", T0 + 30 * MIN)
    job, _ = b.submit("vision", 1, 30)
    assert [n.type for n in b.alive.values()] == ["gpu-m"]  # fallback type
    assert b.stats["capacity_errors"] == 1 and b.backoff_until["gpu-s"] > b.now
    b.run_until_idle()
    assert job.status == "succeeded"


def test_total_outage_keeps_jobs_queued_then_recovers():
    b = make()
    for t in ("gpu-s", "gpu-m", "gpu-l"):
        b.provider.force_outage(t, T0 + 20 * MIN)
    job, _ = b.submit("vision", 1, 30)
    assert job.status == "queued" and not b.alive
    b.advance(15 * MIN)
    assert job.status == "queued"  # still trying, nothing crashed
    b.run_until_idle()
    assert job.status == "succeeded"
    assert job.started_at >= T0 + 20 * MIN


def test_node_that_never_boots_is_replaced_and_billed_to_overhead():
    b = make(never_ready_rate=1.0)
    job, _ = b.submit("vision", 1, 30)
    b.provider.never_ready_rate = 0.0  # only the first launch is doomed
    b.advance(9 * MIN)  # boot timeout -> terminate and launch a replacement
    assert b.stats["boot_timeouts"] == 1 and len(b.alive) == 1
    b.run_until_idle()
    assert job.status == "succeeded"
    report = b.cost_report()
    overhead = next(t for t in report["teams"] if t["team"] == OVERHEAD_TEAM)
    assert overhead["total_cost"] == pytest.approx(10 / 60 * 1.0, abs=0.01)  # 10-min minimum


# ------------------------------------------------------------ reaper
def test_idle_reaper_terminates_after_n_minutes():
    b = make(Config(reaper_idle_minutes=5))
    b.submit("vision", 1, 20, run_minutes=20)
    boot(b)
    b.advance(20 * MIN)
    node = next(iter(b.nodes.values()))
    assert node.state == "ready" and not node.job_ids
    b.advance(4 * MIN)
    assert node.state == "ready"
    b.advance(1 * MIN + 1)
    assert node.state == "terminated"


def test_reaper_never_kills_before_the_paid_minimum():
    b = make(Config(reaper_idle_minutes=0))
    job, _ = b.submit("vision", 1, 1, run_minutes=0.5)
    b.advance(9 * MIN)
    node = next(iter(b.nodes.values()))
    assert job.status == "succeeded" and node.state == "ready"  # 10 min are paid anyway
    b.advance(1 * MIN)
    assert node.state == "terminated"


# ------------------------------------------------------------ lifecycle
def test_cancel_queued_and_running():
    b = make()
    j1, _ = b.submit("vision", 1, 30)
    b.cancel(j1.id)
    assert j1.status == "cancelled" and j1.started_at is None
    j2, _ = b.submit("vision", 1, 30)
    boot(b)
    assert j2.status == "running"
    b.cancel(j2.id)
    assert j2.status == "cancelled"
    assert not b.nodes[j2.node_id].job_ids
    with pytest.raises(Conflict):
        b.cancel(j2.id)


def test_external_completion_and_simulated_outcomes():
    b = make()
    j1, _ = b.submit("vision", 1, 30)
    j2, _ = b.submit("vision", 1, 30, run_minutes=3, outcome="failed")
    boot(b)
    b.complete(j1.id, "succeeded")
    b.advance(3 * MIN)
    assert (j1.status, j2.status) == ("succeeded", "failed")
    with pytest.raises(Conflict):
        b.complete(j1.id)


# ------------------------------------------------------------ fairness
def test_high_priority_goes_first_and_low_priority_ages():
    b = make(Config(max_nodes=1, aging_minutes=60))
    b.submit("vision", 8, 600)  # occupies the only node
    boot(b)
    low, _ = b.submit("vision", 8, 60, priority="low")
    high, _ = b.submit("vision", 8, 60, priority="high")
    assert [j.id for j in b.queue_order()] == [high.id, low.id]
    b.advance(122 * MIN)  # low has aged two levels -> high, and is older
    assert b.queue_order()[0].id == low.id


def test_fair_ordering_prefers_team_holding_fewer_gpus():
    b = make(Config(max_nodes=1))
    b.submit("vision", 8, 60)
    boot(b)
    v, _ = b.submit("vision", 8, 60)
    n, _ = b.submit("nlp", 8, 60)  # submitted later, but nlp holds 0 GPUs
    assert [j.id for j in b.queue_order()] == [n.id, v.id]
    fifo = make(Config(max_nodes=1, ordering="fifo"))
    fifo.submit("vision", 8, 60)
    boot(fifo)
    v2, _ = fifo.submit("vision", 8, 60)
    n2, _ = fifo.submit("nlp", 8, 60)
    assert [j.id for j in fifo.queue_order()] == [v2.id, n2.id]


def test_drain_frees_a_node_for_a_starved_team():
    b = make(Config(max_nodes=2, reaper_idle_minutes=30))
    for _ in range(40):
        b.submit("vision", 8, 30)  # vision grabs both nodes and keeps refilling
    boot(b)
    nlp, _ = b.submit("nlp", 8, 30)
    assert b.stats["drains"] == 1
    b.advance(60 * MIN)
    assert nlp.status in ("running", "succeeded")
    assert nlp.wait_seconds < 45 * MIN


# ------------------------------------------------------------ cost
def test_cost_attribution_busy_idle_and_reconciles_with_provider_bill():
    b = make(Config(reaper_idle_minutes=10))
    b.submit("vision", 1, 60, run_minutes=60)
    b.submit("nlp", 4, 30, run_minutes=30)
    b.run_until_idle()
    rep = b.cost_report()
    by = {t["team"]: t for t in rep["teams"]}
    assert by["vision"]["gpu_hours"] == pytest.approx(1.0)
    assert by["nlp"]["gpu_hours"] == pytest.approx(2.0)
    assert by["vision"]["busy_cost"] == pytest.approx(1.0)
    assert by["nlp"]["busy_cost"] == pytest.approx(2.25)
    assert by["vision"]["idle_cost"] > 0  # boot + reaper wait
    assert rep["total_cost"] == pytest.approx(b.provider.invoice(), abs=0.02)


def test_cost_report_window():
    b = make()
    b.submit("vision", 1, 60, run_minutes=60)
    b.run_until_idle()
    job = next(iter(b.jobs.values()))
    rep = b.cost_report(job.started_at, job.started_at + 30 * MIN)
    by = {t["team"]: t for t in rep["teams"]}
    assert by["vision"]["gpu_hours"] == pytest.approx(0.5)
    assert by["vision"]["busy_cost"] == pytest.approx(0.5)
    assert by["vision"]["idle_cost"] == 0


# ------------------------------------------------------------ restart
def test_restart_keeps_jobs_nodes_cost_and_clock(tmp_path):
    db, pp = str(tmp_path / "b.db"), str(tmp_path / "p.json")
    b = make(store=Store(db), provider_path=pp)
    j1, _ = b.submit("vision", 1, 60, idempotency_key="abc")
    b.submit("nlp", 8, 30)
    b.advance(20 * MIN)
    before = (b.now, {j.id: j.status for j in b.jobs.values()},
              {n.id: n.state for n in b.nodes.values()}, b.cost_report())
    b.store.close()

    b2 = make(store=Store(db), provider_path=pp)
    after = (b2.now, {j.id: j.status for j in b2.jobs.values()},
             {n.id: n.state for n in b2.nodes.values()}, b2.cost_report())
    assert before == after
    j, created = b2.submit("vision", 1, 60, idempotency_key="abc")
    assert not created and j.id == j1.id
    b2.run_until_idle()
    assert all(j.status == "succeeded" for j in b2.jobs.values())
    assert b2.cost_report()["total_cost"] == pytest.approx(b2.provider.invoice(), abs=0.02)


def test_restart_terminates_orphan_provider_nodes(tmp_path):
    db, pp = str(tmp_path / "b.db"), str(tmp_path / "p.json")
    b = make(store=Store(db), provider_path=pp)
    b.submit("vision", 1, 60)
    # simulate a crash right after launch_node() but before the broker commits
    orphan = b.provider.launch_node("gpu-l")
    b.store.close()
    b2 = make(store=Store(db), provider_path=pp)
    assert b2.provider.nodes[orphan].terminated_at is not None
    assert b2.stats["orphans_terminated"] == 1
