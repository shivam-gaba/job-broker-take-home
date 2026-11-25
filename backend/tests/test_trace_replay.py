import os

from broker.config import Config
from broker.replay import percentile, run_once, summarize
from broker.trace import load_trace

TRACE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "01a_trace.csv")


def test_trace_cleaning():
    rows, rep = load_trace(TRACE)
    assert rep["rows_read"] == 494
    assert {r["line"] for r in rep["rejected"]} == {88, 393}  # gpus=3 and gpus=16
    assert len(rep["duplicate_lines_dropped"]) == 14
    assert rep["timezone_offsets_converted"] == 15
    assert [r.submitted_at for r in rows] == sorted(r.submitted_at for r in rows)
    # "+01:00" row converted: 2025-03-03T06:29:00+01:00 == 05:29Z
    assert any(r.submitted_at == 1740979740 and r.team == "vision" for r in rows)


def test_replay_finishes_every_job_and_reconciles_cost():
    rows, _ = load_trace(TRACE)
    res = run_once(rows, Config(), seed=1)
    assert sum(len(w) for w in res["waits"].values()) == len(rows)
    assert abs(res["total_cost"] - res["provider_invoice"]) < 0.05
    s = summarize([res])
    assert s["teams"]["ALL"]["p50"] >= 0


def test_percentile():
    assert percentile([1, 2, 3, 4], 50) == 2.5
    assert percentile([5], 99) == 5
