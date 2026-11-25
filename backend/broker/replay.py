"""Replay the trace through the simulator and compare policies.

    python -m broker.replay ../data/01a_trace.csv [--seeds 5] [--out ../docs/replay_results.md]

Each submission is fed in at its (UTC) timestamp with its actual_minutes as
the hidden run time and its outcome as the terminal state, so failed and
cancelled rows cost money and count for wait time like any other job. The
scheduler only ever sees the estimate. The provider is random (outages,
boot times, never-ready nodes), so every policy is run on the same set of
seeds and wait times are pooled across seeds.
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from dataclasses import replace

from .clock import SimClock, to_iso
from .config import Config
from .core import Broker
from .provider import FakeProvider
from .trace import TraceRow, load_trace

VARIANTS: list[tuple[str, str, Config]] = [
    ("fair", "Chosen policy: fair-share + aging + draining; teams share nodes; low priority "
             "with estimate <= 120 min on interruptible; 1 slot reserved for high; demand "
             "sizing; reaper 10 min", Config()),
    ("fifo", "Alternative: strict priority then FIFO, no draining (everything else equal)",
     Config(ordering="fifo", drain=False)),
    ("fair-isolated", "Every team isolated (prices the Security request)", Config(isolation=True)),
    ("fair-no-spot", "No interruptible nodes", Config(interruptible_for_low=False)),
    ("fair-spot-all", "Interruptible for every low job, no estimate limit",
     Config(interruptible_max_estimate_minutes=None)),
    ("fair-no-reserve", "No reserved high-priority slot", Config(high_reserved_slots=0)),
    ("fair-reaper0", "Reaper N=0 (kill as soon as idle, after the paid 10 min)",
     Config(reaper_idle_minutes=0)),
    ("fair-reaper30", "Reaper N=30", Config(reaper_idle_minutes=30)),
    ("fair-no-rightsize", "Reuse any empty node, however oversized", Config(right_size=False)),
]


def run_once(rows: list[TraceRow], config: Config, seed: int) -> dict:
    clock = SimClock(rows[0].submitted_at)
    broker = Broker(FakeProvider(clock, seed=seed), clock, config)
    for r in rows:
        broker.advance_to(r.submitted_at, commit=False)
        broker.submit(r.team, r.gpus, r.estimated_minutes, r.priority,
                      run_minutes=r.actual_minutes, outcome=r.outcome)
    broker.run_until_idle()
    unfinished = [j for j in broker.jobs.values() if j.status in ("queued", "running")]
    assert not unfinished, f"{len(unfinished)} jobs never finished"
    report = broker.cost_report()
    waits: dict[str, list[float]] = defaultdict(list)
    high_waits: list[float] = []
    for j in broker.jobs.values():
        waits[j.team].append(j.wait_seconds / 60)
        if j.priority == "high":
            high_waits.append(j.wait_seconds / 60)
    return {
        "waits": waits,
        "high_waits": high_waits,
        "total_cost": report["total_cost"],
        "idle_cost": report["total_idle_cost"],
        "cost_by_team": {t["team"]: t["total_cost"] for t in report["teams"]},
        "idle_by_team": {t["team"]: t["idle_cost"] for t in report["teams"]},
        "gpu_hours": report["total_gpu_hours"],
        "provider_invoice": round(broker.provider.invoice(), 2),
        "stats": dict(broker.stats),
        "end": broker.now,
    }


def percentile(values: list[float], p: float) -> float:
    """Linear-interpolated percentile (same as numpy's default)."""
    xs = sorted(values)
    if not xs:
        return float("nan")
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def summarize(runs: list[dict]) -> dict:
    waits: dict[str, list[float]] = defaultdict(list)
    for r in runs:
        for team, ws in r["waits"].items():
            waits[team].extend(ws)
    all_waits = [w for ws in waits.values() for w in ws]
    teams = {}
    for team in sorted(waits) + ["ALL"]:
        ws = all_waits if team == "ALL" else waits[team]
        teams[team] = {
            "jobs": len(ws) // len(runs),
            "p50": percentile(ws, 50), "p90": percentile(ws, 90),
            "p99": percentile(ws, 99), "max": max(ws),
            "within_10": sum(w <= 10 for w in ws) / len(ws),
        }
    n = len(runs)
    stats: dict[str, float] = defaultdict(float)
    for r in runs:
        for k, v in r["stats"].items():
            stats[k] += v / n
    cost_by_team: dict[str, float] = defaultdict(float)
    idle_by_team: dict[str, float] = defaultdict(float)
    for r in runs:
        for t, c in r["cost_by_team"].items():
            cost_by_team[t] += c / n
        for t, c in r["idle_by_team"].items():
            idle_by_team[t] += c / n
    costs = [r["total_cost"] for r in runs]
    high = [w for r in runs for w in r["high_waits"]]
    return {
        "high_p90": percentile(high, 90), "high_within_10": sum(w <= 10 for w in high) / len(high),
        "teams": teams,
        "cost_mean": sum(costs) / n, "cost_min": min(costs), "cost_max": max(costs),
        "idle_mean": sum(r["idle_cost"] for r in runs) / n,
        "gpu_hours": runs[0]["gpu_hours"],
        "cost_by_team": dict(cost_by_team), "idle_by_team": dict(idle_by_team),
        "stats": dict(stats),
        "reconciled": all(abs(r["total_cost"] - r["provider_invoice"]) < 0.05 for r in runs),
    }


def render(trace_report: dict, results: list[tuple[str, str, dict]], seeds: int,
           rows: list[TraceRow]) -> str:
    out = []
    w = out.append
    w("# Trace replay results\n")
    w(f"Generated by `python -m broker.replay` over {seeds} provider seeds "
      f"(wait times pooled across seeds). Trace window "
      f"{to_iso(rows[0].submitted_at)} .. {to_iso(rows[-1].submitted_at)}.\n")
    w("## Data cleaning\n")
    w(f"* rows read: {trace_report['rows_read']}, rows replayed: {trace_report['rows_used']}")
    w(f"* timezone offsets converted to UTC: {trace_report['timezone_offsets_converted']}")
    w(f"* rows out of time order (re-sorted): {trace_report['out_of_order_rows']}")
    w(f"* exact duplicate rows dropped: {len(trace_report['duplicate_lines_dropped'])} "
      f"(csv lines {', '.join(map(str, trace_report['duplicate_lines_dropped']))})")
    for rj in trace_report["rejected"]:
        w(f"* rejected csv line {rj['line']}: {rj['reason']}")
    w(f"* jobs per team: {', '.join(f'{k}={v}' for k, v in sorted(trace_report['per_team'].items()))}\n")

    w("## Policy comparison\n")
    w("| variant | total cost (mean, min-max) | idle pool | p50 wait | p90 wait | p99 wait | started <=10 min | high p90 | launches | capacity errors | reclaims | lost GPU-h | drains |")
    w("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for name, _desc, s in results:
        a = s["teams"]["ALL"]
        st = s["stats"]
        w(f"| {name} | {s['cost_mean']:.0f} ({s['cost_min']:.0f}-{s['cost_max']:.0f}) | "
          f"{s['idle_mean']:.0f} ({s['idle_mean'] / s['cost_mean']:.0%}) | "
          f"{a['p50']:.1f} | {a['p90']:.1f} | {a['p99']:.1f} | {a['within_10']:.0%} | "
          f"{s['high_p90']:.1f} | "
          f"{st.get('launches', 0):.0f} | {st.get('capacity_errors', 0):.0f} | "
          f"{st.get('reclaims', 0):.1f} | {st.get('lost_gpu_seconds', 0) / 3600:.1f} | "
          f"{st.get('drains', 0):.0f} |")
    w("")
    for name, desc, _ in results:
        w(f"* **{name}**: {desc}")
    w("\nWait times are in minutes (submit to first start). Idle pool = everything billed that "
      "is not a job's own GPUs (boot, idle, unused GPUs on busy nodes, 10-min minimum, failed "
      "launches), shared pro-rata by GPU-hours. Costs are in the provider's currency.\n")

    for name, desc, s in results[:2]:
        w(f"## Per-team wait (minutes) and cost: `{name}`\n")
        w("| team | jobs | p50 | p90 | p99 | max | started <=10 min | cost | of which idle share |")
        w("|---|---|---|---|---|---|---|---|---|")
        for team, t in s["teams"].items():
            cost = s["cost_by_team"].get(team, s["cost_mean"] if team == "ALL" else 0)
            idle = s["idle_by_team"].get(team, s["idle_mean"] if team == "ALL" else 0)
            w(f"| {team} | {t['jobs']} | {t['p50']:.1f} | {t['p90']:.1f} | {t['p99']:.1f} | "
              f"{t['max']:.1f} | {t['within_10']:.0%} | {cost:.0f} | {idle:.0f} |")
        w(f"\nLedger reconciles with provider invoice on every seed: {s['reconciled']}.\n")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("trace")
    ap.add_argument("--seeds", type=int, default=5)
    ap.add_argument("--out")
    ap.add_argument("--only", help="comma separated variant names")
    args = ap.parse_args(argv)

    rows, trace_report = load_trace(args.trace)
    variants = VARIANTS
    if args.only:
        keep = set(args.only.split(","))
        variants = [v for v in VARIANTS if v[0] in keep]
    results = []
    for name, desc, cfg in variants:
        t0 = time.time()
        runs = [run_once(rows, replace(cfg), seed) for seed in range(1, args.seeds + 1)]
        results.append((name, desc, summarize(runs)))
        print(f"{name}: {time.time() - t0:.1f}s", file=sys.stderr)
    text = render(trace_report, results, args.seeds, rows)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
