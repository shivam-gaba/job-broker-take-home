"""SQLite persistence for restart safety.

Every mutating broker call ends with `save()`, which writes the changed jobs,
changed nodes, new cost segments and the clock in ONE transaction. A crash
mid-way rolls back to the previous consistent snapshot (including simulated
time), so the broker never sees half an operation. Only rows whose JSON
changed since the last save are written.
"""
from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import asdict

from .models import Job, Node, Segment

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS nodes (id TEXT PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS segments (seq INTEGER PRIMARY KEY, data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(SCHEMA)
        self._cache: dict[tuple[str, str], str] = {}
        self._saved_segments = 0

    def close(self) -> None:
        self.conn.close()

    def load_into(self, broker) -> None:
        cur = self.conn.cursor()
        meta = {k: json.loads(v) for k, v in cur.execute("SELECT key, value FROM meta")}
        if not meta:
            return  # fresh database
        broker.clock.now = meta["now"]
        broker.job_counter = meta["job_counter"]
        broker.backoff_until = meta["backoff_until"]
        broker.backoff_fails = meta["backoff_fails"]
        broker.stats = Counter(meta["stats"])
        for jid, data in cur.execute("SELECT id, data FROM jobs"):
            job = Job(**json.loads(data))
            broker.jobs[jid] = job
            self._cache[("jobs", jid)] = data
            if job.status == "queued":
                broker.queue[jid] = job
            elif job.status == "running":
                broker.running[jid] = job
            if job.idempotency_key:
                broker.idem[job.idempotency_key] = jid
        for nid, data in cur.execute("SELECT id, data FROM nodes"):
            node = Node(**json.loads(data))
            broker.nodes[nid] = node
            self._cache[("nodes", nid)] = data
            if node.alive:
                broker.alive[nid] = node
        broker.segments = [Segment(**json.loads(d))
                           for (d,) in cur.execute("SELECT data FROM segments ORDER BY seq")]
        self._saved_segments = len(broker.segments)

    def save(self, broker) -> None:
        changed: dict[tuple[str, str], str] = {}
        for table, items in (("jobs", broker.jobs), ("nodes", broker.nodes)):
            for key, obj in items.items():
                data = json.dumps(asdict(obj), sort_keys=True)
                if self._cache.get((table, key)) != data:
                    changed[(table, key)] = data
        with self.conn:  # one transaction
            for (table, key), data in changed.items():
                self.conn.execute(
                    f"INSERT OR REPLACE INTO {table} (id, data) VALUES (?, ?)", (key, data))
            new = broker.segments[self._saved_segments:]
            self.conn.executemany(
                "INSERT INTO segments (seq, data) VALUES (?, ?)",
                [(self._saved_segments + i, json.dumps(asdict(s))) for i, s in enumerate(new)])
            meta = {
                "now": broker.clock.now,
                "job_counter": broker.job_counter,
                "backoff_until": broker.backoff_until,
                "backoff_fails": broker.backoff_fails,
                "stats": dict(broker.stats),
            }
            self.conn.executemany("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                                  [(k, json.dumps(v)) for k, v in meta.items()])
        # only after a successful commit
        self._cache.update(changed)
        self._saved_segments = len(broker.segments)
