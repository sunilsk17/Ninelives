"""Chaos: real worker processes are SIGKILLed and frozen at random while workflows run.

Asserts the guarantees the README makes:

* every workflow finishes with the right output;
* every side effect lands exactly once (idempotency keys), although step bodies may run more than once;
* journals are complete and gap-free;
* crashes really happened during the run (otherwise the test proves nothing).
"""

from __future__ import annotations

import asyncio
import os
import random
import signal
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

import pytest

from ninelives import Ninelives, Status
from ninelives.types import EntryStatus

from .conftest import PG_DSN

pytestmark = pytest.mark.chaos

HERE = Path(__file__).parent
WORKFLOWS = 80
STEPS = 6
WORKERS = 4
LEASE_TTL = 1.0


def backends() -> list[str]:
    return ["sqlite"] + (["postgres"] if PG_DSN else [])


@pytest.fixture(params=backends())
async def store_url(request: pytest.FixtureRequest, tmp_path: Path) -> str:
    if request.param == "sqlite":
        return f"sqlite:///{tmp_path}/chaos.db"
    from ninelives.stores.postgres import PostgresStore

    assert PG_DSN
    pg = PostgresStore(PG_DSN)
    await pg.init()
    pool = await pg._get_pool()
    await pool.execute("TRUNCATE workflows, journal, attempts, signals, events, schedules RESTART IDENTITY")
    await pg.close()
    return PG_DSN


class Fleet:
    """A set of worker processes we can kill, freeze and replace."""

    def __init__(self, env: dict[str, str], log_dir: Path):
        self.env = env
        self.log_dir = log_dir
        self.procs: list[subprocess.Popen[bytes]] = []
        self.logs: list[object] = []
        self.kills = 0
        self.freezes = 0
        self.spawned = 0

    def spawn(self) -> None:
        self.spawned += 1
        log = open(self.log_dir / f"worker-{self.spawned}.log", "wb")  # noqa: SIM115 - closed in stop()
        self.logs.append(log)
        cmd = [
            sys.executable, "-m", "ninelives", "worker", f"{HERE / 'chaos_app.py'}:app",
            "--lease-ttl", str(LEASE_TTL), "--poll", "0.02", "--concurrency", "2",
        ]  # fmt: skip
        self.procs.append(subprocess.Popen(cmd, env=self.env, stdout=log, stderr=subprocess.STDOUT))

    def kill_one(self, rng: random.Random) -> None:
        victim = rng.choice(self.procs)
        victim.send_signal(signal.SIGKILL)
        victim.wait()
        self.procs.remove(victim)
        self.kills += 1
        self.spawn()

    def freeze_one(self, rng: random.Random, seconds: float) -> None:
        """Simulate a long GC pause or a VM stall: the worker's lease expires while it is frozen,
        another worker takes over, and when it thaws its writes must be rejected (fencing)."""
        victim = rng.choice(self.procs)
        victim.send_signal(signal.SIGSTOP)
        time.sleep(seconds)
        victim.send_signal(signal.SIGCONT)
        self.freezes += 1

    def stop(self) -> None:
        for p in self.procs:
            p.send_signal(signal.SIGTERM)
        for p in self.procs:
            try:
                p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        for log in self.logs:
            log.close()  # type: ignore[attr-defined]


async def test_workers_killed_and_frozen_at_random(store_url: str, tmp_path: Path) -> None:
    ledger = str(tmp_path / "ledger.db")
    env = {**os.environ, "CHAOS_STORE": store_url, "CHAOS_LEDGER": ledger, "CHAOS_STEPS": str(STEPS)}
    sys.path.insert(0, str(HERE))
    os.environ.update({"CHAOS_STORE": store_url, "CHAOS_LEDGER": ledger})
    from chaos_app import setup_ledger

    setup_ledger(ledger)
    app = Ninelives(store_url)
    await app.init()
    rng = random.Random(1234)
    ids = [await app.start("transfer", n, id=f"t-{n:03d}") for n in range(WORKFLOWS)]

    fleet = Fleet(env, tmp_path)
    for _ in range(WORKERS):
        fleet.spawn()
    deadline = time.monotonic() + 150
    try:
        # Keep crashing workers while there is work in flight.
        while time.monotonic() < deadline:
            counts = await app.store.count_by_status()
            if counts.get("SUCCEEDED", 0) + counts.get("FAILED", 0) == WORKFLOWS:
                break
            await asyncio.sleep(rng.uniform(0.15, 0.45))
            if fleet.kills < 15 and rng.random() < 0.8:
                await asyncio.to_thread(fleet.kill_one, rng)
            elif fleet.freezes < 3:
                await asyncio.to_thread(fleet.freeze_one, rng, LEASE_TTL * 2)
        else:
            pytest.fail(f"workflows did not finish: {await app.store.count_by_status()}")
    finally:
        fleet.stop()

    step_seqs: set[tuple[str, int]] = set()

    def seq_is_step(wf_id: str, seq: int) -> bool:
        return (wf_id, seq) in step_seqs

    # 1. Every workflow succeeded with the right answer.
    recoveries = 0
    for n, wf_id in enumerate(ids):
        rec = await app.get(wf_id)
        assert rec.status == Status.SUCCEEDED, (wf_id, rec.status, rec.error)
        assert rec.output == sum(n * 100 + i for i in range(STEPS))
        recoveries += rec.recoveries
        # 3. Journals are complete and gap-free.
        journal = [e for e in await app.store.get_journal(wf_id) if e.kind == "step"]
        assert [e.seq for e in journal] == sorted(e.seq for e in journal)
        assert len(journal) == STEPS and all(e.status == EntryStatus.COMPLETED for e in journal)
        step_seqs.update((wf_id, e.seq) for e in journal)
    all_seqs = {wf_id: [e.seq for e in await app.store.get_journal(wf_id)] for wf_id in ids}
    for wf_id, seqs in all_seqs.items():
        assert seqs == list(range(1, len(seqs) + 1)), (wf_id, seqs)

    # 2. Side effects: exactly once each, even though some bodies ran more than once.
    with closing(sqlite3.connect(ledger)) as db, db:
        effects = db.execute(
            "SELECT COUNT(*), COUNT(DISTINCT workflow_id || ':' || seq) FROM ledger"
        ).fetchone()
        executions = db.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
        per_key = dict(
            db.execute("SELECT workflow_id || ':' || seq, COUNT(*) FROM executions GROUP BY 1").fetchall()
        )
    assert effects == (WORKFLOWS * STEPS, WORKFLOWS * STEPS)
    assert executions >= WORKFLOWS * STEPS
    assert set(per_key) == {
        f"{wf}:{seq}" for wf, seqs in all_seqs.items() for seq in seqs if seq_is_step(wf, seq)
    }

    # 4. The chaos was real.
    assert fleet.kills >= 5, "not enough crashes happened to make the test meaningful"
    assert recoveries > 0, "no workflow was ever recovered from a dead worker"
    print(
        f"\nchaos[{store_url.split(':')[0]}]: {fleet.kills} kills, {fleet.freezes} freezes, "
        f"{recoveries} recoveries, {executions - WORKFLOWS * STEPS} re-executed step bodies, "
        f"{WORKFLOWS * STEPS} effects applied exactly once"
    )
    await app.close()


async def test_child_workflows_survive_chaos(store_url: str, tmp_path: Path) -> None:
    """Parents fan out to children (with a concurrency limit on parents) while workers die."""
    ledger = str(tmp_path / "ledger.db")
    env = {**os.environ, "CHAOS_STORE": store_url, "CHAOS_LEDGER": ledger, "CHAOS_STEPS": "3"}
    sys.path.insert(0, str(HERE))
    os.environ.update({"CHAOS_STORE": store_url, "CHAOS_LEDGER": ledger})
    from chaos_app import setup_ledger

    setup_ledger(ledger)
    app = Ninelives(store_url)
    await app.init()
    parents = [await app.start("fanout", n, id=f"p-{n:02d}") for n in range(20)]
    total_runs = 20 * 4
    fleet = Fleet(env, tmp_path)
    for _ in range(WORKERS):
        fleet.spawn()
    rng = random.Random(99)
    deadline = time.monotonic() + 150
    try:
        while time.monotonic() < deadline:
            counts = await app.store.count_by_status()
            if counts.get("SUCCEEDED", 0) + counts.get("FAILED", 0) >= total_runs:
                break
            await asyncio.sleep(rng.uniform(0.15, 0.4))
            if fleet.kills < 10:
                await asyncio.to_thread(fleet.kill_one, rng)
        else:
            pytest.fail(f"runs did not finish: {await app.store.count_by_status()}")
    finally:
        fleet.stop()

    recoveries = 0
    for n, parent in enumerate(parents):
        rec = await app.get(parent)
        assert rec.status == Status.SUCCEEDED, (parent, rec.status, rec.error)
        expected = sum(sum((n * 10 + k) * 100 + i for i in range(3)) for k in range(3))
        assert rec.output == expected
        kids = await app.children(parent)
        assert sorted(k.id for k in kids) == [f"{parent}/{i}" for i in (1, 2, 3)]  # no duplicates
        assert await app.get_event(parent, "children") == [f"{parent}/{i}" for i in (1, 2, 3)]
        recoveries += rec.recoveries + sum(k.recoveries for k in kids)
    with closing(sqlite3.connect(ledger)) as db, db:
        effects = db.execute("SELECT COUNT(*), COUNT(DISTINCT key) FROM ledger").fetchone()
    assert effects == (20 * 3 * 3, 20 * 3 * 3)  # every child step's effect exactly once
    assert fleet.kills >= 5 and recoveries > 0
    print(
        f"\nchaos-children[{store_url.split(':')[0]}]: {fleet.kills} kills, {recoveries} recoveries, 180 effects once"
    )
    await app.close()
