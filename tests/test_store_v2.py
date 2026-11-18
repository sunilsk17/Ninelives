"""Store contract for v0.2 features, on every backend: migrations, events, children, messages,
cancellation trees, concurrency limits, priority, schedules, retention and wake-ups."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from contextlib import closing
from pathlib import Path

import pytest

from ninelives import SQLiteStore, Store
from ninelives.errors import LeaseLost, WorkflowNotFound
from ninelives.stores.sql import SCHEMA_VERSION, SchemaTooNew, SqlStore
from ninelives.types import EntryKind, EntryStatus, JournalEntry, Limit, ScheduleDef, Status

from .conftest import PG_DSN

T0 = 1_800_000_000.0  # 2027-01-15T08:00:00Z
NAMES = ["wf"]


def entry(wf_id: str, seq: int, name: str = "x", kind: EntryKind = EntryKind.VALUE, **kw) -> JournalEntry:
    return JournalEntry(
        workflow_id=wf_id, seq=seq, kind=kind, name=name, status=kw.pop("status", EntryStatus.COMPLETED), **kw
    )


async def running(store: Store, wf_id: str, name: str = "wf", **opts) -> str:
    await store.create_workflow(wf_id, name, {"args": [], "kwargs": {}}, T0, **opts)
    claimed = [r for r in await store.claim("w", T0, 30, [name], 50) if r.id == wf_id]
    assert claimed, f"{wf_id} was not claimable"
    token = claimed[0].lease_token
    assert token
    return token


# --------------------------------------------------------------------------- migrations

V01_SCHEMA = """
CREATE TABLE workflows (id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL, input TEXT NOT NULL,
  output TEXT, error TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL, wake_at REAL, lease_token TEXT,
  lease_owner TEXT, lease_expires_at REAL, runs INTEGER NOT NULL DEFAULT 0, recoveries INTEGER NOT NULL DEFAULT 0,
  cancel_requested INTEGER NOT NULL DEFAULT 0, forked_from TEXT, fork_seq INTEGER);
CREATE TABLE journal (workflow_id TEXT NOT NULL, seq INTEGER NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL,
  status TEXT NOT NULL, output TEXT, error TEXT, wake_at REAL, attempts INTEGER NOT NULL DEFAULT 0,
  started_at REAL, completed_at REAL, PRIMARY KEY (workflow_id, seq));
CREATE TABLE attempts (workflow_id TEXT NOT NULL, seq INTEGER NOT NULL, attempt INTEGER NOT NULL, worker TEXT NOT NULL,
  started_at REAL NOT NULL, finished_at REAL, error TEXT, PRIMARY KEY (workflow_id, seq, attempt));
CREATE TABLE signals (id INTEGER PRIMARY KEY AUTOINCREMENT, workflow_id TEXT NOT NULL, name TEXT NOT NULL,
  payload TEXT, created_at REAL NOT NULL, consumed_seq INTEGER);
INSERT INTO workflows (id, name, status, input, created_at, updated_at) VALUES ('old', 'wf', 'PENDING', '{"args":[],"kwargs":{}}', 1, 1);
INSERT INTO journal (workflow_id, seq, kind, name, status, output) VALUES ('old', 1, 'step', 's', 'completed', '42');
"""


async def test_a_v01_sqlite_database_is_upgraded_in_place(tmp_path: Path) -> None:
    path = tmp_path / "v01.db"
    with closing(sqlite3.connect(path)) as db:
        db.executescript(V01_SCHEMA)
    store = SQLiteStore(str(path))
    await store.init()
    await store.init()  # idempotent
    assert await store.schema_version() == SCHEMA_VERSION
    rec = await store.get_workflow("old")
    assert rec is not None and rec.priority == 0 and rec.parent_id is None and rec.deadline is None
    assert [e.output for e in await store.get_journal("old")] == [42]
    [claimed] = await store.claim("w", 10, 30, ["wf"], 5)
    assert claimed.id == "old"
    await store.close()


async def test_a_v01_postgres_database_is_upgraded_in_place() -> None:
    if not PG_DSN:
        pytest.skip("needs NINELIVES_TEST_POSTGRES_DSN")
    import asyncpg

    from ninelives.stores.postgres import PostgresStore

    admin = await asyncpg.connect(PG_DSN)
    await admin.execute("DROP DATABASE IF EXISTS ninelives_upgrade_test")
    await admin.execute("CREATE DATABASE ninelives_upgrade_test")
    await admin.close()
    dsn = PG_DSN.rsplit("/", 1)[0] + "/ninelives_upgrade_test"
    conn = await asyncpg.connect(dsn)
    pg_schema = (
        V01_SCHEMA.replace("REAL", "DOUBLE PRECISION")
        .replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
        .replace(
            "cancel_requested INTEGER NOT NULL DEFAULT 0", "cancel_requested BOOLEAN NOT NULL DEFAULT FALSE"
        )
        .replace("input TEXT NOT NULL", "input JSONB NOT NULL")
        .replace("output TEXT", "output JSONB")
        .replace("payload TEXT", "payload JSONB")
    )
    await conn.execute(pg_schema)
    await conn.close()
    store = PostgresStore(dsn)
    try:
        await store.init()
        await store.init()
        assert await store.schema_version() == SCHEMA_VERSION
        rec = await store.get_workflow("old")
        assert rec is not None and rec.priority == 0
        assert [e.output for e in await store.get_journal("old")] == [42]
    finally:
        await store.close()
    admin = await asyncpg.connect(PG_DSN)
    await admin.execute("DROP DATABASE IF EXISTS ninelives_upgrade_test")
    await admin.close()


async def test_a_database_from_a_newer_version_is_refused(tmp_path: Path) -> None:
    store = SQLiteStore(str(tmp_path / "new.db"))
    await store.init()
    await store.close()
    with closing(sqlite3.connect(tmp_path / "new.db")) as db, db:
        db.execute("INSERT INTO ninelives_schema (version) VALUES (99)")
    store = SQLiteStore(str(tmp_path / "new.db"))
    with pytest.raises(SchemaTooNew, match="99"):
        await store.init()
    await store.close()


# --------------------------------------------------------------------------- steps, events


async def test_complete_step_is_one_fenced_write(store: Store) -> None:
    token = await running(store, "a")
    attempt = await store.start_attempt(token, "a", 1, "w", T0)
    done = entry("a", 1, "s", EntryKind.STEP, output={"ok": 1}, attempts=attempt)
    await store.complete_step(token, done, attempt, T0 + 1, None)
    [att] = await store.get_attempts("a")
    assert att.finished_at == T0 + 1 and att.error is None
    assert (await store.get_journal("a"))[0].output == {"ok": 1}
    with pytest.raises(LeaseLost):
        await store.complete_step("stale", entry("a", 2), 1, T0 + 2)
    assert len(await store.get_journal("a")) == 1  # nothing written by the stale token


async def test_events_are_published_and_overwritten(store: Store) -> None:
    token = await running(store, "a")
    await store.set_event(token, entry("a", 1, "event:progress"), "progress", {"pct": 10}, T0 + 1)
    await store.set_event(token, entry("a", 2, "event:progress"), "progress", {"pct": 90}, T0 + 2)
    await store.set_event(token, entry("a", 3, "event:stage"), "stage", "shipping", T0 + 3)
    events = {e.key: e.value for e in await store.get_events("a")}
    assert events == {"progress": {"pct": 90}, "stage": "shipping"}
    got = await store.get_event("a", "progress")
    assert got is not None and got.updated_at == T0 + 2
    assert await store.get_event("a", "missing") is None
    with pytest.raises(LeaseLost):
        await store.set_event("stale", entry("a", 4), "stage", "x", T0)


# --------------------------------------------------------------------------- children, messages, cancellation


async def test_child_start_is_idempotent_and_reports_back(store: Store) -> None:
    token = await running(store, "parent")
    start = entry("parent", 1, "child:wf", output={"id": "parent/1"})
    child, created = await store.start_child(token, start, "parent/1", "wf", {"args": [1], "kwargs": {}}, T0)
    assert created and child.parent_id == "parent" and child.parent_seq == 1
    _, again = await store.start_child(token, start, "parent/1", "wf", {"args": [1], "kwargs": {}}, T0)
    assert not again
    assert [r.id for r in await store.list_workflows(parent_id="parent")] == ["parent/1"]
    # The parent waits for the child...
    await store.suspend(token, "parent", None, ["child:parent/1"], T0 + 1)
    assert (await store.get_workflow("parent")).wake_at is None  # type: ignore[union-attr]
    # ...and the child's completion wakes it with the outcome.
    [c] = await store.claim("w", T0 + 2, 30, ["wf"], 1)
    await store.finish(c.lease_token or "", "parent/1", Status.SUCCEEDED, {"r": 2}, None, T0 + 3)
    parent = await store.get_workflow("parent")
    assert parent is not None and parent.wake_at == T0 + 3
    [sig] = await store.get_signals("parent")
    assert sig.name == "child:parent/1" and sig.payload == {
        "status": "SUCCEEDED",
        "output": {"r": 2},
        "error": None,
    }


async def test_send_from_a_workflow_is_never_duplicated(store: Store) -> None:
    await store.create_workflow("target", "wf", {}, T0)
    token = await running(store, "sender", name="other")
    sent = entry("sender", 1, "send:go")
    await store.send_from(token, sent, "target", "go", {"n": 1}, T0)
    await store.send_from(token, sent, "target", "go", {"n": 1}, T0)  # e.g. a retried transaction
    assert [(s.name, s.payload) for s in await store.get_signals("target")] == [("go", {"n": 1})]
    with pytest.raises(WorkflowNotFound):
        await store.send_from(token, entry("sender", 2, "send:go"), "nobody", "go", None, T0)
    assert len(await store.get_journal("sender")) == 1  # the failed send journaled nothing


async def test_only_awaited_signals_wake_a_waiting_run(store: Store) -> None:
    token = await running(store, "a")
    await store.suspend(token, "a", T0 + 1000, ["approve"], T0)
    await store.send_signal("a", "noise", None, T0 + 1)
    assert (await store.get_workflow("a")).wake_at == T0 + 1000  # type: ignore[union-attr]
    await store.send_signal("a", "approve", None, T0 + 2)
    assert (await store.get_workflow("a")).wake_at == T0 + 2  # type: ignore[union-attr]


async def test_cancel_cascades_to_descendants(store: Store) -> None:
    token = await running(store, "root")
    await store.start_child(token, entry("root", 1, "child:wf"), "root/1", "wf", {}, T0)
    await store.create_workflow("root/1/1", "wf", {}, T0, parent_id="root/1", parent_seq=1)
    await store.create_workflow("unrelated", "wf", {}, T0)
    rec = await store.request_cancel("root", T0 + 1)
    assert rec.status == Status.RUNNING and rec.cancel_requested  # its worker finishes the job
    statuses = {r.id: r.status for r in await store.list_workflows(limit=10)}
    assert statuses["root/1"] == statuses["root/1/1"] == Status.CANCELLED
    assert statuses["unrelated"] == Status.PENDING
    # The cancelled child told its parent.
    assert [s.name for s in await store.get_signals("root")] == ["child:root/1"]


async def test_a_cancelled_waiting_parent_cancels_its_running_child(store: Store) -> None:
    token = await running(store, "p")
    await store.start_child(token, entry("p", 1, "child:wf"), "p/1", "wf", {}, T0)
    await store.suspend(token, "p", None, ["child:p/1"], T0)
    [c] = await store.claim("w", T0 + 1, 30, ["wf"], 5)
    assert c.id == "p/1"
    await store.request_cancel("p", T0 + 2)
    assert (await store.get_workflow("p")).status == Status.CANCELLED  # type: ignore[union-attr]
    assert (await store.get_workflow("p/1")).cancel_requested  # type: ignore[union-attr]


# --------------------------------------------------------------------------- flow control


async def test_priority_orders_claims(store: Store) -> None:
    await store.create_workflow("low", "wf", {}, T0)
    await store.create_workflow("high", "wf", {}, T0 + 5, priority=10)
    await store.create_workflow("mid", "wf", {}, T0 + 1, priority=5)
    assert [r.id for r in await store.claim("w", T0 + 10, 30, NAMES, 3)] == ["high", "mid", "low"]


async def test_global_concurrency_limit(store: Store) -> None:
    for i in range(5):
        await store.create_workflow(f"j{i}", "wf", {}, T0 + i)
    limits = {"wf": Limit(concurrency=2)}
    first = await store.claim("w1", T0 + 10, 30, NAMES, 10, limits)
    assert [r.id for r in first] == ["j0", "j1"]
    assert await store.claim("w2", T0 + 10, 30, NAMES, 10, limits) == []
    await store.finish(first[0].lease_token or "", "j0", Status.SUCCEEDED, None, None, T0 + 11)
    assert [r.id for r in await store.claim("w2", T0 + 12, 30, NAMES, 10, limits)] == ["j2"]
    # A run whose worker died does not hold its slot once the lease expires.
    later = await store.claim("w3", T0 + 100, 30, NAMES, 10, limits)
    assert len(later) == 2


async def test_per_key_concurrency_limit(store: Store) -> None:
    for i in range(3):
        await store.create_workflow(f"u1-{i}", "wf", {}, T0 + i, concurrency_key="user-1")
    await store.create_workflow("u2-0", "wf", {}, T0 + 5, concurrency_key="user-2")
    await store.create_workflow("nokey", "wf", {}, T0 + 6)
    limits = {"wf": Limit(key_concurrency=1)}
    first = await store.claim("w", T0 + 10, 30, NAMES, 10, limits)
    assert sorted(r.id for r in first) == ["nokey", "u1-0", "u2-0"]
    assert await store.claim("w", T0 + 10, 30, NAMES, 10, limits) == []


async def test_concurrency_limit_holds_under_concurrent_claimers(store: Store) -> None:
    for i in range(20):
        await store.create_workflow(f"c{i}", "wf", {}, T0 + i)
    limits = {"wf": Limit(concurrency=3)}
    results = await asyncio.gather(*(store.claim(f"w{i}", T0 + 30, 30, NAMES, 5, limits) for i in range(6)))
    assert sum(len(r) for r in results) == 3


# --------------------------------------------------------------------------- schedules


async def test_schedules_fire_once_per_tick(store: Store) -> None:
    sched = ScheduleDef(id="every-5", workflow="wf", cron="*/5 * * * *")
    await store.upsert_schedules([sched], T0)
    [s] = await store.list_schedules()
    assert s.next_at == T0 + 300 and s.enabled  # T0 is 08:00:00, next is 08:05
    assert await store.fire_schedules(T0 + 299, ["wf"]) == []
    started = await asyncio.gather(*(store.fire_schedules(T0 + 301, ["wf"]) for _ in range(4)))
    assert sorted(x for r in started for x in r) == ["every-5@2027-01-15T08:05:00Z"]
    s = await store.get_schedule("every-5")
    assert s is not None and s.next_at == T0 + 600 and s.last_run_id == "every-5@2027-01-15T08:05:00Z"
    run = await store.get_workflow("every-5@2027-01-15T08:05:00Z")
    assert run is not None and run.schedule_id == "every-5" and run.input == {"args": [], "kwargs": {}}


async def test_schedule_overlap_skip_and_allow(store: Store) -> None:
    await store.upsert_schedules(
        [
            ScheduleDef(id="skip", workflow="wf", cron="* * * * *"),
            ScheduleDef(id="allow", workflow="wf", cron="* * * * *", overlap="allow"),
        ],
        T0,
    )
    first = await store.fire_schedules(T0 + 60, ["wf"])
    assert len(first) == 2
    second = await store.fire_schedules(T0 + 120, ["wf"])  # the first runs are still PENDING
    assert [r.split("@")[0] for r in second] == ["allow"]
    skip = await store.get_schedule("skip")
    assert skip is not None and skip.next_at == T0 + 180  # the tick was consumed, not queued


async def test_after_downtime_only_the_latest_missed_tick_runs(store: Store) -> None:
    await store.upsert_schedules([ScheduleDef(id="hourly", workflow="wf", cron="@hourly")], T0)
    started = await store.fire_schedules(T0 + 5 * 3600 + 30, ["wf"])
    assert started == ["hourly@2027-01-15T13:00:00Z"]
    s = await store.get_schedule("hourly")
    assert s is not None and s.next_at == T0 + 6 * 3600


async def test_schedule_edit_pause_resume_trigger_delete(store: Store) -> None:
    await store.upsert_schedules([ScheduleDef(id="s", workflow="wf", cron="0 * * * *")], T0)
    await store.upsert_schedules([ScheduleDef(id="s", workflow="wf", cron="30 * * * *")], T0 + 10)
    s = await store.get_schedule("s")
    assert s is not None and s.cron == "30 * * * *" and s.next_at == T0 + 1800
    paused = await store.set_schedule_enabled("s", False, T0 + 20)
    assert not paused.enabled
    assert await store.fire_schedules(T0 + 7200, ["wf"]) == []
    resumed = await store.set_schedule_enabled("s", True, T0 + 7200)
    assert resumed.enabled and resumed.next_at == T0 + 7200 + 1800
    manual = await store.trigger_schedule("s", T0 + 7300)
    assert manual.startswith("s@manual-") and (await store.get_workflow(manual)) is not None
    with pytest.raises(KeyError):
        await store.trigger_schedule("nope", T0)
    assert await store.delete_schedule("s") and not await store.delete_schedule("s")
    assert await store.fire_schedules(T0 + 99999, ["wf"]) == []
    assert await store.fire_schedules(T0, []) == []


async def test_schedules_only_fire_for_workflows_this_worker_knows(store: Store) -> None:
    await store.upsert_schedules([ScheduleDef(id="s", workflow="elsewhere", cron="* * * * *")], T0)
    assert await store.fire_schedules(T0 + 61, ["wf"]) == []
    assert len(await store.fire_schedules(T0 + 61, ["elsewhere"])) == 1


# --------------------------------------------------------------------------- retention, listing, wake-ups


async def test_purge_removes_old_finished_runs_and_their_records(store: Store) -> None:
    token = await running(store, "old")
    await store.set_event(token, entry("old", 1, "event:k"), "k", 1, T0)
    await store.finish(token, "old", Status.SUCCEEDED, None, None, T0 + 1)
    await store.send_signal("old", "late", None, T0 + 2)
    await store.create_workflow("pending", "wf", {}, T0)
    token2 = await running(store, "recent", name="other")
    await store.finish(token2, "recent", Status.FAILED, None, {"type": "X", "message": ""}, T0 + 500)
    assert await store.purge(T0 + 100, batch=1) == 1
    remaining = {r.id for r in await store.list_workflows(limit=10)}
    assert remaining == {"pending", "recent"}
    assert await store.get_journal("old") == [] and await store.get_events("old") == []
    assert await store.get_signals("old") == []


async def test_list_search_and_counts(store: Store) -> None:
    for wf_id in ("order-1", "order-2", "refund_9", "100%"):
        await store.create_workflow(wf_id, "wf", {}, T0)
    assert {r.id for r in await store.list_workflows(search="order")} == {"order-1", "order-2"}
    assert [r.id for r in await store.list_workflows(search="_9")] == ["refund_9"]
    assert [r.id for r in await store.list_workflows(search="%")] == ["100%"]
    assert await store.count_by_name_and_status() == {("wf", "PENDING"): 4}


async def test_wait_for_work_wakes_on_new_work(store: Store) -> None:
    assert isinstance(store, SqlStore)
    waiter = asyncio.create_task(store.wait_for_work(10))
    await asyncio.sleep(0.05)
    t = time.monotonic()
    await store.create_workflow("new", "wf", {}, T0)
    await asyncio.wait_for(waiter, 5)
    assert time.monotonic() - t < 2


async def test_postgres_notify_wakes_other_processes() -> None:
    if not PG_DSN:
        pytest.skip("needs NINELIVES_TEST_POSTGRES_DSN")
    from ninelives.stores.postgres import PostgresStore

    a, b = PostgresStore(PG_DSN), PostgresStore(PG_DSN)  # stand-ins for two worker processes
    await a.init()
    await b.init()
    try:
        await b.wait_for_work(0.01)  # starts LISTENing
        waiter = asyncio.create_task(b.wait_for_work(10))
        await asyncio.sleep(0.05)
        t = time.monotonic()
        await a.create_workflow(f"notify-{time.time_ns()}", "wf", {}, T0)
        await asyncio.wait_for(waiter, 5)
        assert time.monotonic() - t < 2
    finally:
        await a.close()
        await b.close()


def _open_and_write(path: str, n: int, start_at: float) -> str:
    time.sleep(max(0.0, start_at - time.time()))  # all processes open the new file together

    async def go() -> str:
        store = SQLiteStore(path)
        await store.init()
        await store.create_workflow(f"proc-{n}", "wf", {}, T0)
        await store.close()
        return "ok"

    return asyncio.run(go())


def test_many_processes_can_open_a_new_sqlite_database_at_once(tmp_path: Path) -> None:
    """Regression: switching a new file to WAL from several processes at once used to fail with
    'database is locked' in all but one of them."""
    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor(12) as pool:
        list(pool.map(time.sleep, [0.01] * 12))  # warm the pool up so starts line up
        for attempt in range(5):
            path = str(tmp_path / f"race-{attempt}.db")
            start_at = time.time() + 0.3
            results = list(pool.map(_open_and_write, [path] * 12, range(12), [start_at] * 12))
            assert results == ["ok"] * 12
            with closing(sqlite3.connect(path)) as db:
                assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
                assert db.execute("SELECT COUNT(*) FROM workflows").fetchone()[0] == 12
