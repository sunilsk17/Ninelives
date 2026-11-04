"""The same behaviour is required from every store backend."""

from __future__ import annotations

import pytest

from ninelives import Store
from ninelives.errors import LeaseLost, WorkflowNotFound
from ninelives.types import EntryKind, EntryStatus, JournalEntry, Status

T0 = 1_800_000_000.0
NAMES = ["wf"]


def entry(wf_id: str, seq: int, status: EntryStatus = EntryStatus.COMPLETED, **kw) -> JournalEntry:
    return JournalEntry(workflow_id=wf_id, seq=seq, kind=EntryKind.STEP, name=f"s{seq}", status=status, **kw)


async def test_create_is_idempotent(store: Store) -> None:
    rec, created = await store.create_workflow("a", "wf", {"args": [1], "kwargs": {}}, T0)
    assert created and rec.status == Status.PENDING and rec.input == {"args": [1], "kwargs": {}}
    again, created = await store.create_workflow("a", "wf", {"args": [2], "kwargs": {}}, T0 + 1)
    assert not created and again.input == {"args": [1], "kwargs": {}}


async def test_claim_gives_each_workflow_to_one_worker(store: Store) -> None:
    for i in range(5):
        await store.create_workflow(f"w{i}", "wf", {}, T0 + i)
    first = await store.claim("w-1", T0 + 10, 30, NAMES, 3)
    second = await store.claim("w-2", T0 + 10, 30, NAMES, 10)
    assert [r.id for r in first] == ["w0", "w1", "w2"]
    assert [r.id for r in second] == ["w3", "w4"]
    assert all(r.status == Status.RUNNING and r.lease_token and r.runs == 1 for r in first + second)
    assert len({r.lease_token for r in first + second}) == 5
    assert await store.claim("w-3", T0 + 10, 30, NAMES, 10) == []


async def test_claim_respects_names_and_limit(store: Store) -> None:
    await store.create_workflow("x", "other", {}, T0)
    assert await store.claim("w", T0, 30, NAMES, 5) == []
    assert await store.claim("w", T0, 30, [], 5) == []
    assert await store.claim("w", T0, 30, ["other"], 0) == []


async def test_expired_lease_is_recovered_and_fences_old_owner(store: Store) -> None:
    await store.create_workflow("a", "wf", {}, T0)
    [old] = await store.claim("w-1", T0, 10, NAMES, 1)
    assert await store.claim("w-2", T0 + 5, 10, NAMES, 1) == []  # still leased
    [new] = await store.claim("w-2", T0 + 11, 10, NAMES, 1)
    assert new.recoveries == 1 and new.runs == 2 and new.lease_owner == "w-2"
    with pytest.raises(LeaseLost):
        await store.write_entry(old.lease_token, entry("a", 1))
    with pytest.raises(LeaseLost):
        await store.finish(old.lease_token, "a", Status.SUCCEEDED, 1, None, T0 + 12)
    await store.write_entry(new.lease_token, entry("a", 1, output={"v": 1}))
    assert [e.output for e in await store.get_journal("a")] == [{"v": 1}]


async def test_heartbeat_extends_and_reports_cancel(store: Store) -> None:
    await store.create_workflow("a", "wf", {}, T0)
    [rec] = await store.claim("w", T0, 10, NAMES, 1)
    hb = await store.heartbeat("a", rec.lease_token, T0 + 8, 10)
    assert hb.alive and not hb.cancel_requested
    assert await store.claim("w2", T0 + 12, 10, NAMES, 1) == []  # extended to T0 + 18
    await store.request_cancel("a", T0 + 13)
    hb = await store.heartbeat("a", rec.lease_token, T0 + 13, 10)
    assert hb.alive and hb.cancel_requested
    assert not (await store.heartbeat("a", "wrong", T0 + 13, 10)).alive


async def test_journal_upsert_and_order(store: Store) -> None:
    await store.create_workflow("a", "wf", {}, T0)
    [rec] = await store.claim("w", T0, 10, NAMES, 1)
    t = rec.lease_token
    await store.write_entry(t, entry("a", 2, output=[1, "x"]))
    await store.write_entry(t, entry("a", 1, status=EntryStatus.PENDING, wake_at=T0 + 5))
    await store.write_entry(t, entry("a", 1, output=None))
    journal = await store.get_journal("a")
    assert [(e.seq, e.status) for e in journal] == [(1, EntryStatus.COMPLETED), (2, EntryStatus.COMPLETED)]
    assert journal[1].output == [1, "x"]


async def test_attempt_numbers_survive_new_leases(store: Store) -> None:
    await store.create_workflow("a", "wf", {}, T0)
    [r1] = await store.claim("w1", T0, 1, NAMES, 1)
    assert await store.start_attempt(r1.lease_token, "a", 1, "w1", T0) == 1
    await store.finish_attempt(r1.lease_token, "a", 1, 1, T0 + 0.5, "boom")
    [r2] = await store.claim("w2", T0 + 2, 1, NAMES, 1)
    assert await store.start_attempt(r2.lease_token, "a", 1, "w2", T0 + 2) == 2
    attempts = await store.get_attempts("a")
    assert [(a.attempt, a.worker, a.error) for a in attempts] == [(1, "w1", "boom"), (2, "w2", None)]


async def test_signals_are_consumed_once_in_order(store: Store) -> None:
    await store.create_workflow("a", "wf", {}, T0)
    await store.send_signal("a", "go", {"n": 1}, T0)
    await store.send_signal("a", "go", {"n": 2}, T0 + 1)
    await store.send_signal("a", "other", None, T0 + 2)
    [rec] = await store.claim("w", T0 + 3, 10, NAMES, 1)
    wait = JournalEntry(workflow_id="a", seq=1, kind=EntryKind.SIGNAL, name="go", status=EntryStatus.PENDING)
    got = await store.consume_signal(rec.lease_token, wait, "go")
    assert got is not None and got.payload == {"n": 1} and got.consumed_seq == 1
    [journaled] = await store.get_journal("a")
    assert journaled.status == EntryStatus.COMPLETED and journaled.output == {"payload": {"n": 1}}
    wait2 = JournalEntry(workflow_id="a", seq=2, kind=EntryKind.SIGNAL, name="go", status=EntryStatus.PENDING)
    assert (await store.consume_signal(rec.lease_token, wait2, "go")).payload == {"n": 2}
    wait3 = JournalEntry(workflow_id="a", seq=3, kind=EntryKind.SIGNAL, name="go", status=EntryStatus.PENDING)
    assert await store.consume_signal(rec.lease_token, wait3, "go") is None
    with pytest.raises(WorkflowNotFound):
        await store.send_signal("missing", "go", None, T0)


async def test_suspend_wakes_on_signal_and_timer(store: Store) -> None:
    await store.create_workflow("a", "wf", {}, T0)
    [rec] = await store.claim("w", T0, 10, NAMES, 1)
    assert await store.suspend(rec.lease_token, "a", None, ["go"], T0 + 1) == Status.WAITING
    assert await store.claim("w", T0 + 1000, 10, NAMES, 1) == []  # waits for the signal only
    await store.send_signal("a", "go", 1, T0 + 2000)
    [rec] = await store.claim("w", T0 + 2000, 10, NAMES, 1)
    assert await store.suspend(rec.lease_token, "a", T0 + 3000, [], T0 + 2001) == Status.WAITING
    assert await store.claim("w", T0 + 2999, 10, NAMES, 1) == []
    assert len(await store.claim("w", T0 + 3000, 10, NAMES, 1)) == 1


async def test_signal_sent_while_running_is_not_lost(store: Store) -> None:
    await store.create_workflow("a", "wf", {}, T0)
    [rec] = await store.claim("w", T0, 10, NAMES, 1)
    await store.send_signal("a", "go", 1, T0 + 1)  # arrives before the workflow suspends
    await store.suspend(rec.lease_token, "a", None, ["go"], T0 + 2)
    assert (await store.get_workflow("a")).wake_at == T0 + 2
    assert len(await store.claim("w", T0 + 2, 10, NAMES, 1)) == 1


async def test_unrelated_signal_does_not_cause_busy_wakeups(store: Store) -> None:
    await store.create_workflow("a", "wf", {}, T0)
    [rec] = await store.claim("w", T0, 10, NAMES, 1)
    await store.send_signal("a", "noise", 1, T0 + 1)
    await store.suspend(rec.lease_token, "a", None, ["go"], T0 + 2)
    assert (await store.get_workflow("a")).wake_at is None


async def test_cancel_paths(store: Store) -> None:
    for wf in ("p", "r", "w"):
        await store.create_workflow(wf, "wf", {}, T0)
    assert (await store.request_cancel("p", T0)).status == Status.CANCELLED
    recs = {r.id: r for r in await store.claim("x", T0, 10, NAMES, 5)}
    flagged = await store.request_cancel("r", T0 + 1)
    assert flagged.status == Status.RUNNING and flagged.cancel_requested
    await store.suspend(recs["w"].lease_token, "w", T0 + 50, [], T0 + 1)
    assert (await store.request_cancel("w", T0 + 2)).status == Status.CANCELLED
    assert await store.suspend(recs["r"].lease_token, "r", T0 + 50, [], T0 + 3) == Status.CANCELLED
    with pytest.raises(WorkflowNotFound):
        await store.request_cancel("nope", T0)


async def test_finish_release_list_and_counts(store: Store) -> None:
    for i in range(3):
        await store.create_workflow(f"w{i}", "wf" if i < 2 else "other", {}, T0 + i)
    [rec] = await store.claim("w", T0 + 5, 10, NAMES, 1)
    await store.finish(rec.lease_token, rec.id, Status.SUCCEEDED, {"ok": True}, None, T0 + 6)
    done = await store.get_workflow(rec.id)
    assert done.status == Status.SUCCEEDED and done.output == {"ok": True} and done.lease_token is None
    [r2] = await store.claim("w", T0 + 7, 100, NAMES, 1)
    await store.release(r2.lease_token, r2.id)
    assert len(await store.claim("w2", T0 + 8, 100, NAMES, 1)) == 1  # released lease is claimable at once
    assert [w.id for w in await store.list_workflows()] == ["w2", "w1", "w0"]
    assert [w.id for w in await store.list_workflows(status=Status.SUCCEEDED)] == ["w0"]
    assert [w.id for w in await store.list_workflows(name="other")] == ["w2"]
    assert [w.id for w in await store.list_workflows(limit=1, offset=1)] == ["w1"]
    assert await store.count_by_status() == {"SUCCEEDED": 1, "RUNNING": 1, "PENDING": 1}


async def test_create_with_copied_journal(store: Store) -> None:
    rec, _ = await store.create_workflow("f", "wf", {}, T0, [entry("f", 1, output=5)], forked_from="orig", fork_seq=2)
    assert rec.forked_from == "orig" and rec.fork_seq == 2
    assert [e.output for e in await store.get_journal("f")] == [5]
