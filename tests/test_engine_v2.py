"""v0.2 engine features on every backend: versioning, events, child workflows, messages,
timeouts, concurrency limits, priority, schedules, retention and forks across these."""

from __future__ import annotations

import asyncio
from collections import Counter

import pytest

from ninelives import Ninelives, Status, Store
from ninelives.errors import ChildFailed, WorkflowFailed, WorkflowNotFound
from ninelives.types import EntryKind, StartOptions

from .conftest import FakeClock


async def drain(app: Ninelives) -> None:
    await app.worker().run_until_idle()


# --------------------------------------------------------------------------- patched()


async def test_patched_keeps_old_runs_on_the_old_path(store: Store, clock: FakeClock) -> None:
    calls: list[str] = []

    def version(n: int) -> Ninelives:
        app = Ninelives(store, clock=clock)

        @app.step()
        async def send_email() -> str:
            calls.append("email")
            return "email"

        @app.step()
        async def send_sms() -> str:
            calls.append("sms")
            return "sms"

        @app.step()
        async def note(x: str) -> str:
            calls.append(x)
            return x

        @app.workflow(name="flow")
        async def flow(ctx) -> list[str]:
            if n == 1:
                receipt = await send_email()
            elif n == 2:
                receipt = await send_sms() if await ctx.patched("sms") else await send_email()
            else:  # v3: the patch is retired, only the new path is left
                receipt = await send_sms()
            await ctx.sleep(60)
            return [receipt, await note("after")]

        return app

    v1 = version(1)
    old = await v1.start("flow", id="old")
    await drain(v1)  # sent the email, then parked in the sleep
    v2 = version(2)  # deploy the change while "old" is in flight
    new = await v2.start("flow", id="new")
    await drain(v2)
    clock.advance(61)
    await drain(v2)
    assert await v2.result(old) == ["email", "after"]  # the old run keeps its history...
    assert await v2.result(new) == ["sms", "after"]  # ...new runs take the new path
    assert [e.kind for e in await store.get_journal("new")] == [
        EntryKind.PATCH, EntryKind.STEP, EntryKind.SLEEP, EntryKind.STEP
    ]  # fmt: skip
    # v3 drops the patched() call: replaying "new" skips its marker without complaint...
    v3 = version(3)
    replay = await v3.fork("new", 99)
    calls.clear()
    await drain(v3)
    assert await v3.result(replay) == ["sms", "after"] and calls == []
    # ...while a run from before the patch would now be caught, not silently mis-replayed.
    too_early = await v3.fork("old", 99)
    await drain(v3)
    with pytest.raises(WorkflowFailed, match="NonDeterminismError"):
        await v3.result(too_early)


async def test_patched_is_refused_inside_gather_branches(store: Store) -> None:
    app = Ninelives(store)

    @app.workflow()
    async def flow(ctx) -> object:
        async def branch() -> bool:
            return await ctx.patched("x")

        return await ctx.gather(branch(), branch())

    wf_id = await app.start(flow)
    await drain(app)
    with pytest.raises(WorkflowFailed, match=r"cannot be used inside ctx\.gather"):
        await app.result(wf_id)


# --------------------------------------------------------------------------- events


async def test_events_publish_progress_and_survive_replay(store: Store, clock: FakeClock) -> None:
    app = Ninelives(store, clock=clock)

    @app.workflow()
    async def job(ctx, n: int) -> str:
        for i in range(n):
            await ctx.set_event("progress", {"done": i + 1, "of": n})
        await ctx.set_event("stage", "waiting for review")
        await ctx.wait_for_signal("reviewed")
        await ctx.set_event("stage", "done")
        return "ok"

    wf_id = await app.start(job, 3)
    await drain(app)
    assert await app.get_events(wf_id) == {"progress": {"done": 3, "of": 3}, "stage": "waiting for review"}
    stamp = (await store.get_event(wf_id, "progress")).updated_at  # type: ignore[union-attr]
    clock.advance(100)
    await app.signal(wf_id, "reviewed")
    await drain(app)  # replays the set_event calls: they must not publish again
    assert (await store.get_event(wf_id, "progress")).updated_at == stamp  # type: ignore[union-attr]
    assert await app.get_event(wf_id, "stage") == "done"
    with pytest.raises(KeyError):
        await app.get_event(wf_id, "missing")
    with pytest.raises(WorkflowNotFound):
        await app.get_event("no-such-run", "stage")
    detail = await app.describe(wf_id)
    assert {e["key"] for e in detail["events"]} == {"progress", "stage"}


async def test_get_event_waits_for_a_value(store: Store) -> None:
    app = Ninelives(store)
    gate = asyncio.Event()

    @app.step()
    async def slow() -> None:
        await gate.wait()

    @app.workflow()
    async def job(ctx) -> None:
        await slow()
        await ctx.set_event("ready", True)

    wf_id = await app.start(job)
    worker = app.worker(poll_interval=0.01)
    task = asyncio.create_task(worker.run())
    try:
        waiting = asyncio.create_task(app.get_event(wf_id, "ready", timeout=10, poll=0.01))
        await asyncio.sleep(0.05)
        assert not waiting.done()
        gate.set()
        assert await waiting is True
    finally:
        worker.stop()
        await task


# --------------------------------------------------------------------------- children and messages


def family(store: Store, clock: FakeClock | None = None) -> tuple[Ninelives, Counter[str]]:
    app = Ninelives(store, clock=clock) if clock else Ninelives(store)
    runs: Counter[str] = Counter()

    @app.step()
    async def square(x: int) -> int:
        runs[f"square:{x}"] += 1
        if x < 0:
            raise ValueError("negative")
        return x * x

    @app.workflow()
    async def leaf(ctx, x: int) -> int:
        return await square(x)

    @app.workflow()
    async def middle(ctx, x: int) -> int:
        return await ctx.child("leaf", x) + 1

    @app.workflow()
    async def parent(ctx, xs: list[int]) -> dict:
        ids = [await ctx.start_child(leaf, x) for x in xs]  # fan out...
        results = []
        for child_id in ids:  # ...then fan in
            try:
                results.append(await ctx.wait_child(child_id))
            except ChildFailed as exc:
                results.append(f"failed: {exc.error['message']}")  # type: ignore[index]
        nested = await ctx.child(middle, 3, id=f"{ctx.workflow_id}-mid")
        return {"results": results, "nested": nested, "children": ids}

    return app, runs


async def test_children_fan_out_fan_in_and_nest(store: Store) -> None:
    app, _ = family(store)
    wf_id = await app.start("parent", [1, 2, -1])
    await drain(app)
    out = await app.result(wf_id)
    assert out["results"][:2] == [1, 4] and out["nested"] == 10
    assert out["results"][2].startswith("failed:") and "ValueError: negative" in out["results"][2]
    assert out["children"] == [f"{wf_id}/1", f"{wf_id}/2", f"{wf_id}/3"]
    kids = {c.id: c for c in await app.children(wf_id)}
    assert set(kids) == {*out["children"], f"{wf_id}-mid"}
    assert kids[f"{wf_id}/3"].status == Status.FAILED and kids[f"{wf_id}/1"].parent_seq == 1
    grandkids = await app.children(f"{wf_id}-mid")
    assert len(grandkids) == 1 and grandkids[0].status == Status.SUCCEEDED
    detail = await app.describe(wf_id)
    assert [c["id"] for c in detail["children"]][:3] == out["children"]


async def test_a_recovered_parent_does_not_start_its_children_twice(store: Store, clock: FakeClock) -> None:
    app, runs = family(store, clock)
    wf_id = await app.start("parent", [5])
    dying = app.worker(lease_ttl=10)
    # Run the parent until it has started its child and parked, then lose that worker's lease.
    await dying.run_until_idle()
    assert (await app.get(wf_id)).status == Status.SUCCEEDED
    assert runs["square:5"] == 1 and len(await app.children(wf_id)) == 2
    # Replaying the whole parent (a fork of its full journal) starts nothing new.
    replay = await app.fork(wf_id, 99)
    await drain(app)
    assert (await app.result(replay))["results"] == [25]
    assert runs["square:5"] == 1 and await app.children(replay) == []


async def test_child_id_conflicts_and_cancellation_cascade(store: Store) -> None:
    app = Ninelives(store)

    @app.workflow()
    async def sleeper(ctx) -> None:
        await ctx.wait_for_signal("never")

    @app.workflow()
    async def boss(ctx, child_id: str) -> object:
        return await ctx.child(sleeper, id=child_id)

    await app.start(sleeper, id="taken")
    clash = await app.start(boss, "taken")
    await drain(app)
    with pytest.raises(WorkflowFailed, match="already used"):
        await app.result(clash)

    top = await app.start(boss, "kid")
    await drain(app)
    assert (await app.get("kid")).status == Status.WAITING
    await app.cancel(top)
    assert (await app.get(top)).status == Status.CANCELLED
    assert (await app.get("kid")).status == Status.CANCELLED


async def test_send_between_workflows_is_exactly_once(store: Store, clock: FakeClock) -> None:
    app = Ninelives(store, clock=clock)

    @app.workflow()
    async def receiver(ctx) -> list:
        return [await ctx.wait_for_signal("ping"), await ctx.wait_for_signal("ping", timeout=5)]

    @app.workflow()
    async def sender(ctx, target: str) -> str:
        await ctx.send(target, "ping", {"from": ctx.workflow_id})
        await ctx.sleep(30)  # suspend and replay: the send must not repeat
        return "sent"

    rx = await app.start(receiver, id="rx")
    tx = await app.start(sender, rx, id="tx")
    await drain(app)
    clock.advance(31)
    await drain(app)
    assert await app.result(tx) == "sent"
    clock.advance(10)
    await drain(app)
    with pytest.raises(WorkflowFailed, match="SignalTimeout"):
        await app.result(rx)  # the second ping never came: the send was not duplicated
    assert [s.payload for s in await store.get_signals("rx")] == [{"from": "tx"}]

    @app.workflow()
    async def lost(ctx) -> str:
        try:
            await ctx.send("nobody", "ping")
        except WorkflowNotFound:
            return "no such run"
        return "sent"

    lost_id = await app.start(lost)
    await drain(app)
    assert await app.result(lost_id) == "no such run"


# --------------------------------------------------------------------------- timeouts


async def test_workflow_timeouts(store: Store, clock: FakeClock) -> None:
    app = Ninelives(store, clock=clock)

    @app.step()
    async def work() -> str:
        clock.advance(40)  # each step "takes" 40 seconds
        return "done"

    @app.workflow(timeout=100)
    async def slow(ctx) -> str:
        for _ in range(5):
            await work()
        return "finished"

    @app.workflow()
    async def waits(ctx) -> object:
        return await ctx.wait_for_signal("go")

    a = await app.start(slow)  # default timeout from the decorator: fails at the third step
    c = await app.start(slow, options=StartOptions(timeout=10_000))  # an override
    await app.worker(concurrency=1).run_until_idle()
    with pytest.raises(WorkflowFailed, match="WorkflowTimeout"):
        await app.result(a)
    assert await app.result(c) == "finished"
    b = await app.start(waits, options=StartOptions(timeout=60))
    await drain(app)
    parked = await app.get(b)
    assert parked.status == Status.WAITING and parked.wake_at == parked.deadline  # wakes up to fail
    clock.advance(61)
    await drain(app)
    with pytest.raises(WorkflowFailed, match="WorkflowTimeout"):
        await app.result(b)


# --------------------------------------------------------------------------- flow control


async def test_concurrency_limits_hold_across_workers(store: Store) -> None:
    app = Ninelives(store)
    live = Counter[str]()
    peak = Counter[str]()

    @app.step()
    async def busy(key: str) -> None:
        live["all"] += 1
        live[key] += 1
        peak["all"] = max(peak["all"], live["all"])
        peak[key] = max(peak[key], live[key])
        await asyncio.sleep(0.03)
        live["all"] -= 1
        live[key] -= 1

    @app.workflow(concurrency=3, key_concurrency=1)
    async def job(ctx, key: str) -> None:
        await busy(key)

    for i in range(12):
        await app.start(job, f"k{i % 4}", options=StartOptions(concurrency_key=f"k{i % 4}"))
    workers = [app.worker(concurrency=5, poll_interval=0.01) for _ in range(3)]
    await asyncio.gather(*(w.run_until_idle() for w in workers))
    counts = await store.count_by_status()
    assert counts.get("SUCCEEDED") == 12
    assert peak["all"] <= 3 and all(peak[f"k{i}"] == 1 for i in range(4))


async def test_priority_decides_who_runs_first(store: Store) -> None:
    app = Ninelives(store)
    order: list[str] = []

    @app.workflow()
    async def job(ctx, label: str) -> None:
        order.append(label)

    for label, priority in (("low", 0), ("urgent", 100), ("normal", 10)):
        await app.start(job, label, options=StartOptions(priority=priority))
    await app.worker(concurrency=1).run_until_idle()
    assert order == ["urgent", "normal", "low"]


def test_invalid_limits_are_rejected() -> None:
    app = Ninelives("sqlite://:memory:")
    with pytest.raises(ValueError, match="at least 1"):
        app.workflow(concurrency=0)


# --------------------------------------------------------------------------- schedules


async def test_schedules_start_runs_on_time(store: Store, clock: FakeClock) -> None:
    app = Ninelives(store, clock=clock)  # the clock starts at 2027-01-15T08:00:00Z
    seen: list[str] = []

    @app.step()
    async def started(run_id: str) -> None:
        seen.append(run_id)

    @app.workflow()
    async def report(ctx, kind: str) -> str:
        await started(ctx.workflow_id)
        await ctx.wait_for_signal("done", timeout=86400)
        return kind

    app.schedule("quarter-hourly", report, "*/15 * * * *", args=["sales"])
    worker = app.worker()
    await worker.run_until_idle()
    assert seen == []
    clock.advance(15 * 60)
    await worker.run_until_idle()
    assert seen == ["quarter-hourly@2027-01-15T08:15:00Z"]
    clock.advance(15 * 60)  # the 08:15 run is still waiting: overlap="skip" skips 08:30
    await worker.run_until_idle()
    assert len(seen) == 1
    await app.signal(seen[0], "done")
    await worker.run_until_idle()  # the 08:15 run finishes...
    clock.advance(15 * 60)
    await worker.run_until_idle()  # ...so the 08:45 tick starts
    assert seen[-1] == "quarter-hourly@2027-01-15T08:45:00Z"
    assert await app.result(seen[0]) == "sales"
    [s] = await app.list_schedules()
    assert s.last_run_id == seen[-1] and s.next_at == clock() + 15 * 60
    await app.pause_schedule("quarter-hourly")
    clock.advance(3600)
    await worker.run_until_idle()
    assert len(seen) == 2
    manual = await app.trigger_schedule("quarter-hourly")
    await worker.run_until_idle()
    assert seen[-1] == manual
    resumed = await app.resume_schedule("quarter-hourly")
    assert resumed.enabled and resumed.next_at is not None and resumed.next_at > clock()


def test_schedule_definitions_are_validated() -> None:
    app = Ninelives("sqlite://:memory:")

    @app.workflow()
    async def wf(ctx) -> None: ...

    with pytest.raises(ValueError, match="5 fields"):
        app.schedule("bad", wf, "* * *")
    with pytest.raises(ValueError, match="time zone"):
        app.schedule("bad", wf, "* * * * *", tz="Nowhere/City")
    with pytest.raises(ValueError, match="overlap"):
        app.schedule("bad", wf, "* * * * *", overlap="queue")
    with pytest.raises(ValueError, match="'@'"):
        app.schedule("a@b", wf, "* * * * *")


# --------------------------------------------------------------------------- retention and forks


async def test_purge_and_worker_retention(store: Store, clock: FakeClock) -> None:
    app = Ninelives(store, clock=clock)

    @app.workflow()
    async def quick(ctx) -> int:
        return 1

    old = await app.start(quick)
    await drain(app)
    clock.advance(10 * 86400)
    fresh = await app.start(quick)
    await drain(app)
    assert await app.purge(older_than=7 * 86400) == 1
    with pytest.raises(WorkflowNotFound):
        await app.get(old)
    assert (await app.get(fresh)).status == Status.SUCCEEDED
    clock.advance(10 * 86400)
    await app.worker(retention=86400).run_until_idle()
    with pytest.raises(WorkflowNotFound):
        await app.get(fresh)


async def test_fork_hands_over_finished_children_and_copies_events(store: Store) -> None:
    app, _ = family(store)

    @app.workflow()
    async def orchestrate(ctx) -> object:
        child_id = await ctx.start_child("leaf", 4)
        await ctx.set_event("child", child_id)
        await ctx.wait_for_signal("continue")
        return await ctx.wait_child(child_id)

    wf_id = await app.start(orchestrate)
    await drain(app)
    # Re-run from the signal wait: the child was started before the fork point, awaited after it.
    fork = await app.fork(wf_id, 3)
    assert await app.get_events(fork) == {"child": f"{wf_id}/1"}
    await app.signal(fork, "continue")
    await drain(app)
    assert await app.result(fork) == 16

    @app.workflow()
    async def unfinished(ctx) -> object:
        child_id = await ctx.start_child("sleeper_forever")
        await ctx.wait_for_signal("continue")
        return await ctx.wait_child(child_id)

    @app.workflow(name="sleeper_forever")
    async def sleeper_forever(ctx) -> None:
        await ctx.wait_for_signal("never")

    stuck = await app.start(unfinished)
    await drain(app)
    with pytest.raises(ValueError, match="has not finished"):
        await app.fork(stuck, 2)


async def test_run_until_idle_never_stops_with_work_left(store: Store, clock: FakeClock) -> None:
    """Regression: a task finishing while the worker was claiming could make run_until_idle return
    with work left (a parent just woken by its child, or a concurrency slot just freed). A claim
    that returns late makes the window wide enough to hit every time."""
    app, _ = family(store, clock)

    @app.workflow(concurrency=1)
    async def limited(ctx, n: int) -> int:
        return n

    claim = store.claim

    async def late_claim(*args, **kwargs):  # type: ignore[no-untyped-def]
        claimed = await claim(*args, **kwargs)
        await asyncio.sleep(0.02)  # running tasks finish meanwhile
        return claimed

    store.claim = late_claim  # type: ignore[method-assign]
    parent = await app.start("parent", [2])
    queued = [await app.start(limited, n) for n in range(3)]
    await app.worker(lease_ttl=10).run_until_idle()
    assert (await app.get(parent)).status == Status.SUCCEEDED
    assert [(await app.get(q)).status for q in queued] == [Status.SUCCEEDED] * 3


async def test_a_schedule_removed_from_the_code_stops_firing(store: Store, clock: FakeClock) -> None:
    def version(with_schedule: bool) -> Ninelives:
        app = Ninelives(store, clock=clock)

        @app.workflow(name="tick")
        async def tick(ctx) -> None:
            return None

        if with_schedule:
            app.schedule("every-minute", tick, "* * * * *")
        return app

    old = version(True)
    await old.worker().run_until_idle()
    clock.advance(61)
    await old.worker().run_until_idle()
    assert len(await store.list_workflows(schedule_id="every-minute")) == 1
    new = version(False)  # deployed without the schedule
    clock.advance(120)
    await new.worker().run_until_idle()
    assert len(await store.list_workflows(schedule_id="every-minute")) == 1
    assert [s.id for s in await new.list_schedules()] == ["every-minute"]  # still listed, until deleted
