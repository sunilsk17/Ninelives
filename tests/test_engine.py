"""Behaviour of workflows, steps, timers, signals and recovery, on every store backend."""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass

import pytest

from ninelives import (
    Ninelives,
    NonDeterminismError,
    PermanentError,
    RetryPolicy,
    SignalTimeout,
    Status,
    StepFailed,
    Store,
    WorkflowFailed,
    step_info,
)
from ninelives.errors import SerializationError, UnknownWorkflow, WorkflowNotFound

from .conftest import FakeClock

FAST = RetryPolicy(max_attempts=3, initial_delay=0.001, jitter=False)


def make_app(store: Store, clock: FakeClock | None = None) -> Ninelives:
    return Ninelives(store, clock=clock) if clock else Ninelives(store)


async def test_steps_run_and_outputs_round_trip(store: Store) -> None:
    app = make_app(store)

    @dataclass
    class Point:
        x: int
        y: int

    @app.step()
    def sync_step(a: int) -> tuple[int, int]:
        return (a, a * 2)

    @app.step()
    async def async_step(p: dict) -> Point:
        return Point(p["x"], p["x"] + 1)

    @app.workflow()
    async def flow(ctx, a):
        pair = await sync_step(a)
        point = await async_step({"x": pair[1]})
        return {"pair": pair, "point": point}

    wid = await app.start(flow, 3)
    await app.worker().run_until_idle()
    assert await app.result(wid) == {"pair": [3, 6], "point": {"x": 6, "y": 7}}
    assert sync_step(5) == (5, 10)  # outside a workflow it is the plain function


async def test_crash_mid_workflow_resumes_without_repeating_finished_steps(
    store: Store, clock: FakeClock
) -> None:
    app = make_app(store, clock)
    calls: Counter[str] = Counter()
    keys: list[str] = []
    gate = asyncio.Event()

    @app.step()
    async def one() -> int:
        calls["one"] += 1
        return 1

    @app.step()
    async def two() -> int:
        calls["two"] += 1
        keys.append(step_info().idempotency_key)
        if calls["two"] == 1:
            await gate.wait()  # the first worker "dies" while this is in flight
        return 2

    @app.step()
    async def three(a: int, b: int) -> int:
        calls["three"] += 1
        return a + b

    @app.workflow()
    async def flow(ctx):
        return await three(await one(), await two())

    wid = await app.start(flow)
    dying = app.worker(lease_ttl=10, worker_id="dying")
    await dying._claim_once()
    while calls["two"] == 0:
        await asyncio.sleep(0.005)
    for task in list(dying._running.values()):  # kill -9: no cleanup, lease left behind
        task.cancel()
    await asyncio.sleep(0.01)

    clock.advance(11)  # lease expires
    await app.worker(worker_id="rescuer").run_until_idle()
    assert await app.result(wid) == 3
    assert calls == {"one": 1, "two": 2, "three": 1}
    assert keys[0] == keys[1] == f"{wid}:2"  # same idempotency key on both executions
    rec = await app.get(wid)
    assert rec.recoveries == 1 and rec.runs == 2
    attempts = [(a.seq, a.attempt, a.worker) for a in await store.get_attempts(wid)]
    assert attempts == [(1, 1, "dying"), (2, 1, "dying"), (2, 2, "rescuer"), (3, 1, "rescuer")]


async def test_zombie_worker_is_fenced_out(store: Store, clock: FakeClock) -> None:
    app = make_app(store, clock)
    release = asyncio.Event()
    effects: list[str] = []

    @app.step()
    async def slow() -> str:
        me = "first" if not effects else "second"
        effects.append(me)
        if me == "first":
            await release.wait()  # paused (GC, network partition...)
        return me

    @app.workflow()
    async def flow(ctx):
        return await slow()

    wid = await app.start(flow)
    zombie = app.worker(lease_ttl=10, worker_id="zombie")
    await zombie._claim_once()
    while not effects:
        await asyncio.sleep(0.005)
    clock.advance(11)
    await app.worker(worker_id="new").run_until_idle()
    assert await app.result(wid) == "second"

    release.set()  # the zombie wakes up and tries to record its result
    for task in list(zombie._running.values()):
        await asyncio.wait_for(task, 2)
    journal = await store.get_journal(wid)
    assert [e.output for e in journal] == ["second"]
    assert (await app.get(wid)).output == "second"


async def test_retries_with_backoff_then_success(store: Store) -> None:
    app = make_app(store)
    n = Counter[str]()

    @app.step(retry=FAST)
    def flaky() -> str:
        n["flaky"] += 1
        if n["flaky"] < 3:
            raise ConnectionError("try again")
        return "ok"

    @app.workflow()
    async def flow(ctx):
        return await flaky()

    wid = await app.start(flow)
    await app.worker().run_until_idle()
    assert await app.result(wid) == "ok"
    attempts = await store.get_attempts(wid)
    assert [a.error for a in attempts] == ["ConnectionError: try again", "ConnectionError: try again", None]
    [entry] = await store.get_journal(wid)
    assert entry.attempts == 3


async def test_exhausted_retries_raise_step_failed_which_workflows_can_handle(store: Store) -> None:
    app = make_app(store)

    @app.step(retry=FAST)
    async def charge() -> None:
        raise RuntimeError("card declined")

    @app.step()
    async def refund_hold() -> str:
        return "released"

    @app.workflow()
    async def flow(ctx):
        try:
            await charge()
        except StepFailed as exc:
            return {"compensated": await refund_hold(), "why": exc.error["message"]}

    wid = await app.start(flow)
    await app.worker().run_until_idle()
    assert await app.result(wid) == {"compensated": "released", "why": "card declined"}
    assert len(await store.get_attempts(wid)) == 4  # 3 charge attempts + 1 refund


async def test_permanent_and_non_retryable_errors_fail_fast(store: Store) -> None:
    app = make_app(store)

    class Invalid(ValueError):
        pass

    @app.step(retry=FAST)
    async def permanent() -> None:
        raise PermanentError("bad input")

    @app.step(retry=RetryPolicy(max_attempts=5, initial_delay=0.001, non_retryable=(Invalid,)))
    async def invalid() -> None:
        raise Invalid("nope")

    @app.workflow()
    async def flow(ctx, which):
        await (permanent() if which == "p" else invalid())

    a = await app.start(flow, "p")
    b = await app.start(flow, "i")
    await app.worker().run_until_idle()
    for wid, msg in ((a, "bad input"), (b, "nope")):
        with pytest.raises(WorkflowFailed) as info:
            await app.result(wid)
        assert info.value.error["type"] == "StepFailed" and msg in info.value.error["message"]
        assert len(await store.get_attempts(wid)) == 1


async def test_step_timeout_counts_as_a_failed_attempt(store: Store) -> None:
    app = make_app(store)

    @app.step(timeout=0.05, retry=RetryPolicy(max_attempts=2, initial_delay=0.001))
    async def hangs() -> None:
        await asyncio.sleep(5)

    @app.workflow()
    async def flow(ctx):
        await hangs()

    wid = await app.start(flow)
    await app.worker().run_until_idle()
    with pytest.raises(WorkflowFailed):
        await app.result(wid)
    assert [a.error for a in await store.get_attempts(wid)] == ["TimeoutError: ", "TimeoutError: "]


async def test_failed_step_is_replayed_as_a_failure(store: Store, clock: FakeClock) -> None:
    app = make_app(store, clock)
    runs = Counter[str]()

    @app.step(retry=RetryPolicy(max_attempts=1))
    async def boom() -> None:
        runs["boom"] += 1
        raise RuntimeError("x")

    @app.workflow()
    async def flow(ctx):
        try:
            await boom()
        except StepFailed:
            pass
        await ctx.sleep(3600)
        try:
            await boom()  # a new step (seq 3); runs again
        except StepFailed:
            return "done"

    wid = await app.start(flow)
    worker = app.worker()
    await worker.run_until_idle()
    clock.advance(3601)
    await worker.run_until_idle()
    assert await app.result(wid) == "done"
    assert runs["boom"] == 2  # seq 1 replayed as failed, not re-run


async def test_durable_sleep_suspends_and_resumes(store: Store, clock: FakeClock) -> None:
    app = make_app(store, clock)
    before = Counter[str]()

    @app.step()
    async def remind(stage: str) -> str:
        before[stage] += 1
        return stage

    @app.workflow()
    async def flow(ctx):
        await remind("a")
        await ctx.sleep(24 * 3600)
        return await remind("b")

    wid = await app.start(flow)
    worker = app.worker()
    await worker.run_until_idle()
    rec = await app.get(wid)
    assert rec.status == Status.WAITING and rec.wake_at == pytest.approx(clock.t + 24 * 3600)
    assert rec.lease_token is None
    clock.advance(24 * 3600 - 1)
    await worker.run_until_idle()
    assert (await app.get(wid)).status == Status.WAITING
    clock.advance(1)
    await worker.run_until_idle()
    assert await app.result(wid) == "b"
    assert before == {"a": 1, "b": 1}


async def test_short_sleep_runs_inline(store: Store) -> None:
    app = make_app(store)

    @app.workflow()
    async def flow(ctx):
        await ctx.sleep(0.01)
        await ctx.sleep(0)
        return "slept"

    wid = await app.start(flow)
    await app.worker().run_until_idle()
    assert await app.result(wid) == "slept"
    assert (await app.get(wid)).runs == 1


async def test_signal_wait_resume_and_ordering(store: Store) -> None:
    app = make_app(store)

    @app.workflow()
    async def flow(ctx):
        first = await ctx.wait_for_signal("approve")
        second = await ctx.wait_for_signal("approve")
        return [first, second]

    wid = await app.start(flow)
    worker = app.worker()
    await worker.run_until_idle()
    rec = await app.get(wid)
    assert rec.status == Status.WAITING and rec.wake_at is None
    await app.signal(wid, "approve", {"by": "asha"})
    await app.signal(wid, "approve", {"by": "ravi"})
    await worker.run_until_idle()
    assert await app.result(wid) == [{"by": "asha"}, {"by": "ravi"}]
    assert [s.consumed_seq for s in await store.get_signals(wid)] == [1, 2]


async def test_signal_sent_before_the_wait_is_delivered(store: Store) -> None:
    app = make_app(store)

    @app.workflow()
    async def flow(ctx):
        return await ctx.wait_for_signal("go")

    wid = await app.start(flow)
    await app.signal(wid, "go", 42)
    await app.worker().run_until_idle()
    assert await app.result(wid) == 42


async def test_signal_timeout(store: Store, clock: FakeClock) -> None:
    app = make_app(store, clock)

    @app.workflow()
    async def flow(ctx):
        try:
            return await ctx.wait_for_signal("approve", timeout=600)
        except SignalTimeout:
            return "escalated"

    wid = await app.start(flow)
    worker = app.worker()
    await worker.run_until_idle()
    assert (await app.get(wid)).wake_at == pytest.approx(clock.t + 600)
    clock.advance(601)
    await worker.run_until_idle()
    assert await app.result(wid) == "escalated"
    await app.signal(wid, "approve", True)  # too late: kept, never consumed
    assert (await store.get_signals(wid))[0].consumed_seq is None


async def test_cancel_pending_waiting_and_running(store: Store) -> None:
    app = make_app(store)
    started = asyncio.Event()
    hold = asyncio.Event()

    @app.step()
    async def slow() -> None:
        started.set()
        await hold.wait()

    @app.workflow()
    async def runs(ctx):
        await slow()
        await ctx.sleep(0.01)  # first durable op after the cancel request
        return "should not finish"

    @app.workflow()
    async def waits(ctx):
        await ctx.wait_for_signal("never")

    pending = await app.start(runs)
    await app.cancel(pending)
    assert (await app.get(pending)).status == Status.CANCELLED

    waiting = await app.start(waits)
    running = await app.start(runs)
    worker = app.worker(lease_ttl=0.3)
    loop_task = asyncio.create_task(worker.run())
    await asyncio.wait_for(started.wait(), 5)
    await app.cancel(running)
    await asyncio.sleep(0.25)  # a heartbeat delivers the cancel request
    hold.set()
    for _ in range(100):
        if (await app.get(running)).status.terminal:
            break
        await asyncio.sleep(0.02)
    await app.cancel(waiting)
    worker.stop()
    await loop_task
    assert (await app.get(running)).status == Status.CANCELLED
    assert (await app.get(waiting)).status == Status.CANCELLED
    with pytest.raises(WorkflowFailed, match="cancelled"):
        await app.result(running)


async def test_non_determinism_is_detected(store: Store, clock: FakeClock) -> None:
    v1 = make_app(store, clock)

    @v1.step(name="a")
    async def a1() -> int:
        return 1

    @v1.workflow(name="flow")
    async def flow_v1(ctx):
        await a1()
        await ctx.sleep(3600)
        return "v1"

    wid = await v1.start("flow")
    await v1.worker().run_until_idle()

    v2 = make_app(store, clock)  # code changed while the run was sleeping

    @v2.step(name="b")
    async def b2() -> int:
        return 2

    @v2.workflow(name="flow")
    async def flow_v2(ctx):
        await b2()
        await ctx.sleep(3600)
        return "v2"

    clock.advance(3601)
    await v2.worker().run_until_idle()
    with pytest.raises(WorkflowFailed) as info:
        await v2.result(wid)
    assert info.value.error["type"] == NonDeterminismError.__name__


async def test_fork_reuses_journal_and_reruns_from_a_step(store: Store) -> None:
    app = make_app(store)
    runs = Counter[str]()

    @app.step()
    async def fetch() -> int:
        runs["fetch"] += 1
        return 10

    @app.step()
    async def summarise(x: int) -> str:
        runs["summarise"] += 1
        return f"v{runs['summarise']}:{x}"

    @app.workflow()
    async def flow(ctx):
        return await summarise(await fetch())

    wid = await app.start(flow)
    worker = app.worker()
    await worker.run_until_idle()
    assert await app.result(wid) == "v1:10"
    fork = await app.fork(wid, from_seq=2)
    await worker.run_until_idle()
    assert await app.result(fork) == "v2:10"
    assert runs == {"fetch": 1, "summarise": 2}
    rec = await app.get(fork)
    assert rec.forked_from == wid and rec.fork_seq == 2


async def test_fork_refuses_unfinished_history(store: Store) -> None:
    app = make_app(store)

    @app.workflow()
    async def flow(ctx):
        await ctx.wait_for_signal("x")

    wid = await app.start(flow)
    await app.worker().run_until_idle()
    with pytest.raises(ValueError, match="never finished"):
        await app.fork(wid, from_seq=2)


async def test_deterministic_values_are_stable_across_replay(store: Store, clock: FakeClock) -> None:
    app = make_app(store, clock)
    seen: list[tuple] = []

    @app.workflow()
    async def flow(ctx):
        values = (await ctx.now(), await ctx.uuid(), await ctx.random())
        seen.append(values)
        await ctx.sleep(100)
        return list(values)

    wid = await app.start(flow)
    worker = app.worker()
    await worker.run_until_idle()
    clock.advance(101)
    await worker.run_until_idle()
    assert seen[0] == seen[1]
    assert await app.result(wid) == list(seen[0])


async def test_parallel_steps_with_gather(store: Store) -> None:
    app = make_app(store)

    @app.step()
    async def square(x: int) -> int:
        await asyncio.sleep(0.01 * (5 - x))
        return x * x

    @app.workflow()
    async def flow(ctx):
        return await asyncio.gather(*(square(i) for i in range(5)))

    wid = await app.start(flow)
    await app.worker().run_until_idle()
    assert await app.result(wid) == [0, 1, 4, 9, 16]
    assert [e.name for e in await store.get_journal(wid)] == ["square"] * 5


async def test_idempotent_start_and_errors(store: Store) -> None:
    app = make_app(store)

    @app.workflow()
    async def flow(ctx, x):
        raise ValueError(f"bad {x}")

    a = await app.start(flow, 1, id="order-1")
    b = await app.start(flow, 2, id="order-1")
    assert a == b == "order-1"
    assert (await app.get(a)).input == {"args": [1], "kwargs": {}}
    await app.worker().run_until_idle()
    with pytest.raises(WorkflowFailed) as info:
        await app.result(a)
    assert info.value.error["type"] == "ValueError" and "bad 1" in info.value.error["message"]
    with pytest.raises(UnknownWorkflow):
        await app.start(lambda ctx: None)
    with pytest.raises(WorkflowNotFound):
        await app.get("missing")


async def test_result_timeout(store: Store) -> None:
    app = make_app(store)

    @app.workflow()
    async def flow(ctx):
        return 1

    wid = await app.start(flow)
    with pytest.raises(TimeoutError):
        await app.result(wid, timeout=0.05)


async def test_unserializable_output_fails_the_workflow(store: Store) -> None:
    app = make_app(store)

    @app.step()
    async def bad() -> object:
        return object()

    @app.workflow()
    async def flow(ctx):
        return await bad()

    wid = await app.start(flow)
    await app.worker().run_until_idle()
    with pytest.raises(WorkflowFailed) as info:
        await app.result(wid)
    assert info.value.error["type"] == SerializationError.__name__


async def test_many_workers_never_run_a_workflow_twice_at_once(store: Store) -> None:
    app = make_app(store)
    active: set[str] = set()
    overlaps: list[str] = []
    done: Counter[str] = Counter()

    @app.step()
    async def work(wid: str, i: int) -> int:
        if wid in active:
            overlaps.append(wid)
        active.add(wid)
        await asyncio.sleep(0.001)
        active.discard(wid)
        return i

    @app.workflow()
    async def flow(ctx, n):
        total = 0
        for i in range(3):
            total += await work(ctx.workflow_id, i)
        done[ctx.workflow_id] += 1
        return total

    ids = [await app.start(flow, i) for i in range(40)]
    workers = [app.worker(concurrency=5, poll_interval=0.01, worker_id=f"w{i}") for i in range(4)]
    tasks = [asyncio.create_task(w.run()) for w in workers]
    for wid in ids:
        assert await app.result(wid, timeout=30) == 3
    for w in workers:
        w.stop()
    await asyncio.gather(*tasks)
    assert overlaps == []
    assert all(done[w] == 1 for w in ids)
    assert sum(w.completed for w in workers) == 40


async def test_graceful_shutdown_hands_work_over(store: Store) -> None:
    app = make_app(store)
    entered = asyncio.Event()

    @app.step()
    async def long() -> str:
        entered.set()
        await asyncio.sleep(30)
        return "x"

    @app.workflow()
    async def flow(ctx):
        return await long()

    wid = await app.start(flow)
    worker = app.worker(shutdown_grace=0.05)
    task = asyncio.create_task(worker.run())
    await asyncio.wait_for(entered.wait(), 5)
    worker.stop()
    await task
    rec = await app.get(wid)
    assert rec.status == Status.RUNNING and rec.lease_expires_at == 0  # released for an immediate takeover


async def test_worker_that_reclaims_its_own_lapsed_run_replaces_the_stale_execution(
    store: Store, clock: FakeClock
) -> None:
    """A paused worker can wake up and claim a run it is still (stuck) executing. The stale
    execution must be stopped and the bookkeeping must follow the new one."""
    app = make_app(store, clock)
    gate = asyncio.Event()
    calls = Counter[str]()

    @app.step()
    async def slow() -> str:
        calls["slow"] += 1
        if calls["slow"] == 1:
            await gate.wait()  # the first execution hangs, like a paused process
        return "done"

    @app.workflow()
    async def wf(ctx) -> str:
        return await slow()

    wf_id = await app.start(wf)
    # A long TTL keeps heartbeats out of the picture: only the claim itself can notice the problem.
    worker = app.worker(lease_ttl=600.0)
    assert await worker._claim_once() == 1
    first = worker._running[wf_id]
    while not calls["slow"]:
        await asyncio.sleep(0.001)
    clock.advance(1000)  # the lease lapses while the first execution is stuck
    assert await worker._claim_once() == 1
    second = worker._running[wf_id]
    assert second is not first
    assert await app.result(wf_id, timeout=5) == "done"
    await asyncio.wait_for(asyncio.gather(first, second, return_exceptions=True), timeout=5)
    assert first.done() and first.exception() is None  # it stopped cleanly instead of hanging
    assert worker._running == {} and worker._tokens == {} and worker._lost == set()
    assert calls["slow"] == 2
    rec = await app.get(wf_id)
    assert rec.runs == 2 and rec.recoveries == 1


async def test_ctx_gather_numbers_multi_step_branches_deterministically(store: Store) -> None:
    """Branches with several steps each get round-robin sequence numbers, whatever the timing,
    so a full replay (here: forks that copy the whole journal) never re-runs a step."""
    import random

    app = make_app(store)
    calls = Counter[str]()
    rng = random.Random(7)

    @app.step()
    async def work(label: str) -> str:
        calls[label] += 1
        await asyncio.sleep(rng.uniform(0, 0.01))
        return label

    async def branch(name: str, n: int) -> list[str]:
        return [await work(f"{name}{i}") for i in range(n)]

    @app.workflow()
    async def flow(ctx):
        first = await ctx.gather(branch("a", 3), branch("b", 1), branch("c", 4))
        return [*first, await work("after")]

    expected = [["a0", "a1", "a2"], ["b0"], ["c0", "c1", "c2", "c3"], "after"]
    wid = await app.start(flow)
    await app.worker().run_until_idle()
    assert await app.result(wid) == expected
    journal = await store.get_journal(wid)
    assert [e.output for e in journal] == ["a0", "b0", "c0", "a1", "c1", "a2", "c2", "c3", "after"]
    ran = sum(calls.values())
    assert ran == 9
    for _ in range(3):
        replay = await app.fork(wid, len(journal) + 1)
        await app.worker().run_until_idle()
        assert await app.result(replay) == expected
    assert sum(calls.values()) == ran


async def test_raw_asyncio_gather_of_multi_step_branches_is_rejected(store: Store) -> None:
    app = make_app(store)

    @app.step()
    async def work(label: str) -> str:
        return label

    async def branch(name: str) -> list[str]:
        return [await work(f"{name}0"), await work(f"{name}1")]

    @app.workflow()
    async def flow(ctx):
        return await asyncio.gather(branch("a"), branch("b"))

    wid = await app.start(flow)
    await app.worker().run_until_idle()
    with pytest.raises(WorkflowFailed) as info:
        await app.result(wid)
    assert info.value.error["type"] == "NonDeterminismError"
    assert "ctx.gather" in info.value.error["message"]


async def test_ctx_gather_parks_until_the_earliest_wake_up(store: Store, clock: FakeClock) -> None:
    app = make_app(store, clock)

    @app.step()
    async def mark(x: str) -> str:
        return x

    @app.workflow()
    async def flow(ctx):
        async def sleeper() -> str:
            await ctx.sleep(60)
            return await mark("slept")

        async def waiter() -> str:
            return await mark(await ctx.wait_for_signal("go"))

        async def quick() -> str:
            return await mark("quick")

        return await ctx.gather(sleeper(), waiter(), quick())

    t0 = clock()
    wid = await app.start(flow)
    await app.worker().run_until_idle()
    rec = await app.get(wid)
    assert rec.status == Status.WAITING and rec.wake_at == t0 + 60
    await app.signal(wid, "go", "hi")
    await app.worker().run_until_idle()
    rec = await app.get(wid)
    assert rec.status == Status.WAITING and rec.wake_at == t0 + 60  # the signal branch is done
    clock.advance(61)
    await app.worker().run_until_idle()
    assert await app.result(wid) == ["slept", "hi", "quick"]
    names = [e.name for e in await store.get_journal(wid)]
    assert names.count("mark") == 3 and names.count("sleep") == 1 and names.count("go") == 1


async def test_ctx_gather_raises_the_first_failure_after_all_branches_settle(store: Store) -> None:
    app = make_app(store)
    done: list[str] = []

    @app.step(retry=RetryPolicy(max_attempts=1))
    async def fail() -> None:
        raise PermanentError("no")

    @app.step()
    async def slow(x: str) -> str:
        await asyncio.sleep(0.02)
        done.append(x)
        return x

    @app.workflow()
    async def flow(ctx):
        async def chain() -> list[str]:
            return [await slow("s1"), await slow("s2")]

        try:
            await ctx.gather(fail(), chain())
        except StepFailed as exc:
            return f"compensated after {exc.step}"
        return "unreachable"

    wid = await app.start(flow)
    await app.worker().run_until_idle()
    assert await app.result(wid) == "compensated after fail"
    assert done == ["s1", "s2"]


async def test_ctx_gather_cannot_be_nested(store: Store) -> None:
    app = make_app(store)

    @app.step()
    async def one() -> int:
        return 1

    @app.workflow()
    async def flow(ctx):
        async def inner() -> list[int]:
            return await ctx.gather(one(), one())

        return await ctx.gather(inner(), one())

    wid = await app.start(flow)
    await app.worker().run_until_idle()
    with pytest.raises(WorkflowFailed) as info:
        await app.result(wid)
    assert "cannot be nested" in info.value.error["message"]


async def test_ctx_gather_resumes_after_a_crash_mid_branch(store: Store, clock: FakeClock) -> None:
    app = make_app(store, clock)
    gate = asyncio.Event()
    calls = Counter[str]()

    @app.step()
    async def work(label: str) -> str:
        calls[label] += 1
        if label == "b1" and calls[label] == 1:
            await gate.wait()  # the worker dies while b1 is in flight
        await asyncio.sleep(0.001 * len(calls))
        return label

    async def branch(name: str, n: int) -> list[str]:
        return [await work(f"{name}{i}") for i in range(n)]

    @app.workflow()
    async def flow(ctx):
        return await ctx.gather(branch("a", 3), branch("b", 3))

    wid = await app.start(flow)
    dying = app.worker(lease_ttl=10, worker_id="dying")
    await dying._claim_once()
    while calls["b1"] == 0:
        await asyncio.sleep(0.005)
    for task in list(dying._running.values()):
        task.cancel()
    await asyncio.sleep(0.01)
    clock.advance(11)
    await app.worker(worker_id="rescuer").run_until_idle()
    assert await app.result(wid) == [["a0", "a1", "a2"], ["b0", "b1", "b2"]]
    assert calls["b1"] == 2 and calls["a0"] == calls["b0"] == 1  # only the in-flight step re-ran
    seqs = [e.seq for e in await store.get_journal(wid)]
    assert seqs == list(range(1, 7))
