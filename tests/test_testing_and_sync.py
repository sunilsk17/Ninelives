"""The testing helpers and the synchronous client."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest

from ninelives import Ninelives, StartOptions, SyncClient, WorkflowFailed
from ninelives.testing import FakeClock, TestEnv

from .conftest import PG_DSN


def build(store: str) -> Ninelives:
    app = Ninelives(store)

    @app.step()
    async def charge(order: str) -> str:
        return f"charged {order}"

    @app.workflow()
    async def checkout(ctx, order: str) -> dict:
        receipt = await charge(order)
        await ctx.set_event("stage", "awaiting review")
        await ctx.sleep(24 * 3600)
        review = await ctx.wait_for_signal("review", timeout=7 * 86400)
        return {"receipt": receipt, "review": review}

    @app.workflow()
    async def broken(ctx) -> None:
        raise RuntimeError("boom")

    @app.workflow()
    async def ping(ctx) -> str:
        return "pong"

    app.schedule("nightly", ping, "0 3 * * *")
    return app


async def test_test_env_moves_time_and_restores_the_app(tmp_path: Path) -> None:
    app = build(f"sqlite:///{tmp_path}/real.db")
    original_store, original_clock = app.store, app.clock
    async with TestEnv(app) as env:
        assert env.now() == 1_800_000_000.0  # 2027-01-15T08:00:00Z
        run = await env.start("checkout", "o-1")
        await env.run_until_idle()
        assert await env.events(run) == {"stage": "awaiting review"}
        with pytest.raises(AssertionError, match="still WAITING"):
            await env.result(run)
        await env.advance(24 * 3600)
        with pytest.raises(AssertionError, match="waiting for: review"):
            await env.result(run)
        await env.signal(run, "review", {"stars": 5})
        assert await env.result(run) == {"receipt": "charged o-1", "review": {"stars": 5}}
        with pytest.raises(WorkflowFailed, match="boom"):
            await env.run("broken")
        assert await env.run("ping") == "pong"
        # The schedule fires when the clock passes 03:00 the next day.
        await env.advance(20 * 3600)
        nightly = [r.id for r in await app.store.list_workflows(name="ping") if r.schedule_id]
        # 44 hours have passed since 08:00 on the 15th: the 16th and the 17th at 03:00.
        assert sorted(nightly) == ["nightly@2027-01-16T03:00:00Z", "nightly@2027-01-17T03:00:00Z"]
        assert (await env.describe(run))["workflow"]["status"] == "SUCCEEDED"
        assert (await env.get(run)).output["review"] == {"stars": 5}
    assert app.store is original_store and app.clock is original_clock
    await app.close()


def test_fake_clock_only_moves_forward() -> None:
    clock = FakeClock(10)
    clock.advance(5)
    assert clock() == 15
    with pytest.raises(ValueError):
        clock.advance(-1)


def _worker_thread(store: str, stop: threading.Event, errors: list[BaseException]) -> threading.Thread:
    def main() -> None:
        try:
            asyncio.run(go())
        except BaseException as exc:  # surfaced by the test
            errors.append(exc)

    async def go() -> None:
        app = build(store)
        worker = app.worker(poll_interval=0.02)
        task = asyncio.create_task(worker.run())
        while not stop.is_set():
            await asyncio.sleep(0.02)
            if task.done():
                task.result()  # re-raise a crashed worker loop
        worker.stop()
        await task
        await app.close()

    thread = threading.Thread(target=main, daemon=True)
    thread.start()
    return thread


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_sync_client_from_plain_code(backend: str, tmp_path: Path) -> None:
    if backend == "postgres":
        if not PG_DSN:
            pytest.skip("needs NINELIVES_TEST_POSTGRES_DSN")
        store = PG_DSN
    else:
        store = f"sqlite:///{tmp_path}/sync.db"
    app = build(store)
    stop = threading.Event()
    errors: list[BaseException] = []
    thread = _worker_thread(store, stop, errors)
    try:
        with SyncClient(app) as client:
            assert client.result(client.start("ping"), timeout=20) == "pong"
            run = client.start("checkout", "o-9", options=StartOptions(priority=3))
            assert client.get_event(run, "stage", timeout=20) == "awaiting review"
            assert client.get(run).priority == 3
            assert client.get_events(run) == {"stage": "awaiting review"}
            assert client.describe(run)["workflow"]["id"] == run
            assert client.cancel(run).status == "CANCELLED"
            with pytest.raises(WorkflowFailed):
                client.result(run, timeout=5)
            failing = client.start("broken")
            with pytest.raises(WorkflowFailed, match="boom"):
                client.result(failing, timeout=20)
            fork = client.fork(failing, 1)
            assert fork.startswith("broken-fork-")
            client.signal(fork, "anything")
            assert [s.id for s in client.list_schedules()] == ["nightly"]  # registered by the worker
        with pytest.raises(RuntimeError, match="closed"):
            client.start("ping")
        client.close()  # idempotent
    finally:
        stop.set()
        thread.join(timeout=30)
        assert errors == [], errors
    asyncio.run(app.close())
