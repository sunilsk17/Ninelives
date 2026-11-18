"""OpenTelemetry spans for run executions and step attempts."""

from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from ninelives import Ninelives, PermanentError, RetryPolicy, Store

from .conftest import FakeClock

EXPORTER = InMemorySpanExporter()


@pytest.fixture(scope="module", autouse=True)
def tracer_provider() -> None:
    if not isinstance(trace.get_tracer_provider(), TracerProvider):  # set once per process
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(EXPORTER))
        trace.set_tracer_provider(provider)


async def test_spans_for_executions_and_attempts(store: Store, clock: FakeClock) -> None:
    EXPORTER.clear()
    app = Ninelives(store, clock=clock)
    tries = {"n": 0}

    @app.step(retry=RetryPolicy(max_attempts=3, initial_delay=0.001, jitter=False))
    async def flaky() -> str:
        tries["n"] += 1
        if tries["n"] == 1:
            raise ConnectionError("blip")
        return "ok"

    @app.step()
    async def fatal() -> None:
        raise PermanentError("no")

    @app.workflow()
    async def traced(ctx) -> str:
        value = await flaky()
        await ctx.sleep(60)  # suspends: a second execution replays flaky() without running it
        return value

    @app.workflow()
    async def broken(ctx) -> None:
        await fatal()

    run = await app.start(traced, id="traced-1")
    await app.worker().run_until_idle()
    clock.advance(61)
    await app.worker().run_until_idle()
    assert await app.result(run) == "ok"
    await app.start(broken, id="broken-1")
    await app.worker().run_until_idle()

    spans = EXPORTER.get_finished_spans()
    steps = [s for s in spans if s.name == "ninelives.step flaky"]
    assert [s.attributes["ninelives.step.attempt"] for s in steps] == [1, 2]  # replay made no span
    assert steps[0].status.status_code == StatusCode.ERROR and "blip" in (steps[0].status.description or "")
    assert steps[1].status.status_code != StatusCode.ERROR
    runs = [s for s in spans if s.name == "ninelives.run traced"]
    assert [s.attributes["ninelives.outcome"] for s in runs] == ["waiting", "succeeded"]
    assert [s.attributes["ninelives.execution"] for s in runs] == [1, 2]
    assert all(s.attributes["ninelives.workflow.id"] == "traced-1" for s in runs)
    # Step spans are children of the execution that ran them.
    assert steps[0].parent is not None and steps[0].parent.span_id == runs[0].context.span_id
    [failed] = [s for s in spans if s.name == "ninelives.run broken"]
    assert failed.attributes["ninelives.outcome"] == "failed"
    assert failed.status.status_code == StatusCode.ERROR
