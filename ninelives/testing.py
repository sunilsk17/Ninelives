"""Test your workflows without workers, threads or waiting.

```python
from ninelives.testing import TestEnv

async def test_checkout():
    async with TestEnv(app) as env:
        run = await env.start(checkout, "order-1")
        await env.run_until_idle()                # runs everything that can run now
        await env.advance(24 * 3600)              # a day passes: timers fire, schedules tick
        await env.signal(run, "review", {"stars": 5})
        assert await env.result(run) == {...}
```

``TestEnv`` points the app at a fresh in-memory store and a clock you move by hand, and restores
both afterwards. Each ``run_until_idle`` executes workflows in the current task, so exceptions and
breakpoints behave as usual.
"""

from __future__ import annotations

from typing import Any

from .app import Ninelives
from .errors import WorkflowFailed
from .stores import SQLiteStore, Store
from .types import StartOptions, Status, WorkflowRecord

__all__ = ["FakeClock", "TestEnv"]


class FakeClock:
    """A clock that only moves when told to."""

    def __init__(self, start: float = 1_800_000_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("time only moves forward")
        self.now += seconds


class TestEnv:
    """An isolated store and a controllable clock for one test (see the module docstring)."""

    __test__ = False  # not a pytest test class

    def __init__(self, app: Ninelives, *, start: float = 1_800_000_000.0, store: Store | None = None):
        self.app = app
        self.clock = FakeClock(start)
        self.store: Store = store or SQLiteStore(":memory:")
        self._saved: tuple[Store, Any, bool] | None = None

    async def __aenter__(self) -> TestEnv:
        self._saved = (self.app.store, self.app.clock, self.app._initialized)
        self.app.store = self.store
        self.app.clock = self.clock
        self.app._initialized = False
        await self.app.init()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.store.close()
        assert self._saved is not None
        self.app.store, self.app.clock, self.app._initialized = self._saved

    # ------------------------------------------------------------------ time and work
    def now(self) -> float:
        return self.clock()

    async def run_until_idle(self, timeout: float = 30.0) -> None:
        """Run every workflow that can make progress at the current time, until none can."""
        await self.app.worker(lease_ttl=3600, worker_id="test-env").run_until_idle(timeout)

    async def advance(self, seconds: float) -> None:
        """Move the clock forward, then run whatever woke up (timers, timeouts, schedules)."""
        self.clock.advance(seconds)
        await self.run_until_idle()

    # ------------------------------------------------------------------ client shortcuts
    async def start(
        self,
        workflow: Any,
        *args: Any,
        id: str | None = None,
        options: StartOptions | None = None,
        **kwargs: Any,
    ) -> str:
        return await self.app.start(workflow, *args, id=id, options=options, **kwargs)

    async def run(self, workflow: Any, *args: Any, **kwargs: Any) -> Any:
        """Start a workflow, run until idle and return its result (it must have finished)."""
        run_id = await self.start(workflow, *args, **kwargs)
        await self.run_until_idle()
        return await self.result(run_id)

    async def result(self, run_id: str) -> Any:
        """The output of a finished run. Raises WorkflowFailed, or AssertionError if it is not done."""
        rec = await self.app.get(run_id)
        if rec.status == Status.SUCCEEDED:
            return rec.output
        if rec.status in (Status.FAILED, Status.CANCELLED):
            raise WorkflowFailed(run_id, str(rec.status), rec.error)
        waits = [e.name for e in await self.app.store.get_journal(run_id) if e.status == "pending"]
        hint = f" (waiting for: {', '.join(waits)})" if waits else ""
        raise AssertionError(
            f"run {run_id} is still {rec.status}{hint}; send a signal or advance() the clock"
        )

    async def signal(self, run_id: str, name: str, payload: Any = None, *, run: bool = True) -> None:
        """Send a signal and (by default) run until idle."""
        await self.app.signal(run_id, name, payload)
        if run:
            await self.run_until_idle()

    async def get(self, run_id: str) -> WorkflowRecord:
        return await self.app.get(run_id)

    async def describe(self, run_id: str) -> dict[str, Any]:
        return await self.app.describe(run_id)

    async def events(self, run_id: str) -> dict[str, Any]:
        return await self.app.get_events(run_id)
