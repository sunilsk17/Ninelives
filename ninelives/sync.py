"""Use Ninelives from synchronous code: Django views, Flask handlers, scripts, notebooks.

```python
from ninelives import SyncClient
from myproject.workflows import app, checkout

client = SyncClient(app)
run_id = client.start(checkout, "order-1")
print(client.result(run_id, timeout=30))
```

The client runs a private event loop in a background thread, with its own database connections,
so it is safe to use from any number of threads alongside workers running elsewhere.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from typing import Any, TypeVar

from .app import Ninelives
from .types import Schedule, StartOptions, WorkflowRecord

T = TypeVar("T")

__all__ = ["SyncClient"]


class SyncClient:
    def __init__(self, app: Ninelives):
        # Same registry, separate connections: asyncpg pools belong to the loop that made them.
        self.app = Ninelives(app.store.fresh(), clock=app.clock)
        self.app.workflows = app.workflows
        self.app.steps = app.steps
        self.app.schedules = app.schedules
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="ninelives-sync", daemon=True)
        self._thread.start()
        self._closed = False

    def _call(self, coro: Coroutine[Any, Any, T], timeout: float | None = None) -> T:
        if self._closed:
            coro.close()
            raise RuntimeError("this SyncClient is closed")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def start(
        self,
        workflow: Any,
        *args: Any,
        id: str | None = None,
        options: StartOptions | None = None,
        **kwargs: Any,
    ) -> str:
        return self._call(self.app.start(workflow, *args, id=id, options=options, **kwargs))

    def result(self, run_id: str, timeout: float | None = None) -> Any:
        """Block until the run finishes; returns its output or raises WorkflowFailed/TimeoutError."""
        return self._call(self.app.result(run_id, timeout=timeout))

    def get(self, run_id: str) -> WorkflowRecord:
        return self._call(self.app.get(run_id))

    def signal(self, run_id: str, name: str, payload: Any = None) -> None:
        self._call(self.app.signal(run_id, name, payload))

    def cancel(self, run_id: str) -> WorkflowRecord:
        return self._call(self.app.cancel(run_id))

    def get_event(self, run_id: str, key: str, timeout: float | None = None) -> Any:
        return self._call(self.app.get_event(run_id, key, timeout=timeout))

    def get_events(self, run_id: str) -> dict[str, Any]:
        return self._call(self.app.get_events(run_id))

    def describe(self, run_id: str) -> dict[str, Any]:
        return self._call(self.app.describe(run_id))

    def fork(self, run_id: str, from_seq: int) -> str:
        return self._call(self.app.fork(run_id, from_seq))

    def list_schedules(self) -> list[Schedule]:
        return self._call(self.app.list_schedules())

    def trigger_schedule(self, schedule_id: str) -> str:
        return self._call(self.app.trigger_schedule(schedule_id))

    def close(self) -> None:
        if self._closed:
            return
        self._call(self.app.close())
        self._closed = True
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=10)
        self._loop.close()

    def __enter__(self) -> SyncClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
