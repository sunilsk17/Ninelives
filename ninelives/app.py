"""The application object: registry of workflows and steps, plus client operations."""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any, Generic, ParamSpec, TypeVar

from . import serde
from .context import _current_ctx, _current_step
from .errors import UnknownWorkflow, WorkflowFailed, WorkflowNotFound
from .stores import Store, open_store
from .types import EntryStatus, JournalEntry, RetryPolicy, Status, WorkflowRecord

P = ParamSpec("P")
R = TypeVar("R")


@dataclass
class StepSpec:
    name: str
    fn: Callable[..., Any]
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    timeout: float | None = None


@dataclass
class WorkflowSpec:
    name: str
    fn: Callable[..., Any]


class StepFunction(Generic[P, R]):
    """What ``@app.step`` returns. Inside a workflow, awaiting it runs a durable step;
    anywhere else (or inside another step) it is the plain function."""

    def __init__(self, spec: StepSpec):
        self.spec = spec
        self.__name__ = getattr(spec.fn, "__name__", spec.name)
        self.__doc__ = spec.fn.__doc__
        self.__wrapped__ = spec.fn

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        ctx = _current_ctx.get()
        if ctx is not None and _current_step.get() is None:
            return ctx.step(self, *args, **kwargs)
        return self.spec.fn(*args, **kwargs)


class WorkflowFunction(Generic[P, R]):
    def __init__(self, spec: WorkflowSpec):
        self.spec = spec
        self.__name__ = getattr(spec.fn, "__name__", spec.name)
        self.__doc__ = spec.fn.__doc__
        self.__wrapped__ = spec.fn

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.spec.fn(*args, **kwargs)


class Ninelives:
    """Durable execution app.

    >>> app = Ninelives("sqlite:///ninelives.db")      # or "postgresql://..."
    >>> @app.step()
    ... async def charge(order_id: str) -> str: ...
    >>> @app.workflow()
    ... async def checkout(ctx, order_id: str):
    ...     receipt = await charge(order_id)
    ...     await ctx.sleep(3600)
    ...     return receipt
    """

    def __init__(
        self, store: Store | str = "sqlite:///ninelives.db", *, clock: Callable[[], float] = time.time
    ):
        self.store: Store = open_store(store) if isinstance(store, str) else store
        self.clock = clock
        self.workflows: dict[str, WorkflowSpec] = {}
        self.steps: dict[str, StepSpec] = {}
        self._initialized = False
        self._init_lock = asyncio.Lock()

    # ------------------------------------------------------------------ registration
    def workflow(self, fn: Callable[..., Any] | None = None, *, name: str | None = None) -> Any:
        def register(f: Callable[..., Any]) -> WorkflowFunction[Any, Any]:
            if not inspect.iscoroutinefunction(f):
                raise TypeError(
                    f"workflow {f.__name__} must be an async function taking ctx as its first argument"
                )
            wf_name = name or f.__name__
            if wf_name in self.workflows and self.workflows[wf_name].fn is not f:
                raise ValueError(f"a workflow named {wf_name!r} is already registered")
            spec = WorkflowSpec(wf_name, f)
            self.workflows[wf_name] = spec
            return WorkflowFunction(spec)

        return register(fn) if fn is not None else register

    def step(
        self,
        fn: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        retry: RetryPolicy | None = None,
        timeout: float | None = None,
    ) -> Any:
        def register(f: Callable[..., Any]) -> StepFunction[Any, Any]:
            spec = StepSpec(name or f.__name__, f, retry or RetryPolicy(), timeout)
            self.steps[spec.name] = spec
            return StepFunction(spec)

        return register(fn) if fn is not None else register

    def step_spec(self, fn: Any) -> StepSpec:
        if isinstance(fn, StepFunction):
            return fn.spec
        if isinstance(fn, StepSpec):
            return fn
        name = getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None) or repr(fn)
        return self.steps.get(name) or StepSpec(name, fn)

    def _workflow_name(self, workflow: str | WorkflowFunction[Any, Any] | Callable[..., Any]) -> str:
        if isinstance(workflow, str):
            return workflow
        if isinstance(workflow, WorkflowFunction):
            return workflow.spec.name
        for spec in self.workflows.values():
            if spec.fn is workflow:
                return spec.name
        raise UnknownWorkflow(getattr(workflow, "__name__", repr(workflow)))

    # ------------------------------------------------------------------ lifecycle
    async def init(self) -> None:
        async with self._init_lock:
            if not self._initialized:
                await self.store.init()
                self._initialized = True

    async def close(self) -> None:
        await self.store.close()
        self._initialized = False

    async def __aenter__(self) -> Ninelives:
        await self.init()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def worker(self, **options: Any) -> Any:
        from .worker import Worker

        return Worker(self, **options)

    # ------------------------------------------------------------------ client operations
    async def start(
        self,
        workflow: str | WorkflowFunction[Any, Any] | Callable[..., Any],
        *args: Any,
        id: str | None = None,
        **kwargs: Any,
    ) -> str:
        """Start a workflow and return its id. With an explicit ``id`` the call is idempotent."""
        await self.init()
        name = self._workflow_name(workflow)
        wf_id = id or f"{name}-{uuid.uuid4().hex[:12]}"
        payload = {"args": serde.to_jsonable(list(args)), "kwargs": serde.to_jsonable(kwargs)}
        await self.store.create_workflow(wf_id, name, payload, self.clock())
        return wf_id

    async def get(self, wf_id: str) -> WorkflowRecord:
        await self.init()
        rec = await self.store.get_workflow(wf_id)
        if rec is None:
            raise WorkflowNotFound(wf_id)
        return rec

    async def result(self, wf_id: str, timeout: float | None = None, poll: float = 0.05) -> Any:
        """Wait for a workflow to finish and return its output (raises WorkflowFailed)."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            rec = await self.get(wf_id)
            if rec.status == Status.SUCCEEDED:
                return rec.output
            if rec.status in (Status.FAILED, Status.CANCELLED):
                raise WorkflowFailed(wf_id, str(rec.status), rec.error)
            if deadline is not None and time.monotonic() > deadline:
                raise TimeoutError(f"workflow {wf_id} still {rec.status} after {timeout}s")
            await asyncio.sleep(poll)

    async def signal(self, wf_id: str, name: str, payload: Any = None) -> None:
        await self.init()
        await self.store.send_signal(wf_id, name, payload, self.clock())

    async def cancel(self, wf_id: str) -> WorkflowRecord:
        await self.init()
        return await self.store.request_cancel(wf_id, self.clock())

    async def fork(self, wf_id: str, from_seq: int) -> str:
        """Start a new run of the same workflow that reuses this run's journal before ``from_seq``
        and executes everything from ``from_seq`` onwards again. Useful after fixing a bug or a prompt."""
        await self.init()
        rec = await self.get(wf_id)
        entries = [e for e in await self.store.get_journal(wf_id) if e.seq < from_seq]
        unfinished = [e.seq for e in entries if e.status == EntryStatus.PENDING]
        if unfinished:
            raise ValueError(
                f"cannot fork from step {from_seq}: step(s) {unfinished} before it never finished"
            )
        new_id = f"{rec.name}-fork-{uuid.uuid4().hex[:10]}"
        copied = [replace(e, workflow_id=new_id) for e in entries]
        await self.store.create_workflow(
            new_id, rec.name, rec.input, self.clock(), copied, forked_from=wf_id, fork_seq=from_seq
        )
        return new_id

    async def describe(self, wf_id: str) -> dict[str, Any]:
        """Everything about one run: record, journal, attempts and signals."""
        rec = await self.get(wf_id)
        journal: list[JournalEntry] = await self.store.get_journal(wf_id)
        return {
            "workflow": rec.to_dict(),
            "journal": [e.to_dict() for e in journal],
            "attempts": [a.to_dict() for a in await self.store.get_attempts(wf_id)],
            "signals": [s.to_dict() for s in await self.store.get_signals(wf_id)],
        }
