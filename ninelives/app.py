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
from .cron import Cron, validate_timezone
from .errors import UnknownWorkflow, WorkflowFailed, WorkflowNotFound
from .stores import Store, open_store
from .types import (
    EntryKind,
    EntryStatus,
    JournalEntry,
    Limit,
    RetryPolicy,
    Schedule,
    ScheduleDef,
    StartOptions,
    Status,
    WorkflowRecord,
)

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
    #: Default seconds until a run fails with WorkflowTimeout.
    timeout: float | None = None
    #: At most this many runs of this workflow execute at once (waiting runs do not count).
    concurrency: int | None = None
    #: At most this many runs per ``concurrency_key`` execute at once.
    key_concurrency: int | None = None


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
        #: Schedules declared with ``app.schedule``; workers register them in the store.
        self.schedules: dict[str, ScheduleDef] = {}
        self._initialized = False
        self._init_lock = asyncio.Lock()

    # ------------------------------------------------------------------ registration
    def workflow(
        self,
        fn: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        timeout: float | None = None,
        concurrency: int | None = None,
        key_concurrency: int | None = None,
    ) -> Any:
        """Register an ``async def workflow(ctx, ...)``.

        ``timeout``: default seconds until a run fails with WorkflowTimeout. ``concurrency``: at most
        this many runs execute at once across all workers. ``key_concurrency``: at most this many
        per ``StartOptions.concurrency_key`` (for example one per customer).
        """
        for label, value in (("concurrency", concurrency), ("key_concurrency", key_concurrency)):
            if value is not None and value < 1:
                raise ValueError(f"{label} must be at least 1")

        def register(f: Callable[..., Any]) -> WorkflowFunction[Any, Any]:
            if not inspect.iscoroutinefunction(f):
                raise TypeError(
                    f"workflow {f.__name__} must be an async function taking ctx as its first argument"
                )
            wf_name = name or f.__name__
            if wf_name in self.workflows and self.workflows[wf_name].fn is not f:
                raise ValueError(f"a workflow named {wf_name!r} is already registered")
            spec = WorkflowSpec(wf_name, f, timeout, concurrency, key_concurrency)
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

    def schedule(
        self,
        schedule_id: str,
        workflow: str | WorkflowFunction[Any, Any] | Callable[..., Any],
        cron: str,
        *,
        args: Any = (),
        kwargs: dict[str, Any] | None = None,
        tz: str = "UTC",
        overlap: str = "skip",
    ) -> ScheduleDef:
        """Start ``workflow`` on a cron schedule, e.g. ``app.schedule("nightly", report, "0 2 * * *")``.

        Exactly one run starts per tick, however many workers run. ``overlap="skip"`` (default) skips
        a tick while the previous run is unfinished; ``"allow"`` always starts. After downtime, one
        run starts for the most recent missed tick. Run ids are ``<schedule_id>@<tick time>``.
        """
        Cron.parse(cron)
        validate_timezone(tz)
        if overlap not in ("skip", "allow"):
            raise ValueError("overlap must be 'skip' or 'allow'")
        if "@" in schedule_id:
            raise ValueError("schedule ids cannot contain '@'")
        definition = ScheduleDef(
            id=schedule_id,
            workflow=self._workflow_name(workflow),
            cron=cron,
            tz=tz,
            input={"args": serde.to_jsonable(list(args)), "kwargs": serde.to_jsonable(kwargs or {})},
            overlap=overlap,
        )
        self.schedules[schedule_id] = definition
        return definition

    def limits(self) -> dict[str, Limit]:
        """Concurrency limits of the registered workflows."""
        return {
            name: Limit(spec.concurrency, spec.key_concurrency)
            for name, spec in self.workflows.items()
            if spec.concurrency is not None or spec.key_concurrency is not None
        }

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
        options: StartOptions | None = None,
        **kwargs: Any,
    ) -> str:
        """Start a workflow and return its id. With an explicit ``id`` the call is idempotent.

        ``options`` sets priority, a concurrency key and a timeout; every other keyword argument
        goes to the workflow.
        """
        await self.init()
        name = self._workflow_name(workflow)
        wf_id = id or f"{name}-{uuid.uuid4().hex[:12]}"
        payload = {"args": serde.to_jsonable(list(args)), "kwargs": serde.to_jsonable(kwargs)}
        opts = options or StartOptions()
        spec = self.workflows.get(name)
        timeout = opts.timeout if opts.timeout is not None else (spec.timeout if spec else None)
        now = self.clock()
        await self.store.create_workflow(
            wf_id,
            name,
            payload,
            now,
            priority=opts.priority,
            concurrency_key=opts.concurrency_key,
            deadline=now + timeout if timeout is not None else None,
        )
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
        # A child started before the fork point but awaited after it reports to the original run
        # only. If it has finished, hand its outcome to the fork; if not, the fork cannot proceed.
        awaited = {e.name for e in entries if e.kind == EntryKind.SIGNAL}
        handover: list[tuple[str, dict[str, Any]]] = []
        for e in entries:
            if e.kind == EntryKind.CHILD:
                child_id = e.output["id"]
                if f"child:{child_id}" in awaited:
                    continue
                child = await self.get(child_id)
                if not child.status.terminal:
                    raise ValueError(
                        f"cannot fork from step {from_seq}: child {child_id} (step {e.seq}) has not finished"
                    )
                outcome = {"status": str(child.status), "output": child.output, "error": child.error}
                handover.append((f"child:{child_id}", outcome))
        new_id = f"{rec.name}-fork-{uuid.uuid4().hex[:10]}"
        copied = [replace(e, workflow_id=new_id) for e in entries]
        events = [(e.name.removeprefix("event:"), e.output) for e in entries if e.kind == EntryKind.EVENT]
        now = self.clock()
        await self.store.create_workflow(
            new_id,
            rec.name,
            rec.input,
            now,
            copied,
            forked_from=wf_id,
            fork_seq=from_seq,
            priority=rec.priority,
            concurrency_key=rec.concurrency_key,
            events=events,
        )
        for name, outcome in handover:
            await self.store.send_signal(new_id, name, outcome, now)
        return new_id

    # ------------------------------------------------------------------ events, children, schedules
    async def get_event(self, wf_id: str, key: str, timeout: float | None = None, poll: float = 0.1) -> Any:
        """The value a run published with ``ctx.set_event(key, ...)``, waiting up to ``timeout``
        seconds for it to appear (``timeout=None``: do not wait). Raises KeyError if absent."""
        await self.init()
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            event = await self.store.get_event(wf_id, key)
            if event is not None:
                return event.value
            if deadline is None or time.monotonic() > deadline:
                if await self.store.get_workflow(wf_id) is None:
                    raise WorkflowNotFound(wf_id)
                raise KeyError(f"run {wf_id} has not published {key!r}")
            await asyncio.sleep(poll)

    async def get_events(self, wf_id: str) -> dict[str, Any]:
        await self.init()
        return {e.key: e.value for e in await self.store.get_events(wf_id)}

    async def children(self, wf_id: str) -> list[WorkflowRecord]:
        await self.init()
        return await self.store.list_workflows(parent_id=wf_id, limit=1000)

    async def list_schedules(self) -> list[Schedule]:
        await self.init()
        return await self.store.list_schedules()

    async def pause_schedule(self, schedule_id: str) -> Schedule:
        await self.init()
        return await self.store.set_schedule_enabled(schedule_id, False, self.clock())

    async def resume_schedule(self, schedule_id: str) -> Schedule:
        await self.init()
        return await self.store.set_schedule_enabled(schedule_id, True, self.clock())

    async def trigger_schedule(self, schedule_id: str) -> str:
        """Start a run of the schedule's workflow now, outside its cron times."""
        await self.init()
        return await self.store.trigger_schedule(schedule_id, self.clock())

    async def purge(self, older_than: float) -> int:
        """Delete finished runs last updated more than ``older_than`` seconds ago; returns how many."""
        await self.init()
        return await self.store.purge(self.clock() - older_than)

    async def describe(self, wf_id: str) -> dict[str, Any]:
        """Everything about one run: record, journal, attempts and signals."""
        rec = await self.get(wf_id)
        journal: list[JournalEntry] = await self.store.get_journal(wf_id)
        children = await self.store.list_workflows(parent_id=wf_id, limit=1000)
        return {
            "workflow": rec.to_dict(),
            "journal": [e.to_dict() for e in journal],
            "attempts": [a.to_dict() for a in await self.store.get_attempts(wf_id)],
            "signals": [s.to_dict() for s in await self.store.get_signals(wf_id)],
            "events": [e.to_dict() for e in await self.store.get_events(wf_id)],
            "children": [
                {"id": c.id, "name": c.name, "status": str(c.status), "parent_seq": c.parent_seq}
                for c in sorted(children, key=lambda c: (c.parent_seq or 0, c.id))
            ],
        }
