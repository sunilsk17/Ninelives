"""Deterministic replay inside a running workflow.

Each durable operation takes the next sequence number. If the journal already holds an entry
for that number, its recorded result is returned without doing the work again; otherwise the
operation runs and its result is journaled before the workflow moves on.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import random as _random
import uuid as _uuid
import weakref
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from . import serde
from .errors import (
    Cancelled,
    LeaseLost,
    NonDeterminismError,
    PermanentError,
    SerializationError,
    SignalTimeout,
    StepFailed,
)
from .types import EntryKind, EntryStatus, JournalEntry, RetryPolicy, WorkflowRecord

if TYPE_CHECKING:
    from .app import Ninelives, StepSpec

log = logging.getLogger("ninelives")

#: Sleeps shorter than this run in-process instead of suspending the workflow.
INLINE_SLEEP_SECONDS = 1.0


class _Suspend(BaseException):
    """Internal: stop executing and park the workflow until ``wake_at`` or a signal.

    A BaseException so that ``except Exception`` in user code cannot swallow it.
    """

    def __init__(self, wake_at: float | None, waiting_for: tuple[str, ...]):
        super().__init__(wake_at, waiting_for)
        self.wake_at = wake_at
        self.waiting_for = waiting_for


class _Abandon(BaseException):
    """Internal: this worker lost its lease; stop without writing anything."""


@dataclass(frozen=True)
class StepInfo:
    workflow_id: str
    seq: int
    name: str
    attempt: int

    @property
    def idempotency_key(self) -> str:
        """Stable across retries and crash recoveries: pass it to the systems your step calls."""
        return f"{self.workflow_id}:{self.seq}"


class _Group:
    """The branches of one ``ctx.gather`` call.

    Sequence numbers are handed out round-robin over the branches that have not finished, so the
    numbering is a function of the code and the journal only, never of I/O timing. A branch that
    parks (on a sleep or a signal) keeps its place in the rotation; when the turn reaches it, the
    other branches wait, and once nobody can move the whole workflow suspends.
    """

    def __init__(self, size: int):
        self.active = list(range(size))
        self.pos = 0
        self.parked: set[int] = set()
        self.waiting: set[int] = set()
        self.changed = asyncio.Condition()

    def turn(self) -> int | None:
        return self.active[self.pos] if self.active else None

    def advance(self, index: int) -> None:
        self.pos = (self.active.index(index) + 1) % len(self.active)

    def finish(self, index: int) -> None:
        i = self.active.index(index)
        self.active.pop(i)
        self.waiting.discard(index)
        if self.active:
            if i < self.pos:
                self.pos -= 1
            self.pos %= len(self.active)

    def stalled(self) -> bool:
        """The turn sits with a parked branch and every other branch is parked or waiting."""
        return self.turn() in self.parked and all(i in self.parked or i in self.waiting for i in self.active)


class _Stalled(BaseException):
    """Internal: a branch cannot get its turn this execution because a sibling is parked."""


@dataclass(frozen=True)
class _Branch:
    ctx: WorkflowContext
    group: _Group
    index: int


_current_ctx: ContextVar[WorkflowContext | None] = ContextVar("ninelives_ctx", default=None)
_current_step: ContextVar[StepInfo | None] = ContextVar("ninelives_step", default=None)
_current_branch: ContextVar[_Branch | None] = ContextVar("ninelives_branch", default=None)


def step_info() -> StepInfo:
    """Information about the step currently executing (inside a step function only)."""
    info = _current_step.get()
    if info is None:
        raise RuntimeError("step_info() is only available inside a step")
    return info


def current_context() -> WorkflowContext:
    ctx = _current_ctx.get()
    if ctx is None:
        raise RuntimeError("not inside a workflow")
    return ctx


class WorkflowContext:
    """Passed as the first argument to every workflow function."""

    def __init__(
        self,
        app: Ninelives,
        record: WorkflowRecord,
        token: str,
        worker_id: str,
        journal: dict[int, JournalEntry],
    ):
        self._app = app
        self._store = app.store
        self._clock = app.clock
        self.record = record
        self.workflow_id = record.id
        self.name = record.name
        self._token = token
        self._worker_id = worker_id
        self._journal = journal
        self._seq = 0
        self.cancel_requested = record.cancel_requested
        self._main_task = asyncio.current_task()
        self._raw_tasks: weakref.WeakSet[asyncio.Task[Any]] = weakref.WeakSet()

    # ------------------------------------------------------------------ helpers
    @property
    def replaying(self) -> bool:
        """True while the next operation will be answered from the journal."""
        return (self._seq + 1) in self._journal

    async def _next(self) -> int:
        """Allocate the sequence number of the next durable operation."""
        branch = _current_branch.get()
        if branch is not None and branch.ctx is self:
            # Inside ctx.gather: wait for this branch's turn, take a number, pass the turn on.
            group, index = branch.group, branch.index
            async with group.changed:
                group.waiting.add(index)
                group.changed.notify_all()
                try:
                    await group.changed.wait_for(lambda: group.turn() == index or group.stalled())
                finally:
                    group.waiting.discard(index)
                if group.turn() != index:
                    raise _Stalled()
                self._seq += 1
                group.advance(index)
                group.changed.notify_all()
                return self._seq
        task = asyncio.current_task()
        if task is not None and self._main_task is not None and task is not self._main_task:
            # A raw asyncio task (asyncio.gather / create_task). One durable call per task is
            # deterministic (it is numbered when the task starts); more is not.
            if task in self._raw_tasks:
                raise NonDeterminismError(
                    f"workflow {self.workflow_id}: a concurrent branch made more than one durable call. "
                    "Their order would depend on timing; run such branches with ctx.gather(...) instead "
                    "of asyncio.gather(...)."
                )
            self._raw_tasks.add(task)
        self._seq += 1
        return self._seq

    async def gather(self, *aws: Awaitable[Any]) -> list[Any]:
        """Run branches concurrently and return their results in order, like ``asyncio.gather``.

        Unlike ``asyncio.gather`` it is replay-safe for branches that make several durable calls:
        sequence numbers are handed out round-robin across the branches. Every branch runs until
        it finishes or fails; if one fails, the first failure (in argument order) is raised once
        the others have settled.

        A branch that parks on a long sleep or a signal keeps its place in the rotation, so its
        siblings pause at their next durable call until it resumes, and the workflow suspends
        until the earliest wake-up. Branches cannot themselves call ``ctx.gather``.
        """
        if not aws:
            return []
        current = _current_branch.get()
        if current is not None and current.ctx is self:
            for aw in aws:
                if inspect.iscoroutine(aw):
                    aw.close()
            raise RuntimeError("ctx.gather cannot be nested inside another ctx.gather branch")
        group = _Group(len(aws))

        async def run(index: int, aw: Awaitable[Any]) -> Any:
            _current_branch.set(_Branch(self, group, index))
            try:
                result = await aw
            except _Suspend:
                async with group.changed:
                    group.parked.add(index)
                    group.changed.notify_all()
                raise
            except BaseException:
                async with group.changed:
                    if index not in group.parked:
                        group.finish(index)
                    group.changed.notify_all()
                raise
            async with group.changed:
                group.finish(index)
                group.changed.notify_all()
            return result

        tasks = [asyncio.ensure_future(run(i, aw)) for i, aw in enumerate(aws)]
        try:
            await asyncio.wait(tasks)
        except BaseException:  # this execution is being torn down (lease lost or shutdown)
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        if any(t.cancelled() for t in tasks):
            raise asyncio.CancelledError()
        errors = [e for e in (t.exception() for t in tasks) if e is not None]
        for err in errors:
            if isinstance(err, _Abandon):
                raise err
        for err in errors:
            if not isinstance(err, (_Suspend, _Stalled)):
                raise err  # type: ignore[misc]
        parked = [e for e in errors if isinstance(e, _Suspend)]
        if parked:
            wakes = [e.wake_at for e in parked if e.wake_at is not None]
            names = tuple(dict.fromkeys(n for e in parked for n in e.waiting_for))
            raise _Suspend(min(wakes) if wakes else None, names)
        return [t.result() for t in tasks]

    def _expect(self, seq: int, kind: EntryKind, name: str) -> JournalEntry | None:
        entry = self._journal.get(seq)
        if entry is not None and (entry.kind != kind or entry.name != name):
            raise NonDeterminismError(
                f"workflow {self.workflow_id} at step {seq}: journal has {entry.kind}:{entry.name!r} "
                f"but the code now does {kind}:{name!r}. Workflow code must not change while runs are in flight."
            )
        return entry

    def _check_cancel(self) -> None:
        if self.cancel_requested:
            raise Cancelled(f"workflow {self.workflow_id} was cancelled")

    async def _write(self, entry: JournalEntry) -> None:
        try:
            await self._store.write_entry(self._token, entry)
        except LeaseLost as exc:
            raise _Abandon() from exc
        self._journal[entry.seq] = entry

    async def _guard(self, coro: Awaitable[Any]) -> Any:
        try:
            return await coro
        except LeaseLost as exc:
            raise _Abandon() from exc

    # ------------------------------------------------------------------ steps
    async def step(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run ``fn`` as a durable step (registered with ``@app.step`` or any callable)."""
        spec = self._app.step_spec(fn)
        self._check_cancel()
        seq = await self._next()
        entry = self._expect(seq, EntryKind.STEP, spec.name)
        if entry is not None:
            if entry.status == EntryStatus.COMPLETED:
                return entry.output
            if entry.status == EntryStatus.FAILED:
                raise StepFailed(spec.name, entry.error or {})
        return await self._run_step(seq, spec, args, kwargs)

    async def _run_step(self, seq: int, spec: StepSpec, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        policy: RetryPolicy = spec.retry
        started = self._clock()
        while True:
            attempt = await self._guard(
                self._store.start_attempt(self._token, self.workflow_id, seq, self._worker_id, self._clock())
            )
            if attempt > policy.max_attempts:
                # Earlier attempts were lost to crashes; the budget is spent.
                error = {
                    "type": "RetriesExhausted",
                    "message": f"{attempt - 1} attempts used (max {policy.max_attempts})",
                }
                await self._guard(
                    self._store.finish_attempt(
                        self._token, self.workflow_id, seq, attempt, self._clock(), "budget exhausted"
                    )
                )
                await self._write(self._failed_entry(seq, spec.name, error, attempt - 1, started))
                raise StepFailed(spec.name, error)
            token = _current_step.set(StepInfo(self.workflow_id, seq, spec.name, attempt))
            failure: Exception | None = None
            try:
                result = await self._invoke(spec, args, kwargs)
            except (asyncio.CancelledError, _Abandon, _Suspend):
                raise
            except Exception as exc:
                failure = exc
            finally:
                _current_step.reset(token)
            if failure is not None:
                await self._guard(
                    self._store.finish_attempt(
                        self._token,
                        self.workflow_id,
                        seq,
                        attempt,
                        self._clock(),
                        f"{type(failure).__name__}: {failure}",
                    )
                )
                permanent = isinstance(failure, (PermanentError, *policy.non_retryable))
                if permanent or attempt >= policy.max_attempts:
                    error = serde.error_dict(failure)
                    await self._write(self._failed_entry(seq, spec.name, error, attempt, started))
                    raise StepFailed(spec.name, error) from failure
                delay = policy.delay(attempt)
                log.info(
                    "step %s of %s failed (attempt %d), retrying in %.2fs",
                    spec.name,
                    self.workflow_id,
                    attempt,
                    delay,
                )
                await asyncio.sleep(delay)
                self._check_cancel()
                continue
            try:
                value = serde.loads(serde.dumps(result))  # the same value replay will return
            except SerializationError as exc:
                # A programming error, not a transient one: record the attempt and fail the run.
                await self._guard(
                    self._store.finish_attempt(
                        self._token,
                        self.workflow_id,
                        seq,
                        attempt,
                        self._clock(),
                        f"SerializationError: {exc}",
                    )
                )
                raise
            await self._guard(
                self._store.finish_attempt(self._token, self.workflow_id, seq, attempt, self._clock(), None)
            )
            await self._write(
                JournalEntry(
                    workflow_id=self.workflow_id,
                    seq=seq,
                    kind=EntryKind.STEP,
                    name=spec.name,
                    status=EntryStatus.COMPLETED,
                    output=value,
                    attempts=attempt,
                    started_at=started,
                    completed_at=self._clock(),
                )
            )
            return value

    async def _invoke(self, spec: StepSpec, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        fn = spec.fn
        if inspect.iscoroutinefunction(fn):
            call: Awaitable[Any] = fn(*args, **kwargs)
        else:
            call = asyncio.to_thread(fn, *args, **kwargs)
        if spec.timeout is not None:
            return await asyncio.wait_for(call, spec.timeout)
        return await call

    def _failed_entry(
        self, seq: int, name: str, error: dict[str, Any], attempts: int, started: float
    ) -> JournalEntry:
        return JournalEntry(
            workflow_id=self.workflow_id,
            seq=seq,
            kind=EntryKind.STEP,
            name=name,
            status=EntryStatus.FAILED,
            error=error,
            attempts=attempts,
            started_at=started,
            completed_at=self._clock(),
        )

    # ------------------------------------------------------------------ time
    async def sleep(self, seconds: float) -> None:
        """Durable sleep: survives restarts; long sleeps free the worker entirely."""
        self._check_cancel()
        seq = await self._next()
        entry = self._expect(seq, EntryKind.SLEEP, "sleep")
        now = self._clock()
        if entry is None:
            entry = JournalEntry(
                workflow_id=self.workflow_id,
                seq=seq,
                kind=EntryKind.SLEEP,
                name="sleep",
                status=EntryStatus.PENDING,
                wake_at=now + max(0.0, seconds),
                started_at=now,
            )
            await self._write(entry)
        if entry.status == EntryStatus.COMPLETED:
            return
        assert entry.wake_at is not None
        remaining = entry.wake_at - now
        if remaining > INLINE_SLEEP_SECONDS:
            raise _Suspend(entry.wake_at, ())
        if remaining > 0:
            await asyncio.sleep(remaining)
        await self._write(replace(entry, status=EntryStatus.COMPLETED, completed_at=self._clock()))

    async def wait_for_signal(self, name: str, timeout: float | None = None) -> Any:
        """Wait until ``send_signal(workflow_id, name, payload)`` arrives; returns the payload.

        Signals sent before the wait are kept and delivered in order. Raises SignalTimeout.
        """
        self._check_cancel()
        seq = await self._next()
        entry = self._expect(seq, EntryKind.SIGNAL, name)
        now = self._clock()
        if entry is None:
            entry = JournalEntry(
                workflow_id=self.workflow_id,
                seq=seq,
                kind=EntryKind.SIGNAL,
                name=name,
                status=EntryStatus.PENDING,
                wake_at=(now + timeout) if timeout is not None else None,
                started_at=now,
            )
            await self._write(entry)
        if entry.status == EntryStatus.COMPLETED:
            out = entry.output or {}
            if out.get("timeout"):
                raise SignalTimeout(f"no {name!r} signal within the timeout")
            return out.get("payload")
        signal = await self._guard(
            self._store.consume_signal(self._token, replace(entry, completed_at=now), name)
        )
        if signal is not None:
            self._journal[seq] = replace(
                entry, status=EntryStatus.COMPLETED, output={"payload": signal.payload}, completed_at=now
            )
            return signal.payload
        if entry.wake_at is not None and now >= entry.wake_at:
            await self._write(
                replace(entry, status=EntryStatus.COMPLETED, output={"timeout": True}, completed_at=now)
            )
            raise SignalTimeout(f"no {name!r} signal within the timeout")
        raise _Suspend(entry.wake_at, (name,))

    # ------------------------------------------------------------------ deterministic values
    async def _value(self, name: str, make: Callable[[], Any]) -> Any:
        self._check_cancel()
        seq = await self._next()
        entry = self._expect(seq, EntryKind.VALUE, name)
        if entry is not None:
            return entry.output
        value = serde.loads(serde.dumps(make()))
        now = self._clock()
        await self._write(
            JournalEntry(
                workflow_id=self.workflow_id,
                seq=seq,
                kind=EntryKind.VALUE,
                name=name,
                status=EntryStatus.COMPLETED,
                output=value,
                started_at=now,
                completed_at=now,
            )
        )
        return value

    async def now(self) -> float:
        """Current time, recorded once so replays see the same value."""
        return float(await self._value("now", self._clock))

    async def uuid(self) -> str:
        return str(await self._value("uuid", lambda: str(_uuid.uuid4())))

    async def random(self) -> float:
        return float(await self._value("random", _random.random))
