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
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from . import serde
from .errors import Cancelled, LeaseLost, NonDeterminismError, PermanentError, SignalTimeout, StepFailed
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


_current_ctx: ContextVar[WorkflowContext | None] = ContextVar("ninelives_ctx", default=None)
_current_step: ContextVar[StepInfo | None] = ContextVar("ninelives_step", default=None)


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

    # ------------------------------------------------------------------ helpers
    @property
    def replaying(self) -> bool:
        """True while the next operation will be answered from the journal."""
        return (self._seq + 1) in self._journal

    def _next(self) -> int:
        self._seq += 1
        return self._seq

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
        seq = self._next()
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
            attempt = await self._guard(self._store.start_attempt(self._token, self.workflow_id, seq, self._worker_id, self._clock()))
            if attempt > policy.max_attempts:
                # Earlier attempts were lost to crashes; the budget is spent.
                error = {"type": "RetriesExhausted", "message": f"{attempt - 1} attempts used (max {policy.max_attempts})"}
                await self._guard(self._store.finish_attempt(self._token, self.workflow_id, seq, attempt, self._clock(), "budget exhausted"))
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
                exc = failure
                await self._guard(
                    self._store.finish_attempt(self._token, self.workflow_id, seq, attempt, self._clock(), f"{type(exc).__name__}: {exc}")
                )
                permanent = isinstance(exc, PermanentError) or isinstance(exc, policy.non_retryable)
                if permanent or attempt >= policy.max_attempts:
                    error = serde.error_dict(exc)
                    await self._write(self._failed_entry(seq, spec.name, error, attempt, started))
                    raise StepFailed(spec.name, error) from exc
                delay = policy.delay(attempt)
                log.info("step %s of %s failed (attempt %d), retrying in %.2fs", spec.name, self.workflow_id, attempt, delay)
                await asyncio.sleep(delay)
                self._check_cancel()
                continue
            value = serde.loads(serde.dumps(result))  # the same value replay will return
            await self._guard(self._store.finish_attempt(self._token, self.workflow_id, seq, attempt, self._clock(), None))
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

    def _failed_entry(self, seq: int, name: str, error: dict[str, Any], attempts: int, started: float) -> JournalEntry:
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
        seq = self._next()
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
        seq = self._next()
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
        signal = await self._guard(self._store.consume_signal(self._token, replace(entry, completed_at=now), name))
        if signal is not None:
            self._journal[seq] = replace(entry, status=EntryStatus.COMPLETED, output={"payload": signal.payload}, completed_at=now)
            return signal.payload
        if entry.wake_at is not None and now >= entry.wake_at:
            await self._write(replace(entry, status=EntryStatus.COMPLETED, output={"timeout": True}, completed_at=now))
            raise SignalTimeout(f"no {name!r} signal within the timeout")
        raise _Suspend(entry.wake_at, (name,))

    # ------------------------------------------------------------------ deterministic values
    async def _value(self, name: str, make: Callable[[], Any]) -> Any:
        self._check_cancel()
        seq = self._next()
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
