"""Records shared by the engine, the stores and the API."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class Status(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def terminal(self) -> bool:
        return self in (Status.SUCCEEDED, Status.FAILED, Status.CANCELLED)


class EntryKind(StrEnum):
    STEP = "step"
    SLEEP = "sleep"
    SIGNAL = "signal"
    VALUE = "value"
    EVENT = "event"
    CHILD = "child"
    SEND = "send"
    PATCH = "patch"


class EntryStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    PENDING = "pending"


@dataclass
class WorkflowRecord:
    id: str
    name: str
    status: Status
    input: dict[str, Any]
    created_at: float
    updated_at: float
    output: Any = None
    error: dict[str, Any] | None = None
    wake_at: float | None = None
    lease_token: str | None = None
    lease_owner: str | None = None
    lease_expires_at: float | None = None
    runs: int = 0
    recoveries: int = 0
    cancel_requested: bool = False
    forked_from: str | None = None
    fork_seq: int | None = None
    #: The run that started this one with ctx.start_child, and the journal position of that call.
    parent_id: str | None = None
    parent_seq: int | None = None
    #: Higher runs first when several are waiting to be claimed.
    priority: int = 0
    #: Runs sharing a key count together against the workflow's ``key_concurrency`` limit.
    concurrency_key: str | None = None
    #: Epoch seconds after which the run fails with WorkflowTimeout.
    deadline: float | None = None
    #: The schedule that started this run, if any.
    schedule_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "status": str(self.status),
            "input": self.input,
            "output": self.output,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "wake_at": self.wake_at,
            "lease_owner": self.lease_owner,
            "lease_expires_at": self.lease_expires_at,
            "runs": self.runs,
            "recoveries": self.recoveries,
            "cancel_requested": self.cancel_requested,
            "forked_from": self.forked_from,
            "fork_seq": self.fork_seq,
            "parent_id": self.parent_id,
            "parent_seq": self.parent_seq,
            "priority": self.priority,
            "concurrency_key": self.concurrency_key,
            "deadline": self.deadline,
            "schedule_id": self.schedule_id,
        }


@dataclass
class JournalEntry:
    workflow_id: str
    seq: int
    kind: EntryKind
    name: str
    status: EntryStatus
    output: Any = None
    error: dict[str, Any] | None = None
    wake_at: float | None = None
    attempts: int = 0
    started_at: float | None = None
    completed_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "kind": str(self.kind),
            "name": self.name,
            "status": str(self.status),
            "output": self.output,
            "error": self.error,
            "wake_at": self.wake_at,
            "attempts": self.attempts,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }


@dataclass
class Attempt:
    workflow_id: str
    seq: int
    attempt: int
    worker: str
    started_at: float
    finished_at: float | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "attempt": self.attempt,
            "worker": self.worker,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
        }


@dataclass
class Signal:
    id: int
    workflow_id: str
    name: str
    payload: Any
    created_at: float
    consumed_seq: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "payload": self.payload,
            "created_at": self.created_at,
            "consumed_seq": self.consumed_seq,
        }


@dataclass(frozen=True)
class StartOptions:
    """How to start a run (``app.start(..., options=StartOptions(...))``)."""

    #: Higher runs are claimed first.
    priority: int = 0
    #: Runs sharing a key count together against the workflow's ``key_concurrency``.
    concurrency_key: str | None = None
    #: Seconds until the run fails with WorkflowTimeout (overrides the workflow's default).
    timeout: float | None = None


@dataclass
class Event:
    """A key/value pair a workflow published with ``ctx.set_event``."""

    workflow_id: str
    key: str
    value: Any
    updated_at: float

    def to_dict(self) -> dict[str, Any]:
        return {"key": self.key, "value": self.value, "updated_at": self.updated_at}


@dataclass(frozen=True)
class Limit:
    """Concurrency limits of one workflow type. ``None`` means unlimited."""

    concurrency: int | None = None
    key_concurrency: int | None = None


@dataclass(frozen=True)
class ScheduleDef:
    """A schedule as declared in code with ``app.schedule(...)``."""

    id: str
    workflow: str
    cron: str
    tz: str = "UTC"
    input: dict[str, Any] = field(default_factory=lambda: {"args": [], "kwargs": {}})
    #: "skip": do not start a tick while the previous run is unfinished. "allow": always start.
    overlap: str = "skip"


@dataclass
class Schedule:
    """A schedule as stored, with its firing state."""

    id: str
    workflow: str
    cron: str
    tz: str
    input: dict[str, Any]
    overlap: str
    enabled: bool
    next_at: float | None
    last_at: float | None
    last_run_id: str | None
    created_at: float
    updated_at: float
    seen_at: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "workflow": self.workflow,
            "cron": self.cron,
            "tz": self.tz,
            "input": self.input,
            "overlap": self.overlap,
            "enabled": self.enabled,
            "next_at": self.next_at,
            "last_at": self.last_at,
            "last_run_id": self.last_run_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "seen_at": self.seen_at,
        }


@dataclass(frozen=True)
class RetryPolicy:
    """How a step is retried. Delays grow exponentially with full jitter."""

    max_attempts: int = 3
    initial_delay: float = 0.5
    backoff: float = 2.0
    max_delay: float = 30.0
    jitter: bool = True
    #: Exception types that are never retried (in addition to ninelives.PermanentError).
    non_retryable: tuple[type[BaseException], ...] = field(default=())

    def delay(self, attempt: int, rng: random.Random | None = None) -> float:
        """Delay before attempt number ``attempt + 1`` (attempts are 1-based)."""
        base = min(self.max_delay, self.initial_delay * (self.backoff ** max(0, attempt - 1)))
        if not self.jitter:
            return base
        return (rng or random).uniform(base / 2, base)


NO_RETRY = RetryPolicy(max_attempts=1)
