"""The storage contract. Every durable fact lives behind this interface.

Times are epoch seconds passed in by the caller, so stores never read the clock themselves
(this keeps tests deterministic). Methods taking a ``token`` are *fenced*: they run in a
transaction that first checks the workflow's current lease token and raise ``LeaseLost``
if another worker has taken the workflow over.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ..types import Attempt, Event, JournalEntry, Limit, Schedule, ScheduleDef, Signal, Status, WorkflowRecord


@dataclass
class Heartbeat:
    alive: bool
    cancel_requested: bool


class Store(ABC):
    # ------------------------------------------------------------------ lifecycle
    @abstractmethod
    async def init(self) -> None:
        """Create the tables, or migrate them to the current schema version."""

    @abstractmethod
    async def close(self) -> None: ...

    # ------------------------------------------------------------------ client side
    @abstractmethod
    async def create_workflow(
        self,
        wf_id: str,
        name: str,
        input: dict[str, Any],
        now: float,
        journal: Sequence[JournalEntry] = (),
        forked_from: str | None = None,
        fork_seq: int | None = None,
        *,
        parent_id: str | None = None,
        parent_seq: int | None = None,
        priority: int = 0,
        concurrency_key: str | None = None,
        deadline: float | None = None,
        schedule_id: str | None = None,
        events: Sequence[tuple[str, Any]] = (),
    ) -> tuple[WorkflowRecord, bool]:
        """Insert a PENDING workflow (with an optional copied journal and events). If ``wf_id``
        exists, return it with ``created=False``."""

    @abstractmethod
    async def get_workflow(self, wf_id: str) -> WorkflowRecord | None: ...

    @abstractmethod
    async def list_workflows(
        self,
        status: Status | None = None,
        name: str | None = None,
        limit: int = 50,
        offset: int = 0,
        *,
        parent_id: str | None = None,
        search: str | None = None,
        schedule_id: str | None = None,
    ) -> list[WorkflowRecord]:
        """Newest first. ``search`` matches a substring of the run id."""

    @abstractmethod
    async def count_by_name_and_status(self) -> dict[tuple[str, str], int]: ...

    @abstractmethod
    async def get_events(self, wf_id: str) -> list[Event]: ...

    @abstractmethod
    async def get_event(self, wf_id: str, key: str) -> Event | None: ...

    @abstractmethod
    async def count_by_status(self) -> dict[str, int]: ...

    @abstractmethod
    async def send_signal(self, wf_id: str, name: str, payload: Any, now: float) -> Signal:
        """Store a signal and wake the workflow if it is waiting. Raises WorkflowNotFound."""

    @abstractmethod
    async def request_cancel(self, wf_id: str, now: float) -> WorkflowRecord:
        """Cancel PENDING/WAITING workflows at once and flag RUNNING ones for their worker;
        the same happens to every unfinished descendant (child workflows)."""

    @abstractmethod
    async def get_journal(self, wf_id: str) -> list[JournalEntry]: ...

    @abstractmethod
    async def get_attempts(self, wf_id: str) -> list[Attempt]: ...

    @abstractmethod
    async def get_signals(self, wf_id: str) -> list[Signal]: ...

    # ------------------------------------------------------------------ worker side
    @abstractmethod
    async def claim(
        self,
        worker_id: str,
        now: float,
        ttl: float,
        names: Sequence[str],
        limit: int,
        limits: Mapping[str, Limit] | None = None,
    ) -> list[WorkflowRecord]:
        """Atomically take up to ``limit`` runnable workflows, each with a fresh lease token.

        Runnable: PENDING; WAITING with ``wake_at <= now``; RUNNING with an expired lease. Higher
        priority first. ``limits`` caps live (RUNNING, unexpired lease) runs per workflow name and
        per concurrency key.
        """

    @abstractmethod
    async def heartbeat(self, wf_id: str, token: str, now: float, ttl: float) -> Heartbeat: ...

    @abstractmethod
    async def write_entry(self, token: str, entry: JournalEntry) -> None:
        """Insert or replace the journal entry at ``(workflow_id, seq)``. Fenced."""

    @abstractmethod
    async def start_attempt(self, token: str, wf_id: str, seq: int, worker: str, now: float) -> int:
        """Record that a step attempt began; returns its 1-based number (counts survive crashes). Fenced."""

    @abstractmethod
    async def finish_attempt(
        self, token: str, wf_id: str, seq: int, attempt: int, now: float, error: str | None
    ) -> None: ...

    @abstractmethod
    async def complete_step(
        self, token: str, entry: JournalEntry, attempt: int, now: float, attempt_error: str | None = None
    ) -> None:
        """Finish a step attempt and journal the step's outcome in one transaction. Fenced."""

    @abstractmethod
    async def set_event(self, token: str, entry: JournalEntry, key: str, value: Any, now: float) -> None:
        """Publish (or overwrite) an event and journal it in one transaction. Fenced."""

    @abstractmethod
    async def start_child(
        self,
        token: str,
        entry: JournalEntry,
        child_id: str,
        name: str,
        input: dict[str, Any],
        now: float,
        *,
        priority: int = 0,
        concurrency_key: str | None = None,
        deadline: float | None = None,
    ) -> tuple[WorkflowRecord, bool]:
        """Create a child run and journal its start in one transaction. Fenced on the parent."""

    @abstractmethod
    async def send_from(
        self, token: str, entry: JournalEntry, target_id: str, name: str, payload: Any, now: float
    ) -> Signal:
        """Signal another run and journal the send in one transaction (never duplicated). Fenced."""

    @abstractmethod
    async def consume_signal(self, token: str, entry: JournalEntry, signal_name: str) -> Signal | None:
        """Take the oldest unconsumed signal named ``signal_name`` and journal it as ``entry``
        (status completed, output = payload) in one transaction. Returns None if there is none. Fenced."""

    @abstractmethod
    async def finish(
        self, token: str, wf_id: str, status: Status, output: Any, error: dict[str, Any] | None, now: float
    ) -> None:
        """Move to a terminal status and clear the lease. Fenced.

        A child's outcome is delivered to its parent as the signal ``child:<id>``; a cancelled run
        cancels its unfinished children."""

    @abstractmethod
    async def suspend(
        self, token: str, wf_id: str, wake_at: float | None, waiting_for: Sequence[str], now: float
    ) -> Status:
        """Move to WAITING and clear the lease. Fenced.

        If an unconsumed signal named in ``waiting_for`` already exists, ``wake_at`` becomes ``now``
        so a signal sent while the workflow was running is never lost. If cancellation was
        requested, the workflow becomes CANCELLED instead. Returns the resulting status.
        """

    @abstractmethod
    async def release(self, token: str, wf_id: str) -> None:
        """Give up a lease early (graceful shutdown) so another worker can claim at once."""

    async def wait_for_work(self, timeout: float) -> None:
        """Return when new work may be available, or after ``timeout`` seconds."""
        import asyncio

        await asyncio.sleep(timeout)

    # ------------------------------------------------------------------ schedules and retention
    @abstractmethod
    async def upsert_schedules(self, defs: Sequence[ScheduleDef], now: float) -> None:
        """Insert new schedules and update changed ones (the code is the source of truth)."""

    @abstractmethod
    async def fire_schedules(
        self, now: float, names: Sequence[str], limit: int = 20, ids: Sequence[str] | None = None
    ) -> list[str]:
        """Start runs for due schedules (exactly one starter per tick); returns the run ids.
        Only schedules of the workflows in ``names`` fire, and only those in ``ids`` if given."""

    @abstractmethod
    async def list_schedules(self) -> list[Schedule]: ...

    @abstractmethod
    async def get_schedule(self, schedule_id: str) -> Schedule | None: ...

    @abstractmethod
    async def set_schedule_enabled(self, schedule_id: str, enabled: bool, now: float) -> Schedule: ...

    @abstractmethod
    async def trigger_schedule(self, schedule_id: str, now: float) -> str: ...

    @abstractmethod
    async def delete_schedule(self, schedule_id: str) -> bool: ...

    @abstractmethod
    async def purge(self, before: float, batch: int = 500) -> int:
        """Delete finished runs last updated before ``before``; returns how many."""

    @abstractmethod
    async def schema_version(self) -> int: ...

    def fresh(self) -> Store:
        """A store for the same database that is safe to use from another event loop."""
        return self
