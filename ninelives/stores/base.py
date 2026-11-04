"""The storage contract. Every durable fact lives behind this interface.

Times are epoch seconds passed in by the caller, so stores never read the clock themselves
(this keeps tests deterministic). Methods taking a ``token`` are *fenced*: they run in a
transaction that first checks the workflow's current lease token and raise ``LeaseLost``
if another worker has taken the workflow over.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from ..types import Attempt, JournalEntry, Signal, Status, WorkflowRecord


@dataclass
class Heartbeat:
    alive: bool
    cancel_requested: bool


class Store(ABC):
    # ------------------------------------------------------------------ lifecycle
    @abstractmethod
    async def init(self) -> None:
        """Create tables if needed."""

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
    ) -> tuple[WorkflowRecord, bool]:
        """Insert a PENDING workflow. If ``wf_id`` exists, return it with ``created=False``."""

    @abstractmethod
    async def get_workflow(self, wf_id: str) -> WorkflowRecord | None: ...

    @abstractmethod
    async def list_workflows(
        self, status: Status | None = None, name: str | None = None, limit: int = 50, offset: int = 0
    ) -> list[WorkflowRecord]: ...

    @abstractmethod
    async def count_by_status(self) -> dict[str, int]: ...

    @abstractmethod
    async def send_signal(self, wf_id: str, name: str, payload: Any, now: float) -> Signal:
        """Store a signal and wake the workflow if it is waiting. Raises WorkflowNotFound."""

    @abstractmethod
    async def request_cancel(self, wf_id: str, now: float) -> WorkflowRecord:
        """Cancel PENDING/WAITING workflows at once; flag RUNNING ones for the worker."""

    @abstractmethod
    async def get_journal(self, wf_id: str) -> list[JournalEntry]: ...

    @abstractmethod
    async def get_attempts(self, wf_id: str) -> list[Attempt]: ...

    @abstractmethod
    async def get_signals(self, wf_id: str) -> list[Signal]: ...

    # ------------------------------------------------------------------ worker side
    @abstractmethod
    async def claim(
        self, worker_id: str, now: float, ttl: float, names: Sequence[str], limit: int
    ) -> list[WorkflowRecord]:
        """Atomically take up to ``limit`` runnable workflows, each with a fresh lease token.

        Runnable: PENDING; WAITING with ``wake_at <= now``; RUNNING with an expired lease.
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
    async def consume_signal(self, token: str, entry: JournalEntry, signal_name: str) -> Signal | None:
        """Take the oldest unconsumed signal named ``signal_name`` and journal it as ``entry``
        (status completed, output = payload) in one transaction. Returns None if there is none. Fenced."""

    @abstractmethod
    async def finish(
        self, token: str, wf_id: str, status: Status, output: Any, error: dict[str, Any] | None, now: float
    ) -> None:
        """Move to a terminal status and clear the lease. Fenced."""

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
