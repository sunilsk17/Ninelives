"""SQLite store: zero-dependency, safe across processes on one machine (WAL + BEGIN IMMEDIATE)."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import uuid
from collections.abc import Callable, Sequence
from typing import Any, TypeVar

from .. import serde
from ..errors import LeaseLost, WorkflowNotFound
from ..types import Attempt, EntryKind, EntryStatus, JournalEntry, Signal, Status, WorkflowRecord
from .base import Heartbeat, Store

T = TypeVar("T")

SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL,
  input TEXT NOT NULL,
  output TEXT,
  error TEXT,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  wake_at REAL,
  lease_token TEXT,
  lease_owner TEXT,
  lease_expires_at REAL,
  runs INTEGER NOT NULL DEFAULT 0,
  recoveries INTEGER NOT NULL DEFAULT 0,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  forked_from TEXT,
  fork_seq INTEGER
);
CREATE INDEX IF NOT EXISTS workflows_status_wake ON workflows (status, wake_at);
CREATE INDEX IF NOT EXISTS workflows_created ON workflows (created_at);
CREATE TABLE IF NOT EXISTS journal (
  workflow_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  kind TEXT NOT NULL,
  name TEXT NOT NULL,
  status TEXT NOT NULL,
  output TEXT,
  error TEXT,
  wake_at REAL,
  attempts INTEGER NOT NULL DEFAULT 0,
  started_at REAL,
  completed_at REAL,
  PRIMARY KEY (workflow_id, seq)
);
CREATE TABLE IF NOT EXISTS attempts (
  workflow_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  worker TEXT NOT NULL,
  started_at REAL NOT NULL,
  finished_at REAL,
  error TEXT,
  PRIMARY KEY (workflow_id, seq, attempt)
);
CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  workflow_id TEXT NOT NULL,
  name TEXT NOT NULL,
  payload TEXT,
  created_at REAL NOT NULL,
  consumed_seq INTEGER
);
CREATE INDEX IF NOT EXISTS signals_lookup ON signals (workflow_id, name, consumed_seq);
"""

WF_COLS = (
    "id, name, status, input, output, error, created_at, updated_at, wake_at, lease_token, lease_owner, "
    "lease_expires_at, runs, recoveries, cancel_requested, forked_from, fork_seq"
)


def _wf(row: sqlite3.Row | tuple[Any, ...]) -> WorkflowRecord:
    return WorkflowRecord(
        id=row[0],
        name=row[1],
        status=Status(row[2]),
        input=serde.loads(row[3]),
        output=serde.loads(row[4]),
        error=serde.loads(row[5]),
        created_at=row[6],
        updated_at=row[7],
        wake_at=row[8],
        lease_token=row[9],
        lease_owner=row[10],
        lease_expires_at=row[11],
        runs=row[12],
        recoveries=row[13],
        cancel_requested=bool(row[14]),
        forked_from=row[15],
        fork_seq=row[16],
    )


def _entry(row: tuple[Any, ...]) -> JournalEntry:
    return JournalEntry(
        workflow_id=row[0],
        seq=row[1],
        kind=EntryKind(row[2]),
        name=row[3],
        status=EntryStatus(row[4]),
        output=serde.loads(row[5]),
        error=serde.loads(row[6]),
        wake_at=row[7],
        attempts=row[8],
        started_at=row[9],
        completed_at=row[10],
    )


def _entry_params(e: JournalEntry) -> tuple[Any, ...]:
    return (
        e.workflow_id,
        e.seq,
        str(e.kind),
        e.name,
        str(e.status),
        serde.dumps(e.output) if e.output is not None else None,
        serde.dumps(e.error) if e.error is not None else None,
        e.wake_at,
        e.attempts,
        e.started_at,
        e.completed_at,
    )


INSERT_ENTRY = (
    "INSERT OR REPLACE INTO journal (workflow_id, seq, kind, name, status, output, error, wake_at, attempts, "
    "started_at, completed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
)


class SQLiteStore(Store):
    """``SQLiteStore("ninelives.db")`` or ``SQLiteStore(":memory:")`` for tests."""

    def __init__(self, path: str = "ninelives.db", busy_timeout_ms: int = 30_000):
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ plumbing
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None, timeout=self.busy_timeout_ms / 1000
        )
        conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        if self.path != ":memory:":
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    async def _run(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        def call() -> T:
            with self._lock:
                if self._conn is None:
                    self._conn = self._connect()
                return fn(self._conn)

        return await asyncio.to_thread(call)

    @staticmethod
    def _tx(conn: sqlite3.Connection, body: Callable[[sqlite3.Connection], T]) -> T:
        conn.execute("BEGIN IMMEDIATE")
        try:
            result = body(conn)
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
        return result

    @staticmethod
    def _check_lease(conn: sqlite3.Connection, wf_id: str, token: str) -> None:
        row = conn.execute("SELECT lease_token FROM workflows WHERE id = ?", (wf_id,)).fetchone()
        if row is None or row[0] != token:
            raise LeaseLost(wf_id)

    def _fenced(
        self, wf_id: str, token: str, body: Callable[[sqlite3.Connection], T]
    ) -> Callable[[sqlite3.Connection], T]:
        def run(conn: sqlite3.Connection) -> T:
            def tx(c: sqlite3.Connection) -> T:
                self._check_lease(c, wf_id, token)
                return body(c)

            return self._tx(conn, tx)

        return run

    # ------------------------------------------------------------------ lifecycle
    async def init(self) -> None:
        await self._run(lambda c: c.executescript(SCHEMA))

    async def close(self) -> None:
        def close(_: sqlite3.Connection) -> None:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

        if self._conn is not None:
            await self._run(close)

    # ------------------------------------------------------------------ client side
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
        def body(c: sqlite3.Connection) -> tuple[WorkflowRecord, bool]:
            row = c.execute(f"SELECT {WF_COLS} FROM workflows WHERE id = ?", (wf_id,)).fetchone()
            if row is not None:
                return _wf(row), False
            c.execute(
                "INSERT INTO workflows (id, name, status, input, created_at, updated_at, forked_from, fork_seq) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (wf_id, name, str(Status.PENDING), serde.dumps(input), now, now, forked_from, fork_seq),
            )
            for e in journal:
                c.execute(INSERT_ENTRY, _entry_params(e))
            row = c.execute(f"SELECT {WF_COLS} FROM workflows WHERE id = ?", (wf_id,)).fetchone()
            return _wf(row), True

        return await self._run(lambda c: self._tx(c, body))

    async def get_workflow(self, wf_id: str) -> WorkflowRecord | None:
        row = await self._run(
            lambda c: c.execute(f"SELECT {WF_COLS} FROM workflows WHERE id = ?", (wf_id,)).fetchone()
        )
        return _wf(row) if row else None

    async def list_workflows(
        self, status: Status | None = None, name: str | None = None, limit: int = 50, offset: int = 0
    ) -> list[WorkflowRecord]:
        where, params = [], []
        if status is not None:
            where.append("status = ?")
            params.append(str(status))
        if name is not None:
            where.append("name = ?")
            params.append(name)
        sql = f"SELECT {WF_COLS} FROM workflows"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
        rows = await self._run(lambda c: c.execute(sql, (*params, limit, offset)).fetchall())
        return [_wf(r) for r in rows]

    async def count_by_status(self) -> dict[str, int]:
        rows = await self._run(
            lambda c: c.execute("SELECT status, COUNT(*) FROM workflows GROUP BY status").fetchall()
        )
        return {r[0]: r[1] for r in rows}

    async def send_signal(self, wf_id: str, name: str, payload: Any, now: float) -> Signal:
        def body(c: sqlite3.Connection) -> Signal:
            row = c.execute("SELECT status FROM workflows WHERE id = ?", (wf_id,)).fetchone()
            if row is None:
                raise WorkflowNotFound(wf_id)
            cur = c.execute(
                "INSERT INTO signals (workflow_id, name, payload, created_at) VALUES (?,?,?,?)",
                (wf_id, name, serde.dumps(payload), now),
            )
            c.execute(
                "UPDATE workflows SET wake_at = ?, updated_at = ? WHERE id = ? AND status = ?",
                (now, now, wf_id, str(Status.WAITING)),
            )
            return Signal(
                id=int(cur.lastrowid or 0),
                workflow_id=wf_id,
                name=name,
                payload=serde.loads(serde.dumps(payload)),
                created_at=now,
            )

        return await self._run(lambda c: self._tx(c, body))

    async def request_cancel(self, wf_id: str, now: float) -> WorkflowRecord:
        def body(c: sqlite3.Connection) -> WorkflowRecord:
            row = c.execute("SELECT status FROM workflows WHERE id = ?", (wf_id,)).fetchone()
            if row is None:
                raise WorkflowNotFound(wf_id)
            status = Status(row[0])
            if status in (Status.PENDING, Status.WAITING):
                c.execute(
                    "UPDATE workflows SET status = ?, cancel_requested = 1, error = ?, wake_at = NULL, updated_at = ? WHERE id = ?",
                    (
                        str(Status.CANCELLED),
                        serde.dumps({"type": "Cancelled", "message": "cancelled by request"}),
                        now,
                        wf_id,
                    ),
                )
            elif status == Status.RUNNING:
                c.execute(
                    "UPDATE workflows SET cancel_requested = 1, updated_at = ? WHERE id = ?", (now, wf_id)
                )
            return _wf(c.execute(f"SELECT {WF_COLS} FROM workflows WHERE id = ?", (wf_id,)).fetchone())

        return await self._run(lambda c: self._tx(c, body))

    async def get_journal(self, wf_id: str) -> list[JournalEntry]:
        rows = await self._run(
            lambda c: c.execute(
                "SELECT workflow_id, seq, kind, name, status, output, error, wake_at, attempts, started_at, completed_at "
                "FROM journal WHERE workflow_id = ? ORDER BY seq",
                (wf_id,),
            ).fetchall()
        )
        return [_entry(r) for r in rows]

    async def get_attempts(self, wf_id: str) -> list[Attempt]:
        rows = await self._run(
            lambda c: c.execute(
                "SELECT workflow_id, seq, attempt, worker, started_at, finished_at, error FROM attempts "
                "WHERE workflow_id = ? ORDER BY seq, attempt",
                (wf_id,),
            ).fetchall()
        )
        return [Attempt(*r) for r in rows]

    async def get_signals(self, wf_id: str) -> list[Signal]:
        rows = await self._run(
            lambda c: c.execute(
                "SELECT id, workflow_id, name, payload, created_at, consumed_seq FROM signals WHERE workflow_id = ? ORDER BY id",
                (wf_id,),
            ).fetchall()
        )
        return [Signal(r[0], r[1], r[2], serde.loads(r[3]), r[4], r[5]) for r in rows]

    # ------------------------------------------------------------------ worker side
    async def claim(
        self, worker_id: str, now: float, ttl: float, names: Sequence[str], limit: int
    ) -> list[WorkflowRecord]:
        if not names or limit <= 0:
            return []
        marks = ",".join("?" for _ in names)

        def body(c: sqlite3.Connection) -> list[WorkflowRecord]:
            rows = c.execute(
                f"SELECT id, status FROM workflows WHERE name IN ({marks}) AND ("
                "status = 'PENDING' OR (status = 'WAITING' AND wake_at IS NOT NULL AND wake_at <= ?) "
                "OR (status = 'RUNNING' AND lease_expires_at < ?)) "
                "ORDER BY COALESCE(wake_at, created_at), created_at LIMIT ?",
                (*names, now, now, limit),
            ).fetchall()
            out = []
            for wf_id, prev in rows:
                c.execute(
                    "UPDATE workflows SET status = 'RUNNING', lease_token = ?, lease_owner = ?, lease_expires_at = ?, "
                    "runs = runs + 1, recoveries = recoveries + ?, updated_at = ? WHERE id = ?",
                    (uuid.uuid4().hex, worker_id, now + ttl, 1 if prev == "RUNNING" else 0, now, wf_id),
                )
                out.append(
                    _wf(c.execute(f"SELECT {WF_COLS} FROM workflows WHERE id = ?", (wf_id,)).fetchone())
                )
            return out

        return await self._run(lambda c: self._tx(c, body))

    async def heartbeat(self, wf_id: str, token: str, now: float, ttl: float) -> Heartbeat:
        def body(c: sqlite3.Connection) -> Heartbeat:
            cur = c.execute(
                "UPDATE workflows SET lease_expires_at = ? WHERE id = ? AND lease_token = ? AND status = 'RUNNING'",
                (now + ttl, wf_id, token),
            )
            if cur.rowcount == 0:
                return Heartbeat(alive=False, cancel_requested=False)
            row = c.execute("SELECT cancel_requested FROM workflows WHERE id = ?", (wf_id,)).fetchone()
            return Heartbeat(alive=True, cancel_requested=bool(row[0]))

        return await self._run(lambda c: self._tx(c, body))

    async def write_entry(self, token: str, entry: JournalEntry) -> None:
        await self._run(
            self._fenced(entry.workflow_id, token, lambda c: c.execute(INSERT_ENTRY, _entry_params(entry)))
        )

    async def start_attempt(self, token: str, wf_id: str, seq: int, worker: str, now: float) -> int:
        def body(c: sqlite3.Connection) -> int:
            row = c.execute(
                "SELECT COALESCE(MAX(attempt), 0) FROM attempts WHERE workflow_id = ? AND seq = ?",
                (wf_id, seq),
            ).fetchone()
            n = int(row[0]) + 1
            c.execute(
                "INSERT INTO attempts (workflow_id, seq, attempt, worker, started_at) VALUES (?,?,?,?,?)",
                (wf_id, seq, n, worker, now),
            )
            return n

        return await self._run(self._fenced(wf_id, token, body))

    async def finish_attempt(
        self, token: str, wf_id: str, seq: int, attempt: int, now: float, error: str | None
    ) -> None:
        await self._run(
            self._fenced(
                wf_id,
                token,
                lambda c: c.execute(
                    "UPDATE attempts SET finished_at = ?, error = ? WHERE workflow_id = ? AND seq = ? AND attempt = ?",
                    (now, error, wf_id, seq, attempt),
                ),
            )
        )

    async def consume_signal(self, token: str, entry: JournalEntry, signal_name: str) -> Signal | None:
        def body(c: sqlite3.Connection) -> Signal | None:
            row = c.execute(
                "SELECT id, workflow_id, name, payload, created_at FROM signals "
                "WHERE workflow_id = ? AND name = ? AND consumed_seq IS NULL ORDER BY id LIMIT 1",
                (entry.workflow_id, signal_name),
            ).fetchone()
            if row is None:
                return None
            c.execute("UPDATE signals SET consumed_seq = ? WHERE id = ?", (entry.seq, row[0]))
            payload = serde.loads(row[3])
            done = JournalEntry(
                **{**entry.__dict__, "status": EntryStatus.COMPLETED, "output": {"payload": payload}}
            )
            c.execute(INSERT_ENTRY, _entry_params(done))
            return Signal(row[0], row[1], row[2], payload, row[4], entry.seq)

        return await self._run(self._fenced(entry.workflow_id, token, body))

    async def finish(
        self, token: str, wf_id: str, status: Status, output: Any, error: dict[str, Any] | None, now: float
    ) -> None:
        await self._run(
            self._fenced(
                wf_id,
                token,
                lambda c: c.execute(
                    "UPDATE workflows SET status = ?, output = ?, error = ?, wake_at = NULL, lease_token = NULL, "
                    "lease_expires_at = NULL, updated_at = ? WHERE id = ?",
                    (
                        str(status),
                        serde.dumps(output) if output is not None else None,
                        serde.dumps(error) if error is not None else None,
                        now,
                        wf_id,
                    ),
                ),
            )
        )

    async def suspend(
        self, token: str, wf_id: str, wake_at: float | None, waiting_for: Sequence[str], now: float
    ) -> Status:
        def body(c: sqlite3.Connection) -> Status:
            cancel = c.execute("SELECT cancel_requested FROM workflows WHERE id = ?", (wf_id,)).fetchone()[0]
            if cancel:
                c.execute(
                    "UPDATE workflows SET status = 'CANCELLED', error = ?, wake_at = NULL, lease_token = NULL, "
                    "lease_expires_at = NULL, updated_at = ? WHERE id = ?",
                    (serde.dumps({"type": "Cancelled", "message": "cancelled by request"}), now, wf_id),
                )
                return Status.CANCELLED
            wake = wake_at
            if waiting_for:
                marks = ",".join("?" for _ in waiting_for)
                ready = c.execute(
                    f"SELECT 1 FROM signals WHERE workflow_id = ? AND consumed_seq IS NULL AND name IN ({marks}) LIMIT 1",
                    (wf_id, *waiting_for),
                ).fetchone()
                if ready:
                    wake = now
            c.execute(
                "UPDATE workflows SET status = 'WAITING', wake_at = ?, lease_token = NULL, lease_expires_at = NULL, "
                "updated_at = ? WHERE id = ?",
                (wake, now, wf_id),
            )
            return Status.WAITING

        return await self._run(self._fenced(wf_id, token, body))

    async def release(self, token: str, wf_id: str) -> None:
        await self._run(
            lambda c: c.execute(
                "UPDATE workflows SET lease_expires_at = 0 WHERE id = ? AND lease_token = ?", (wf_id, token)
            )
        )
