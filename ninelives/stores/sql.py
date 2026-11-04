"""Store logic shared by SQLite and Postgres.

Every operation is written once, as a *transaction script*: a generator that yields statements
(``Q``) and receives their results. A driver runs the script inside one transaction. The SQLite
driver runs the whole script in its worker thread (one thread hop per transaction); the Postgres
driver runs it on an asyncpg connection. Dialect differences stay in the drivers:

* placeholders are written ``?`` (Postgres gets ``$1..$n``);
* ``/*FOR_UPDATE*/`` and ``/*SKIP_LOCKED*/`` become row locks on Postgres and vanish on SQLite,
  where ``BEGIN IMMEDIATE`` already serialises writers;
* ``Lock(key)`` is a Postgres advisory transaction lock and a no-op on SQLite;
* ``Notify()`` wakes waiting workers after commit (``pg_notify`` and in-process waiters).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Generator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Literal, TypeVar

from .. import serde
from ..cron import Cron
from ..errors import LeaseLost, NinelivesError, WorkflowNotFound
from ..types import (
    Attempt,
    EntryKind,
    EntryStatus,
    Event,
    JournalEntry,
    Limit,
    Schedule,
    ScheduleDef,
    Signal,
    Status,
    WorkflowRecord,
)
from .base import Heartbeat, Store

T = TypeVar("T")

# --------------------------------------------------------------------------- statements


@dataclass(frozen=True)
class Q:
    sql: str
    args: tuple[Any, ...] = ()
    mode: Literal["exec", "all", "one", "val"] = "exec"


@dataclass(frozen=True)
class Lock:
    key: str


@dataclass(frozen=True)
class Notify:
    pass


Op = Q | Lock | Notify
Script = Generator[Op, Any, T]


def run(sql: str, *args: Any) -> Q:
    """Execute; the result is the number of affected rows."""
    return Q(sql, args, "exec")


def rows(sql: str, *args: Any) -> Q:
    return Q(sql, args, "all")


def one(sql: str, *args: Any) -> Q:
    return Q(sql, args, "one")


def val(sql: str, *args: Any) -> Q:
    return Q(sql, args, "val")


class SchemaTooNew(NinelivesError):
    """The database was migrated by a newer Ninelives than this one."""


# --------------------------------------------------------------------------- schema

# Type tokens are filled in by each dialect: {JSON} {REAL} {BOOL} {TRUE} {FALSE} {SERIAL}.
MIGRATIONS: list[list[str]] = [
    # 1 - the v0.1 schema
    [
        """CREATE TABLE IF NOT EXISTS workflows (
          id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL, input {JSON} NOT NULL,
          output {JSON}, error {JSON}, created_at {REAL} NOT NULL, updated_at {REAL} NOT NULL,
          wake_at {REAL}, lease_token TEXT, lease_owner TEXT, lease_expires_at {REAL},
          runs INTEGER NOT NULL DEFAULT 0, recoveries INTEGER NOT NULL DEFAULT 0,
          cancel_requested {BOOL} NOT NULL DEFAULT {FALSE}, forked_from TEXT, fork_seq INTEGER)""",
        "CREATE INDEX IF NOT EXISTS workflows_status_wake ON workflows (status, wake_at)",
        "CREATE INDEX IF NOT EXISTS workflows_created ON workflows (created_at)",
        """CREATE TABLE IF NOT EXISTS journal (
          workflow_id TEXT NOT NULL, seq INTEGER NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL,
          status TEXT NOT NULL, output {JSON}, error {JSON}, wake_at {REAL},
          attempts INTEGER NOT NULL DEFAULT 0, started_at {REAL}, completed_at {REAL},
          PRIMARY KEY (workflow_id, seq))""",
        """CREATE TABLE IF NOT EXISTS attempts (
          workflow_id TEXT NOT NULL, seq INTEGER NOT NULL, attempt INTEGER NOT NULL, worker TEXT NOT NULL,
          started_at {REAL} NOT NULL, finished_at {REAL}, error TEXT,
          PRIMARY KEY (workflow_id, seq, attempt))""",
        """CREATE TABLE IF NOT EXISTS signals (
          id {SERIAL}, workflow_id TEXT NOT NULL, name TEXT NOT NULL, payload {JSON},
          created_at {REAL} NOT NULL, consumed_seq INTEGER)""",
        "CREATE INDEX IF NOT EXISTS signals_lookup ON signals (workflow_id, name, consumed_seq)",
    ],
    # 2 - v0.2: children, events, schedules, flow control, deadlines, deduplicated signals
    [
        "ALTER TABLE workflows ADD COLUMN parent_id TEXT",
        "ALTER TABLE workflows ADD COLUMN parent_seq INTEGER",
        "ALTER TABLE workflows ADD COLUMN priority INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE workflows ADD COLUMN concurrency_key TEXT",
        "ALTER TABLE workflows ADD COLUMN deadline {REAL}",
        "ALTER TABLE workflows ADD COLUMN schedule_id TEXT",
        "ALTER TABLE workflows ADD COLUMN waiting_for {JSON}",
        "CREATE INDEX IF NOT EXISTS workflows_parent ON workflows (parent_id) WHERE parent_id IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS workflows_name_status ON workflows (name, status)",
        "CREATE INDEX IF NOT EXISTS workflows_finished ON workflows (status, updated_at)",
        "CREATE INDEX IF NOT EXISTS workflows_schedule ON workflows (schedule_id) WHERE schedule_id IS NOT NULL",
        "ALTER TABLE signals ADD COLUMN dedupe_key TEXT",
        "CREATE UNIQUE INDEX IF NOT EXISTS signals_dedupe ON signals (dedupe_key) WHERE dedupe_key IS NOT NULL",
        """CREATE TABLE IF NOT EXISTS events (
          workflow_id TEXT NOT NULL, key TEXT NOT NULL, value {JSON}, updated_at {REAL} NOT NULL,
          PRIMARY KEY (workflow_id, key))""",
        """CREATE TABLE IF NOT EXISTS schedules (
          id TEXT PRIMARY KEY, workflow TEXT NOT NULL, cron TEXT NOT NULL, tz TEXT NOT NULL,
          input {JSON} NOT NULL, overlap TEXT NOT NULL, enabled {BOOL} NOT NULL DEFAULT {TRUE},
          next_at {REAL}, last_at {REAL}, last_run_id TEXT, created_at {REAL} NOT NULL,
          updated_at {REAL} NOT NULL, seen_at {REAL})""",
        "CREATE INDEX IF NOT EXISTS schedules_due ON schedules (enabled, next_at)",
    ],
]
SCHEMA_VERSION = len(MIGRATIONS)

WF_COLS = (
    "id, name, status, input, output, error, created_at, updated_at, wake_at, lease_token, lease_owner, "
    "lease_expires_at, runs, recoveries, cancel_requested, forked_from, fork_seq, parent_id, parent_seq, "
    "priority, concurrency_key, deadline, schedule_id"
)
ENTRY_COLS = (
    "workflow_id, seq, kind, name, status, output, error, wake_at, attempts, started_at, completed_at"
)
SIGNAL_COLS = "id, workflow_id, name, payload, created_at, consumed_seq"
SCHEDULE_COLS = "id, workflow, cron, tz, input, overlap, enabled, next_at, last_at, last_run_id, created_at, updated_at, seen_at"
UPSERT_ENTRY = (
    f"INSERT INTO journal ({ENTRY_COLS}) VALUES (?,?,?,?,?,?,?,?,?,?,?) "
    "ON CONFLICT (workflow_id, seq) DO UPDATE SET kind = excluded.kind, name = excluded.name, "
    "status = excluded.status, output = excluded.output, error = excluded.error, wake_at = excluded.wake_at, "
    "attempts = excluded.attempts, started_at = excluded.started_at, completed_at = excluded.completed_at"
)
ACTIVE = ("PENDING", "RUNNING", "WAITING")
TERMINAL = ("SUCCEEDED", "FAILED", "CANCELLED")
CANCELLED_ERROR = {"type": "Cancelled", "message": "cancelled by request"}


def _json(value: Any) -> str | None:
    return None if value is None else serde.dumps(value)


def _wf(r: Any) -> WorkflowRecord:
    return WorkflowRecord(
        id=r[0],
        name=r[1],
        status=Status(r[2]),
        input=serde.loads(r[3]),
        output=serde.loads(r[4]),
        error=serde.loads(r[5]),
        created_at=r[6],
        updated_at=r[7],
        wake_at=r[8],
        lease_token=r[9],
        lease_owner=r[10],
        lease_expires_at=r[11],
        runs=r[12],
        recoveries=r[13],
        cancel_requested=bool(r[14]),
        forked_from=r[15],
        fork_seq=r[16],
        parent_id=r[17],
        parent_seq=r[18],
        priority=r[19],
        concurrency_key=r[20],
        deadline=r[21],
        schedule_id=r[22],
    )


def _entry(r: Any) -> JournalEntry:
    return JournalEntry(
        workflow_id=r[0],
        seq=r[1],
        kind=EntryKind(r[2]),
        name=r[3],
        status=EntryStatus(r[4]),
        output=serde.loads(r[5]),
        error=serde.loads(r[6]),
        wake_at=r[7],
        attempts=r[8],
        started_at=r[9],
        completed_at=r[10],
    )


def _entry_args(e: JournalEntry) -> tuple[Any, ...]:
    return (
        e.workflow_id,
        e.seq,
        str(e.kind),
        e.name,
        str(e.status),
        _json(e.output),
        _json(e.error),
        e.wake_at,
        e.attempts,
        e.started_at,
        e.completed_at,
    )


def _signal(r: Any) -> Signal:
    return Signal(r[0], r[1], r[2], serde.loads(r[3]), r[4], r[5])


def _schedule(r: Any) -> Schedule:
    return Schedule(
        id=r[0],
        workflow=r[1],
        cron=r[2],
        tz=r[3],
        input=serde.loads(r[4]),
        overlap=r[5],
        enabled=bool(r[6]),
        next_at=r[7],
        last_at=r[8],
        last_run_id=r[9],
        created_at=r[10],
        updated_at=r[11],
        seen_at=r[12],
    )


def _marks(n: int) -> str:
    return ",".join("?" for _ in range(n))


def tick_label(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class _Waiters:
    """In-process wake-ups for workers sharing this store object."""

    futures: set[asyncio.Future[None]]

    def wake(self) -> None:
        for fut in list(self.futures):
            if not fut.done():
                fut.get_loop().call_soon_threadsafe(_resolve, fut)


def _resolve(fut: asyncio.Future[None]) -> None:
    if not fut.done():
        fut.set_result(None)


class SqlStore(Store):
    """All store behaviour, expressed as transaction scripts. Subclasses provide ``_run``."""

    def __init__(self) -> None:
        self._waiters = _Waiters(set())

    # ------------------------------------------------------------------ driver hooks
    async def _run(self, script: Callable[[], Script[T]], *, write: bool = True) -> T:
        """Run ``script()`` in one transaction (``write=False``: a read-only snapshot)."""
        raise NotImplementedError

    def _fenced(self, wf_id: str, token: str, body: Callable[[], Script[T]]) -> Callable[[], Script[T]]:
        def script() -> Script[T]:
            current = yield val("SELECT lease_token FROM workflows WHERE id = ? /*FOR_UPDATE*/", wf_id)
            if current is None or current != token:
                raise LeaseLost(wf_id)
            return (yield from body())

        return script

    async def wait_for_work(self, timeout: float) -> None:
        """Return when new work may be available, or after ``timeout`` seconds."""
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        self._waiters.futures.add(fut)
        try:
            await asyncio.wait_for(fut, timeout)
        except TimeoutError:
            pass
        finally:
            self._waiters.futures.discard(fut)

    # ------------------------------------------------------------------ migrations
    def _migrate(self, table_exists_sql: str) -> Script[int]:
        yield Lock("ninelives:schema")
        exists = yield val(table_exists_sql, "ninelives_schema")
        if not exists:
            yield run("CREATE TABLE IF NOT EXISTS ninelives_schema (version INTEGER NOT NULL)")
        current = (yield val("SELECT MAX(version) FROM ninelives_schema")) or 0
        if current > SCHEMA_VERSION:
            raise SchemaTooNew(
                f"the database schema is version {current}, newer than this Ninelives understands "
                f"({SCHEMA_VERSION}); upgrade the ninelives package"
            )
        for version in range(current + 1, SCHEMA_VERSION + 1):
            for statement in MIGRATIONS[version - 1]:
                yield run(self._ddl(statement))
            yield run("INSERT INTO ninelives_schema (version) VALUES (?)", version)
        return SCHEMA_VERSION

    def _ddl(self, statement: str) -> str:
        raise NotImplementedError

    async def schema_version(self) -> int:
        def script() -> Script[int]:
            return int((yield val("SELECT MAX(version) FROM ninelives_schema")) or 0)

        return await self._run(script, write=False)

    # ------------------------------------------------------------------ creating runs
    def _create(
        self,
        wf_id: str,
        name: str,
        input: dict[str, Any],
        now: float,
        journal: Sequence[JournalEntry] = (),
        *,
        forked_from: str | None = None,
        fork_seq: int | None = None,
        parent_id: str | None = None,
        parent_seq: int | None = None,
        priority: int = 0,
        concurrency_key: str | None = None,
        deadline: float | None = None,
        schedule_id: str | None = None,
    ) -> Script[tuple[WorkflowRecord, bool]]:
        inserted = yield run(
            "INSERT INTO workflows (id, name, status, input, created_at, updated_at, forked_from, fork_seq, "
            "parent_id, parent_seq, priority, concurrency_key, deadline, schedule_id) "
            "VALUES (?,?,'PENDING',?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (id) DO NOTHING",
            wf_id,
            name,
            serde.dumps(input),
            now,
            now,
            forked_from,
            fork_seq,
            parent_id,
            parent_seq,
            int(priority),
            concurrency_key,
            deadline,
            schedule_id,
        )
        if inserted:
            for e in journal:
                yield run(UPSERT_ENTRY, *_entry_args(e))
            yield Notify()
        row = yield one(f"SELECT {WF_COLS} FROM workflows WHERE id = ?", wf_id)
        return _wf(row), bool(inserted)

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
    ) -> tuple[WorkflowRecord, bool]:
        return await self._run(
            lambda: self._create(
                wf_id,
                name,
                input,
                now,
                journal,
                forked_from=forked_from,
                fork_seq=fork_seq,
                parent_id=parent_id,
                parent_seq=parent_seq,
                priority=priority,
                concurrency_key=concurrency_key,
                deadline=deadline,
                schedule_id=schedule_id,
            )
        )

    # ------------------------------------------------------------------ reads
    async def get_workflow(self, wf_id: str) -> WorkflowRecord | None:
        def script() -> Script[WorkflowRecord | None]:
            row = yield one(f"SELECT {WF_COLS} FROM workflows WHERE id = ?", wf_id)
            return _wf(row) if row else None

        return await self._run(script, write=False)

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
        where: list[str] = []
        args: list[Any] = []
        for column, value in (
            ("status", status),
            ("name", name),
            ("parent_id", parent_id),
            ("schedule_id", schedule_id),
        ):
            if value is not None:
                where.append(f"{column} = ?")
                args.append(str(value))
        if search:
            where.append("id LIKE ?")
            args.append("%" + search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
        sql = f"SELECT {WF_COLS} FROM workflows"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
        if search:
            sql = sql.replace("id LIKE ?", "id LIKE ? ESCAPE '\\'")

        def script() -> Script[list[WorkflowRecord]]:
            found = yield rows(sql, *args, int(limit), int(offset))
            return [_wf(r) for r in found]

        return await self._run(script, write=False)

    async def count_by_status(self) -> dict[str, int]:
        def script() -> Script[dict[str, int]]:
            found = yield rows("SELECT status, COUNT(*) FROM workflows GROUP BY status")
            return {r[0]: int(r[1]) for r in found}

        return await self._run(script, write=False)

    async def count_by_name_and_status(self) -> dict[tuple[str, str], int]:
        def script() -> Script[dict[tuple[str, str], int]]:
            found = yield rows("SELECT name, status, COUNT(*) FROM workflows GROUP BY name, status")
            return {(r[0], r[1]): int(r[2]) for r in found}

        return await self._run(script, write=False)

    async def get_journal(self, wf_id: str) -> list[JournalEntry]:
        def script() -> Script[list[JournalEntry]]:
            found = yield rows(f"SELECT {ENTRY_COLS} FROM journal WHERE workflow_id = ? ORDER BY seq", wf_id)
            return [_entry(r) for r in found]

        return await self._run(script, write=False)

    async def get_attempts(self, wf_id: str) -> list[Attempt]:
        def script() -> Script[list[Attempt]]:
            found = yield rows(
                "SELECT workflow_id, seq, attempt, worker, started_at, finished_at, error FROM attempts "
                "WHERE workflow_id = ? ORDER BY seq, attempt",
                wf_id,
            )
            return [Attempt(*tuple(r)) for r in found]

        return await self._run(script, write=False)

    async def get_signals(self, wf_id: str) -> list[Signal]:
        def script() -> Script[list[Signal]]:
            found = yield rows(f"SELECT {SIGNAL_COLS} FROM signals WHERE workflow_id = ? ORDER BY id", wf_id)
            return [_signal(r) for r in found]

        return await self._run(script, write=False)

    async def get_events(self, wf_id: str) -> list[Event]:
        def script() -> Script[list[Event]]:
            found = yield rows(
                "SELECT workflow_id, key, value, updated_at FROM events WHERE workflow_id = ? ORDER BY key",
                wf_id,
            )
            return [Event(r[0], r[1], serde.loads(r[2]), r[3]) for r in found]

        return await self._run(script, write=False)

    async def get_event(self, wf_id: str, key: str) -> Event | None:
        def script() -> Script[Event | None]:
            r = yield one(
                "SELECT workflow_id, key, value, updated_at FROM events WHERE workflow_id = ? AND key = ?",
                wf_id,
                key,
            )
            return Event(r[0], r[1], serde.loads(r[2]), r[3]) if r else None

        return await self._run(script, write=False)

    # ------------------------------------------------------------------ signals and cancellation
    def _send(
        self, wf_id: str, name: str, payload: Any, now: float, dedupe_key: str | None = None
    ) -> Script[Signal]:
        target = yield one("SELECT status, waiting_for FROM workflows WHERE id = ? /*FOR_UPDATE*/", wf_id)
        if target is None:
            raise WorkflowNotFound(wf_id)
        if dedupe_key is not None:
            existing = yield one(f"SELECT {SIGNAL_COLS} FROM signals WHERE dedupe_key = ?", dedupe_key)
            if existing is not None:
                return _signal(existing)
        sid = yield val(
            "INSERT INTO signals (workflow_id, name, payload, created_at, dedupe_key) VALUES (?,?,?,?,?) RETURNING id",
            wf_id,
            name,
            serde.dumps(payload),
            now,
            dedupe_key,
        )
        waiting_for = serde.loads(target[1])
        # Wake a waiting run only if it waits for this signal (older rows store no list: always wake).
        if target[0] == "WAITING" and (waiting_for is None or name in waiting_for):
            yield run(
                "UPDATE workflows SET wake_at = ?, updated_at = ? WHERE id = ? AND status = 'WAITING'",
                now,
                now,
                wf_id,
            )
            yield Notify()
        return Signal(int(sid), wf_id, name, serde.loads(serde.dumps(payload)), now)

    async def send_signal(self, wf_id: str, name: str, payload: Any, now: float) -> Signal:
        return await self._run(lambda: self._send(wf_id, name, payload, now))

    def _tell_parent(self, child_id: str, status: str, output: Any, error: Any, now: float) -> Script[None]:
        """Deliver a finished child's outcome to its parent as the signal ``child:<id>``."""
        parent = yield val("SELECT parent_id FROM workflows WHERE id = ?", child_id)
        if parent is None:
            return None
        exists = yield val("SELECT 1 FROM workflows WHERE id = ?", parent)
        if exists:
            payload = {"status": status, "output": output, "error": error}
            yield from self._send(
                parent, f"child:{child_id}", payload, now, dedupe_key=f"child-done:{child_id}"
            )
        return None

    def _cancel_tree(self, wf_id: str, now: float) -> Script[None]:
        """Cancel a run and its unfinished descendants (breadth first)."""
        queue = [wf_id]
        while queue:
            current = queue.pop(0)
            status = yield val("SELECT status FROM workflows WHERE id = ? /*FOR_UPDATE*/", current)
            if status in ("PENDING", "WAITING"):
                yield run(
                    "UPDATE workflows SET status = 'CANCELLED', cancel_requested = ?, error = ?, wake_at = NULL, "
                    "updated_at = ? WHERE id = ?",
                    True,
                    serde.dumps(CANCELLED_ERROR),
                    now,
                    current,
                )
                yield from self._tell_parent(current, "CANCELLED", None, CANCELLED_ERROR, now)
            elif status == "RUNNING":
                yield run(
                    "UPDATE workflows SET cancel_requested = ?, updated_at = ? WHERE id = ?",
                    True,
                    now,
                    current,
                )
            children = yield rows(
                f"SELECT id FROM workflows WHERE parent_id = ? AND status IN ({_marks(len(ACTIVE))})",
                current,
                *ACTIVE,
            )
            queue.extend(r[0] for r in children)
        return None

    async def request_cancel(self, wf_id: str, now: float) -> WorkflowRecord:
        def script() -> Script[WorkflowRecord]:
            exists = yield val("SELECT 1 FROM workflows WHERE id = ?", wf_id)
            if not exists:
                raise WorkflowNotFound(wf_id)
            yield from self._cancel_tree(wf_id, now)
            return _wf((yield one(f"SELECT {WF_COLS} FROM workflows WHERE id = ?", wf_id)))

        return await self._run(script)

    # ------------------------------------------------------------------ claiming
    async def claim(
        self,
        worker_id: str,
        now: float,
        ttl: float,
        names: Sequence[str],
        limit: int,
        limits: Mapping[str, Limit] | None = None,
    ) -> list[WorkflowRecord]:
        if not names or limit <= 0:
            return []
        active = {
            n: lim
            for n, lim in (limits or {}).items()
            if n in names and (lim.concurrency or lim.key_concurrency)
        }
        free = [n for n in names if n not in active]
        runnable = (
            "(status = 'PENDING' OR (status = 'WAITING' AND wake_at IS NOT NULL AND wake_at <= ?) "
            "OR (status = 'RUNNING' AND lease_expires_at < ?))"
        )
        order = "ORDER BY priority DESC, COALESCE(wake_at, created_at), created_at"

        def script() -> Script[list[WorkflowRecord]]:
            candidates: list[Any] = []
            if free:
                candidates += yield rows(
                    f"SELECT id, status, name, concurrency_key, priority, COALESCE(wake_at, created_at), created_at "
                    f"FROM workflows WHERE name IN ({_marks(len(free))}) AND {runnable} {order} LIMIT ? /*SKIP_LOCKED*/",
                    *free,
                    now,
                    now,
                    limit,
                )
            live: dict[str, int] = {}
            live_keys: dict[tuple[str, str], int] = {}
            for name in sorted(active):
                yield Lock(f"ninelives:claim:{name}")  # one claimer at a time sees the slot counts
                lim = active[name]
                live[name] = int(
                    (
                        yield val(
                            "SELECT COUNT(*) FROM workflows WHERE name = ? AND status = 'RUNNING' AND lease_expires_at >= ?",
                            name,
                            now,
                        )
                    )
                    or 0
                )
                if lim.concurrency is not None and live[name] >= lim.concurrency:
                    continue
                full: list[str] = []
                if lim.key_concurrency is not None:
                    counted = yield rows(
                        "SELECT concurrency_key, COUNT(*) FROM workflows WHERE name = ? AND status = 'RUNNING' "
                        "AND lease_expires_at >= ? AND concurrency_key IS NOT NULL GROUP BY concurrency_key",
                        name,
                        now,
                    )
                    for key, n in counted:
                        live_keys[(name, key)] = int(n)
                        if int(n) >= lim.key_concurrency:
                            full.append(key)
                exclude = (
                    f"AND (concurrency_key IS NULL OR concurrency_key NOT IN ({_marks(len(full))}))"
                    if full
                    else ""
                )
                candidates += yield rows(
                    f"SELECT id, status, name, concurrency_key, priority, COALESCE(wake_at, created_at), created_at "
                    f"FROM workflows WHERE name = ? AND {runnable} {exclude} {order} LIMIT ? /*SKIP_LOCKED*/",
                    name,
                    now,
                    now,
                    *full,
                    limit * 4,
                )
            candidates.sort(key=lambda c: (-c[4], c[5], c[6]))
            chosen: list[tuple[str, str]] = []
            for wf_id, prev, name, key, *_ in candidates:
                if len(chosen) >= limit:
                    break
                limited = active.get(name)
                if limited is not None:
                    if limited.concurrency is not None and live[name] >= limited.concurrency:
                        continue
                    if limited.key_concurrency is not None and key is not None:
                        if live_keys.get((name, key), 0) >= limited.key_concurrency:
                            continue
                        live_keys[(name, key)] = live_keys.get((name, key), 0) + 1
                    live[name] += 1
                chosen.append((wf_id, prev))
            out: list[WorkflowRecord] = []
            for wf_id, prev in chosen:
                yield run(
                    "UPDATE workflows SET status = 'RUNNING', lease_token = ?, lease_owner = ?, lease_expires_at = ?, "
                    "runs = runs + 1, recoveries = recoveries + ?, updated_at = ? WHERE id = ?",
                    uuid.uuid4().hex,
                    worker_id,
                    now + ttl,
                    1 if prev == "RUNNING" else 0,
                    now,
                    wf_id,
                )
                out.append(_wf((yield one(f"SELECT {WF_COLS} FROM workflows WHERE id = ?", wf_id))))
            return out

        return await self._run(script)

    async def heartbeat(self, wf_id: str, token: str, now: float, ttl: float) -> Heartbeat:
        def script() -> Script[Heartbeat]:
            updated = yield run(
                "UPDATE workflows SET lease_expires_at = ? WHERE id = ? AND lease_token = ? AND status = 'RUNNING'",
                now + ttl,
                wf_id,
                token,
            )
            if not updated:
                return Heartbeat(alive=False, cancel_requested=False)
            cancel = yield val("SELECT cancel_requested FROM workflows WHERE id = ?", wf_id)
            return Heartbeat(alive=True, cancel_requested=bool(cancel))

        return await self._run(script)

    async def release(self, token: str, wf_id: str) -> None:
        def script() -> Script[None]:
            yield run(
                "UPDATE workflows SET lease_expires_at = 0 WHERE id = ? AND lease_token = ?", wf_id, token
            )
            yield Notify()
            return None

        await self._run(script)

    # ------------------------------------------------------------------ journal (fenced)
    async def write_entry(self, token: str, entry: JournalEntry) -> None:
        def body() -> Script[None]:
            yield run(UPSERT_ENTRY, *_entry_args(entry))
            return None

        await self._run(self._fenced(entry.workflow_id, token, body))

    async def start_attempt(self, token: str, wf_id: str, seq: int, worker: str, now: float) -> int:
        def body() -> Script[int]:
            n = (
                int(
                    (
                        yield val(
                            "SELECT COALESCE(MAX(attempt), 0) FROM attempts WHERE workflow_id = ? AND seq = ?",
                            wf_id,
                            seq,
                        )
                    )
                    or 0
                )
                + 1
            )
            yield run(
                "INSERT INTO attempts (workflow_id, seq, attempt, worker, started_at) VALUES (?,?,?,?,?)",
                wf_id,
                seq,
                n,
                worker,
                now,
            )
            return n

        return await self._run(self._fenced(wf_id, token, body))

    def _finish_attempt(
        self, wf_id: str, seq: int, attempt: int, now: float, error: str | None
    ) -> Script[None]:
        yield run(
            "UPDATE attempts SET finished_at = ?, error = ? WHERE workflow_id = ? AND seq = ? AND attempt = ?",
            now,
            error,
            wf_id,
            seq,
            attempt,
        )
        return None

    async def finish_attempt(
        self, token: str, wf_id: str, seq: int, attempt: int, now: float, error: str | None
    ) -> None:
        await self._run(
            self._fenced(wf_id, token, lambda: self._finish_attempt(wf_id, seq, attempt, now, error))
        )

    async def complete_step(
        self, token: str, entry: JournalEntry, attempt: int, now: float, attempt_error: str | None = None
    ) -> None:
        def body() -> Script[None]:
            yield from self._finish_attempt(entry.workflow_id, entry.seq, attempt, now, attempt_error)
            yield run(UPSERT_ENTRY, *_entry_args(entry))
            return None

        await self._run(self._fenced(entry.workflow_id, token, body))

    async def consume_signal(self, token: str, entry: JournalEntry, signal_name: str) -> Signal | None:
        def body() -> Script[Signal | None]:
            r = yield one(
                f"SELECT {SIGNAL_COLS} FROM signals WHERE workflow_id = ? AND name = ? AND consumed_seq IS NULL "
                "ORDER BY id LIMIT 1 /*FOR_UPDATE*/",
                entry.workflow_id,
                signal_name,
            )
            if r is None:
                return None
            yield run("UPDATE signals SET consumed_seq = ? WHERE id = ?", entry.seq, r[0])
            payload = serde.loads(r[3])
            yield run(
                UPSERT_ENTRY,
                *_entry_args(replace(entry, status=EntryStatus.COMPLETED, output={"payload": payload})),
            )
            return Signal(r[0], r[1], r[2], payload, r[4], entry.seq)

        return await self._run(self._fenced(entry.workflow_id, token, body))

    async def set_event(self, token: str, entry: JournalEntry, key: str, value: Any, now: float) -> None:
        def body() -> Script[None]:
            yield run(
                "INSERT INTO events (workflow_id, key, value, updated_at) VALUES (?,?,?,?) "
                "ON CONFLICT (workflow_id, key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                entry.workflow_id,
                key,
                serde.dumps(value),
                now,
            )
            yield run(UPSERT_ENTRY, *_entry_args(entry))
            return None

        await self._run(self._fenced(entry.workflow_id, token, body))

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
        def body() -> Script[tuple[WorkflowRecord, bool]]:
            created = yield from self._create(
                child_id,
                name,
                input,
                now,
                parent_id=entry.workflow_id,
                parent_seq=entry.seq,
                priority=priority,
                concurrency_key=concurrency_key,
                deadline=deadline,
            )
            yield run(UPSERT_ENTRY, *_entry_args(entry))
            return created

        return await self._run(self._fenced(entry.workflow_id, token, body))

    async def send_from(
        self, token: str, entry: JournalEntry, target_id: str, name: str, payload: Any, now: float
    ) -> Signal:
        def body() -> Script[Signal]:
            sent = yield from self._send(
                target_id, name, payload, now, dedupe_key=f"send:{entry.workflow_id}:{entry.seq}"
            )
            yield run(UPSERT_ENTRY, *_entry_args(entry))
            return sent

        return await self._run(self._fenced(entry.workflow_id, token, body))

    # ------------------------------------------------------------------ outcomes (fenced)
    async def finish(
        self, token: str, wf_id: str, status: Status, output: Any, error: dict[str, Any] | None, now: float
    ) -> None:
        def body() -> Script[None]:
            yield run(
                "UPDATE workflows SET status = ?, output = ?, error = ?, wake_at = NULL, lease_token = NULL, "
                "lease_expires_at = NULL, waiting_for = NULL, updated_at = ? WHERE id = ?",
                str(status),
                _json(output),
                _json(error),
                now,
                wf_id,
            )
            yield from self._tell_parent(wf_id, str(status), output, error, now)
            if status == Status.CANCELLED:
                children = yield rows(
                    f"SELECT id FROM workflows WHERE parent_id = ? AND status IN ({_marks(len(ACTIVE))})",
                    wf_id,
                    *ACTIVE,
                )
                for (child,) in children:
                    yield from self._cancel_tree(child, now)
            return None

        await self._run(self._fenced(wf_id, token, body))

    async def suspend(
        self, token: str, wf_id: str, wake_at: float | None, waiting_for: Sequence[str], now: float
    ) -> Status:
        def body() -> Script[Status]:
            r = yield one("SELECT cancel_requested, deadline FROM workflows WHERE id = ?", wf_id)
            if r[0]:
                yield run(
                    "UPDATE workflows SET status = 'CANCELLED', error = ?, wake_at = NULL, lease_token = NULL, "
                    "lease_expires_at = NULL, waiting_for = NULL, updated_at = ? WHERE id = ?",
                    serde.dumps(CANCELLED_ERROR),
                    now,
                    wf_id,
                )
                yield from self._tell_parent(wf_id, "CANCELLED", None, CANCELLED_ERROR, now)
                children = yield rows(
                    f"SELECT id FROM workflows WHERE parent_id = ? AND status IN ({_marks(len(ACTIVE))})",
                    wf_id,
                    *ACTIVE,
                )
                for (child,) in children:
                    yield from self._cancel_tree(child, now)
                return Status.CANCELLED
            wake = wake_at
            deadline = r[1]
            if deadline is not None and (wake is None or deadline < wake):
                wake = deadline  # wake up in time to fail with WorkflowTimeout
            if waiting_for:
                ready = yield val(
                    f"SELECT 1 FROM signals WHERE workflow_id = ? AND consumed_seq IS NULL AND name IN ({_marks(len(waiting_for))}) LIMIT 1",
                    wf_id,
                    *waiting_for,
                )
                if ready:
                    wake = now
            yield run(
                "UPDATE workflows SET status = 'WAITING', wake_at = ?, lease_token = NULL, lease_expires_at = NULL, "
                "waiting_for = ?, updated_at = ? WHERE id = ?",
                wake,
                serde.dumps(list(waiting_for)),
                now,
                wf_id,
            )
            if wake is not None and wake <= now:
                yield Notify()
            return Status.WAITING

        return await self._run(self._fenced(wf_id, token, body))

    # ------------------------------------------------------------------ schedules
    async def upsert_schedules(self, defs: Sequence[ScheduleDef], now: float) -> None:
        def script() -> Script[None]:
            for d in defs:
                cron = Cron.parse(d.cron)
                current = yield one(
                    f"SELECT {SCHEDULE_COLS} FROM schedules WHERE id = ? /*FOR_UPDATE*/", d.id
                )
                if current is None:
                    yield run(
                        f"INSERT INTO schedules ({SCHEDULE_COLS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) "
                        "ON CONFLICT (id) DO NOTHING",
                        d.id,
                        d.workflow,
                        d.cron,
                        d.tz,
                        serde.dumps(d.input),
                        d.overlap,
                        True,
                        cron.next_timestamp(now, d.tz),
                        None,
                        None,
                        now,
                        now,
                        now,
                    )
                    continue
                s = _schedule(current)
                if (s.workflow, s.cron, s.tz, s.input, s.overlap) != (
                    d.workflow,
                    d.cron,
                    d.tz,
                    d.input,
                    d.overlap,
                ):
                    next_at = s.next_at
                    if (s.cron, s.tz) != (d.cron, d.tz) or next_at is None:
                        next_at = cron.next_timestamp(now, d.tz)
                    yield run(
                        "UPDATE schedules SET workflow = ?, cron = ?, tz = ?, input = ?, overlap = ?, next_at = ?, "
                        "updated_at = ?, seen_at = ? WHERE id = ?",
                        d.workflow,
                        d.cron,
                        d.tz,
                        serde.dumps(d.input),
                        d.overlap,
                        next_at,
                        now,
                        now,
                        d.id,
                    )
                else:
                    yield run("UPDATE schedules SET seen_at = ? WHERE id = ?", now, d.id)
            return None

        await self._run(script)

    async def fire_schedules(self, now: float, names: Sequence[str], limit: int = 20) -> list[str]:
        """Start runs for due schedules of the given workflow types; returns the run ids started."""
        if not names:
            return []

        def script() -> Script[list[str]]:
            due = yield rows(
                f"SELECT {SCHEDULE_COLS} FROM schedules WHERE enabled = ? AND next_at <= ? "
                f"AND workflow IN ({_marks(len(names))}) ORDER BY next_at LIMIT ? /*SKIP_LOCKED*/",
                True,
                now,
                *names,
                limit,
            )
            started: list[str] = []
            for r in due:
                s = _schedule(r)
                assert s.next_at is not None
                cron = Cron.parse(s.cron)
                tick = s.next_at
                # After downtime, run once for the most recent missed tick.
                for _ in range(100_000):
                    following = cron.next_timestamp(tick, s.tz)
                    if following > now:
                        break
                    tick = following
                next_at = cron.next_timestamp(tick, s.tz)
                run_id: str | None = None
                busy = False
                if s.overlap == "skip" and s.last_run_id:
                    last = yield val("SELECT status FROM workflows WHERE id = ?", s.last_run_id)
                    busy = last in ACTIVE
                if not busy:
                    run_id = f"{s.id}@{tick_label(tick)}"
                    yield from self._create(run_id, s.workflow, s.input, now, schedule_id=s.id)
                    started.append(run_id)
                yield run(
                    "UPDATE schedules SET next_at = ?, last_at = ?, last_run_id = COALESCE(?, last_run_id), "
                    "updated_at = ? WHERE id = ? AND next_at = ?",
                    next_at,
                    tick,
                    run_id,
                    now,
                    s.id,
                    s.next_at,
                )
            return started

        return await self._run(script)

    async def list_schedules(self) -> list[Schedule]:
        def script() -> Script[list[Schedule]]:
            found = yield rows(f"SELECT {SCHEDULE_COLS} FROM schedules ORDER BY id")
            return [_schedule(r) for r in found]

        return await self._run(script, write=False)

    async def get_schedule(self, schedule_id: str) -> Schedule | None:
        def script() -> Script[Schedule | None]:
            r = yield one(f"SELECT {SCHEDULE_COLS} FROM schedules WHERE id = ?", schedule_id)
            return _schedule(r) if r else None

        return await self._run(script, write=False)

    async def set_schedule_enabled(self, schedule_id: str, enabled: bool, now: float) -> Schedule:
        def script() -> Script[Schedule]:
            r = yield one(f"SELECT {SCHEDULE_COLS} FROM schedules WHERE id = ? /*FOR_UPDATE*/", schedule_id)
            if r is None:
                raise KeyError(schedule_id)
            s = _schedule(r)
            # Resuming starts from now: ticks missed while paused are skipped.
            next_at = Cron.parse(s.cron).next_timestamp(now, s.tz) if enabled and not s.enabled else s.next_at
            yield run(
                "UPDATE schedules SET enabled = ?, next_at = ?, updated_at = ? WHERE id = ?",
                enabled,
                next_at,
                now,
                schedule_id,
            )
            return _schedule((yield one(f"SELECT {SCHEDULE_COLS} FROM schedules WHERE id = ?", schedule_id)))

        return await self._run(script)

    async def trigger_schedule(self, schedule_id: str, now: float) -> str:
        def script() -> Script[str]:
            r = yield one(f"SELECT {SCHEDULE_COLS} FROM schedules WHERE id = ?", schedule_id)
            if r is None:
                raise KeyError(schedule_id)
            s = _schedule(r)
            run_id = f"{s.id}@manual-{tick_label(now)}-{uuid.uuid4().hex[:6]}"
            yield from self._create(run_id, s.workflow, s.input, now, schedule_id=s.id)
            yield run("UPDATE schedules SET last_run_id = ?, updated_at = ? WHERE id = ?", run_id, now, s.id)
            return run_id

        return await self._run(script)

    async def delete_schedule(self, schedule_id: str) -> bool:
        def script() -> Script[bool]:
            return bool((yield run("DELETE FROM schedules WHERE id = ?", schedule_id)))

        return await self._run(script)

    # ------------------------------------------------------------------ retention
    async def purge(self, before: float, batch: int = 500) -> int:
        """Delete finished runs last updated before ``before``, with all their records."""

        def script() -> Script[int]:
            found = yield rows(
                f"SELECT id FROM workflows WHERE status IN ({_marks(len(TERMINAL))}) AND updated_at < ? LIMIT ?",
                *TERMINAL,
                before,
                batch,
            )
            ids = [r[0] for r in found]
            if not ids:
                return 0
            marks = _marks(len(ids))
            for table in ("journal", "attempts", "signals", "events"):
                yield run(f"DELETE FROM {table} WHERE workflow_id IN ({marks})", *ids)
            yield run(f"DELETE FROM workflows WHERE id IN ({marks})", *ids)
            return len(ids)

        total = 0
        while True:
            n = await self._run(script)
            total += n
            if n < batch:
                return total
