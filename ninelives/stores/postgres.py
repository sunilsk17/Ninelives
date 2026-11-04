"""Postgres store (asyncpg). Workers claim with FOR UPDATE SKIP LOCKED; fenced writes lock the
workflow row and compare the lease token inside the same transaction."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar

from .. import serde
from ..errors import LeaseLost, WorkflowNotFound
from ..types import Attempt, EntryKind, EntryStatus, JournalEntry, Signal, Status, WorkflowRecord
from .base import Heartbeat, Store

try:  # optional dependency
    import asyncpg
except ImportError:  # pragma: no cover - exercised only without the extra
    asyncpg = None

T = TypeVar("T")

SCHEMA = """
CREATE TABLE IF NOT EXISTS workflows (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL,
  input JSONB NOT NULL,
  output JSONB,
  error JSONB,
  created_at DOUBLE PRECISION NOT NULL,
  updated_at DOUBLE PRECISION NOT NULL,
  wake_at DOUBLE PRECISION,
  lease_token TEXT,
  lease_owner TEXT,
  lease_expires_at DOUBLE PRECISION,
  runs INTEGER NOT NULL DEFAULT 0,
  recoveries INTEGER NOT NULL DEFAULT 0,
  cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
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
  output JSONB,
  error JSONB,
  wake_at DOUBLE PRECISION,
  attempts INTEGER NOT NULL DEFAULT 0,
  started_at DOUBLE PRECISION,
  completed_at DOUBLE PRECISION,
  PRIMARY KEY (workflow_id, seq)
);
CREATE TABLE IF NOT EXISTS attempts (
  workflow_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  worker TEXT NOT NULL,
  started_at DOUBLE PRECISION NOT NULL,
  finished_at DOUBLE PRECISION,
  error TEXT,
  PRIMARY KEY (workflow_id, seq, attempt)
);
CREATE TABLE IF NOT EXISTS signals (
  id BIGSERIAL PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  name TEXT NOT NULL,
  payload JSONB,
  created_at DOUBLE PRECISION NOT NULL,
  consumed_seq INTEGER
);
CREATE INDEX IF NOT EXISTS signals_lookup ON signals (workflow_id, name, consumed_seq);
"""

WF_COLS = (
    "id, name, status, input, output, error, created_at, updated_at, wake_at, lease_token, lease_owner, "
    "lease_expires_at, runs, recoveries, cancel_requested, forked_from, fork_seq"
)
ENTRY_COLS = "workflow_id, seq, kind, name, status, output, error, wake_at, attempts, started_at, completed_at"

UPSERT_ENTRY = (
    f"INSERT INTO journal ({ENTRY_COLS}) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) "
    "ON CONFLICT (workflow_id, seq) DO UPDATE SET kind = EXCLUDED.kind, name = EXCLUDED.name, "
    "status = EXCLUDED.status, output = EXCLUDED.output, error = EXCLUDED.error, wake_at = EXCLUDED.wake_at, "
    "attempts = EXCLUDED.attempts, started_at = EXCLUDED.started_at, completed_at = EXCLUDED.completed_at"
)


def _wf(r: Any) -> WorkflowRecord:
    return WorkflowRecord(
        id=r["id"],
        name=r["name"],
        status=Status(r["status"]),
        input=serde.loads(r["input"]),
        output=serde.loads(r["output"]),
        error=serde.loads(r["error"]),
        created_at=r["created_at"],
        updated_at=r["updated_at"],
        wake_at=r["wake_at"],
        lease_token=r["lease_token"],
        lease_owner=r["lease_owner"],
        lease_expires_at=r["lease_expires_at"],
        runs=r["runs"],
        recoveries=r["recoveries"],
        cancel_requested=r["cancel_requested"],
        forked_from=r["forked_from"],
        fork_seq=r["fork_seq"],
    )


def _entry(r: Any) -> JournalEntry:
    return JournalEntry(
        workflow_id=r["workflow_id"],
        seq=r["seq"],
        kind=EntryKind(r["kind"]),
        name=r["name"],
        status=EntryStatus(r["status"]),
        output=serde.loads(r["output"]),
        error=serde.loads(r["error"]),
        wake_at=r["wake_at"],
        attempts=r["attempts"],
        started_at=r["started_at"],
        completed_at=r["completed_at"],
    )


def _entry_args(e: JournalEntry) -> tuple[Any, ...]:
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


async def _init_conn(conn: Any) -> None:
    # Keep JSONB as text both ways; serde handles (de)serialization.
    await conn.set_type_codec("jsonb", encoder=lambda v: v, decoder=lambda v: v, schema="pg_catalog", format="text")


class PostgresStore(Store):
    """``PostgresStore("postgresql://user:pass@host/db")``."""

    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 10):
        if asyncpg is None:  # pragma: no cover
            raise ImportError("PostgresStore needs asyncpg: pip install 'ninelives[postgres]'")
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self._pool: Any = None

    async def _get_pool(self) -> Any:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self.dsn, min_size=self.min_size, max_size=self.max_size, init=_init_conn)
        return self._pool

    async def _fenced(self, wf_id: str, token: str, body: Callable[[Any], Awaitable[T]]) -> T:
        pool = await self._get_pool()
        async with pool.acquire() as conn, conn.transaction():
            current = await conn.fetchval("SELECT lease_token FROM workflows WHERE id = $1 FOR UPDATE", wf_id)
            if current != token:
                raise LeaseLost(wf_id)
            return await body(conn)

    # ------------------------------------------------------------------ lifecycle
    async def init(self) -> None:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            # Serialise concurrent schema creation from several processes.
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(727274)")
                await conn.execute(SCHEMA)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

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
        pool = await self._get_pool()
        async with pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                f"INSERT INTO workflows (id, name, status, input, created_at, updated_at, forked_from, fork_seq) "
                f"VALUES ($1,$2,'PENDING',$3,$4,$4,$5,$6) ON CONFLICT (id) DO NOTHING RETURNING {WF_COLS}",
                wf_id,
                name,
                serde.dumps(input),
                now,
                forked_from,
                fork_seq,
            )
            if row is None:
                existing = await conn.fetchrow(f"SELECT {WF_COLS} FROM workflows WHERE id = $1", wf_id)
                return _wf(existing), False
            for e in journal:
                await conn.execute(UPSERT_ENTRY, *_entry_args(e))
            return _wf(row), True

    async def get_workflow(self, wf_id: str) -> WorkflowRecord | None:
        pool = await self._get_pool()
        row = await pool.fetchrow(f"SELECT {WF_COLS} FROM workflows WHERE id = $1", wf_id)
        return _wf(row) if row else None

    async def list_workflows(
        self, status: Status | None = None, name: str | None = None, limit: int = 50, offset: int = 0
    ) -> list[WorkflowRecord]:
        pool = await self._get_pool()
        rows = await pool.fetch(
            f"SELECT {WF_COLS} FROM workflows WHERE ($1::text IS NULL OR status = $1) AND ($2::text IS NULL OR name = $2) "
            "ORDER BY created_at DESC, id DESC LIMIT $3 OFFSET $4",
            str(status) if status is not None else None,
            name,
            limit,
            offset,
        )
        return [_wf(r) for r in rows]

    async def count_by_status(self) -> dict[str, int]:
        pool = await self._get_pool()
        rows = await pool.fetch("SELECT status, COUNT(*) AS n FROM workflows GROUP BY status")
        return {r["status"]: r["n"] for r in rows}

    async def send_signal(self, wf_id: str, name: str, payload: Any, now: float) -> Signal:
        pool = await self._get_pool()
        async with pool.acquire() as conn, conn.transaction():
            exists = await conn.fetchval("SELECT status FROM workflows WHERE id = $1 FOR UPDATE", wf_id)
            if exists is None:
                raise WorkflowNotFound(wf_id)
            sid = await conn.fetchval(
                "INSERT INTO signals (workflow_id, name, payload, created_at) VALUES ($1,$2,$3,$4) RETURNING id",
                wf_id,
                name,
                serde.dumps(payload),
                now,
            )
            await conn.execute(
                "UPDATE workflows SET wake_at = $1, updated_at = $1 WHERE id = $2 AND status = 'WAITING'", now, wf_id
            )
            return Signal(id=sid, workflow_id=wf_id, name=name, payload=serde.loads(serde.dumps(payload)), created_at=now)

    async def request_cancel(self, wf_id: str, now: float) -> WorkflowRecord:
        pool = await self._get_pool()
        async with pool.acquire() as conn, conn.transaction():
            status = await conn.fetchval("SELECT status FROM workflows WHERE id = $1 FOR UPDATE", wf_id)
            if status is None:
                raise WorkflowNotFound(wf_id)
            if status in ("PENDING", "WAITING"):
                await conn.execute(
                    "UPDATE workflows SET status = 'CANCELLED', cancel_requested = TRUE, error = $1, wake_at = NULL, "
                    "updated_at = $2 WHERE id = $3",
                    serde.dumps({"type": "Cancelled", "message": "cancelled by request"}),
                    now,
                    wf_id,
                )
            elif status == "RUNNING":
                await conn.execute("UPDATE workflows SET cancel_requested = TRUE, updated_at = $1 WHERE id = $2", now, wf_id)
            return _wf(await conn.fetchrow(f"SELECT {WF_COLS} FROM workflows WHERE id = $1", wf_id))

    async def get_journal(self, wf_id: str) -> list[JournalEntry]:
        pool = await self._get_pool()
        rows = await pool.fetch(f"SELECT {ENTRY_COLS} FROM journal WHERE workflow_id = $1 ORDER BY seq", wf_id)
        return [_entry(r) for r in rows]

    async def get_attempts(self, wf_id: str) -> list[Attempt]:
        pool = await self._get_pool()
        rows = await pool.fetch(
            "SELECT workflow_id, seq, attempt, worker, started_at, finished_at, error FROM attempts "
            "WHERE workflow_id = $1 ORDER BY seq, attempt",
            wf_id,
        )
        return [Attempt(*tuple(r)) for r in rows]

    async def get_signals(self, wf_id: str) -> list[Signal]:
        pool = await self._get_pool()
        rows = await pool.fetch(
            "SELECT id, workflow_id, name, payload, created_at, consumed_seq FROM signals WHERE workflow_id = $1 ORDER BY id",
            wf_id,
        )
        return [Signal(r["id"], r["workflow_id"], r["name"], serde.loads(r["payload"]), r["created_at"], r["consumed_seq"]) for r in rows]

    # ------------------------------------------------------------------ worker side
    async def claim(self, worker_id: str, now: float, ttl: float, names: Sequence[str], limit: int) -> list[WorkflowRecord]:
        if not names or limit <= 0:
            return []
        pool = await self._get_pool()
        rows = await pool.fetch(
            f"""
            WITH c AS (
              SELECT id, status AS prev FROM workflows
              WHERE name = ANY($1::text[]) AND (
                status = 'PENDING'
                OR (status = 'WAITING' AND wake_at IS NOT NULL AND wake_at <= $2)
                OR (status = 'RUNNING' AND lease_expires_at < $2))
              ORDER BY COALESCE(wake_at, created_at), created_at
              LIMIT $3
              FOR UPDATE SKIP LOCKED
            )
            UPDATE workflows w SET
              status = 'RUNNING', lease_token = md5(random()::text || clock_timestamp()::text || w.id),
              lease_owner = $4, lease_expires_at = $2 + $5, runs = w.runs + 1,
              recoveries = w.recoveries + CASE WHEN c.prev = 'RUNNING' THEN 1 ELSE 0 END, updated_at = $2
            FROM c WHERE w.id = c.id
            RETURNING {", ".join("w." + col.strip() for col in WF_COLS.split(","))}
            """,
            list(names),
            now,
            limit,
            worker_id,
            ttl,
        )
        return sorted((_wf(r) for r in rows), key=lambda w: (w.wake_at or w.created_at, w.created_at))

    async def heartbeat(self, wf_id: str, token: str, now: float, ttl: float) -> Heartbeat:
        pool = await self._get_pool()
        row = await pool.fetchrow(
            "UPDATE workflows SET lease_expires_at = $1 WHERE id = $2 AND lease_token = $3 AND status = 'RUNNING' "
            "RETURNING cancel_requested",
            now + ttl,
            wf_id,
            token,
        )
        if row is None:
            return Heartbeat(alive=False, cancel_requested=False)
        return Heartbeat(alive=True, cancel_requested=row["cancel_requested"])

    async def write_entry(self, token: str, entry: JournalEntry) -> None:
        async def body(conn: Any) -> None:
            await conn.execute(UPSERT_ENTRY, *_entry_args(entry))

        await self._fenced(entry.workflow_id, token, body)

    async def start_attempt(self, token: str, wf_id: str, seq: int, worker: str, now: float) -> int:
        async def body(conn: Any) -> int:
            n = await conn.fetchval(
                "SELECT COALESCE(MAX(attempt), 0) + 1 FROM attempts WHERE workflow_id = $1 AND seq = $2", wf_id, seq
            )
            await conn.execute(
                "INSERT INTO attempts (workflow_id, seq, attempt, worker, started_at) VALUES ($1,$2,$3,$4,$5)",
                wf_id,
                seq,
                n,
                worker,
                now,
            )
            return int(n)

        return await self._fenced(wf_id, token, body)

    async def finish_attempt(self, token: str, wf_id: str, seq: int, attempt: int, now: float, error: str | None) -> None:
        async def body(conn: Any) -> None:
            await conn.execute(
                "UPDATE attempts SET finished_at = $1, error = $2 WHERE workflow_id = $3 AND seq = $4 AND attempt = $5",
                now,
                error,
                wf_id,
                seq,
                attempt,
            )

        await self._fenced(wf_id, token, body)

    async def consume_signal(self, token: str, entry: JournalEntry, signal_name: str) -> Signal | None:
        async def body(conn: Any) -> Signal | None:
            row = await conn.fetchrow(
                "SELECT id, workflow_id, name, payload, created_at FROM signals "
                "WHERE workflow_id = $1 AND name = $2 AND consumed_seq IS NULL ORDER BY id LIMIT 1 FOR UPDATE",
                entry.workflow_id,
                signal_name,
            )
            if row is None:
                return None
            await conn.execute("UPDATE signals SET consumed_seq = $1 WHERE id = $2", entry.seq, row["id"])
            payload = serde.loads(row["payload"])
            done = JournalEntry(**{**entry.__dict__, "status": EntryStatus.COMPLETED, "output": {"payload": payload}})
            await conn.execute(UPSERT_ENTRY, *_entry_args(done))
            return Signal(row["id"], row["workflow_id"], row["name"], payload, row["created_at"], entry.seq)

        return await self._fenced(entry.workflow_id, token, body)

    async def finish(self, token: str, wf_id: str, status: Status, output: Any, error: dict[str, Any] | None, now: float) -> None:
        async def body(conn: Any) -> None:
            await conn.execute(
                "UPDATE workflows SET status = $1, output = $2, error = $3, wake_at = NULL, lease_token = NULL, "
                "lease_expires_at = NULL, updated_at = $4 WHERE id = $5",
                str(status),
                serde.dumps(output) if output is not None else None,
                serde.dumps(error) if error is not None else None,
                now,
                wf_id,
            )

        await self._fenced(wf_id, token, body)

    async def suspend(self, token: str, wf_id: str, wake_at: float | None, waiting_for: Sequence[str], now: float) -> Status:
        async def body(conn: Any) -> Status:
            cancel = await conn.fetchval("SELECT cancel_requested FROM workflows WHERE id = $1", wf_id)
            if cancel:
                await conn.execute(
                    "UPDATE workflows SET status = 'CANCELLED', error = $1, wake_at = NULL, lease_token = NULL, "
                    "lease_expires_at = NULL, updated_at = $2 WHERE id = $3",
                    serde.dumps({"type": "Cancelled", "message": "cancelled by request"}),
                    now,
                    wf_id,
                )
                return Status.CANCELLED
            wake = wake_at
            if waiting_for:
                ready = await conn.fetchval(
                    "SELECT 1 FROM signals WHERE workflow_id = $1 AND consumed_seq IS NULL AND name = ANY($2::text[]) LIMIT 1",
                    wf_id,
                    list(waiting_for),
                )
                if ready:
                    wake = now
            await conn.execute(
                "UPDATE workflows SET status = 'WAITING', wake_at = $1, lease_token = NULL, lease_expires_at = NULL, "
                "updated_at = $2 WHERE id = $3",
                wake,
                now,
                wf_id,
            )
            return Status.WAITING

        return await self._fenced(wf_id, token, body)

    async def release(self, token: str, wf_id: str) -> None:
        pool = await self._get_pool()
        await pool.execute("UPDATE workflows SET lease_expires_at = 0 WHERE id = $1 AND lease_token = $2", wf_id, token)
