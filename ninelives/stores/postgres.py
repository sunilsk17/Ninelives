"""Postgres driver (asyncpg).

Workers claim with ``FOR UPDATE SKIP LOCKED``; fenced writes lock the workflow row and compare the
lease token inside the same transaction. New work is announced with ``NOTIFY ninelives`` so idle
workers wake at once instead of waiting for their next poll.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable
from typing import Any, TypeVar

from .sql import Lock, Notify, Q, Script, SqlStore

try:  # optional dependency
    import asyncpg
except ImportError:  # pragma: no cover - exercised only without the extra
    asyncpg = None

T = TypeVar("T")
log = logging.getLogger("ninelives")
CHANNEL = "ninelives"
_TYPES = {
    "{JSON}": "JSONB",
    "{REAL}": "DOUBLE PRECISION",
    "{BOOL}": "BOOLEAN",
    "{TRUE}": "TRUE",
    "{FALSE}": "FALSE",
    "{SERIAL}": "BIGSERIAL PRIMARY KEY",
}
_RETRYABLE = ("40P01", "40001")  # deadlock detected, serialization failure


async def _init_conn(conn: Any) -> None:
    # Keep JSONB as text both ways; serde handles (de)serialization.
    await conn.set_type_codec(
        "jsonb", encoder=lambda v: v, decoder=lambda v: v, schema="pg_catalog", format="text"
    )


def _rowcount(status: str) -> int:
    last = status.rsplit(" ", 1)[-1]
    return int(last) if last.isdigit() else 0


class PostgresStore(SqlStore):
    """``PostgresStore("postgresql://user:pass@host/db")``."""

    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 10, *, listen: bool = True):
        if asyncpg is None:  # pragma: no cover
            raise ImportError("PostgresStore needs asyncpg: pip install 'ninelives[postgres]'")
        super().__init__()
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.listen = listen
        self._pool: Any = None
        self._listener: Any = None
        self._listen_lock = asyncio.Lock()
        self._sql_cache: dict[str, str] = {}

    def fresh(self) -> PostgresStore:
        return PostgresStore(self.dsn, self.min_size, self.max_size, listen=self.listen)

    async def _get_pool(self) -> Any:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self.dsn, min_size=self.min_size, max_size=self.max_size, init=_init_conn
            )
        return self._pool

    # ------------------------------------------------------------------ driver
    def _sql(self, text: str) -> str:
        cached = self._sql_cache.get(text)
        if cached is None:
            counter = iter(range(1, 10_000))
            cached = re.sub(r"\?", lambda _: f"${next(counter)}", text)
            cached = cached.replace("/*FOR_UPDATE*/", "FOR UPDATE").replace(
                "/*SKIP_LOCKED*/", "FOR UPDATE SKIP LOCKED"
            )
            self._sql_cache[text] = cached
        return cached

    def _ddl(self, statement: str) -> str:
        for token, sql_type in _TYPES.items():
            statement = statement.replace(token, sql_type)
        return statement

    async def _execute(self, conn: Any, op: Q) -> Any:
        sql = self._sql(op.sql)
        if op.mode == "exec":
            return _rowcount(await conn.execute(sql, *op.args))
        if op.mode == "all":
            return await conn.fetch(sql, *op.args)
        if op.mode == "one":
            return await conn.fetchrow(sql, *op.args)
        return await conn.fetchval(sql, *op.args)

    async def _run(self, script: Callable[[], Script[T]], *, write: bool = True) -> T:
        pool = await self._get_pool()
        for attempt in range(1, 6):
            notify = False
            try:
                async with pool.acquire() as conn, conn.transaction(readonly=not write):
                    gen = script()
                    result: Any = None
                    while True:
                        try:
                            op = gen.send(result)
                        except StopIteration as stop:
                            value: T = stop.value
                            break
                        if isinstance(op, Q):
                            result = await self._execute(conn, op)
                        elif isinstance(op, Lock):
                            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", op.key)
                            result = None
                        elif isinstance(op, Notify):
                            if not notify:
                                await conn.execute("SELECT pg_notify($1, '')", CHANNEL)
                            notify = True
                            result = None
                        else:  # pragma: no cover
                            raise TypeError(f"unknown operation {op!r}")
            except asyncpg.PostgresError as exc:
                if getattr(exc, "sqlstate", None) in _RETRYABLE and attempt < 5:
                    log.debug("retrying a transaction after %s", type(exc).__name__)
                    await asyncio.sleep(0.005 * attempt)
                    continue
                raise
            if notify:
                self._waiters.wake()
            return value
        raise AssertionError("unreachable")  # pragma: no cover

    # ------------------------------------------------------------------ notifications
    async def _ensure_listener(self) -> None:
        if not self.listen or self._listener is not None:
            return
        async with self._listen_lock:
            if self._listener is not None:
                return
            try:
                conn = await asyncpg.connect(self.dsn)
                await conn.add_listener(CHANNEL, lambda *_: self._waiters.wake())
                conn.add_termination_listener(lambda _c: self._drop_listener())
                self._listener = conn
            except Exception:  # LISTEN is an optimisation: fall back to polling
                log.warning("could not LISTEN for new work; falling back to polling", exc_info=True)
                self.listen = False

    def _drop_listener(self) -> None:
        self._listener = None

    async def wait_for_work(self, timeout: float) -> None:
        await self._ensure_listener()
        await super().wait_for_work(timeout)

    # ------------------------------------------------------------------ lifecycle
    async def init(self) -> None:
        await self._run(lambda: self._migrate("SELECT to_regclass($1) IS NOT NULL".replace("$1", "?")))

    async def close(self) -> None:
        if self._listener is not None:
            listener, self._listener = self._listener, None
            try:
                await listener.close()
            except Exception:  # pragma: no cover - closing a broken connection
                pass
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
