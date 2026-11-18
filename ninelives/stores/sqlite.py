"""SQLite driver: zero dependencies, safe across processes on one machine (WAL + BEGIN IMMEDIATE).

Each transaction script runs entirely inside a worker thread, so a transaction costs one thread
hop no matter how many statements it has.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import sqlite3
import threading
import time
from collections.abc import Callable
from typing import Any, TypeVar

from .sql import Lock, Notify, Q, Script, SqlStore

T = TypeVar("T")

if sqlite3.sqlite_version_info < (3, 35, 0):  # pragma: no cover - depends on the platform's SQLite
    raise ImportError(
        f"Ninelives needs SQLite 3.35 or newer (RETURNING); this Python has {sqlite3.sqlite_version}"
    )

_TYPES = {
    "{JSON}": "TEXT",
    "{REAL}": "REAL",
    "{BOOL}": "INTEGER",
    "{TRUE}": "1",
    "{FALSE}": "0",
    "{SERIAL}": "INTEGER PRIMARY KEY AUTOINCREMENT",
}


class SQLiteStore(SqlStore):
    """``SQLiteStore("ninelives.db")``, or ``SQLiteStore(":memory:")`` for tests."""

    def __init__(self, path: str = "ninelives.db", busy_timeout_ms: int = 30_000):
        super().__init__()
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ driver
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None, timeout=self.busy_timeout_ms / 1000
        )
        conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        if self.path != ":memory:":
            self._enable_wal(conn)
            conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    def _enable_wal(self, conn: sqlite3.Connection) -> None:
        """Switch to WAL. Changing the journal mode needs exclusive access and does not wait on the
        busy timeout, so when several processes open a new database at once, retry until one of
        them has done it."""
        deadline = time.monotonic() + self.busy_timeout_ms / 1000
        while True:
            try:
                conn.execute("PRAGMA journal_mode = WAL")
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc) and "busy" not in str(exc):
                    raise
                with contextlib.suppress(sqlite3.OperationalError):
                    if str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower() == "wal":
                        return
                if time.monotonic() > deadline:
                    raise
                time.sleep(random.uniform(0.005, 0.03))

    @staticmethod
    def _sql(text: str) -> str:
        return text.replace("/*FOR_UPDATE*/", "").replace("/*SKIP_LOCKED*/", "")

    def _ddl(self, statement: str) -> str:
        for token, sql_type in _TYPES.items():
            statement = statement.replace(token, sql_type)
        return statement

    def _execute(self, conn: sqlite3.Connection, op: Q) -> Any:
        cur = conn.execute(self._sql(op.sql), op.args)
        if op.mode == "exec":
            return cur.rowcount
        if op.mode == "all":
            return cur.fetchall()
        row = cur.fetchone()
        if op.mode == "one":
            return row
        return None if row is None else row[0]

    async def _run(self, script: Callable[[], Script[T]], *, write: bool = True) -> T:
        def call() -> tuple[T, bool]:
            with self._lock:
                if self._conn is None:
                    self._conn = self._connect()
                conn = self._conn
                conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
                notify = False
                try:
                    gen = script()
                    result: Any = None
                    while True:
                        try:
                            op = gen.send(result)
                        except StopIteration as stop:
                            value: T = stop.value
                            break
                        if isinstance(op, Q):
                            result = self._execute(conn, op)
                        elif isinstance(op, Notify):
                            notify = True
                            result = None
                        elif isinstance(op, Lock):
                            result = None  # BEGIN IMMEDIATE already serialises writers
                        else:  # pragma: no cover
                            raise TypeError(f"unknown operation {op!r}")
                except BaseException:
                    conn.execute("ROLLBACK")
                    raise
                conn.execute("COMMIT")
                return value, notify

        value, notify = await asyncio.to_thread(call)
        if notify:
            self._waiters.wake()
        return value

    # ------------------------------------------------------------------ lifecycle
    async def init(self) -> None:
        await self._run(
            lambda: self._migrate("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")
        )

    async def close(self) -> None:
        def close() -> None:
            with self._lock:
                if self._conn is not None:
                    self._conn.close()
                    self._conn = None

        await asyncio.to_thread(close)
