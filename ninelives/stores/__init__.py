from __future__ import annotations

from .base import Heartbeat, Store
from .sqlite import SQLiteStore


def open_store(url: str) -> Store:
    """Build a store from a URL.

    - ``postgresql://user:pass@host:5432/db``
    - ``sqlite:///relative/path.db``, ``sqlite:////absolute/path.db``, ``sqlite://:memory:``
    - a bare file path is treated as SQLite
    """
    if url.startswith(("postgresql://", "postgres://")):
        from .postgres import PostgresStore

        return PostgresStore(url)
    if url.startswith("sqlite://"):
        path = url.removeprefix("sqlite://")
        if path.startswith("/"):
            path = path[1:]
        return SQLiteStore(path or ":memory:")
    return SQLiteStore(url)


__all__ = ["Heartbeat", "SQLiteStore", "Store", "open_store"]
