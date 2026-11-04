from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest

from ninelives import Ninelives, SQLiteStore, Store

PG_DSN = os.environ.get("NINELIVES_TEST_POSTGRES_DSN")


class FakeClock:
    """A clock tests can move forward by hand."""

    def __init__(self, start: float = 1_800_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


async def _postgres_store() -> Store:
    from ninelives.stores.postgres import PostgresStore

    store = PostgresStore(PG_DSN or "")
    await store.init()
    pool = await store._get_pool()
    await pool.execute("TRUNCATE workflows, journal, attempts, signals, events, schedules RESTART IDENTITY")
    return store


BACKENDS = ["sqlite-memory", "sqlite-file"] + (["postgres"] if PG_DSN else [])


@pytest.fixture(params=BACKENDS)
async def store(request: pytest.FixtureRequest, tmp_path) -> AsyncIterator[Store]:
    if request.param == "sqlite-memory":
        s: Store = SQLiteStore(":memory:")
        await s.init()
    elif request.param == "sqlite-file":
        s = SQLiteStore(str(tmp_path / "nl.db"))
        await s.init()
    else:
        s = await _postgres_store()
    yield s
    await s.close()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
async def app(store: Store) -> AsyncIterator[Ninelives]:
    a = Ninelives(store)
    await a.init()
    yield a
