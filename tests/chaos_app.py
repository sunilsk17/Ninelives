"""The workload the chaos test runs in real worker processes.

Every step writes two rows to a separate "outside world" SQLite database:

* ``executions``: one row per time the step body ran (at-least-once, so duplicates are allowed);
* ``ledger``: the side effect, keyed by the step's idempotency key (must end up exactly once).
"""

from __future__ import annotations

import asyncio
import os
import random
import sqlite3
from contextlib import closing

from ninelives import Ninelives, RetryPolicy, step_info

app = Ninelives(os.environ["CHAOS_STORE"])
LEDGER = os.environ["CHAOS_LEDGER"]
STEPS = int(os.environ.get("CHAOS_STEPS", "6"))


def setup_ledger(path: str) -> None:
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE IF NOT EXISTS executions (workflow_id TEXT, seq INTEGER, pid INTEGER)")
        db.execute(
            "CREATE TABLE IF NOT EXISTS ledger (key TEXT PRIMARY KEY, workflow_id TEXT, seq INTEGER, amount INTEGER)"
        )


def _record(amount: int) -> None:
    info = step_info()
    db = sqlite3.connect(LEDGER, timeout=30)
    try:
        with db:
            db.execute("INSERT INTO executions VALUES (?, ?, ?)", (info.workflow_id, info.seq, os.getpid()))
        with db:
            # What a payment provider does with an idempotency key: the second request is a no-op.
            db.execute(
                "INSERT INTO ledger VALUES (?, ?, ?, ?) ON CONFLICT(key) DO NOTHING",
                (info.idempotency_key, info.workflow_id, info.seq, amount),
            )
    finally:
        db.close()


@app.step(retry=RetryPolicy(max_attempts=20, initial_delay=0.01, max_delay=0.1))
async def move_money(amount: int) -> int:
    await asyncio.sleep(random.uniform(0.02, 0.1))  # widen the window for a crash mid-step
    await asyncio.to_thread(_record, amount)
    await asyncio.sleep(random.uniform(0.0, 0.02))  # ...and between the side effect and the journal write
    return amount


@app.workflow()
async def transfer(ctx, n: int) -> int:
    total = 0
    for i in range(STEPS):
        total += await move_money(n * 100 + i)
        if i == STEPS // 2 and n % 3 == 0:
            await ctx.sleep(1.2)  # long enough to suspend, so timers are exercised under chaos too
    return total
