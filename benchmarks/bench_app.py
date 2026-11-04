"""Workloads for bench.py."""

from __future__ import annotations

import asyncio
import os

from ninelives import Ninelives

app = Ninelives(os.environ["BENCH_STORE"])


@app.step()
async def noop(i: int) -> int:
    return i


@app.step()
async def slow(seconds: float) -> str:
    await asyncio.sleep(seconds)
    return "done"


@app.workflow()
async def chain(ctx, steps: int) -> int:
    total = 0
    for i in range(steps):
        total += await noop(i)
    return total


@app.workflow()
async def crashy(ctx, seconds: float) -> str:
    return await slow(seconds)
