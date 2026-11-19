"""Measure throughput, per-step overhead and crash-recovery latency.

    uv run python benchmarks/bench.py                       # SQLite
    uv run python benchmarks/bench.py --postgres postgresql://localhost/ninelives

Workers are real processes started with the ``ninelives worker`` command.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import platform
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from ninelives import Ninelives, Status

HERE = Path(__file__).parent


def spawn(store: str, *extra: str) -> subprocess.Popen[bytes]:
    poll = [] if "--poll" in extra else ["--poll", "0.01"]
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "ninelives",
            "worker",
            f"{HERE / 'bench_app.py'}:app",
            *poll,
            *extra,
        ],
        env={**os.environ, "BENCH_STORE": store},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def stop(procs: list[subprocess.Popen[bytes]]) -> None:
    for p in procs:
        p.send_signal(signal.SIGTERM)
    for p in procs:
        p.wait(timeout=30)


async def reset(store: str) -> Ninelives:
    """Start every measurement from an empty store."""
    if store.startswith("sqlite:///"):
        for suffix in ("", "-wal", "-shm"):
            Path(store.removeprefix("sqlite:///") + suffix).unlink(missing_ok=True)
    app = Ninelives(store)
    await app.init()
    if store.startswith("postgres"):
        pool = await app.store._get_pool()  # type: ignore[attr-defined]
        await pool.execute(
            "TRUNCATE workflows, journal, attempts, signals, events, schedules RESTART IDENTITY"
        )
    return app


async def wait_all(app: Ninelives, n: int, timeout: float = 600) -> None:
    deadline = time.monotonic() + timeout
    while (await app.store.count_by_status()).get(str(Status.SUCCEEDED), 0) < n:
        if time.monotonic() > deadline:
            raise TimeoutError(await app.store.count_by_status())
        await asyncio.sleep(0.02)


async def throughput(store: str, workflows: int, steps: int, workers: int) -> dict[str, float]:
    app = await reset(store)
    for i in range(workflows):
        await app.start("chain", steps, id=f"tp-{i}")
    procs = [spawn(store, "--concurrency", "32") for _ in range(workers)]
    t0 = time.perf_counter()
    try:
        await wait_all(app, workflows)
    finally:
        elapsed = time.perf_counter() - t0
        stop(procs)
        await app.close()
    return {
        "seconds": elapsed,
        "workflows_per_s": workflows / elapsed,
        "steps_per_s": workflows * steps / elapsed,
    }


async def step_latency(store: str, steps: int) -> float:
    """Wall time per step of one long sequential workflow: the cost of journaling a step."""
    app = await reset(store)
    await app.start("chain", steps, id="latency")
    proc = spawn(store)
    t0 = time.perf_counter()
    try:
        await wait_all(app, 1)
    finally:
        elapsed = time.perf_counter() - t0
        stop([proc])
        await app.close()
    return elapsed / steps * 1000


async def pickup(store: str, runs: int = 20) -> float:
    """Median time from start() to a finished one-step run, with an idle worker in another process
    (default 200 ms poll interval). Postgres workers are woken by NOTIFY; SQLite workers poll."""
    app = await reset(store)
    proc = spawn(store, "--poll", "0.2")
    samples = []
    try:
        await asyncio.sleep(1.0)  # let the worker start and go idle
        for i in range(runs):
            t0 = time.perf_counter()
            await app.start("chain", 1, id=f"pickup-{i}")
            await app.result(f"pickup-{i}", timeout=30, poll=0.002)
            samples.append(time.perf_counter() - t0)
            await asyncio.sleep(0.05)
    finally:
        stop([proc])
        await app.close()
    return sorted(samples)[len(samples) // 2] * 1000


async def recovery(store: str, lease_ttl: float, trials: int) -> list[float]:
    """SIGKILL the worker in the middle of a step; time until a second worker finishes the run."""
    results = []
    for t in range(trials):
        app = await reset(store)
        wf_id = await app.start("crashy", 0.5, id=f"crash-{t}")
        victim = spawn(store, "--lease-ttl", str(lease_ttl))
        while (await app.get(wf_id)).status != Status.RUNNING:
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.1)  # now inside the 0.5s step
        victim.send_signal(signal.SIGKILL)
        victim.wait()
        killed = time.perf_counter()
        rescuer = spawn(store, "--lease-ttl", str(lease_ttl))
        try:
            await wait_all(app, 1)
            results.append(time.perf_counter() - killed)
        finally:
            stop([rescuer])
            await app.close()
    return results


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--postgres", help="also benchmark this Postgres DSN")
    parser.add_argument("--workflows", type=int, default=500)
    parser.add_argument("--steps", type=int, default=10)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    tmp = tempfile.mkdtemp(prefix="ninelives-bench-")
    stores = {"SQLite (WAL)": f"sqlite:///{tmp}/bench.db"}
    if args.postgres:
        stores["Postgres"] = args.postgres

    print(
        f"# Ninelives benchmark\n\n{platform.platform()}, Python {platform.python_version()}, {os.cpu_count()} CPUs\n"
    )
    print(f"Throughput: {args.workflows} workflows x {args.steps} steps, {args.workers} worker processes.\n")
    print(
        "| Store | Workflows/s | Steps/s | Per-step latency | Start-to-finish, idle worker "
        "| Recovery after SIGKILL (lease TTL 2s) |"
    )
    print("|---|---|---|---|---|---|")
    for label, url in stores.items():
        tp = await throughput(url, args.workflows, args.steps, args.workers)
        lat = await step_latency(url, 300)
        pick = await pickup(url)
        rec = await recovery(url, 2.0, 3)
        print(
            f"| {label} | {tp['workflows_per_s']:.0f} | {tp['steps_per_s']:.0f} | {lat:.2f} ms "
            f"| {pick:.0f} ms | {min(rec):.2f}–{max(rec):.2f} s |",
            flush=True,
        )
    print(
        "\nRecovery time is bounded by the lease TTL (time for the dead worker's lease to expire) plus one poll."
    )


if __name__ == "__main__":
    asyncio.run(main())
