# Getting started

This page takes you from nothing to a workflow that survives a crash, in about five minutes.

## Install

Ninelives needs Python 3.11 or newer. The core has no dependencies; extras add what you use:

```bash
pip install "ninelives[all] @ git+https://github.com/sunilsk17/Ninelives"
```

| Extra | Adds | You need it for |
| --- | --- | --- |
| `postgres` | asyncpg | Postgres instead of SQLite |
| `server` | FastAPI, uvicorn | the dashboard and HTTP API (`ninelives serve`) |
| `agents` | anthropic, httpx | the built-in LLM providers |
| `otel` | opentelemetry-api | tracing spans |
| `all` | everything above | |

## Your first workflow

Create `orders.py`:

```python
import asyncio
from ninelives import Ninelives

app = Ninelives("sqlite:///orders.db")  # or "postgresql://user:pass@host/db"


@app.step()
async def charge(order_id: str, cents: int) -> str:
    print(f"charging {order_id}")
    return f"ch_{order_id}"


@app.step()
async def ship(order_id: str) -> str:
    print(f"shipping {order_id}")
    return f"TRK-{order_id}"


@app.workflow()
async def checkout(ctx, order_id: str, cents: int) -> dict:
    receipt = await charge(order_id, cents)
    await ctx.sleep(10)  # a durable timer
    tracking = await ship(order_id)
    return {"receipt": receipt, "tracking": tracking}


async def main() -> None:
    async with app:
        run_id = await app.start(checkout, "A-1", 4999, id="checkout-A-1")
        worker = app.worker()
        task = asyncio.create_task(worker.run())
        print(await app.result(run_id, timeout=60))
        worker.stop()
        await task


try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("stopped: run me again to resume")
```

Run `python orders.py`. While it waits on the 10-second timer, press Ctrl+C, then run it again.
`charging A-1` is not printed a second time: the finished step came back from the journal, and the
run picked up at the timer. That is the whole idea of Ninelives.

A few rules make this work:

- **Workflows** are `async def` functions that take `ctx` first. They re-run from the top every time a
  worker resumes them, so they must be *deterministic*. Do I/O, time and randomness through steps or
  `ctx` helpers, never directly.
- **Steps** are where side effects happen. A finished step is recorded and never runs again.
- **Ids** passed to `app.start(..., id=...)` make starting idempotent. Starting the same id twice
  returns the existing run.

## Run workers separately

In a real deployment, the code that starts runs (your web app) and the workers that execute them
are different processes. Start any number of workers with the CLI:

```bash
ninelives worker orders.py:app                 # module path or file path, then the app variable
ninelives start orders.py:app checkout --args '["A-2", 1500]'
ninelives status orders.py:app checkout-A-1
```

`APP` is `path/to/file.py:variable` or `package.module:variable`. Every process must import the
same workflow code.

## Open the dashboard

```bash
ninelives serve orders.py:app --worker         # API + dashboard + a worker, on 127.0.0.1:8000
```

The dashboard lists runs, shows each run's timeline (every step and attempt, with timings and
errors), and lets you send signals, approve agent actions, cancel runs and re-run from any step.

## SQLite or Postgres?

| | SQLite | Postgres |
| --- | --- | --- |
| Setup | none: a file | a server you already run |
| Workers | processes on **one** machine | any number of machines |
| New-work wake-up | polling (200 ms by default) | instant (LISTEN/NOTIFY) |
| Good for | development, tests, single-box apps | production with several hosts |

Both pass the same test suite, including the crash tests. Switching is a one-line change of URL;
there is no data migration between them.

## Where next

- [Workflows and steps](workflows.md): retries, timers, signals, cancellation, parallel branches.
- [Composition](composition.md): child workflows, messages between runs, progress events.
- [Schedules and flow control](scheduling-and-flow-control.md): cron, concurrency limits, priority, timeouts.
- [Changing code safely](versioning.md): deploying changes while runs are in flight.
- [AI agents](agents.md): durable tool-calling agents with approvals.
- [Testing](testing.md): unit-test workflows without workers or waiting.
- [Operations](operations.md): deployment, the CLI, the HTTP API, metrics and tracing.
