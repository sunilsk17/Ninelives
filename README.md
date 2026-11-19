# Ninelives

**Durable execution for Python.** Write long-running workflows and AI agents as ordinary `async`
Python. Ninelives records every step in Postgres or SQLite. If a worker is killed halfway (a
deploy, an out-of-memory error, `kill -9`), another worker picks the run up from the last finished
step: nothing is done twice, nothing is lost.

[![CI](https://github.com/sunilsk17/Ninelives/actions/workflows/ci.yml/badge.svg)](https://github.com/sunilsk17/Ninelives/actions/workflows/ci.yml)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

![A checkout run in the dashboard: the payment step failed twice, its worker was killed, and a second worker finished it on the third attempt](docs/images/run-timeline.png)

```python
from ninelives import Ninelives, step_info

app = Ninelives("postgresql://localhost/shop")     # or "sqlite:///shop.db"

@app.step()
async def charge(order_id: str, cents: int) -> str:
    # Retried with backoff if it fails. The key is stable across retries and crashes.
    return await payments.charge(order_id, cents, idempotency_key=step_info().idempotency_key)

@app.workflow()
async def checkout(ctx, order_id: str, cents: int) -> dict:
    receipt = await charge(order_id, cents)
    await ctx.sleep(24 * 3600)                          # no process waits; the run waits in the database
    review = await ctx.wait_for_signal("review", timeout=7 * 24 * 3600)
    return {"receipt": receipt, "review": review}
```

## Why

Anything that takes more than a few seconds will eventually be interrupted halfway: a deploy
restarts the machine, a container runs out of memory, a payment API times out, an AI agent's 40th
model call fails after real money was spent on the first 39. The usual fix is a queue, a status
column, retry loops and a lot of "where was I?" code.

Ninelives replaces that with one idea, the one behind Temporal and DBOS: **record each step, and
after a restart replay the record instead of redoing the work.** It is a library, not a platform.
There is no extra server to run, only the database you already have.

## What you get

- **Steps with retries.** Exponential backoff, timeouts, errors that should not be retried. Attempt
  counts survive crashes.
- **Durable timers and signals.** Sleep for a week, or wait for a human decision or a webhook,
  without holding a process.
- **Child workflows and messages.** Fan out to child runs and wait for their results, or signal
  other runs exactly once.
- **Progress events.** Publish progress or partial results while a run works, and read them from
  anywhere.
- **Cron schedules.** Time-zone and DST aware, with exactly one run per tick however many workers
  you have.
- **Flow control.** Concurrency limits per workflow and per key (say, one run per customer),
  priority, and timeouts.
- **Safe deploys.** Changed code is detected, not silently mis-replayed. `ctx.patched()` lets old
  and new runs take different paths. Database schema upgrades are automatic.
- **Crash safety, tested hard.** Leases, heartbeats and fencing tokens mean a frozen "zombie" worker
  can never overwrite the worker that replaced it.
- **Durable AI agents.** Every model call and tool call is a step. Risky tools wait for human
  approval. Use any OpenAI-compatible model, local models, or Anthropic.
- **A dashboard, HTTP API and CLI.** Timelines with every attempt, approvals, signals, schedules,
  cancel, and "re-run from this step".
- **Built for real use.** Testing helpers with a fake clock, a sync client for Django or Flask,
  Prometheus metrics, OpenTelemetry tracing, API tokens and retention.

## Quick start

```bash
git clone https://github.com/sunilsk17/Ninelives && cd Ninelives
uv sync --all-extras                  # or: pip install -e ".[all]"

uv run python examples/hello.py       # press Ctrl+C during the sleep, run it again: it resumes
```

To use it in your own project:

```bash
pip install "ninelives[all] @ git+https://github.com/sunilsk17/Ninelives"
```

The core has no dependencies. The extras are `postgres`, `server` (dashboard and API), `agents` and
`otel`. `all` installs them all.

### The demo shop

```bash
uv run ninelives serve examples/shop.py:app --worker     # then open http://127.0.0.1:8000
```

[`examples/shop.py`](examples/shop.py) is a small shop with:

- a checkout with a flaky payment provider and compensation,
- an expense approval that waits for a manager,
- a nightly report that fans out to child runs,
- a support agent whose refunds need a human.

Start runs from the dashboard, or:

```bash
uv run ninelives start examples/shop.py:app checkout --args '["order-1", 4999]'
uv run ninelives start examples/shop.py:app expense --args '["dana", 1200]' --id expense-dana
uv run ninelives signal examples/shop.py:app expense-dana decision --payload '{"approved": true, "by": "sam"}'
uv run ninelives schedules examples/shop.py:app trigger nightly-report
```

The support agent runs on a scripted stand-in model, so the demo works offline with no API key.
To use a real model, set `LLM_MODEL` (plus `LLM_BASE_URL` and `LLM_API_KEY` for a hosted
OpenAI-compatible API, or a local server like Ollama), or set `ANTHROPIC_API_KEY`.

### With Docker: Postgres, the dashboard and three workers

```bash
docker compose up --build                                               # http://localhost:8000
docker compose kill -s SIGKILL worker && docker compose up -d worker    # crash every worker; runs resume
```

## Documentation

| | |
| --- | --- |
| [Getting started](docs/guide/getting-started.md) | install, a first workflow, workers, the dashboard, SQLite vs Postgres |
| [Workflows and steps](docs/guide/workflows.md) | the replay model, retries, idempotency, timers, signals, cancel, fork, parallel branches |
| [Composition](docs/guide/composition.md) | child workflows, messages between runs, progress events |
| [Schedules and flow control](docs/guide/scheduling-and-flow-control.md) | cron, concurrency limits, priority, timeouts, retention |
| [Changing code safely](docs/guide/versioning.md) | deploying while runs are in flight, `ctx.patched`, upgrades |
| [AI agents](docs/guide/agents.md) | tools, model providers, approvals, budgets |
| [Testing](docs/guide/testing.md) | unit-testing workflows with a fake clock |
| [Operations](docs/guide/operations.md) | deployment, worker settings, HTTP API, CLI, security, metrics, tracing |
| [Architecture](docs/ARCHITECTURE.md) | how it works inside: data model, leases and fencing, guarantees |

## Durable AI agents

```python
from ninelives.agents import Agent, OpenAICompatibleLLM, tool

@tool
def lookup_order(order_id: str) -> dict:
    """Look up an order.

    order_id: the order id, like A-1001
    """
    return orders.get(order_id)

@tool(requires_approval=True)
def refund(order_id: str, amount_cents: int) -> dict:
    """Refund an order. A person approves every refund."""
    return payments.refund(order_id, amount_cents)

support = Agent(
    app, "support",
    llm=OpenAICompatibleLLM("llama3.1", base_url="http://localhost:11434/v1"),   # any compatible API
    tools=[lookup_order, refund],
    system="You are the support agent for a small shop.",
    max_turns=10,
)
run_id = await support.start("My teapot arrived broken, order A-1001")
```

If the worker dies after the third tool call, the agent resumes at the fourth, and the earlier model
responses come from the journal, so they are not paid for again. A refund waits, durably and for
free, until someone approves or denies it in the dashboard, the API or code.

![An agent run waiting for approval to issue a refund](docs/images/agent-approval.png)

## How it works

A workflow is an `async` function. Each durable operation (a step, timer, signal wait, child start
or event) gets the next position in the run's journal. When a worker runs a workflow, it starts at
the top. Operations already in the journal return their recorded result immediately, so execution
fast-forwards to the first unfinished one.

Workers claim runs with an expiring lease and heartbeat while they work. Every write a worker makes
checks its lease token in the same transaction. When a worker dies, its lease expires and another
worker replays the run. When a worker merely freezes and comes back, it finds its token rejected
and stops. The full design is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

| Guarantee | What it means |
| --- | --- |
| Workflow code | re-executed on every resume, so it must be deterministic: use steps and `ctx` helpers for I/O, time and randomness |
| Steps | at least once; a step re-runs only if its worker dies between the side effect and recording it |
| Side effects | exactly once when the system you call honours `step_info().idempotency_key` |
| Signals and messages | delivered exactly once, in order |
| Schedules | exactly one run per tick |
| Retries | `max_attempts` holds across crashes |

## How do we know it works?

- **390 automated tests**, run on in-memory SQLite, file SQLite and Postgres 16, on Python 3.11–3.13.
- **Chaos tests** ([`tests/test_chaos.py`](tests/test_chaos.py)) run real worker processes and
  `SIGKILL` them (and freeze them with `SIGSTOP` past their lease) at random:
  - while 80 workflows run,
  - and again while 20 parents fan out to 60 child runs.

  Every run must succeed, journals must have no gaps, and an external ledger must record each of the
  hundreds of side effects exactly once, even though some steps really did run twice.
- **12 browser tests** drive the dashboard against a live server: starting runs, signals, approvals,
  forks, cancellation, schedules, child runs, search, sign-in and the mobile layout.
- **A Docker test** in CI starts Postgres, the server and workers with `docker compose`, then runs a
  workflow end to end.
- **Benchmarks** ([`benchmarks/RESULTS.md`](benchmarks/RESULTS.md)), from one 4-vCPU container:

  | Store | Workflows/s (10 steps each) | Per-step latency | New run picked up by an idle worker | Recovery after `kill -9` (2 s lease) |
  |---|---|---|---|---|
  | SQLite | 127 | 0.8 ms | 152 ms (polling) | 2.4 s |
  | Postgres 16 | 168 | 3.0 ms | 11 ms (LISTEN/NOTIFY) | 2.4 s |

## How it compares

| | Ninelives | Temporal | DBOS | Celery |
| --- | --- | --- | --- | --- |
| Infrastructure | your Postgres or SQLite | a Temporal cluster | your Postgres | a broker (Redis, RabbitMQ) |
| Resumes a function mid-way after a crash | yes | yes | yes | no, the task restarts |
| Durable timers, signals, child workflows | yes | yes | yes | no |
| Cron schedules, concurrency limits | yes | yes | yes | partly |
| Agent tool approvals | built in | build it yourself | build it yourself | no |
| Maturity | young and small: you can read all of it | battle-tested | production | battle-tested |

Choose Temporal or DBOS for large production fleets with dedicated teams. Ninelives is for teams who
want the same model as a small, readable library inside their app, on the database they already run.

## Project layout

```text
ninelives/             the library
  app.py               registering workflows and steps; starting, signalling, inspecting runs
  context.py           replay, steps, timers, signals, children, events, ctx.gather, patched()
  worker.py            claiming, leases, heartbeats, schedules, recovery
  stores/              one transaction-script implementation, SQLite and Postgres drivers, migrations
  agents/              the agent loop, tools, model providers
  server/              HTTP API and the built dashboard
  cli.py, cron.py, testing.py, sync.py, tracing.py
dashboard/             dashboard source (React, Vite, Tailwind) and its browser tests
examples/              hello.py and shop.py
tests/                 unit, contract, engine, API, CLI, chaos and tracing tests
benchmarks/            the benchmark script and results
docs/                  guide, architecture, research and roadmap
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development setup, how to run each test suite, and
the release process. Changes are listed in [CHANGELOG.md](CHANGELOG.md). Security issues:
[SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE) © Sunil Kumar
