# Changelog

All notable changes to this project are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [0.2.0] - 2026-11-19

### Added

- **Child workflows.** `ctx.start_child`, `ctx.wait_child` and `ctx.child`. Cancelling a parent
  cancels its children, and forks hand over finished children's results.
- **Messages between runs.** `ctx.send` signals another run exactly once.
- **Events.** `ctx.set_event` publishes progress or partial results; read them with `app.get_event`
  and `app.get_events`, through the API, or in the dashboard's Progress panel.
- **Cron schedules.** `app.schedule(...)` takes five-field cron with names and `@daily`-style
  shortcuts, time zones and DST handling. It starts exactly one run per tick, with an overlap policy
  and catch-up after downtime. Schedules can be paused, resumed and triggered.
- **Flow control.** `@app.workflow(concurrency=..., key_concurrency=...)` limits concurrent runs;
  `StartOptions(priority=..., concurrency_key=..., timeout=...)` sets per-run options.
- **Workflow timeouts.** A run fails with `WorkflowTimeout` at its deadline, including while it
  waits on a timer or signal.
- **Versioning.** `ctx.patched()` gives in-flight and new runs separate code paths.
- **Retention.** `app.purge()`, `ninelives purge`, and `Worker(retention=...)`.
- **Schema migrations.** Databases from 0.1 are upgraded in place on start, and databases from a
  newer version are refused.
- **Low-latency pickup on Postgres.** Workers wake on `LISTEN/NOTIFY` instead of waiting for the
  next poll.
- **Testing helpers.** `ninelives.testing.TestEnv`: an in-memory store and a clock you move by hand.
- **Synchronous client.** `ninelives.SyncClient` for Django, Flask and scripts.
- **Generic model provider.** `OpenAICompatibleLLM` for any chat-completions API in the OpenAI
  format, including local servers such as Ollama and vLLM.
- **Agent options.** `publish_progress` and `parallel_tools`.
- **HTTP API.** Bearer-token authentication (`NINELIVES_API_TOKEN`), start options, run search and
  filters, events, schedules, purge, and Prometheus `/metrics`.
- **CLI.** `--version`, `events`, `purge` and `schedules`; start options, `worker --retention`,
  `serve --token`; one-line errors instead of tracebacks.
- **Dashboard.** Sign-in, a Schedules page, child runs and parent links, progress, search, and run
  options.
- **OpenTelemetry spans** for run executions and step attempts (the `otel` extra).
- **Docs.** A full user guide under `docs/guide/`, plus `py.typed`, project URLs and classifiers.

### Changed

- **One transaction per step.** A finished step is recorded in one transaction instead of two;
  Postgres throughput went up about a third on the benchmark machine.
- **Shared store logic.** Store logic is written once as transaction scripts and run by thin SQLite
  and Postgres drivers.
- **Fewer spurious wake-ups.** A waiting run wakes only for the signals it waits for.
- **Schedules only fire if declared.** Workers fire only the schedules their code declares.

### Fixed

- **Child runs in the API.** Run ids containing `/` (child runs) returned 404 from the API.
- **Concurrent SQLite opens.** Several processes opening a new SQLite database at once could fail
  with "database is locked".
- **`run_until_idle` returned early.** It could return with work left when a task finished during a
  claim, such as a parent woken by its child or a freed concurrency slot.
- **Stale executions.** A worker that re-claimed its own lapsed run kept a stale execution alive.

## [0.1.0] - 2026-11-04

### Added

- **The durable execution engine:**
  - journaled steps with retries,
  - durable timers and signals,
  - cancellation, forks and `ctx.gather`,
  - deterministic helpers.
- **Stores and workers.** SQLite and Postgres stores, with workers that use leases, heartbeats and
  fencing tokens.
- **Durable agents** with tool approvals and budgets.
- **Interfaces.** HTTP API, dashboard and CLI.
- **Tests and tooling.** Multi-process chaos tests, browser tests, benchmarks, Docker Compose and CI.
