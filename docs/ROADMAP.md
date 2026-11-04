# Ninelives v0.2 plan

Derived from [RESEARCH.md](RESEARCH.md). v0.1 delivered a correct engine; v0.2 makes it a library
other teams can adopt: composition, flow control, schedules, change management, operations and
developer experience. Every item ships with tests on SQLite and Postgres, docs, and (where visible)
dashboard support.

Status legend: ✅ done · 🚧 in progress · ⏳ planned

## Work items, in build order

| # | Item | Why (research §) | Status |
|---|---|---|---|
| 1 | Schema migrations | Upgrades must not need manual SQL (§4) | ✅ |
| 2 | One transaction per finished step | Hot-path cost; Postgres queue guidance (§3.3) | ✅ |
| 3 | `ctx.patched()` versioning | Main production failure mode (§3.1) | ✅ |
| 4 | Events: `ctx.set_event` / `app.get_event` | Progress and results while running (§2) | ✅ |
| 5 | Child workflows and `ctx.send` | Composition (§2, §5.1) | ✅ |
| 6 | Workflow timeouts | Flow control (§2) | ✅ |
| 7 | Concurrency limits (global and per key) and priority | Flow control (§3.4) | ✅ |
| 8 | Cron schedules | Time-based work (§2) | ✅ |
| 9 | Retention / purge | Operations (§2) | ✅ |
| 10 | Postgres LISTEN/NOTIFY wake-ups | Latency (§2) | ✅ |
| 11 | Testing helpers and a sync client | Developer experience (§4) | ⏳ |
| 12 | API: tokens, events, schedules, metrics; CLI additions | Operations (§3.6, §4) | ⏳ |
| 13 | Dashboard: sign-in, search, events, children, schedules | Debuggability (§3.2) | ⏳ |
| 14 | Agents: progress events, parallel tool calls | Agents (§3.5) | ⏳ |
| 15 | OpenTelemetry spans (optional dependency) | Observability (§3.6) | ⏳ |
| 16 | Packaging, docs set, changelog, release workflow | Adoption (§4) | ⏳ |

## Designs

### 1. Schema migrations
A `ninelives_schema(version)` table. `Store.init()` takes a lock (Postgres: an advisory
transaction lock; SQLite: `BEGIN IMMEDIATE`), reads the version and applies the missing migrations in
order, in the same transaction. Migration 1 is the v0.1 schema. A database created by v0.1 (tables
present, no version table) is recognised as version 1 and upgraded in place.

### 2. One transaction per finished step
v0.1 committed a successful step in two fenced transactions: finishing the attempt, then writing
the journal entry. `Store.complete_step(token, entry, attempt, error)` does both in one, and also
covers a step's final failure. Measured with `benchmarks/bench.py` before and after.

### 3. `ctx.patched(patch_id)`
```python
if await ctx.patched("send-sms"):
    await send_sms(order)  # new code
else:
    await send_email(order)  # what runs that started earlier still do
```
- **Replaying** (the next journal position exists): returns `True` and consumes the position only if that entry is the marker `patch:<id>`. Otherwise it returns `False` without consuming anything, because the run executed this point before the patch existed.
- **Not replaying:** journals the marker and returns `True`.
- **Removing a patch later:** a patch marker is skipped transparently when the code at that position no longer calls `patched()`.
- **Not allowed inside `ctx.gather` branches,** where positions are shared: it raises with a clear message.

### 4. Events
`await ctx.set_event(key, value)` publishes (or overwrites) a key/value pair visible outside the
workflow. It is journaled, so a replay does not publish again. The journal entry and the event row
are written in one fenced transaction. `await app.get_event(wf_id, key, timeout=None)` waits for a
key; `app.get_events(wf_id)` returns them all. They are exposed by the API and shown in the dashboard
as "Progress". Use them for progress bars, partial results and agent status.

### 5. Child workflows, `ctx.send`
```python
child_id = await ctx.start_child(resize, image_id)  # durable; id defaults to "<parent>/<seq>"
result = await ctx.wait_child(child_id, timeout=3600)  # raises ChildFailed
result = await ctx.child(resize, image_id)  # both at once
await ctx.send(other_id, "go", payload)  # signal another run, exactly once
```
- **Starting.** `start_child` journals the start and creates the child row (with `parent_id` and `parent_seq`) in one fenced transaction. A replay finds the journaled id and creates nothing.
- **Completion.** When a child finishes, the same transaction inserts a signal `child:<id>` into the parent with `{status, output, error}` and wakes the parent. `wait_child` is a signal wait, so it is durable, costs nothing while waiting, and needs no polling.
- **Cancellation** cascades to non-terminal descendants.
- **`ctx.send`** journals the send and inserts the signal in one fenced transaction, so a crash cannot duplicate it.

### 6. Workflow timeouts
Set with `app.start(..., timeout=s)` or `@app.workflow(timeout=s)`, which stores a `deadline`.
- **While running:** every durable operation checks the deadline and raises `WorkflowTimeout`. It behaves like `Cancelled`: catchable for cleanup, but raised again at the next operation.
- **While suspended:** the wake time is capped at the deadline, so a waiting run wakes up to fail.
- **Result:** the run finishes `FAILED` with an error of type `WorkflowTimeout`.

### 7. Concurrency limits and priority
`@app.workflow(concurrency=10, key_concurrency=1)` and
`app.start(..., concurrency_key="user-42", priority=5)`.
- **Limits count live runs only:** `RUNNING` with an unexpired lease. Waiting runs do not count, as in Inngest.
- **Claim order:** `priority DESC`, then wake or creation time.
- **Enforcement** happens inside the claim transaction. Postgres takes an advisory transaction lock per limited workflow name, so concurrent workers cannot both see a free slot; SQLite's `BEGIN IMMEDIATE` already serialises claims.

### 8. Cron schedules
```python
app.schedule("nightly-report", report, cron="0 2 * * *", tz="Europe/Berlin", args=[...], overlap="skip")
```
- **Cron syntax:** five fields with lists, ranges, steps and names, `@hourly`/`@daily`/`@weekly`/`@monthly`/`@yearly`, and standard day-of-month OR day-of-week semantics. Times are computed in the given time zone with `zoneinfo` (DST-safe).
- **Storage:** schedules live in a `schedules` table with `next_at`. The code is the source of truth: workers upsert registered schedules at start-up, and schedules no longer in the code are disabled.
- **Firing:** each worker poll fires due schedules with a compare-and-set on `next_at`, so exactly one worker wins each tick. The run id is `<schedule>@<UTC ISO tick>`, which also makes the start idempotent.
- **Overlap:** `skip` (the default) does not start a tick while the previous run is unfinished; `allow` starts anyway.
- **After downtime:** only the most recent missed tick runs.
- **API and dashboard:** list, pause and resume schedules, or trigger one now.

### 9. Retention
`await app.purge(older_than=30 * 86400)` deletes finished runs (and their journal, attempts, signals
and events) last updated before the cutoff. The same is available as `ninelives purge APP --older-than 30d`
and `Worker(retention=...)`, which purges once an hour.

### 10. LISTEN/NOTIFY
The Postgres store sends `NOTIFY ninelives` on new work: a start, a signal, a child completion or a
schedule fire. Workers wait on the notification or the poll interval, whichever comes first, so
pickup latency drops from the poll interval to milliseconds. Polling remains the fallback, and
SQLite keeps polling.

### 11. Testing helpers and sync client
```python
from ninelives.testing import TestEnv

async with TestEnv(app) as env:  # in-memory store, fake clock, no threads
    run = await env.start(checkout, "o-1")
    await env.run_until_idle()
    await env.advance(3600)  # moves the clock and runs what woke up
    assert await env.result(run) == {...}
```
`ninelives.SyncClient(app)` exposes `start`, `result`, `signal`, `cancel`, `get` and `get_event`
for Django/Flask code. It runs a private event loop in a background thread.

### 12. API and CLI
- **Authentication:** if `NINELIVES_API_TOKEN` (or `serve --token`) is set, every `/api` route except health requires `Authorization: Bearer <token>`.
- **New routes:** events, schedules (list, pause, resume, trigger), purge, children, and `/metrics` in Prometheus text format.
- **New CLI commands:** `purge`, `schedules`, and `events`.

### 13. Dashboard
- Token sign-in.
- Search by run id.
- A progress (events) panel.
- Parent and child links.
- Deadline, priority and concurrency key on the run page.
- A schedules page with pause, resume and run-now.

### 14. Agents
- **Progress events:** after each turn the agent publishes `agent.progress`: turn, tokens and the last action.
- **Parallel tool calls:** `parallel_tools=True` runs one turn's tool calls through `ctx.gather`, and results return in call order.

### 15. OpenTelemetry
If `opentelemetry-api` is installed and tracing is enabled, the worker emits a span per workflow
execution and a span per step attempt. Attributes cover workflow id and name, sequence number,
attempt and outcome. Replayed steps emit nothing, because they do not execute. There is no hard
dependency.

### 16. Packaging and docs
- **Package:** version 0.2.0, `py.typed` and project URLs.
- **Repository files:** `CHANGELOG.md`, `CONTRIBUTING.md` and `SECURITY.md`.
- **Docs:** `docs/guide/` with one page per feature, plus a tag-triggered release workflow for PyPI trusted publishing. The repository owner enables it on PyPI; nothing is published automatically.
