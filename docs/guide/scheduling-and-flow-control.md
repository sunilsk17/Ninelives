# Schedules and flow control

## Cron schedules

```python
app.schedule("nightly-report", build_report, "0 2 * * *", tz="Europe/Berlin", args=["full"], overlap="skip")
```

Workers register the app's schedules when they start and fire them on time. However many workers
run, **exactly one run starts per tick**, with the id `<schedule id>@<tick time in UTC>`.

**Cron syntax** has five fields: minute, hour, day of month, month, day of week.

| Example | Meaning |
| --- | --- |
| `*/15 * * * *` | every 15 minutes |
| `0 9 * * MON-FRI` | 09:00 on weekdays |
| `30 8-18/2 * * *` | 08:30, 10:30 … 18:30 |
| `0 0 1 * *` | midnight on the 1st of each month |
| `0 0 13 * FRI` | the 13th **or** any Friday (classic cron: either day field matches) |
| `@hourly` `@daily` `@weekly` `@monthly` `@yearly` | the usual shortcuts |

Lists, ranges, steps and month/day names (`JAN`, `MON`) work; Sunday is 0 or 7.

**Time zones and DST.** Times follow the wall clock of `tz`. A time skipped by a daylight-saving
jump does not fire that day; a time that occurs twice fires once.

**Overlap.** With `overlap="skip"` (the default), a tick is skipped while the previous run of the
schedule is still unfinished. `overlap="allow"` always starts a new run.

**After downtime**, one run starts for the most recent missed tick. Missed ticks are not replayed.

**Manage schedules** from code, the CLI or the dashboard's Schedules page:

```python
await app.list_schedules()
await app.pause_schedule("nightly-report")  # resuming skips ticks missed while paused
await app.resume_schedule("nightly-report")
await app.trigger_schedule("nightly-report")  # run now, outside the cron times
```

```bash
ninelives schedules app.py:app                   # list
ninelives schedules app.py:app trigger nightly-report
```

The code is the source of truth. Changing a schedule's cron, time zone, input or overlap in code
updates it when a worker starts. Workers only fire schedules their own code declares, so removing
one from the code stops it. It stays listed (with its history) until you delete it with
`await app.store.delete_schedule(id)`.

## Concurrency limits

Protect a rate-limited API or a fragile downstream system:

```python
@app.workflow(concurrency=10, key_concurrency=1)
async def sync_customer(ctx, customer_id: str) -> None: ...


await app.start(sync_customer, "c-42", options=StartOptions(concurrency_key="c-42"))
```

- `concurrency=10`: at most ten runs of this workflow execute at once, across all workers.
- `key_concurrency=1`: at most one run per `concurrency_key` at once (here, per customer).
- Only runs that are actively executing count. Runs waiting on a timer or signal do not hold a slot.
- Limits are enforced inside the claim transaction, so concurrent workers cannot exceed them.

## Priority

```python
await app.start(send_invoice, invoice_id, options=StartOptions(priority=10))
```

Higher priority runs are picked up first. The default is 0, and negative values are allowed.

## Timeouts

```python
@app.workflow(timeout=3600)  # default for every run of this workflow
async def generate(ctx, doc_id: str) -> str: ...


await app.start(generate, "d-1", options=StartOptions(timeout=600))  # this run only
```

When a run passes its deadline, `WorkflowTimeout` is raised at its next durable operation, and the
run fails. A run that is waiting on a timer or signal wakes up at the deadline to fail. Step-level
timeouts are separate (`@app.step(timeout=...)`).

## Retention

Finished runs stay in the database until you remove them:

```python
await app.purge(older_than=30 * 86400)  # delete finished runs older than 30 days
```

```bash
ninelives purge app.py:app --older-than 30d
ninelives worker app.py:app --retention 30d   # a worker purges hourly
```

Purging deletes a run's journal, attempts, signals and events with it. Unfinished runs are never purged.
