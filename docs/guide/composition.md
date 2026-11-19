# Composition: child workflows, messages and progress

## Child workflows

Split big processes into runs of their own. Each child has its own journal, retries, timeline and
dashboard page, and the parent waits for it durably:

```python
@app.workflow()
async def onboard_customer(ctx, customer_id: str) -> dict:
    # Fan out...
    account = await ctx.start_child(create_account, customer_id)
    billing = await ctx.start_child(setup_billing, customer_id)
    # ...and fan in.
    return {
        "account": await ctx.wait_child(account),
        "billing": await ctx.wait_child(billing),
    }


@app.workflow()
async def nightly(ctx) -> int:
    report = await ctx.child(build_report, "2026-11-18")  # start and wait in one call
    return report["rows"]
```

- **Starting is durable.** A parent that crashes and resumes does not start its children again.
- **Ids.** A child's run id is `<parent id>/<position>` unless you pass `id=...`. The id must not
  belong to an unrelated run.
- **Options.** Children accept `options=StartOptions(...)` for priority, a concurrency key and a timeout.
- **Failure.** If a child fails or is cancelled, `wait_child` raises `ChildFailed` with `.status`
  and `.error`. `wait_child(child_id, timeout=...)` raises `SignalTimeout` after the timeout.
  Wait for each child once.
- **Cancellation** of the parent cancels all of its unfinished descendants.
- **Forks** of the parent hand over finished children's results. A fork that would need a child
  that is still running is refused with a clear error.

## Messages between runs

`ctx.send` signals another run, exactly once, even if the sender crashes and resumes:

```python
@app.workflow()
async def payment_watcher(ctx, order_run_id: str) -> None:
    await wait_for_settlement()
    await ctx.send(order_run_id, "paid", {"at": await ctx.now()})
```

The receiver uses `ctx.wait_for_signal("paid")` as usual. Sending to a run that does not exist
raises `WorkflowNotFound` inside the sender.

## Progress and partial results (events)

A run can publish key/value **events** while it works, for progress bars, intermediate results or
status for a UI:

```python
@app.workflow()
async def import_file(ctx, path: str) -> int:
    rows = await read_rows(path)
    for i, chunk in enumerate(chunks(rows, 1000)):
        await load_chunk(chunk)
        await ctx.set_event("progress", {"done": (i + 1) * 1000, "total": len(rows)})
    return len(rows)
```

Read them from outside:

```python
await app.get_event(run_id, "progress")  # latest value (KeyError if never set)
await app.get_event(run_id, "progress", timeout=30)  # wait up to 30 s for it to appear
await app.get_events(run_id)  # every key
```

Setting a key again overwrites it. Events are journaled, so a replay does not publish again.
The dashboard shows them in a "Progress" panel, and they are available at
`GET /api/workflows/<id>/events`.
