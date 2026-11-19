# Changing code safely

Runs can live for days, so you will deploy new code while some are in flight. This page explains
what is safe, what is not, and the tools for the rest.

## What is always safe

- **Changing a step's body.** Recorded results are reused, and new executions use the new code.
- **Changing what happens after the last durable call** that running instances have reached.
- **Adding new workflows, steps or schedules.**
- **Upgrading Ninelives.** The database schema is migrated automatically and in place when a store
  starts (`init()`). A database written by a *newer* version is refused with a clear error rather
  than misread.

## What needs care

Changing the sequence of durable calls (adding, removing, reordering or renaming steps, sleeps,
signal waits, child starts) before the point where running instances are parked. On resume, such a
run would see different calls than its journal records. Ninelives detects this and fails the run
with `NonDeterminismError` instead of silently mixing old and new behaviour.

Three ways to handle it:

1. **Let old runs finish first**: drain, then deploy. This is the simplest option when runs are short.
2. **Fork after the fix.** For a failed run, `await app.fork(run_id, from_seq=N)` re-runs it from
   step N with the new code, reusing the steps before N.
3. **Branch with `ctx.patched`** so old and new runs each follow their own path:

```python
@app.workflow()
async def checkout(ctx, order_id: str) -> None:
    await charge(order_id)
    if await ctx.patched("sms-receipts"):
        await send_sms_receipt(order_id)      # new behaviour
    else:
        await send_email_receipt(order_id)    # what runs that passed here earlier did
    await ship(order_id)
```

`patched("sms-receipts")` returns:

- **True** for runs that reach this point running the new code (it records a small marker),
- **False** for runs that already passed this point before the change.

Once no old run remains, delete the `patched` call and the old branch. Markers left in journals are
skipped automatically. Keep patch ids unique and descriptive. `ctx.patched` cannot be used inside
`ctx.gather` branches.

## Agents

Enabling `publish_progress` or `parallel_tools` on an existing agent changes the shape of its
journal. Turn them on for a new agent name, or once old runs have finished.
