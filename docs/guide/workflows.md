# Workflows and steps

## How a run executes

A workflow is an `async def` whose first argument is the context, `ctx`. Every durable operation
it performs (a step, a timer, a signal wait) gets the next position in the run's **journal**. When a
worker runs the workflow, operations already in the journal return their recorded result
immediately, so the code fast-forwards to the first unfinished operation and continues from there.

That is why workflow code must be deterministic: on every resume it must make the same durable
calls in the same order. Concretely:

| Don't, in workflow code | Do instead |
| --- | --- |
| call APIs, read files, query databases | put that in a step |
| `time.time()`, `datetime.now()` | `await ctx.now()` |
| `uuid.uuid4()`, `random.random()` | `await ctx.uuid()`, `await ctx.random()` |
| `asyncio.sleep(3600)` | `await ctx.sleep(3600)` |
| branch on something that can change between runs | branch on step results |

If the code changes so that a resumed run no longer matches its journal, the run fails with
`NonDeterminismError` instead of silently doing the wrong thing. See [Changing code safely](versioning.md).

## Steps

```python
from ninelives import RetryPolicy, PermanentError, step_info


@app.step(retry=RetryPolicy(max_attempts=5, initial_delay=1, backoff=2, max_delay=60), timeout=30)
async def charge(order_id: str, cents: int) -> dict:
    key = step_info().idempotency_key  # stable across retries and crashes
    if cents > 1_000_000:
        raise PermanentError("over the limit")  # fail now, no retries
    return await payments.charge(order_id, cents, idempotency_key=key)
```

- **Calling.** Inside a workflow, `await charge(...)` runs the step durably. Outside a workflow, the
  same function is just a function, which keeps steps easy to unit-test.
- **Sync or async.** Steps may be `def` or `async def`. Synchronous steps run in a thread.
- **Return values** must be JSON-serialisable: dataclasses, dates, `Decimal`, sets and enums are
  converted. The value your workflow receives on the first run is exactly what a replay returns.
- **Retries** use exponential backoff with jitter. Attempt counts are stored, so a crash does not
  reset them. `RetryPolicy(non_retryable=(ValueError,))` fails fast for listed exception types.
- **Timeouts** fail an attempt that runs too long. An `async def` step is cancelled; a `def` step
  keeps running in its thread until it returns, because Python cannot kill threads.
- **Failure.** When retries run out, the workflow sees `StepFailed` with `.step` and `.error`
  (type, message, traceback). Catch it to compensate:

```python
from ninelives import StepFailed


@app.workflow()
async def checkout(ctx, order_id: str, cents: int) -> dict:
    hold = await reserve_stock(order_id)
    try:
        payment = await charge(order_id, cents)
    except StepFailed as failure:
        await release_stock(hold)  # undo what already happened
        return {"status": "payment_failed", "reason": failure.error["message"]}
    return {"status": "paid", "payment": payment}
```

### Exactly-once side effects

Steps run **at least once**: if a worker dies after a step's side effect but before the result is
recorded, the step runs again. To make the *effect* happen once, pass
`step_info().idempotency_key` (`"<run id>:<position>"`) to the system you call. Payment providers,
email APIs and your own tables with a unique key all support this. The chaos test in this
repository kills workers at random and checks, through such a key, that every effect lands exactly once.

## Timers

```python
await ctx.sleep(3 * 24 * 3600)  # three days
```

Sleeps longer than a second park the run in the database: no process holds it, and it survives
restarts. Shorter sleeps happen in-process.

## Signals

Signals carry data into a running workflow, such as a human decision, a webhook or a payment
confirmation:

```python
@app.workflow()
async def expense(ctx, employee: str, amount: int) -> dict:
    try:
        decision = await ctx.wait_for_signal("decision", timeout=7 * 24 * 3600)
    except SignalTimeout:
        return {"status": "expired"}
    return {"status": "approved" if decision["approved"] else "rejected"}
```

Send one from anywhere:

```python
await app.signal(run_id, "decision", {"approved": True})
```

The same works from the CLI (`ninelives signal ...`), the HTTP API and the dashboard. Signals sent
before the workflow reaches its wait are kept and delivered in order. Each is delivered exactly
once, and a waiting run costs nothing.

## Cancellation

`await app.cancel(run_id)` cancels a waiting or pending run at once. A running one is cancelled at
its next durable operation, where `Cancelled` is raised inside the workflow: catch it to clean up,
but the run ends cancelled either way. Child runs are cancelled with their parent.

## Results, inspection and forks

```python
output = await app.result(run_id, timeout=60)  # raises WorkflowFailed if it failed or was cancelled
record = await app.get(run_id)  # status, timings, executions, recoveries, ...
detail = await app.describe(run_id)  # plus journal, attempts, signals, events, children
new_id = await app.fork(run_id, from_seq=4)  # re-run from step 4, reusing steps 1-3
```

`fork` is how you recover from a bug or a bad prompt. Fix the code, then fork the failed run from
the step that went wrong. Earlier steps are copied, not re-executed.

## Running branches in parallel

`asyncio.gather` is fine for running several *single* steps at once:

```python
a, b = await asyncio.gather(fetch_prices(), fetch_stock())
```

For branches that each do several durable calls, use `ctx.gather`. It numbers their operations
deterministically, so replays line up however the I/O interleaved:

```python
async def enrich(customer_id):
    profile = await load_profile(customer_id)
    return await score(profile)


results = await ctx.gather(*(enrich(c) for c in customer_ids))
```

A raw `asyncio.gather` over multi-step branches fails with a clear `NonDeterminismError` rather
than replaying wrongly. Inside `ctx.gather`, a branch that sleeps or waits for a signal pauses its
siblings at their next durable call until it resumes, so keep long waits outside parallel sections
(or put them in [child workflows](composition.md)).
