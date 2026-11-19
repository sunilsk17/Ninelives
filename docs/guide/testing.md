# Testing your workflows

`ninelives.testing.TestEnv` runs your app against an in-memory database and a clock you move by
hand. There are no worker processes and no real waiting, so a workflow that sleeps for a week tests
in milliseconds.

```python
import pytest
from ninelives.testing import TestEnv
from myapp.workflows import app, checkout

async def test_checkout_waits_for_review_then_ships():
    async with TestEnv(app) as env:
        run = await env.start(checkout, "A-1", 4999)
        await env.run_until_idle()                 # runs everything that can run right now

        assert await env.events(run) == {"stage": "awaiting review"}
        await env.advance(24 * 3600)               # a day passes: timers fire
        await env.signal(run, "review", {"stars": 5})

        assert await env.result(run) == {"status": "shipped", "stars": 5}

async def test_quick_ones_in_one_line():
    async with TestEnv(app) as env:
        assert await env.run(checkout, "A-2", 100) == {...}
```

What it gives you:

| | |
| --- | --- |
| `env.start(...)`, `env.signal(...)` | like the app's, with `signal` running until idle afterwards |
| `env.run_until_idle()` | execute every run that can make progress now; returns when nothing can |
| `env.advance(seconds)` | move the clock forward, then run what woke up: timers, timeouts, schedules |
| `env.result(run)` | the output, or `WorkflowFailed`; an unfinished run raises `AssertionError` naming what it waits for |
| `env.run(workflow, ...)` | start, run until idle, return the result |
| `env.events(run)`, `env.describe(run)`, `env.get(run)` | inspection |
| `env.now()` | the fake time (starts at 2027-01-15 08:00 UTC) |

`TestEnv` restores the app's real store and clock when the block exits. Workflows run in the test's
own event loop, so breakpoints and exceptions behave normally.

## Testing agents

Use `ScriptedLLM` instead of a real model:

```python
from ninelives.agents import ScriptedLLM

say, call = ScriptedLLM.say, ScriptedLLM.call
llm = ScriptedLLM([call("lookup_order", {"order_id": "A-1"}), say("It shipped.")])
```

`ScriptedLLM` also accepts a function of the conversation, for model behaviour that depends on
tool results, and counts its calls (`llm.calls`), which is handy for asserting that a resumed agent
did not call the model again.

## Testing steps

Outside a workflow, a step is a plain function: `await charge("A-1", 100)` simply runs it.
