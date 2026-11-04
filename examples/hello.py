"""The smallest useful Ninelives program.

    python examples/hello.py

Run it, press Ctrl+C while it is sleeping, and run it again: it picks up where it stopped
and never repeats a finished step.
"""

from __future__ import annotations

import asyncio

from ninelives import Ninelives

app = Ninelives("sqlite:///hello.db")


@app.step()
async def fetch_greeting(name: str) -> str:
    print(f"  fetching a greeting for {name} (this line prints once per run, ever)")
    return f"Hello, {name}"


@app.step()
async def shout(text: str) -> str:
    print("  shouting")
    return text.upper() + "!"


@app.workflow()
async def greet(ctx, name: str) -> str:
    print("  workflow body running (it replays on resume, but finished steps return their saved results)")
    greeting = await fetch_greeting(name)
    print("  sleeping 5s; press Ctrl+C now, then run the script again")
    await ctx.sleep(5)  # a durable timer: it survives restarts
    return await shout(greeting)


async def main() -> None:
    async with app:
        run_id = await app.start(greet, "world", id="hello-world")  # a fixed id makes this idempotent
        print(f"run {run_id}")
        worker = app.worker(poll_interval=0.1)
        task = asyncio.create_task(worker.run())
        try:
            print("result:", await app.result(run_id, timeout=60))
        finally:
            worker.stop()
            await task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nstopped. Run the script again to resume where it left off.")
