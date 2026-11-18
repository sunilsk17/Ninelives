"""Workers claim runnable workflows with a lease, execute them, heartbeat and record outcomes."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import os
import socket
import time
import uuid
from typing import TYPE_CHECKING, Any

from . import serde
from .context import WorkflowContext, _Abandon, _current_ctx, _Suspend
from .errors import Cancelled, LeaseLost
from .types import Status, WorkflowRecord

if TYPE_CHECKING:
    from .app import Ninelives

log = logging.getLogger("ninelives")


class Worker:
    """Runs workflows registered on ``app``.

    Any number of workers (threads, processes, machines) can share one store. A worker that dies
    loses its leases after ``lease_ttl`` seconds and another worker resumes its workflows.
    """

    def __init__(
        self,
        app: Ninelives,
        *,
        concurrency: int = 16,
        poll_interval: float = 0.2,
        lease_ttl: float = 15.0,
        worker_id: str | None = None,
        shutdown_grace: float = 10.0,
        retention: float | None = None,
        schedule_interval: float = 1.0,
    ):
        """
        concurrency: workflows this worker executes at once.
        poll_interval: seconds between checks for work when idle (Postgres wakes earlier on NOTIFY).
        lease_ttl: seconds a silent worker keeps its claims; recovery after a crash takes this long.
        shutdown_grace: seconds ``stop()`` waits for running workflows before handing them over.
        retention: if set, delete finished runs older than this many seconds (checked hourly).
        schedule_interval: seconds between checks for due schedules.
        """
        self.app = app
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.lease_ttl = lease_ttl
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"
        self.shutdown_grace = shutdown_grace
        self.retention = retention
        self.schedule_interval = schedule_interval
        self._schedules_registered = False
        self._next_schedule_check = 0.0
        self._next_purge = 0.0
        #: The current execution of each workflow this worker holds, and its lease token.
        self._running: dict[str, asyncio.Task[None]] = {}
        self._tokens: dict[str, str] = {}
        #: Executions whose lease is gone; they are being cancelled and must not record anything.
        self._lost: set[asyncio.Task[Any]] = set()
        self._stopping = asyncio.Event()
        self.completed = 0
        #: Executions that have ended (any outcome); lets run_until_idle detect work it raced past.
        self._settled = 0

    # ------------------------------------------------------------------ loop
    async def run(self) -> None:
        """Run until ``stop()`` is called."""
        await self.app.init()
        log.info("worker %s started (%d workflow types)", self.worker_id, len(self.app.workflows))
        try:
            while not self._stopping.is_set():
                await self._housekeeping()
                claimed = await self._claim_once()
                if claimed == 0:
                    await self._idle()
        finally:
            await self._shutdown()

    async def _idle(self) -> None:
        """Sleep until new work is announced, the poll interval passes, or stop() is called."""
        work = asyncio.ensure_future(self.app.store.wait_for_work(self.poll_interval))
        stop = asyncio.ensure_future(self._stopping.wait())
        try:
            await asyncio.wait(
                {work, stop}, timeout=self.poll_interval * 2, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for fut in (work, stop):
                fut.cancel()
            await asyncio.gather(work, stop, return_exceptions=True)

    async def _housekeeping(self, force: bool = False) -> None:
        """Register and fire schedules, and purge old runs. Failures are logged, never fatal."""
        store = self.app.store
        clock = time.monotonic()
        try:
            if self.app.schedules and not self._schedules_registered:
                await store.upsert_schedules(list(self.app.schedules.values()), self.app.clock())
                self._schedules_registered = True
            if self.app.schedules and (force or clock >= self._next_schedule_check):
                self._next_schedule_check = clock + self.schedule_interval
                started = await store.fire_schedules(self.app.clock(), list(self.app.workflows))
                for run_id in started:
                    log.info("schedule started %s", run_id)
            if self.retention is not None and clock >= self._next_purge:
                self._next_purge = clock + 3600
                removed = await store.purge(self.app.clock() - self.retention)
                if removed:
                    log.info("purged %d finished run(s) older than %ss", removed, self.retention)
        except Exception:
            log.exception("worker housekeeping failed; retrying on the next loop")

    def stop(self) -> None:
        self._stopping.set()

    async def run_until_idle(self, timeout: float = 30.0) -> None:
        """Process workflows until nothing is runnable and nothing is in flight (for tests/scripts)."""
        await self.app.init()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            await self._housekeeping(force=True)
            settled = self._settled
            claimed = await self._claim_once()
            # Idle only if nothing finished while we looked: a task that ends during the claim
            # can create work the claim did not see (a woken parent, a freed concurrency slot).
            if claimed == 0 and not self._running and settled == self._settled:
                return
            if loop.time() > deadline:
                raise TimeoutError("worker did not become idle")
            if claimed == 0:
                await asyncio.sleep(0.01)

    async def _claim_once(self) -> int:
        free = self.concurrency - len(self._running)
        if free <= 0:
            return 0
        records = await self.app.store.claim(
            self.worker_id,
            self.app.clock(),
            self.lease_ttl,
            list(self.app.workflows),
            free,
            self.app.limits(),
        )
        for rec in records:
            assert rec.lease_token is not None
            stale = self._running.get(rec.id)
            if stale is not None:
                # This worker's own lease lapsed (the process was paused or starved) and it just
                # claimed the run again. The old execution's token is dead: stop it, start afresh.
                log.warning(
                    "worker %s re-claimed %s after its lease lapsed; restarting it", self.worker_id, rec.id
                )
                self._lost.add(stale)
                stale.cancel()
            self._tokens[rec.id] = rec.lease_token
            task = asyncio.create_task(self._execute(rec), name=f"ninelives:{rec.id}")
            self._running[rec.id] = task
            task.add_done_callback(functools.partial(self._forget, rec.id))
        return len(records)

    def _forget(self, wf_id: str, task: asyncio.Task[None]) -> None:
        self._settled += 1
        if self._running.get(wf_id) is task:
            del self._running[wf_id]

    async def _shutdown(self) -> None:
        if not self._running:
            return
        log.info("worker %s draining %d workflow(s)", self.worker_id, len(self._running))
        tasks = list(self._running.values())
        _done, pending = await asyncio.wait(tasks, timeout=self.shutdown_grace)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        # Hand unfinished workflows to other workers immediately instead of waiting for lease expiry.
        for wf_id, token in list(self._tokens.items()):
            with contextlib.suppress(Exception):
                await self.app.store.release(token, wf_id)

    # ------------------------------------------------------------------ one workflow
    async def _heartbeat(self, rec: WorkflowRecord, ctx: WorkflowContext, task: asyncio.Task[Any]) -> None:
        token = rec.lease_token or ""
        interval = max(0.05, self.lease_ttl / 3)
        while True:
            await asyncio.sleep(interval)
            try:
                hb = await self.app.store.heartbeat(rec.id, token, self.app.clock(), self.lease_ttl)
            except Exception:  # transient store error: try again next tick
                log.warning("heartbeat failed for %s", rec.id, exc_info=True)
                continue
            if not hb.alive:
                log.warning("worker %s lost the lease on %s", self.worker_id, rec.id)
                self._lost.add(task)
                task.cancel()
                return
            if hb.cancel_requested:
                ctx.cancel_requested = True

    async def _execute(self, rec: WorkflowRecord) -> None:
        token = rec.lease_token or ""
        store = self.app.store
        spec = self.app.workflows[rec.name]
        current = asyncio.current_task()
        assert current is not None
        hb: asyncio.Task[None] | None = None
        try:
            journal = {e.seq: e for e in await store.get_journal(rec.id)}
            ctx = WorkflowContext(self.app, rec, token, self.worker_id, journal)
            hb = asyncio.create_task(self._heartbeat(rec, ctx, current))
            ctx_token = _current_ctx.set(ctx)
            try:
                args = rec.input.get("args", [])
                kwargs = rec.input.get("kwargs", {})
                output = await spec.fn(ctx, *args, **kwargs)
            finally:
                _current_ctx.reset(ctx_token)
            await store.finish(
                token, rec.id, Status.SUCCEEDED, serde.loads(serde.dumps(output)), None, self.app.clock()
            )
            self.completed += 1
        except _Suspend as s:
            await self._safe(store.suspend(token, rec.id, s.wake_at, s.waiting_for, self.app.clock()))
        except (_Abandon, LeaseLost):
            log.info("abandoning %s: another worker owns it now", rec.id)
        except Cancelled as exc:
            await self._safe(
                store.finish(token, rec.id, Status.CANCELLED, None, serde.error_dict(exc), self.app.clock())
            )
        except asyncio.CancelledError:
            if current in self._lost:
                return  # lease lost mid-run; the new owner carries on
            raise  # worker shutdown: the lease is released in _shutdown
        except Exception as exc:
            log.exception("workflow %s failed", rec.id)
            await self._safe(
                store.finish(token, rec.id, Status.FAILED, None, serde.error_dict(exc), self.app.clock())
            )
        finally:
            if hb is not None:
                hb.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await hb
            lost = current in self._lost
            self._lost.discard(current)
            # On shutdown the token is kept so _shutdown can release the lease right away.
            shutting_down = not lost and (current.cancelled() or current.cancelling() > 0)
            if not shutting_down and self._tokens.get(rec.id) == token:
                del self._tokens[rec.id]

    async def _safe(self, op: Any) -> None:
        try:
            await op
        except LeaseLost:
            log.info("lease lost while recording an outcome; another worker will finish it")
