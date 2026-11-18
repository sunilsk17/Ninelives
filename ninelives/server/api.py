"""HTTP API, Prometheus metrics and the dashboard host."""

from __future__ import annotations

import asyncio
import contextlib
import os
import secrets
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .. import __version__
from ..app import Ninelives
from ..errors import WorkflowNotFound
from ..types import StartOptions, Status

STATIC_DIR = Path(__file__).parent / "static"
TOKEN_ENV = "NINELIVES_API_TOKEN"


class StartRequest(BaseModel):
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    id: str | None = Field(default=None, min_length=1, max_length=200)
    priority: int = 0
    concurrency_key: str | None = None
    timeout: float | None = Field(default=None, gt=0)


class ForkRequest(BaseModel):
    from_seq: int = Field(ge=1)


class PurgeRequest(BaseModel):
    older_than: float = Field(
        gt=0, description="Delete finished runs last updated more than this many seconds ago"
    )


def _pending_approvals(detail: dict[str, Any]) -> list[dict[str, Any]]:
    """Agent tool calls waiting for a human, with the tool name and arguments."""
    calls: dict[str, dict[str, Any]] = {}
    for entry in detail["journal"]:
        output = entry.get("output")
        if entry["kind"] == "step" and entry["name"].endswith(":llm") and isinstance(output, dict):
            for block in output.get("content", []):
                if isinstance(block, dict) and block.get("type") == "tool_use":
                    calls[block["id"]] = {"tool": block["name"], "input": block.get("input")}
    out = []
    for entry in detail["journal"]:
        if (
            entry["kind"] == "signal"
            and entry["status"] == "pending"
            and entry["name"].startswith("approval:")
        ):
            call_id = entry["name"].removeprefix("approval:")
            out.append(
                {
                    "signal": entry["name"],
                    "call_id": call_id,
                    "seq": entry["seq"],
                    "deadline": entry["wake_at"],
                    **calls.get(call_id, {}),
                }
            )
    return out


def _metric_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def create_api(
    app: Ninelives,
    *,
    token: str | None = None,
    run_worker: bool = False,
    worker_options: dict[str, Any] | None = None,
) -> FastAPI:
    """Build the FastAPI app.

    ``token`` (default: the ``NINELIVES_API_TOKEN`` environment variable) makes every route except
    ``/api/health`` require ``Authorization: Bearer <token>``. With ``run_worker=True`` a worker
    runs in the same process, which is handy for demos and small deployments.
    """
    token = token if token is not None else os.environ.get(TOKEN_ENV) or None

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await app.init()
        worker = None
        task = None
        if run_worker:
            worker = app.worker(**(worker_options or {}))
            task = asyncio.create_task(worker.run())
        try:
            yield
        finally:
            if worker is not None and task is not None:
                worker.stop()
                await task

    async def authorize(request: Request) -> None:
        if token is None:
            return
        header = request.headers.get("authorization", "")
        scheme, _, supplied = header.partition(" ")
        if scheme.lower() != "bearer" or not secrets.compare_digest(supplied.encode(), token.encode()):
            raise HTTPException(401, "missing or invalid API token", headers={"WWW-Authenticate": "Bearer"})

    api = FastAPI(title="Ninelives", version=__version__, lifespan=lifespan)
    guarded = [Depends(authorize)]

    def not_found(wf_id: str) -> HTTPException:
        return HTTPException(404, f"workflow {wf_id} not found")

    # ------------------------------------------------------------------ service
    @api.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    @api.get("/api/stats", dependencies=guarded)
    async def stats() -> dict[str, Any]:
        await app.init()
        counts = await app.store.count_by_status()
        return {
            "version": __version__,
            "counts": {s.value: counts.get(s.value, 0) for s in Status},
            "workflows": sorted(app.workflows),
            "schedules": len(await app.store.list_schedules()),
        }

    @api.get("/metrics", dependencies=guarded, response_class=PlainTextResponse, include_in_schema=False)
    async def metrics() -> str:
        await app.init()
        lines = [
            "# HELP ninelives_runs Workflow runs by workflow name and status.",
            "# TYPE ninelives_runs gauge",
        ]
        for (name, status), n in sorted((await app.store.count_by_name_and_status()).items()):
            lines.append(f'ninelives_runs{{workflow="{_metric_label(name)}",status="{status}"}} {n}')
        schedules = await app.store.list_schedules()
        lines += [
            "# HELP ninelives_schedules Registered schedules by state.",
            "# TYPE ninelives_schedules gauge",
            f'ninelives_schedules{{enabled="true"}} {sum(1 for s in schedules if s.enabled)}',
            f'ninelives_schedules{{enabled="false"}} {sum(1 for s in schedules if not s.enabled)}',
            "# HELP ninelives_info Build information.",
            "# TYPE ninelives_info gauge",
            f'ninelives_info{{version="{__version__}"}} 1',
        ]
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------ runs
    @api.get("/api/workflows", dependencies=guarded)
    async def list_workflows(
        status: Status | None = None,
        name: str | None = None,
        q: str | None = Query(None, description="substring of the run id"),
        parent_id: str | None = None,
        schedule_id: str | None = None,
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ) -> dict[str, Any]:
        await app.init()
        rows = await app.store.list_workflows(
            status=status,
            name=name,
            limit=limit,
            offset=offset,
            search=q or None,
            parent_id=parent_id,
            schedule_id=schedule_id,
        )
        return {"workflows": [r.to_dict() for r in rows]}

    @api.post("/api/workflows/{name}", status_code=201, dependencies=guarded)
    async def start(name: str, body: StartRequest | None = None) -> dict[str, Any]:
        if name not in app.workflows:
            raise HTTPException(404, f"no workflow named {name!r} is registered")
        req = body or StartRequest()
        options = StartOptions(
            priority=req.priority, concurrency_key=req.concurrency_key, timeout=req.timeout
        )
        wf_id = await app.start(name, *req.args, id=req.id, options=options, **req.kwargs)
        return {"id": wf_id}

    # Run ids may contain "/" (child runs are "<parent>/<seq>"), hence the path converters. Routes
    # with a suffix are declared before the bare one so they match first.
    @api.post("/api/workflows/{wf_id:path}/signals/{signal:path}", dependencies=guarded)
    async def send_signal(wf_id: str, signal: str, payload: Any = Body(None)) -> dict[str, Any]:
        try:
            await app.signal(wf_id, signal, payload)
        except WorkflowNotFound:
            raise not_found(wf_id) from None
        return {"ok": True}

    @api.post("/api/workflows/{wf_id:path}/cancel", dependencies=guarded)
    async def cancel(wf_id: str) -> dict[str, Any]:
        try:
            rec = await app.cancel(wf_id)
        except WorkflowNotFound:
            raise not_found(wf_id) from None
        return rec.to_dict()

    @api.post("/api/workflows/{wf_id:path}/fork", status_code=201, dependencies=guarded)
    async def fork(wf_id: str, body: ForkRequest) -> dict[str, Any]:
        try:
            new_id = await app.fork(wf_id, body.from_seq)
        except WorkflowNotFound:
            raise not_found(wf_id) from None
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        return {"id": new_id}

    @api.get("/api/workflows/{wf_id:path}/events", dependencies=guarded)
    async def events(wf_id: str) -> dict[str, Any]:
        await app.init()
        if await app.store.get_workflow(wf_id) is None:
            raise not_found(wf_id)
        return {"events": [e.to_dict() for e in await app.store.get_events(wf_id)]}

    @api.get("/api/workflows/{wf_id:path}", dependencies=guarded)
    async def get_workflow(wf_id: str) -> dict[str, Any]:
        try:
            detail = await app.describe(wf_id)
        except WorkflowNotFound:
            raise not_found(wf_id) from None
        detail["approvals"] = _pending_approvals(detail)
        return detail

    # ------------------------------------------------------------------ schedules and retention
    @api.get("/api/schedules", dependencies=guarded)
    async def list_schedules() -> dict[str, Any]:
        return {"schedules": [s.to_dict() for s in await app.list_schedules()]}

    async def schedule_action(schedule_id: str, action: str) -> Any:
        try:
            if action == "pause":
                return (await app.pause_schedule(schedule_id)).to_dict()
            if action == "resume":
                return (await app.resume_schedule(schedule_id)).to_dict()
            return {"id": await app.trigger_schedule(schedule_id)}
        except KeyError:
            raise HTTPException(404, f"schedule {schedule_id} not found") from None

    @api.post("/api/schedules/{schedule_id}/pause", dependencies=guarded)
    async def pause_schedule(schedule_id: str) -> Any:
        return await schedule_action(schedule_id, "pause")

    @api.post("/api/schedules/{schedule_id}/resume", dependencies=guarded)
    async def resume_schedule(schedule_id: str) -> Any:
        return await schedule_action(schedule_id, "resume")

    @api.post("/api/schedules/{schedule_id}/trigger", status_code=201, dependencies=guarded)
    async def trigger_schedule(schedule_id: str) -> Any:
        return await schedule_action(schedule_id, "trigger")

    @api.post("/api/purge", dependencies=guarded)
    async def purge(body: PurgeRequest) -> dict[str, int]:
        return {"deleted": await app.purge(body.older_than)}

    # ------------------------------------------------------------------ dashboard
    if (STATIC_DIR / "index.html").exists():
        api.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets", check_dir=False), name="assets")

        @api.get("/", include_in_schema=False)
        async def index() -> FileResponse:
            return FileResponse(STATIC_DIR / "index.html")
    else:

        @api.get("/", include_in_schema=False)
        async def no_dashboard() -> HTMLResponse:
            return HTMLResponse(
                "<h1>Ninelives API</h1><p>Dashboard not built. See <a href='/docs'>/docs</a>.</p>"
            )

    return api
