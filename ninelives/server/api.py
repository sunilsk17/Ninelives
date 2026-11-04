"""HTTP API and dashboard host."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from ..app import Ninelives
from ..errors import WorkflowNotFound
from ..types import Status

STATIC_DIR = Path(__file__).parent / "static"


class StartRequest(BaseModel):
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    id: str | None = None


class ForkRequest(BaseModel):
    from_seq: int = Field(ge=1)


def _pending_approvals(detail: dict[str, Any]) -> list[dict[str, Any]]:
    """Find agent tool calls waiting for a human, with the tool name and arguments."""
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


def create_api(
    app: Ninelives, *, run_worker: bool = False, worker_options: dict[str, Any] | None = None
) -> FastAPI:
    """Build the FastAPI app. With ``run_worker=True`` a worker runs in the same process (handy for demos)."""

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

    api = FastAPI(title="Ninelives", version="0.1.0", lifespan=lifespan)

    async def describe(wf_id: str) -> dict[str, Any]:
        try:
            detail = await app.describe(wf_id)
        except WorkflowNotFound:
            raise HTTPException(404, f"workflow {wf_id} not found") from None
        detail["approvals"] = _pending_approvals(detail)
        return detail

    @api.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @api.get("/api/stats")
    async def stats() -> dict[str, Any]:
        await app.init()
        counts = await app.store.count_by_status()
        return {
            "counts": {s.value: counts.get(s.value, 0) for s in Status},
            "workflows": sorted(app.workflows),
        }

    @api.get("/api/workflows")
    async def list_workflows(
        status: Status | None = None,
        name: str | None = None,
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ) -> dict[str, Any]:
        await app.init()
        rows = await app.store.list_workflows(status=status, name=name, limit=limit, offset=offset)
        return {"workflows": [r.to_dict() for r in rows]}

    @api.post("/api/workflows/{name}", status_code=201)
    async def start(name: str, body: StartRequest | None = None) -> dict[str, Any]:
        if name not in app.workflows:
            raise HTTPException(404, f"no workflow named {name!r} is registered")
        req = body or StartRequest()
        wf_id = await app.start(name, *req.args, id=req.id, **req.kwargs)
        return {"id": wf_id}

    @api.get("/api/workflows/{wf_id}")
    async def get_workflow(wf_id: str) -> dict[str, Any]:
        return await describe(wf_id)

    @api.post("/api/workflows/{wf_id}/signals/{signal}")
    async def send_signal(wf_id: str, signal: str, payload: Any = Body(None)) -> dict[str, Any]:
        try:
            await app.signal(wf_id, signal, payload)
        except WorkflowNotFound:
            raise HTTPException(404, f"workflow {wf_id} not found") from None
        return {"ok": True}

    @api.post("/api/workflows/{wf_id}/cancel")
    async def cancel(wf_id: str) -> dict[str, Any]:
        try:
            rec = await app.cancel(wf_id)
        except WorkflowNotFound:
            raise HTTPException(404, f"workflow {wf_id} not found") from None
        return rec.to_dict()

    @api.post("/api/workflows/{wf_id}/fork", status_code=201)
    async def fork(wf_id: str, body: ForkRequest) -> dict[str, Any]:
        try:
            new_id = await app.fork(wf_id, body.from_seq)
        except WorkflowNotFound:
            raise HTTPException(404, f"workflow {wf_id} not found") from None
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        return {"id": new_id}

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
