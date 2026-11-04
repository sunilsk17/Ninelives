"""Command line: ``ninelives worker|serve|start|status|signal|cancel|ls APP ...``.

APP is ``module:attribute`` (``examples.orders:app``) or ``path/to/file.py:attribute``.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.util
import json
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

from .app import Ninelives
from .types import Status


def load_app(spec: str) -> Ninelives:
    if ":" not in spec:
        raise SystemExit(f"APP must look like module:attribute, got {spec!r}")
    target, attr = spec.rsplit(":", 1)
    if target.endswith(".py") or os.sep in target:
        path = Path(target).resolve()
        sys.path.insert(0, str(path.parent))
        module_spec = importlib.util.spec_from_file_location(path.stem, path)
        if module_spec is None or module_spec.loader is None:
            raise SystemExit(f"cannot load {path}")
        module = importlib.util.module_from_spec(module_spec)
        sys.modules[path.stem] = module
        module_spec.loader.exec_module(module)
    else:
        sys.path.insert(0, os.getcwd())
        module = importlib.import_module(target)
    app = getattr(module, attr, None)
    if not isinstance(app, Ninelives):
        raise SystemExit(f"{spec} is not a Ninelives app")
    return app


def _json(text: str | None, default: Any) -> Any:
    return default if text is None else json.loads(text)


async def _worker(app: Ninelives, args: argparse.Namespace) -> None:
    worker = app.worker(concurrency=args.concurrency, lease_ttl=args.lease_ttl, poll_interval=args.poll)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop)
    print(
        f"ninelives worker {worker.worker_id} running {len(app.workflows)} workflow type(s); Ctrl+C to stop",
        flush=True,
    )
    await worker.run()


async def _client(app: Ninelives, args: argparse.Namespace) -> Any:
    await app.init()
    try:
        if args.command == "start":
            wf_id = await app.start(
                args.workflow, *_json(args.args, []), id=args.id, **_json(args.kwargs, {})
            )
            if not args.wait:
                return {"id": wf_id}
            return {"id": wf_id, "output": await app.result(wf_id, timeout=args.timeout)}
        if args.command == "status":
            return await app.describe(args.id)
        if args.command == "signal":
            await app.signal(args.id, args.name, _json(args.payload, None))
            return {"ok": True}
        if args.command == "cancel":
            return (await app.cancel(args.id)).to_dict()
        if args.command == "ls":
            rows = await app.store.list_workflows(
                status=Status(args.status) if args.status else None, limit=args.limit
            )
            return [{k: r.to_dict()[k] for k in ("id", "name", "status", "runs", "recoveries")} for r in rows]
    finally:
        await app.close()
    raise SystemExit(f"unknown command {args.command}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ninelives", description="Durable execution for Python workflows and AI agents"
    )
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)

    w = sub.add_parser("worker", help="run a worker")
    w.add_argument("app")
    w.add_argument("--concurrency", type=int, default=16)
    w.add_argument("--lease-ttl", type=float, default=15.0)
    w.add_argument("--poll", type=float, default=0.2)

    s = sub.add_parser("serve", help="run the HTTP API and dashboard")
    s.add_argument("app")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8000)
    s.add_argument("--worker", action="store_true", help="also run a worker in this process")

    st = sub.add_parser("start", help="start a workflow")
    st.add_argument("app")
    st.add_argument("workflow")
    st.add_argument("--args", help="JSON list of positional arguments")
    st.add_argument("--kwargs", help="JSON object of keyword arguments")
    st.add_argument("--id")
    st.add_argument("--wait", action="store_true", help="wait for the result (a worker must be running)")
    st.add_argument("--timeout", type=float, default=60.0)

    for name, helptext in (("status", "show a workflow"), ("cancel", "cancel a workflow")):
        c = sub.add_parser(name, help=helptext)
        c.add_argument("app")
        c.add_argument("id")

    sg = sub.add_parser("signal", help="send a signal")
    sg.add_argument("app")
    sg.add_argument("id")
    sg.add_argument("name")
    sg.add_argument("--payload", help="JSON payload")

    ls = sub.add_parser("ls", help="list workflows")
    ls.add_argument("app")
    ls.add_argument("--status", choices=[s.value for s in Status])
    ls.add_argument("--limit", type=int, default=20)
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    app = load_app(args.app)
    if args.command == "worker":
        asyncio.run(_worker(app, args))
    elif args.command == "serve":
        import uvicorn

        from .server import create_api

        uvicorn.run(create_api(app, run_worker=args.worker), host=args.host, port=args.port)
    else:
        print(json.dumps(asyncio.run(_client(app, args)), indent=2, default=str))


if __name__ == "__main__":  # pragma: no cover
    main()
