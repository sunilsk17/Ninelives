"""Command line: ``ninelives worker|serve|start|status|signal|cancel|events|ls|purge|schedules APP ...``.

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

from . import __version__
from .app import Ninelives
from .errors import NinelivesError
from .types import StartOptions, Status


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


UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 7 * 86400}


def parse_duration(text: str) -> float:
    """``90``, ``90s``, ``15m``, ``12h``, ``30d`` or ``2w`` -> seconds."""
    value = text.strip().lower()
    try:
        seconds = float(value[:-1]) * UNITS[value[-1]] if value and value[-1] in UNITS else float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a duration: {text!r} (try 90s, 15m, 12h or 30d)") from None
    if seconds <= 0:
        raise argparse.ArgumentTypeError("a duration must be positive")
    return seconds


async def _worker(app: Ninelives, args: argparse.Namespace) -> None:
    worker = app.worker(
        concurrency=args.concurrency,
        lease_ttl=args.lease_ttl,
        poll_interval=args.poll,
        retention=args.retention,
    )
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
            options = StartOptions(
                priority=args.priority, concurrency_key=args.concurrency_key, timeout=args.run_timeout
            )
            wf_id = await app.start(
                args.workflow, *_json(args.args, []), id=args.id, options=options, **_json(args.kwargs, {})
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
                status=Status(args.status) if args.status else None,
                name=args.name,
                search=args.search,
                limit=args.limit,
            )
            return [{k: r.to_dict()[k] for k in ("id", "name", "status", "runs", "recoveries")} for r in rows]
        if args.command == "events":
            await app.get(args.id)
            return await app.get_events(args.id)
        if args.command == "purge":
            return {"deleted": await app.purge(args.older_than)}
        if args.command == "schedules":
            if args.action == "list":
                return [s.to_dict() for s in await app.list_schedules()]
            if not args.schedule_id:
                raise SystemExit(f"schedules {args.action} needs a schedule id")
            if args.action == "trigger":
                return {"id": await app.trigger_schedule(args.schedule_id)}
            action = app.pause_schedule if args.action == "pause" else app.resume_schedule
            return (await action(args.schedule_id)).to_dict()
    finally:
        await app.close()
    raise SystemExit(f"unknown command {args.command}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ninelives", description="Durable execution for Python workflows and AI agents"
    )
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--version", action="version", version=f"ninelives {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    w = sub.add_parser("worker", help="run a worker")
    w.add_argument("app")
    w.add_argument("--concurrency", type=int, default=16)
    w.add_argument("--lease-ttl", type=float, default=15.0)
    w.add_argument("--poll", type=float, default=0.2)
    w.add_argument("--retention", type=parse_duration, help="delete finished runs older than this (e.g. 30d)")

    s = sub.add_parser("serve", help="run the HTTP API and dashboard")
    s.add_argument("app")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8000)
    s.add_argument("--worker", action="store_true", help="also run a worker in this process")
    s.add_argument("--token", help="require this bearer token on the API (default: $NINELIVES_API_TOKEN)")

    st = sub.add_parser("start", help="start a workflow")
    st.add_argument("app")
    st.add_argument("workflow")
    st.add_argument("--args", help="JSON list of positional arguments")
    st.add_argument("--kwargs", help="JSON object of keyword arguments")
    st.add_argument("--id")
    st.add_argument("--wait", action="store_true", help="wait for the result (a worker must be running)")
    st.add_argument("--timeout", type=float, default=60.0, help="seconds to wait with --wait")
    st.add_argument("--priority", type=int, default=0, help="higher runs are picked up first")
    st.add_argument("--concurrency-key", help="group for the workflow's key_concurrency limit")
    st.add_argument("--run-timeout", type=parse_duration, help="fail the run if it takes longer (e.g. 1h)")

    for name, helptext in (
        ("status", "show a run: record, journal, attempts, signals, events, children"),
        ("cancel", "cancel a run and its unfinished children"),
        ("events", "show the events a run has published"),
    ):
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
    ls.add_argument("--name", help="only this workflow")
    ls.add_argument("--search", help="substring of the run id")
    ls.add_argument("--limit", type=int, default=20)

    pg = sub.add_parser("purge", help="delete finished runs older than a duration")
    pg.add_argument("app")
    pg.add_argument("--older-than", type=parse_duration, required=True, help="e.g. 30d")

    sc = sub.add_parser("schedules", help="list, pause, resume or trigger schedules")
    sc.add_argument("app")
    sc.add_argument("action", nargs="?", default="list", choices=["list", "pause", "resume", "trigger"])
    sc.add_argument("schedule_id", nargs="?")
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

        uvicorn.run(create_api(app, token=args.token, run_worker=args.worker), host=args.host, port=args.port)
    else:
        try:
            result = asyncio.run(_client(app, args))
        except (NinelivesError, KeyError, ValueError, TimeoutError) as exc:
            message = exc.args[0] if isinstance(exc, KeyError) and exc.args else exc
            print(f"error: {message}", file=sys.stderr)
            raise SystemExit(1) from None
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":  # pragma: no cover
    main()
