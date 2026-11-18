"""Command line: app loading, client commands, and a real worker process stopped with SIGTERM."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from ninelives.cli import build_parser, load_app, main

APP_SOURCE = textwrap.dedent(
    """
    import os
    from ninelives import Ninelives

    app = Ninelives(os.environ["NL_TEST_DB"])
    not_an_app = 42

    @app.step()
    async def double(x: int) -> int:
        return 2 * x

    @app.workflow()
    async def calc(ctx, x: int) -> int:
        return await double(x)

    @app.workflow()
    async def gate(ctx) -> object:
        return await ctx.wait_for_signal("go", timeout=600)

    @app.workflow()
    async def report(ctx) -> str:
        await ctx.set_event("rows", 42)
        return "ok"

    app.schedule("nightly", report, "0 2 * * *")
    """
)


@pytest.fixture
def app_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "cliapp.py"
    path.write_text(APP_SOURCE)
    monkeypatch.setenv("NL_TEST_DB", f"sqlite:///{tmp_path}/cli.db")
    sys.modules.pop("cliapp", None)
    return path


def run(capsys: pytest.CaptureFixture[str], *argv: str) -> object:
    main(list(argv))
    return json.loads(capsys.readouterr().out)


def test_load_app_from_file_and_module(app_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = load_app(f"{app_file}:app")
    assert set(app.workflows) == {"calc", "gate", "report"}
    monkeypatch.chdir(app_file.parent)
    sys.modules.pop("cliapp", None)
    assert set(load_app("cliapp:app").workflows) == {"calc", "gate", "report"}
    with pytest.raises(SystemExit, match="module:attribute"):
        load_app("cliapp")
    with pytest.raises(SystemExit, match="not a Ninelives app"):
        load_app(f"{app_file}:not_an_app")
    with pytest.raises(SystemExit, match="not a Ninelives app"):
        load_app(f"{app_file}:missing")


def test_client_commands(app_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    spec = f"{app_file}:app"
    assert run(capsys, "start", spec, "calc", "--args", "[4]", "--id", "c1") == {"id": "c1"}
    assert run(capsys, "start", spec, "gate", "--id", "g1") == {"id": "g1"}
    listed = run(capsys, "ls", spec)
    assert {r["id"]: r["status"] for r in listed} == {"c1": "PENDING", "g1": "PENDING"}  # type: ignore[union-attr]
    assert run(capsys, "ls", spec, "--status", "SUCCEEDED") == []
    status = run(capsys, "status", spec, "c1")
    assert status["workflow"]["input"] == {"args": [4], "kwargs": {}}  # type: ignore[index]
    assert run(capsys, "signal", spec, "g1", "go", "--payload", '{"k": 1}') == {"ok": True}
    assert run(capsys, "cancel", spec, "g1")["status"] == "CANCELLED"  # type: ignore[index]


def test_start_with_kwargs_and_wait(app_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """--wait needs a worker, so run one in a background process."""
    spec = f"{app_file}:app"
    worker = subprocess.Popen(
        [sys.executable, "-m", "ninelives", "worker", spec, "--poll", "0.02"],
        env={**os.environ},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        out = run(capsys, "start", spec, "calc", "--kwargs", '{"x": 8}', "--wait", "--timeout", "30")
        assert out["output"] == 16  # type: ignore[index]
    finally:
        worker.send_signal(signal.SIGTERM)
        output, _ = worker.communicate(timeout=30)
    assert worker.returncode == 0, output
    assert "running 3 workflow type(s)" in output


def test_worker_process_resumes_after_restart(app_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A workflow waiting on a signal survives a worker restart and finishes on the next one."""
    spec = f"{app_file}:app"
    cmd = [sys.executable, "-m", "ninelives", "worker", spec, "--poll", "0.02"]
    run(capsys, "start", spec, "gate", "--id", "g")
    first = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 20
    while run(capsys, "status", spec, "g")["workflow"]["status"] != "WAITING":  # type: ignore[index]
        assert time.time() < deadline
        time.sleep(0.05)
    first.send_signal(signal.SIGKILL)
    first.wait(timeout=10)
    run(capsys, "signal", spec, "g", "go", "--payload", '"hello"')
    second = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.time() + 20
        while (s := run(capsys, "status", spec, "g"))["workflow"]["status"] != "SUCCEEDED":  # type: ignore[index]
            assert time.time() < deadline, s
            time.sleep(0.05)
        assert s["workflow"]["output"] == "hello"  # type: ignore[index]
    finally:
        second.send_signal(signal.SIGTERM)
        second.wait(timeout=30)


def test_parser_rejects_unknown_status() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["ls", "x:app", "--status", "nope"])


def test_durations() -> None:
    from argparse import ArgumentTypeError

    from ninelives.cli import parse_duration

    assert [parse_duration(t) for t in ("90", "90s", "15m", "12h", "30d", "2w", "1.5h")] == [
        90, 90, 900, 43200, 2592000, 1209600, 5400
    ]  # fmt: skip
    for bad in ("", "abc", "10x", "-5m", "0"):
        with pytest.raises(ArgumentTypeError):
            parse_duration(bad)


def test_v2_commands(app_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    spec = f"{app_file}:app"
    started = run(
        capsys, "start", spec, "calc", "--args", "[2]", "--id", "vip",
        "--priority", "7", "--concurrency-key", "acme", "--run-timeout", "1h",
    )  # fmt: skip
    assert started == {"id": "vip"}
    record = run(capsys, "status", spec, "vip")["workflow"]  # type: ignore[index]
    assert record["priority"] == 7 and record["concurrency_key"] == "acme"
    assert record["deadline"] - record["created_at"] == pytest.approx(3600)
    run(capsys, "start", spec, "report", "--id", "rep-1")
    assert [r["id"] for r in run(capsys, "ls", spec, "--search", "rep")] == ["rep-1"]  # type: ignore[union-attr]
    assert [r["id"] for r in run(capsys, "ls", spec, "--name", "calc")] == ["vip"]  # type: ignore[union-attr]
    assert run(capsys, "events", spec, "rep-1") == {}
    assert run(capsys, "purge", spec, "--older-than", "30d") == {"deleted": 0}
    assert run(capsys, "schedules", spec) == []  # workers register schedules
    with pytest.raises(SystemExit) as exit_info:
        main(["events", spec, "missing"])
    assert exit_info.value.code == 1 and "error: missing" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        main(["schedules", spec, "trigger", "no-such-schedule"])
    assert "no-such-schedule" in capsys.readouterr().err


def test_schedules_and_events_after_a_worker_ran(app_file: Path, capsys: pytest.CaptureFixture[str]) -> None:
    import asyncio

    spec = f"{app_file}:app"
    run(capsys, "start", spec, "report", "--id", "rep-2")
    app = load_app(spec)
    asyncio.run(app.worker().run_until_idle())
    asyncio.run(app.close())
    assert run(capsys, "events", spec, "rep-2") == {"rows": 42}
    [schedule] = run(capsys, "schedules", spec, "list")  # type: ignore[misc]
    assert schedule["id"] == "nightly" and schedule["enabled"] is True
    assert run(capsys, "schedules", spec, "pause", "nightly")["enabled"] is False  # type: ignore[index]
    assert run(capsys, "schedules", spec, "resume", "nightly")["enabled"] is True  # type: ignore[index]
    assert run(capsys, "schedules", spec, "trigger", "nightly")["id"].startswith("nightly@manual-")  # type: ignore[index]
    with pytest.raises(SystemExit, match="needs a schedule id"):
        main(["schedules", spec, "pause"])
