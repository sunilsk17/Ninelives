"""HTTP API: every endpoint, error mapping, agent approvals and the in-process worker."""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from ninelives import Ninelives, Store
from ninelives.agents import Agent, ScriptedLLM, tool
from ninelives.server import create_api
from ninelives.server.api import STATIC_DIR

say, call = ScriptedLLM.say, ScriptedLLM.call


def build(store: Store) -> Ninelives:
    app = Ninelives(store)

    @app.step()
    async def double(x: int) -> int:
        return 2 * x

    @app.workflow()
    async def calc(ctx, x: int) -> int:
        return await double(x)

    @app.workflow()
    async def gate(ctx, label: str) -> dict:
        first = await double(1)
        payload = await ctx.wait_for_signal("go", timeout=3600)
        return {"label": label, "first": first, "payload": payload}

    return app


@pytest.fixture
async def setup(store: Store) -> AsyncIterator[tuple[Ninelives, httpx.AsyncClient]]:
    app = build(store)
    await app.init()
    api = create_api(app)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api), base_url="http://test") as client:
        yield app, client


async def test_health_and_stats(setup) -> None:
    _, client = setup
    assert (await client.get("/api/health")).json() == {"status": "ok"}
    await client.post("/api/workflows/calc", json={"args": [2]})
    stats = (await client.get("/api/stats")).json()
    assert stats["counts"]["PENDING"] == 1 and stats["counts"]["SUCCEEDED"] == 0
    assert stats["workflows"] == ["calc", "gate"]


async def test_start_run_and_describe(setup) -> None:
    app, client = setup
    r = await client.post("/api/workflows/calc", json={"kwargs": {"x": 21}})
    assert r.status_code == 201
    wf_id = r.json()["id"]
    assert wf_id.startswith("calc-")
    await app.worker().run_until_idle()
    detail = (await client.get(f"/api/workflows/{wf_id}")).json()
    assert detail["workflow"]["status"] == "SUCCEEDED" and detail["workflow"]["output"] == 42
    assert [(e["seq"], e["name"], e["status"]) for e in detail["journal"]] == [(1, "double", "completed")]
    assert (
        detail["attempts"][0]["error"] is None
        and detail["attempts"][0]["finished_at"]
        and detail["approvals"] == []
    )


async def test_start_is_idempotent_with_an_id_and_validates_input(setup) -> None:
    _, client = setup
    a = await client.post("/api/workflows/calc", json={"args": [1], "id": "order-7"})
    b = await client.post("/api/workflows/calc", json={"args": [99], "id": "order-7"})
    assert a.json() == b.json() == {"id": "order-7"}
    assert (await client.get("/api/workflows/order-7")).json()["workflow"]["input"] == {
        "args": [1],
        "kwargs": {},
    }
    assert (await client.post("/api/workflows/nope", json={})).status_code == 404
    assert (await client.post("/api/workflows/calc", json={"args": "not-a-list"})).status_code == 422
    # a body is optional
    assert (await client.post("/api/workflows/gate")).status_code == 201


async def test_list_filters_and_paginates(setup) -> None:
    app, client = setup
    for i in range(3):
        await client.post("/api/workflows/calc", json={"args": [i], "id": f"c{i}"})
    await client.post("/api/workflows/gate", json={"args": ["x"], "id": "g"})
    await app.worker().run_until_idle()
    every = (await client.get("/api/workflows")).json()["workflows"]
    assert {w["id"] for w in every} == {"c0", "c1", "c2", "g"}
    assert {w["id"] for w in (await client.get("/api/workflows?status=WAITING")).json()["workflows"]} == {"g"}
    assert len((await client.get("/api/workflows?name=calc")).json()["workflows"]) == 3
    page = (await client.get("/api/workflows?limit=2&offset=2")).json()["workflows"]
    assert len(page) == 2
    assert (await client.get("/api/workflows?status=BOGUS")).status_code == 422
    assert (await client.get("/api/workflows?limit=0")).status_code == 422


async def test_signal_with_json_payload_resumes_the_run(setup) -> None:
    app, client = setup
    await client.post("/api/workflows/gate", json={"args": ["a"], "id": "g1"})
    await app.worker().run_until_idle()
    assert (await client.get("/api/workflows/g1")).json()["workflow"]["status"] == "WAITING"
    r = await client.post("/api/workflows/g1/signals/go", json={"ok": True, "n": [1, 2]})
    assert r.json() == {"ok": True}
    await app.worker().run_until_idle()
    out = (await client.get("/api/workflows/g1")).json()
    assert out["workflow"]["output"] == {"label": "a", "first": 2, "payload": {"ok": True, "n": [1, 2]}}
    assert out["signals"][0]["consumed_seq"] == 2
    assert (await client.post("/api/workflows/missing/signals/go", json=1)).status_code == 404


async def test_signal_without_body_sends_null(setup) -> None:
    app, client = setup
    await client.post("/api/workflows/gate", json={"args": ["b"], "id": "g2"})
    await client.post("/api/workflows/g2/signals/go")
    await app.worker().run_until_idle()
    assert (await client.get("/api/workflows/g2")).json()["workflow"]["output"]["payload"] is None


async def test_cancel(setup) -> None:
    app, client = setup
    await client.post("/api/workflows/gate", json={"args": ["c"], "id": "g3"})
    await app.worker().run_until_idle()
    rec = (await client.post("/api/workflows/g3/cancel")).json()
    assert rec["status"] == "CANCELLED" and rec["cancel_requested"] is True
    assert (await client.post("/api/workflows/missing/cancel")).status_code == 404


async def test_fork(setup) -> None:
    app, client = setup
    await client.post("/api/workflows/gate", json={"args": ["d"], "id": "g4"})
    await app.worker().run_until_idle()
    # step 2 (the signal wait) is still pending, so forking after it is refused
    conflict = await client.post("/api/workflows/g4/fork", json={"from_seq": 3})
    assert conflict.status_code == 409 and "never finished" in conflict.json()["detail"]
    r = await client.post("/api/workflows/g4/fork", json={"from_seq": 2})
    assert r.status_code == 201
    new_id = r.json()["id"]
    detail = (await client.get(f"/api/workflows/{new_id}")).json()
    assert detail["workflow"]["forked_from"] == "g4" and detail["workflow"]["fork_seq"] == 2
    assert [e["seq"] for e in detail["journal"]] == [1]
    assert (await client.post("/api/workflows/g4/fork", json={"from_seq": 0})).status_code == 422
    assert (await client.post("/api/workflows/missing/fork", json={"from_seq": 1})).status_code == 404


async def test_unknown_workflow_is_404(setup) -> None:
    _, client = setup
    r = await client.get("/api/workflows/nope")
    assert r.status_code == 404 and "nope" in r.json()["detail"]


async def test_pending_agent_approvals_are_listed_and_answered(store: Store) -> None:
    app = Ninelives(store)

    @tool(requires_approval=True)
    def refund(order_id: str, amount_cents: int) -> dict:
        """Refund an order."""
        return {"refunded": amount_cents}

    llm = ScriptedLLM(
        [call("refund", {"order_id": "A1", "amount_cents": 500}, id="toolu_1"), say("Refunded.")]
    )
    agent = Agent(app, "api-support", llm=llm, tools=[refund])
    api = create_api(app)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api), base_url="http://test") as client:
        run = (await client.post("/api/workflows/agent:api-support", json={"args": ["Refund A1"]})).json()[
            "id"
        ]
        await app.worker().run_until_idle()
        detail = (await client.get(f"/api/workflows/{run}")).json()
        assert detail["workflow"]["status"] == "WAITING"
        [pending] = detail["approvals"]
        assert pending["tool"] == "refund" and pending["input"] == {"order_id": "A1", "amount_cents": 500}
        assert pending["call_id"] == "toolu_1" and pending["signal"] == "approval:toolu_1"
        assert pending["deadline"] is not None
        await client.post(
            f"/api/workflows/{run}/signals/{pending['signal']}", json={"approved": True, "note": "ok"}
        )
        await app.worker().run_until_idle()
        out = await agent.result(run)
    assert out["status"] == "completed" and out["actions"][0]["outcome"] == "ok"
    assert out["actions"][0]["approval"] == {"approved": True, "note": "ok"}


async def test_lifespan_runs_an_in_process_worker(tmp_path) -> None:
    app = build(f"sqlite:///{tmp_path}/api.db")  # type: ignore[arg-type]
    api = create_api(app, run_worker=True, worker_options={"poll_interval": 0.01})
    transport = httpx.ASGITransport(app=api)
    async with (
        api.router.lifespan_context(api),
        httpx.AsyncClient(transport=transport, base_url="http://test") as client,
    ):
        wf_id = (await client.post("/api/workflows/calc", json={"args": [5]})).json()["id"]
        assert await app.result(wf_id, timeout=10) == 10
    await app.close()


async def test_root_serves_the_dashboard_or_a_pointer(setup) -> None:
    _, client = setup
    r = await client.get("/")
    assert r.status_code == 200
    if (STATIC_DIR / "index.html").exists():
        assert '<div id="root">' in r.text
    else:
        assert "/docs" in r.text
    assert (await client.get("/openapi.json")).json()["info"]["title"] == "Ninelives"
