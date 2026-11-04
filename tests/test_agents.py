"""Durable agents: tool loop, approvals, budgets, crash recovery without repeated LLM calls."""

from __future__ import annotations

import asyncio
import enum
from collections import Counter
from typing import Literal

import httpx
import pytest

from ninelives import Ninelives, PermanentError, RetryPolicy, Status, Store, step_info
from ninelives.agents import Agent, AnthropicLLM, ScriptedLLM, approval_signal, json_schema, tool

from .conftest import FakeClock

say, call = ScriptedLLM.say, ScriptedLLM.call


def shop(app: Ninelives, llm: ScriptedLLM, name: str = "support", **kw) -> tuple[Agent, Counter[str], list[str]]:
    effects: Counter[str] = Counter()
    keys: list[str] = []

    @tool
    def lookup_order(order_id: str) -> dict:
        """Look up an order.

        order_id: the order number
        """
        effects["lookup"] += 1
        return {"order_id": order_id, "total_cents": 4999, "status": "delivered"}

    @tool(requires_approval=True, retry=RetryPolicy(max_attempts=1))
    def refund(order_id: str, amount_cents: int) -> dict:
        """Refund an order (needs a human)."""
        effects["refund"] += 1
        keys.append(step_info().idempotency_key)
        return {"refund_id": f"R-{order_id}", "amount_cents": amount_cents}

    agent = Agent(app, name, llm=llm, tools=[lookup_order, refund], system="You are support.", **kw)
    return agent, effects, keys


async def test_agent_uses_tools_and_answers(store: Store) -> None:
    app = Ninelives(store)
    llm = ScriptedLLM([call("lookup_order", {"order_id": "A1"}), say("Order A1 was delivered.")])
    agent, effects, _ = shop(app, llm)
    run = await agent.start("Where is A1?")
    await app.worker().run_until_idle()
    out = await agent.result(run)
    assert out["status"] == "completed" and out["answer"] == "Order A1 was delivered."
    assert out["turns"] == 2 and llm.calls == 2 and effects == {"lookup": 1}
    assert [a["outcome"] for a in out["actions"]] == ["ok"]
    names = [e.name for e in await store.get_journal(run)]
    assert names == ["agent:support:llm", "tool:lookup_order", "agent:support:llm"]


async def test_approval_flow_approve_and_deny(store: Store) -> None:
    app = Ninelives(store)
    refund_call = call("refund", {"order_id": "A1", "amount_cents": 4999}, id="toolu_refund")
    approve_llm = ScriptedLLM([refund_call, say("Refunded.")])
    agent, effects, keys = shop(app, approve_llm)
    run = await agent.start("Refund A1")
    worker = app.worker()
    await worker.run_until_idle()
    rec = await app.get(run)
    assert rec.status == Status.WAITING and effects["refund"] == 0
    await agent.approve(run, "toolu_refund", note="ok by ops")
    await worker.run_until_idle()
    out = await agent.result(run)
    assert out["answer"] == "Refunded." and effects["refund"] == 1
    assert out["actions"][0]["approval"] == {"approved": True, "note": "ok by ops"}
    assert keys == [f"{run}:3"]  # llm=1, approval wait=2, refund=3

    deny_app = Ninelives(store)
    seen: list[dict] = []

    def replies(messages: list[dict]) -> dict:
        if len(messages) == 1:
            return call("refund", {"order_id": "B2", "amount_cents": 10}, id="toolu_b2")
        seen.append(messages[-1]["content"][0])
        return say("I could not refund it.")

    denier, deny_effects, _ = shop(deny_app, ScriptedLLM(replies), name="denier")
    run2 = await denier.start("Refund B2")
    w2 = deny_app.worker()
    await w2.run_until_idle()
    await deny_app.signal(run2, approval_signal("toolu_b2"), {"approved": False, "note": "fraud risk"})
    await w2.run_until_idle()
    out2 = await denier.result(run2)
    assert out2["status"] == "completed" and deny_effects["refund"] == 0
    assert seen[0]["is_error"] is True and "fraud risk" in seen[0]["content"]


async def test_approval_timeout_counts_as_denial(store: Store, clock: FakeClock) -> None:
    app = Ninelives(store, clock=clock)
    agent, effects, _ = shop(
        app,
        ScriptedLLM([call("refund", {"order_id": "A1", "amount_cents": 1}), say("Escalated to a human.")]),
        approval_timeout=3600,
    )
    run = await agent.start("Refund A1")
    worker = app.worker()
    await worker.run_until_idle()
    clock.advance(3601)
    await worker.run_until_idle()
    out = await agent.result(run)
    assert out["actions"][0]["outcome"] == "denied" and effects["refund"] == 0


async def test_crashed_agent_resumes_without_repeating_llm_calls(store: Store, clock: FakeClock) -> None:
    app = Ninelives(store, clock=clock)
    llm = ScriptedLLM([call("slow_report", {"topic": "q3"}), say("Report sent.")])
    executions: list[str] = []
    gate = asyncio.Event()

    @tool
    async def slow_report(topic: str) -> str:
        """Build and email a report."""
        executions.append(step_info().idempotency_key)
        if len(executions) == 1:
            await gate.wait()  # worker dies here, mid-tool
        return f"report:{topic}"

    agent = Agent(app, "reporter", llm=llm, tools=[slow_report])
    run = await agent.start("Send the Q3 report")
    dying = app.worker(lease_ttl=10)
    await dying._claim_once()
    while not executions:
        await asyncio.sleep(0.005)
    for task in list(dying._running.values()):
        task.cancel()
    clock.advance(11)
    await app.worker().run_until_idle()
    out = await agent.result(run)
    assert out["answer"] == "Report sent."
    assert llm.calls == 2  # the first LLM turn was replayed from the journal, not re-billed
    assert executions == [f"{run}:2", f"{run}:2"]  # tool re-run once, same idempotency key


async def test_refusal_budget_and_turn_limits(store: Store) -> None:
    app = Ninelives(store)
    refused, _, _ = shop(app, ScriptedLLM([ScriptedLLM.refuse()]))
    r1 = await refused.start("something disallowed")

    loop_app = Ninelives(store)
    looping, _, _ = shop(loop_app, ScriptedLLM(lambda m: call("lookup_order", {"order_id": f"X{len(m)}"})), name="looper", max_turns=3)
    budget_app = Ninelives(store)
    capped, _, _ = shop(budget_app, ScriptedLLM(lambda m: call("lookup_order", {"order_id": "Y"})), name="capped", max_total_tokens=1)

    r2 = await looping.start("loop forever")
    r3 = await capped.start("expensive")
    await app.worker().run_until_idle()
    await loop_app.worker().run_until_idle()
    await budget_app.worker().run_until_idle()
    assert (await refused.result(r1))["status"] == "refused"
    out2 = await looping.result(r2)
    assert out2["status"] == "max_turns" and out2["turns"] == 3
    assert (await capped.result(r3))["status"] == "budget_exhausted"


async def test_unknown_tool_bad_arguments_and_failures_go_back_to_the_model(store: Store) -> None:
    app = Ninelives(store)
    fed_back: list[str] = []

    @tool(retry=RetryPolicy(max_attempts=1))
    def explode(x: int) -> int:
        """Always fails."""
        raise RuntimeError("disk full")

    def replies(messages: list[dict]) -> dict:
        n = len(messages)
        if n == 1:
            return ScriptedLLM.calls_(call("nope", {}, id="a"), call("explode", {"y": 1}, id="b"), call("explode", {"x": 1}, id="c"))
        fed_back.extend(r["content"] for r in messages[-1]["content"])
        return say("done")

    agent = Agent(app, "errs", llm=ScriptedLLM(replies), tools=[explode])
    run = await agent.start("go")
    await app.worker().run_until_idle()
    out = await agent.result(run)
    assert [a["outcome"] for a in out["actions"]] == ["unknown_tool", "bad_arguments", "failed"]
    assert "Unknown tool" in fed_back[0] and "Invalid arguments" in fed_back[1] and "disk full" in fed_back[2]


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


def test_tool_schemas_from_type_hints() -> None:
    @tool(description="Paint things")
    def paint(item: str, color: Color, coats: int = 2, tags: list[str] | None = None, mode: Literal["fast", "slow"] = "fast") -> None:
        """Ignored because description is given.

        item: what to paint
        """

    assert paint.description == "Paint things"
    schema = paint.input_schema
    assert schema["required"] == ["item", "color"]
    assert schema["properties"]["item"] == {"type": "string", "description": "what to paint"}
    assert schema["properties"]["color"] == {"enum": ["red", "blue"]}
    assert schema["properties"]["tags"] == {"type": "array", "items": {"type": "string"}}
    assert schema["properties"]["mode"] == {"type": "string", "enum": ["fast", "slow"]}
    assert json_schema(dict[str, int]) == {"type": "object", "additionalProperties": {"type": "integer"}}
    assert json_schema(int | str) == {"anyOf": [{"type": "integer"}, {"type": "string"}]}
    assert paint.spec()["name"] == "paint"


def test_duplicate_tools_rejected() -> None:
    app = Ninelives("sqlite://:memory:")

    def a() -> None: ...

    with pytest.raises(ValueError, match="duplicate"):
        Agent(app, "x", llm=ScriptedLLM([]), tools=[a, a])


# ---------------------------------------------------------------------- AnthropicLLM without network


class FakeMessages:
    def __init__(self, response: object = None, error: Exception | None = None):
        self.response = response
        self.error = error
        self.kwargs: dict = {}

    async def create(self, **kwargs):
        self.kwargs = kwargs
        if self.error:
            raise self.error
        return self.response


class FakeClient:
    def __init__(self, messages: FakeMessages):
        self.messages = messages
        self.beta = type("Beta", (), {"messages": messages})()


def sdk_message(content: list[dict], stop_reason: str = "end_turn"):
    from anthropic.types.beta import BetaMessage

    return BetaMessage.model_validate(
        {
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": "claude-opus-5-5",
            "content": content,
            "stop_reason": stop_reason,
            "stop_sequence": None,
            "usage": {"input_tokens": 120, "output_tokens": 30},
        }
    )


async def test_anthropic_llm_request_shape_and_round_trip() -> None:
    blocks = [
        {"type": "thinking", "thinking": "", "signature": "sig-abc"},
        {"type": "text", "text": "Checking."},
        {"type": "tool_use", "id": "toolu_1", "name": "lookup_order", "input": {"order_id": "A1"}},
    ]
    fake = FakeMessages(sdk_message(blocks, "tool_use"))
    llm = AnthropicLLM(client=FakeClient(fake))
    tools = [{"name": "lookup_order", "description": "d", "input_schema": {"type": "object", "properties": {}}}]
    out = await llm.complete("sys", [{"role": "user", "content": "hi"}], tools)
    sent = fake.kwargs
    assert sent["model"] == "claude-opus-5-5" and sent["system"] == "sys" and sent["tools"] == tools
    assert sent["output_config"] == {"effort": "high"}
    assert sent["fallbacks"] == "default" and sent["betas"] == ["server-side-fallback-2026-07-01"]
    assert "thinking" not in sent and "tool_choice" not in sent
    assert out["stop_reason"] == "tool_use" and out["usage"]["input_tokens"] == 120
    assert out["content"][0] == {"type": "thinking", "thinking": "", "signature": "sig-abc"}  # passed back unchanged
    assert out["content"][2]["input"] == {"order_id": "A1"}


async def test_anthropic_llm_without_fallbacks_and_error_mapping() -> None:
    import anthropic

    fake = FakeMessages(sdk_message([{"type": "text", "text": "hi"}]))
    llm = AnthropicLLM(model="claude-sonnet-5-5", effort="medium", fallbacks=False, client=FakeClient(fake))
    out = await llm.complete(None, [{"role": "user", "content": "x"}], [])
    assert "betas" not in fake.kwargs and "system" not in fake.kwargs and "tools" not in fake.kwargs
    assert out["content"] == [{"type": "text", "text": "hi"}]

    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    bad = anthropic.BadRequestError("bad", response=httpx.Response(400, request=request), body=None)
    with pytest.raises(PermanentError):
        await AnthropicLLM(client=FakeClient(FakeMessages(error=bad))).complete(None, [], [])
    limited = anthropic.RateLimitError("slow down", response=httpx.Response(429, request=request), body=None)
    with pytest.raises(anthropic.RateLimitError):  # retryable: left to the step's retry policy
        await AnthropicLLM(client=FakeClient(FakeMessages(error=limited))).complete(None, [], [])
