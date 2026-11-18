"""The OpenAI-compatible provider: message conversion, response mapping, errors, and a full agent
run against a fake chat-completions server."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from ninelives import Ninelives, PermanentError, Store
from ninelives.agents import Agent, OpenAICompatibleLLM, ProviderError, tool
from ninelives.agents.openai_compat import from_chat_response, to_chat_messages, to_chat_tools


def fake_server(
    replies: list[dict[str, Any]], seen: list[dict[str, Any]], status: int = 200
) -> httpx.AsyncClient:
    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(
            {"url": str(request.url), "headers": dict(request.headers), "body": json.loads(request.content)}
        )
        if status != 200:
            return httpx.Response(status, text="nope")
        return httpx.Response(200, json=replies[len(seen) - 1])

    return httpx.AsyncClient(transport=httpx.MockTransport(handle))


def chat_reply(
    content: str | None = None, calls: list[tuple[str, str, dict]] | None = None, finish: str = "stop"
) -> dict:
    message: dict[str, Any] = {"role": "assistant", "content": content}
    if calls:
        message["tool_calls"] = [
            {"id": cid, "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}
            for cid, name, args in calls
        ]
        finish = "tool_calls"
    return {
        "model": "test-model",
        "choices": [{"index": 0, "message": message, "finish_reason": finish}],
        "usage": {"prompt_tokens": 12, "completion_tokens": 5},
    }


def test_conversation_is_converted_to_chat_messages() -> None:
    messages = [
        {"role": "user", "content": "Where is A1?"},
        {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "Checking."},
                {"type": "tool_use", "id": "c1", "name": "lookup", "input": {"id": "A1"}},
                {"type": "tool_use", "id": "c2", "name": "refund", "input": {"id": "A1"}},
            ],
        },
        {
            "role": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": "c1", "content": '{"status": "late"}'},
                {"type": "tool_result", "tool_use_id": "c2", "content": "denied", "is_error": True},
            ],
        },
    ]
    chat = to_chat_messages("Be brief.", messages)
    assert chat[0] == {"role": "system", "content": "Be brief."}
    assert chat[1] == {"role": "user", "content": "Where is A1?"}
    assert chat[2]["content"] == "Checking." and [c["id"] for c in chat[2]["tool_calls"]] == ["c1", "c2"]
    assert json.loads(chat[2]["tool_calls"][0]["function"]["arguments"]) == {"id": "A1"}
    assert chat[3] == {"role": "tool", "tool_call_id": "c1", "content": '{"status": "late"}'}
    assert chat[4] == {"role": "tool", "tool_call_id": "c2", "content": "Error: denied"}
    assert to_chat_messages(
        None, [{"role": "assistant", "content": [{"type": "thinking", "thinking": ""}]}]
    ) == [{"role": "assistant", "content": None}]
    assert to_chat_tools([{"name": "lookup", "description": "Find", "input_schema": {"type": "object"}}]) == [
        {
            "type": "function",
            "function": {"name": "lookup", "description": "Find", "parameters": {"type": "object"}},
        }
    ]


def test_responses_are_mapped_back() -> None:
    turn = from_chat_response(chat_reply("Hi"))
    assert turn == {
        "content": [{"type": "text", "text": "Hi"}],
        "stop_reason": "end_turn",
        "model": "test-model",
        "usage": {"input_tokens": 12, "output_tokens": 5},
    }
    called = from_chat_response(chat_reply(calls=[("c9", "lookup", {"id": "B2"})]))
    assert called["stop_reason"] == "tool_use"
    assert called["content"] == [{"type": "tool_use", "id": "c9", "name": "lookup", "input": {"id": "B2"}}]
    assert from_chat_response(chat_reply("cut", finish="length"))["stop_reason"] == "max_tokens"
    assert from_chat_response(chat_reply("x", finish="content_filter"))["stop_reason"] == "refusal"
    refused = {
        "choices": [
            {"message": {"role": "assistant", "content": None, "refusal": "no"}, "finish_reason": "stop"}
        ]
    }
    assert from_chat_response(refused)["stop_reason"] == "refusal"
    broken = chat_reply(calls=[("c1", "lookup", {})])
    broken["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"] = "{not json"
    assert from_chat_response(broken)["content"][0]["input"] == {"_unparseable_arguments": "{not json"}
    with pytest.raises(ProviderError):
        from_chat_response({"choices": []})


async def test_request_shape_auth_and_options() -> None:
    seen: list[dict[str, Any]] = []
    llm = OpenAICompatibleLLM(
        "local-model",
        base_url="http://localhost:11434/v1/",
        api_key="sk-test",
        max_tokens=256,
        max_tokens_param="max_completion_tokens",
        temperature=0.2,
        headers={"X-Team": "ops"},
        extra_body={"seed": 7},
        client=fake_server([chat_reply("ok")], seen),
    )
    out = await llm.complete("sys", [{"role": "user", "content": "hi"}], [{"name": "t", "input_schema": {}}])
    assert out["content"] == [{"type": "text", "text": "ok"}]
    [req] = seen
    assert req["url"] == "http://localhost:11434/v1/chat/completions"
    assert req["headers"]["authorization"] == "Bearer sk-test" and req["headers"]["x-team"] == "ops"
    body = req["body"]
    assert body["model"] == "local-model" and body["max_completion_tokens"] == 256
    assert body["temperature"] == 0.2 and body["seed"] == 7 and body["tools"][0]["function"]["name"] == "t"
    with pytest.raises(ValueError):
        OpenAICompatibleLLM("")


async def test_key_from_environment_and_no_key_for_local_servers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_PROVIDER_KEY", "from-env")
    seen: list[dict[str, Any]] = []
    llm = OpenAICompatibleLLM("m", api_key_env="MY_PROVIDER_KEY", client=fake_server([chat_reply("a")], seen))
    await llm.complete(None, [{"role": "user", "content": "x"}], [])
    assert seen[0]["headers"]["authorization"] == "Bearer from-env"
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    seen.clear()
    local = OpenAICompatibleLLM(
        "m", base_url="http://localhost:8000/v1", client=fake_server([chat_reply("b")], seen)
    )
    await local.complete(None, [{"role": "user", "content": "x"}], [])
    assert "authorization" not in seen[0]["headers"]


@pytest.mark.parametrize(
    ("status", "error"),
    [(401, PermanentError), (400, PermanentError), (429, ProviderError), (503, ProviderError)],
)
async def test_errors_are_permanent_or_retryable(status: int, error: type[Exception]) -> None:
    llm = OpenAICompatibleLLM("m", api_key="k", client=fake_server([], [], status=status))
    with pytest.raises(error):
        await llm.complete(None, [{"role": "user", "content": "x"}], [])


async def test_an_agent_runs_its_tool_loop_through_the_provider(store: Store) -> None:
    app = Ninelives(store)
    looked_up: list[str] = []

    @tool
    def lookup_order(order_id: str) -> dict:
        """Look up an order.

        order_id: the order id
        """
        looked_up.append(order_id)
        return {"order_id": order_id, "status": "shipped"}

    seen: list[dict[str, Any]] = []
    replies = [
        chat_reply("Let me check.", calls=[("call_1", "lookup_order", {"order_id": "A-7"})]),
        chat_reply("Order A-7 has shipped."),
    ]
    llm = OpenAICompatibleLLM("m", api_key="k", client=fake_server(replies, seen))
    agent = Agent(app, "chat-support", llm=llm, tools=[lookup_order], system="You help customers.")
    run = await agent.start("Where is A-7?")
    await app.worker().run_until_idle()
    out = await agent.result(run)
    assert out["status"] == "completed" and out["answer"] == "Order A-7 has shipped."
    assert looked_up == ["A-7"] and out["usage"] == {"input_tokens": 24, "output_tokens": 10}
    second = seen[1]["body"]["messages"]
    assert second[0] == {"role": "system", "content": "You help customers."}
    assert second[-1] == {
        "role": "tool",
        "tool_call_id": "call_1",
        "content": json.dumps({"order_id": "A-7", "status": "shipped"}),
    }
    await llm.aclose()
