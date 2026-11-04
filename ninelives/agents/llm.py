"""LLM providers. Conversations use the Claude Messages API shape (role + content blocks);
other providers can implement ``LLM`` by converting to and from it."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Sequence
from typing import Any, Protocol

from ..errors import PermanentError


class LLM(Protocol):
    """A model that takes a conversation and tool definitions and returns one turn.

    ``complete`` returns a JSON-serialisable dict:
    ``{"content": [blocks], "stop_reason": str, "usage": {...}, "model": str}``.
    ``content`` is appended to the conversation unchanged, so provider-specific blocks
    (thinking, fallback markers) survive the round trip.
    """

    name: str

    async def complete(
        self, system: str | None, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> dict[str, Any]: ...


def text_of(content: Sequence[dict[str, Any]]) -> str:
    return "".join(b.get("text", "") for b in content if b.get("type") == "text")


def tool_calls_of(content: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"id": b["id"], "name": b["name"], "input": b.get("input") or {}}
        for b in content
        if b.get("type") == "tool_use"
    ]


class AnthropicLLM:
    """Claude through the official ``anthropic`` SDK.

    Defaults follow current guidance for Claude Opus 5.5: adaptive thinking is always on,
    so depth is set with ``effort``; tool choice is left to the model (``auto``); and
    server-side refusal fallbacks are enabled (``fallbacks="default"``), so a declined
    request is retried on Anthropic's recommended fallback model inside the same call.
    """

    FALLBACK_BETA = "server-side-fallback-2026-07-01"

    def __init__(
        self,
        model: str = "claude-opus-5-5",
        *,
        max_tokens: int = 16000,
        effort: str = "high",
        fallbacks: bool = True,
        client: Any = None,
    ):
        self.model = model
        self.name = f"anthropic:{model}"
        self.max_tokens = max_tokens
        self.effort = effort
        self.fallbacks = fallbacks
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            try:
                import anthropic
            except ImportError as exc:  # pragma: no cover
                raise ImportError("AnthropicLLM needs the SDK: pip install 'ninelives[agents]'") from exc
            self._client = anthropic.AsyncAnthropic()
        return self._client

    async def complete(
        self, system: str | None, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> dict[str, Any]:
        import anthropic

        kwargs: dict[str, Any] = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "messages": messages,
            "output_config": {"effort": self.effort},
        }
        if system:
            kwargs["system"] = system
        if tools:
            kwargs["tools"] = tools
        try:
            if self.fallbacks:
                response = await self.client.beta.messages.create(
                    **kwargs, betas=[self.FALLBACK_BETA], fallbacks="default"
                )
            else:
                response = await self.client.messages.create(**kwargs)
        except (
            anthropic.BadRequestError,
            anthropic.AuthenticationError,
            anthropic.PermissionDeniedError,
            anthropic.NotFoundError,
        ) as exc:
            # Retrying cannot fix these; fail the step at once.
            raise PermanentError(f"{type(exc).__name__}: {exc}") from exc
        # RateLimitError, 5xx and connection errors propagate and are retried by the step's policy.
        usage = response.usage
        out: dict[str, Any] = {
            "content": [block.model_dump(mode="json", exclude_none=True) for block in response.content],
            "stop_reason": response.stop_reason,
            "model": response.model,
            "usage": {
                "input_tokens": usage.input_tokens or 0,
                "output_tokens": usage.output_tokens or 0,
                "cache_read_input_tokens": getattr(usage, "cache_read_input_tokens", 0) or 0,
            },
        }
        details = getattr(response, "stop_details", None)
        if details is not None:
            out["stop_details"] = details.model_dump(mode="json", exclude_none=True)
        return out


Reply = dict[str, Any]


class ScriptedLLM:
    """A deterministic stand-in for tests, demos and CI: no network, no API key.

    Give it a list of replies (built with ``ScriptedLLM.say`` / ``ScriptedLLM.call``) or a
    function ``(messages) -> reply``. ``calls`` counts real invocations, which is how tests
    prove that recovered agents do not repeat LLM calls.
    """

    def __init__(
        self, replies: Sequence[Reply] | Callable[[list[dict[str, Any]]], Reply], name: str = "scripted"
    ):
        self.name = name
        self._replies = replies
        self.calls = 0

    @staticmethod
    def say(text: str) -> Reply:
        return {"content": [{"type": "text", "text": text}], "stop_reason": "end_turn"}

    @staticmethod
    def call(name: str, input: dict[str, Any], *, id: str | None = None, text: str | None = None) -> Reply:
        blocks: list[dict[str, Any]] = [{"type": "text", "text": text}] if text else []
        blocks.append(
            {
                "type": "tool_use",
                "id": id
                or "toolu_"
                + hashlib.sha1(f"{name}:{json.dumps(input, sort_keys=True)}".encode()).hexdigest()[:12],
                "name": name,
                "input": input,
            }
        )
        return {"content": blocks, "stop_reason": "tool_use"}

    @staticmethod
    def calls_(*calls: Reply) -> Reply:
        """Several tool calls in one turn."""
        return {"content": [b for c in calls for b in c["content"]], "stop_reason": "tool_use"}

    @staticmethod
    def refuse() -> Reply:
        return {
            "content": [],
            "stop_reason": "refusal",
            "stop_details": {"type": "refusal", "category": "cyber"},
        }

    async def complete(
        self, system: str | None, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> dict[str, Any]:
        index = self.calls
        self.calls += 1
        if callable(self._replies):
            reply = self._replies(messages)
        else:
            if index >= len(self._replies):
                raise PermanentError(f"ScriptedLLM ran out of replies after {len(self._replies)}")
            reply = self._replies[index]
        words = sum(len(str(m.get("content", ""))) for m in messages) // 4
        return {
            "content": reply["content"],
            "stop_reason": reply["stop_reason"],
            "model": self.name,
            "usage": {"input_tokens": words, "output_tokens": 20, "cache_read_input_tokens": 0},
            **({"stop_details": reply["stop_details"]} if "stop_details" in reply else {}),
        }
