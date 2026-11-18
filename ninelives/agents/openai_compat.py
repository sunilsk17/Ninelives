"""A provider for any chat-completions API in the OpenAI wire format.

That covers OpenAI itself and the many services and servers that copy its API: Groq, Together,
OpenRouter, DeepSeek, Mistral, Fireworks, Azure OpenAI's v1 endpoint, and local servers such as
Ollama (``http://localhost:11434/v1``), vLLM and LM Studio.

Conversations inside Ninelives are kept in one neutral shape (messages made of ``text``,
``tool_use`` and ``tool_result`` blocks). This module converts that shape to chat-completions
messages and converts the reply back, so the agent loop does not care which provider it talks to.
"""

from __future__ import annotations

import json
import os
from typing import Any

from ..errors import PermanentError

# Provider errors that a retry cannot fix: the request itself is wrong, or so is the key.
PERMANENT_STATUS = {400, 401, 403, 404, 405, 409, 413, 415, 422}
FINISH_REASONS = {
    "stop": "end_turn",
    "tool_calls": "tool_use",
    "function_call": "tool_use",
    "length": "max_tokens",
    "content_filter": "refusal",
}


class ProviderError(RuntimeError):
    """A provider failure worth retrying (rate limit, overload, server error)."""

    def __init__(self, status: int, message: str):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status


def to_chat_messages(system: str | None, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ninelives conversation -> chat-completions ``messages``."""
    out: list[dict[str, Any]] = [{"role": "system", "content": system}] if system else []
    for message in messages:
        role, content = message["role"], message["content"]
        if isinstance(content, str):
            out.append({"role": role, "content": content})
            continue
        texts = [b.get("text", "") for b in content if b.get("type") == "text"]
        if role == "assistant":
            calls = [
                {
                    "id": b["id"],
                    "type": "function",
                    "function": {"name": b["name"], "arguments": json.dumps(b.get("input") or {})},
                }
                for b in content
                if b.get("type") == "tool_use"
            ]
            turn: dict[str, Any] = {"role": "assistant", "content": "".join(texts) or None}
            if calls:
                turn["tool_calls"] = calls
            out.append(turn)
            continue
        # A user turn: tool results become "tool" messages, any text a user message after them.
        for b in content:
            if b.get("type") == "tool_result":
                text = b.get("content")
                if not isinstance(text, str):
                    text = json.dumps(text)
                if b.get("is_error"):
                    text = f"Error: {text}"
                out.append({"role": "tool", "tool_call_id": b["tool_use_id"], "content": text})
        if texts:
            out.append({"role": "user", "content": "".join(texts)})
    return out


def to_chat_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t.get("description", ""),
                "parameters": t.get("input_schema") or {"type": "object", "properties": {}},
            },
        }
        for t in tools
    ]


def from_chat_response(data: dict[str, Any]) -> dict[str, Any]:
    """Chat-completions response -> one Ninelives turn."""
    choices = data.get("choices") or []
    if not choices:
        raise ProviderError(502, "the provider returned no choices")
    choice = choices[0]
    message = choice.get("message") or {}
    content: list[dict[str, Any]] = []
    if message.get("content"):
        content.append({"type": "text", "text": message["content"]})
    for call in message.get("tool_calls") or []:
        function = call.get("function") or {}
        raw = function.get("arguments") or "{}"
        try:
            arguments = json.loads(raw) if isinstance(raw, str) else raw
        except json.JSONDecodeError:
            arguments = {"_unparseable_arguments": raw}  # reported back to the model as bad arguments
        if not isinstance(arguments, dict):
            arguments = {"_unparseable_arguments": raw}
        content.append(
            {
                "type": "tool_use",
                "id": call.get("id", ""),
                "name": function.get("name", ""),
                "input": arguments,
            }
        )
    finish = choice.get("finish_reason") or "stop"
    stop_reason = "tool_use" if message.get("tool_calls") else FINISH_REASONS.get(finish, finish)
    if message.get("refusal") and not content:
        stop_reason = "refusal"
    usage = data.get("usage") or {}
    return {
        "content": content,
        "stop_reason": stop_reason,
        "model": data.get("model", ""),
        "usage": {
            "input_tokens": int(usage.get("prompt_tokens") or 0),
            "output_tokens": int(usage.get("completion_tokens") or 0),
        },
    }


class OpenAICompatibleLLM:
    """A chat-completions model reached over HTTP.

    >>> llm = OpenAICompatibleLLM("llama3.1", base_url="http://localhost:11434/v1")        # Ollama
    >>> llm = OpenAICompatibleLLM("my-model", base_url="https://api.example.com/v1",
    ...                           api_key_env="EXAMPLE_API_KEY")

    The key is read from ``api_key`` or the ``api_key_env`` environment variable (default
    ``OPENAI_API_KEY``); local servers usually need none. Rate limits, 5xx responses and
    network errors raise retryable errors, so the agent's LLM step retries them with backoff;
    a rejected request or key fails at once.
    """

    def __init__(
        self,
        model: str,
        *,
        base_url: str = "https://api.openai.com/v1",
        api_key: str | None = None,
        api_key_env: str = "OPENAI_API_KEY",
        max_tokens: int | None = None,
        max_tokens_param: str = "max_tokens",
        temperature: float | None = None,
        timeout: float = 120.0,
        headers: dict[str, str] | None = None,
        extra_body: dict[str, Any] | None = None,
        client: Any = None,
    ):
        if not model:
            raise ValueError("model is required")
        self.model = model
        self.name = f"openai-compatible:{model}"
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key if api_key is not None else os.environ.get(api_key_env)
        self.max_tokens = max_tokens
        self.max_tokens_param = max_tokens_param
        self.temperature = temperature
        self.timeout = timeout
        self.headers = dict(headers or {})
        self.extra_body = dict(extra_body or {})
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            try:
                import httpx
            except ImportError as exc:  # pragma: no cover
                raise ImportError("OpenAICompatibleLLM needs httpx: pip install 'ninelives[agents]'") from exc
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    def request_body(
        self, system: str | None, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"model": self.model, "messages": to_chat_messages(system, messages)}
        if tools:
            body["tools"] = to_chat_tools(tools)
        if self.max_tokens is not None:
            body[self.max_tokens_param] = self.max_tokens
        if self.temperature is not None:
            body["temperature"] = self.temperature
        body.update(self.extra_body)
        return body

    async def complete(
        self, system: str | None, messages: list[dict[str, Any]], tools: list[dict[str, Any]]
    ) -> dict[str, Any]:
        headers = {"Content-Type": "application/json", **self.headers}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        response = await self.client.post(
            f"{self.base_url}/chat/completions",
            json=self.request_body(system, messages, tools),
            headers=headers,
        )
        if response.status_code >= 400:
            detail = response.text[:500]
            if response.status_code in PERMANENT_STATUS:
                raise PermanentError(f"provider rejected the request (HTTP {response.status_code}): {detail}")
            raise ProviderError(response.status_code, detail)
        return from_chat_response(response.json())

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
