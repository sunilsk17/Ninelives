"""A durable tool-calling agent: every LLM call and every tool call is a journaled step."""

from __future__ import annotations

import inspect
import json
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from ..context import WorkflowContext
from ..errors import SignalTimeout, StepFailed
from ..types import RetryPolicy
from .llm import LLM, text_of, tool_calls_of
from .tools import Tool, make_tool

if TYPE_CHECKING:
    from ..app import Ninelives

LLM_RETRY = RetryPolicy(max_attempts=4, initial_delay=2.0, backoff=2.0, max_delay=30.0)


def approval_signal(call_id: str) -> str:
    return f"approval:{call_id}"


class Agent:
    """Register a durable agent on ``app``.

    >>> support = Agent(app, "support", llm=AnthropicLLM(), tools=[lookup_order, refund],
    ...                 system="You are a support agent.", max_turns=10)
    >>> run_id = await support.start("Customer 42 wants a refund for order A-17")

    Each run is a workflow named ``agent:<name>``. Crash it at any point and it resumes from the
    last finished LLM call or tool call, without paying for the same tokens twice. Tools created
    with ``requires_approval=True`` pause the run until ``approve()`` (or the dashboard) answers.
    """

    def __init__(
        self,
        app: Ninelives,
        name: str,
        *,
        llm: LLM,
        tools: Sequence[Tool | Callable[..., Any]] = (),
        system: str | None = None,
        max_turns: int = 12,
        max_total_tokens: int | None = None,
        approval_timeout: float | None = 24 * 3600,
        llm_retry: RetryPolicy = LLM_RETRY,
    ):
        self.app = app
        self.name = name
        self.llm = llm
        self.system = system
        self.max_turns = max_turns
        self.max_total_tokens = max_total_tokens
        self.approval_timeout = approval_timeout
        self.tools: dict[str, Tool] = {}
        for t in tools:
            spec = t if isinstance(t, Tool) else make_tool(t)
            if spec.name in self.tools:
                raise ValueError(f"duplicate tool name {spec.name!r}")
            self.tools[spec.name] = spec
        self.workflow_name = f"agent:{name}"
        self._llm_step = app.step(name=f"agent:{name}:llm", retry=llm_retry)(self._call_llm)
        self._tool_steps = {
            t.name: app.step(name=f"tool:{t.name}", retry=t.retry, timeout=t.timeout)(t.fn) for t in self.tools.values()
        }
        app.workflow(name=self.workflow_name)(self._run)

    async def _call_llm(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        return await self.llm.complete(self.system, messages, [t.spec() for t in self.tools.values()])

    # ------------------------------------------------------------------ client helpers
    async def start(self, prompt: str, *, id: str | None = None) -> str:
        return await self.app.start(self.workflow_name, prompt, id=id)

    async def approve(self, run_id: str, call_id: str, approved: bool = True, note: str | None = None) -> None:
        await self.app.signal(run_id, approval_signal(call_id), {"approved": approved, "note": note})

    async def result(self, run_id: str, timeout: float | None = None) -> dict[str, Any]:
        out = await self.app.result(run_id, timeout=timeout)
        assert isinstance(out, dict)
        return out

    # ------------------------------------------------------------------ the loop
    async def _run(self, ctx: WorkflowContext, prompt: str) -> dict[str, Any]:
        messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]
        usage = {"input_tokens": 0, "output_tokens": 0}
        actions: list[dict[str, Any]] = []

        def finish(status: str, answer: str | None = None, **extra: Any) -> dict[str, Any]:
            return {"status": status, "answer": answer, "turns": turn + 1, "usage": usage, "actions": actions, **extra}

        turn = 0
        for turn in range(self.max_turns):
            response = await ctx.step(self._llm_step, messages)
            usage["input_tokens"] += response.get("usage", {}).get("input_tokens", 0)
            usage["output_tokens"] += response.get("usage", {}).get("output_tokens", 0)
            stop = response.get("stop_reason")
            content = response.get("content", [])
            if stop == "refusal":
                # Server-side fallbacks (if enabled) already tried another model.
                return finish("refused", None, details=response.get("stop_details"))
            # Append the turn unchanged so thinking blocks and other provider blocks round-trip.
            messages.append({"role": "assistant", "content": content})
            if stop == "pause_turn":
                continue
            calls = tool_calls_of(content)
            if not calls:
                return finish("completed", text_of(content), stop_reason=stop)

            results: list[dict[str, Any]] = []
            for call in calls:
                results.append(await self._run_tool(ctx, call, actions))
            # All results of one turn go back in a single user message.
            messages.append({"role": "user", "content": results})

            if self.max_total_tokens is not None and usage["input_tokens"] + usage["output_tokens"] >= self.max_total_tokens:
                return finish("budget_exhausted", text_of(content))
        return finish("max_turns", text_of(messages[-2]["content"]) if len(messages) >= 2 else None)

    async def _run_tool(self, ctx: WorkflowContext, call: dict[str, Any], actions: list[dict[str, Any]]) -> dict[str, Any]:
        tool = self.tools.get(call["name"])
        record: dict[str, Any] = {"tool": call["name"], "call_id": call["id"], "input": call["input"]}
        actions.append(record)
        if tool is None:
            record["outcome"] = "unknown_tool"
            return _result(call["id"], f"Unknown tool {call['name']!r}. Available: {', '.join(self.tools)}", error=True)
        if tool.requires_approval:
            try:
                decision = await ctx.wait_for_signal(approval_signal(call["id"]), timeout=self.approval_timeout) or {}
            except SignalTimeout:
                decision = {"approved": False, "note": "no decision before the approval timeout"}
            record["approval"] = decision
            if not decision.get("approved"):
                record["outcome"] = "denied"
                note = decision.get("note") or "no reason given"
                return _result(call["id"], f"A human reviewer denied this action ({note}). Do not retry it.", error=True)
        try:
            # Check the model's arguments against the signature before running anything.
            inspect.signature(tool.fn).bind(**call["input"])
        except TypeError as exc:
            record["outcome"] = "bad_arguments"
            return _result(call["id"], f"Invalid arguments: {exc}", error=True)
        try:
            output = await ctx.step(self._tool_steps[tool.name], **call["input"])
        except StepFailed as exc:
            record["outcome"] = "failed"
            return _result(call["id"], f"The tool failed: {exc.error.get('message')}", error=True)
        record["outcome"] = "ok"
        record["output"] = output
        return _result(call["id"], output)


def _result(call_id: str, output: Any, *, error: bool = False) -> dict[str, Any]:
    text = output if isinstance(output, str) else json.dumps(output, ensure_ascii=False)
    block: dict[str, Any] = {"type": "tool_result", "tool_use_id": call_id, "content": text}
    if error:
        block["is_error"] = True
    return block
