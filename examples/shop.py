"""A small online shop on Ninelives: checkout with a flaky payment provider, expense approvals that
wait for a human, and a support agent whose refunds need sign-off.

Run the dashboard with a worker in one process:

    ninelives serve examples/shop.py:app --worker
    # open http://127.0.0.1:8000

Start runs from the dashboard, or from the command line:

    ninelives start examples/shop.py:app checkout --args '["order-1", 4999]'
    ninelives start examples/shop.py:app expense --args '["dana", 1200]'
    ninelives start examples/shop.py:app agent:support --args '["Please refund order A-1001, it arrived broken"]'

The support agent needs a language model. With no configuration it uses a scripted stand-in, so
the whole demo works offline. To use a real model, set one of:

    LLM_MODEL=<model> [LLM_BASE_URL=<url>] [LLM_API_KEY=<key>]   # any OpenAI-compatible API, or a local
                                                                 # server such as Ollama or vLLM
    ANTHROPIC_API_KEY=<key> [LLM_MODEL=<model>]                  # Anthropic's API
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
from typing import Any

from ninelives import (
    Ninelives,
    PermanentError,
    RetryPolicy,
    SignalTimeout,
    StepFailed,
    WorkflowContext,
    step_info,
)
from ninelives.agents import LLM, Agent, AnthropicLLM, OpenAICompatibleLLM, ScriptedLLM, tool

app = Ninelives(os.environ.get("NINELIVES_DB", "sqlite:///shop.db"))

#: Share of payment calls that fail with a transient error, so retries are visible in the dashboard.
FLAKINESS = float(os.environ.get("SHOP_FLAKINESS", "0.4"))
#: Seconds the checkout waits before sending the follow-up email (a durable timer).
FOLLOW_UP_DELAY = float(os.environ.get("SHOP_FOLLOW_UP_DELAY", "3"))

ORDERS: dict[str, dict[str, Any]] = {
    "A-1001": {"order_id": "A-1001", "item": "ceramic teapot", "total_cents": 4999, "status": "delivered"},
    "A-1002": {"order_id": "A-1002", "item": "walnut desk", "total_cents": 64900, "status": "in transit"},
}


# --------------------------------------------------------------------------- checkout
@app.step()
async def reserve_inventory(order_id: str, items: list[str]) -> dict[str, Any]:
    return {"reservation_id": f"res-{order_id}", "items": items}


@app.step(retry=RetryPolicy(max_attempts=6, initial_delay=0.2, max_delay=2.0))
async def charge_card(order_id: str, amount_cents: int) -> dict[str, Any]:
    """Payment providers deduplicate on an idempotency key; ours is stable across retries and crashes."""
    key = step_info().idempotency_key
    if amount_cents > 100_000:
        raise PermanentError("card declined: amount over limit")  # retrying will not help
    if random.random() < FLAKINESS:
        raise ConnectionError("payment provider timed out")  # retried with backoff
    return {
        "charge_id": "ch_" + hashlib.sha1(key.encode()).hexdigest()[:10],
        "amount_cents": amount_cents,
        "idempotency_key": key,
    }


@app.step()
async def release_inventory(reservation_id: str) -> str:
    return f"released {reservation_id}"


@app.step()
async def create_shipment(order_id: str) -> dict[str, Any]:
    return {"tracking": f"TRK-{order_id.upper()}", "carrier": "parcelco"}


@app.step()
async def send_email(order_id: str, template: str) -> str:
    return f"sent {template} email for {order_id}"


@app.workflow()
async def checkout(
    ctx: WorkflowContext, order_id: str, amount_cents: int, items: list[str] | None = None
) -> dict[str, Any]:
    reservation = await reserve_inventory(order_id, items or ["sku-teapot"])
    try:
        payment = await charge_card(order_id, amount_cents)
    except StepFailed as failure:
        # Compensate: undo what already happened, then finish cleanly.
        await release_inventory(reservation["reservation_id"])
        return {"order_id": order_id, "status": "payment_failed", "reason": failure.error["message"]}
    shipment = await create_shipment(order_id)
    await send_email(order_id, "order_confirmed")
    await ctx.sleep(FOLLOW_UP_DELAY)
    await send_email(order_id, "how_was_it")
    return {
        "order_id": order_id,
        "status": "shipped",
        "charge": payment["charge_id"],
        "tracking": shipment["tracking"],
    }


# --------------------------------------------------------------------------- human in the loop
@app.step()
async def pay_out(employee: str, amount: int) -> str:
    return f"paid {amount} to {employee}"


@app.workflow()
async def expense(ctx: WorkflowContext, employee: str, amount: int) -> dict[str, Any]:
    """Small expenses are paid at once; larger ones wait (durably, for up to a week) for a manager."""
    if amount <= 500:
        return {"status": "paid", "receipt": await pay_out(employee, amount), "approved_by": "policy"}
    try:
        decision = await ctx.wait_for_signal("decision", timeout=7 * 24 * 3600) or {}
    except SignalTimeout:
        return {"status": "expired"}
    if not decision.get("approved"):
        return {"status": "rejected", "by": decision.get("by")}
    return {"status": "paid", "receipt": await pay_out(employee, amount), "approved_by": decision.get("by")}


# --------------------------------------------------------------------------- support agent
@tool
def lookup_order(order_id: str) -> dict[str, Any]:
    """Look up an order by id.

    order_id: the order id, like A-1001
    """
    order = ORDERS.get(order_id.upper())
    if order is None:
        raise PermanentError(f"no order {order_id}")
    return order


@tool(requires_approval=True, retry=RetryPolicy(max_attempts=1))
def refund(order_id: str, amount_cents: int, reason: str) -> dict[str, Any]:
    """Refund an order. A human must approve every refund.

    order_id: the order to refund
    amount_cents: how much to refund, in cents
    reason: a short reason shown to the reviewer
    """
    return {
        "refund_id": f"rf-{order_id}",
        "amount_cents": amount_cents,
        "idempotency_key": step_info().idempotency_key,
    }


def scripted_support(messages: list[dict[str, Any]]) -> dict[str, Any]:
    """A rule-based stand-in for the model, so the demo runs without an API key."""
    say, call = ScriptedLLM.say, ScriptedLLM.call
    prompt = messages[0]["content"]
    last = messages[-1]
    if len(messages) == 1:
        match = re.search(r"\b([A-Z]-\d{4})\b", prompt.upper())
        if not match:
            return say("Which order is this about? Please send the order id, like A-1001.")
        return call("lookup_order", {"order_id": match.group(1)}, text="Let me look that order up.")
    result = last["content"][0]
    previous = next(b for b in messages[-2]["content"] if b.get("type") == "tool_use")
    if previous["name"] == "lookup_order":
        if result.get("is_error"):
            return say(f"I couldn't find that order: {result['content']}")
        order = json.loads(result["content"])
        if "refund" in prompt.lower():
            return call(
                "refund",
                {"order_id": order["order_id"], "amount_cents": order["total_cents"], "reason": prompt[:120]},
                text="I'll request a full refund; a teammate needs to approve it.",
            )
        return say(f"Your {order['item']} (order {order['order_id']}) is {order['status']}.")
    if result.get("is_error"):
        return say(f"I wasn't able to refund this order. {result['content']}")
    refund_out = json.loads(result["content"])
    return say(
        f"Done: refund {refund_out['refund_id']} for ${refund_out['amount_cents'] / 100:.2f} is on its way."
    )


def choose_llm() -> LLM:
    """Pick the support agent's model from the environment (see the module docstring)."""
    model = os.environ.get("LLM_MODEL")
    if os.environ.get("ANTHROPIC_API_KEY") and not os.environ.get("LLM_BASE_URL"):
        return AnthropicLLM(model, effort="medium") if model else AnthropicLLM(effort="medium")
    if model:
        return OpenAICompatibleLLM(
            model,
            base_url=os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1"),
            api_key=os.environ.get("LLM_API_KEY") or os.environ.get("OPENAI_API_KEY"),
        )
    return ScriptedLLM(scripted_support)


llm = choose_llm()

support = Agent(
    app,
    "support",
    llm=llm,
    tools=[lookup_order, refund],
    system=(
        "You are the support agent for a small online shop. Look orders up before answering. "
        "Offer a refund only when the customer asks for one or the item arrived damaged."
    ),
    max_turns=8,
)
