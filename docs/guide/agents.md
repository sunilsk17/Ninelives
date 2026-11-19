# AI agents

An agent is a loop: send the conversation to a language model, run the tools it asks for, send back
the results, repeat. Ninelives makes every model call and every tool call a durable step, which
gives you three things:

- **Crash safety.** If a worker dies after the third tool call, the agent resumes at the fourth.
  Earlier model responses come from the journal, so you don't pay for them twice.
- **Humans in the loop.** Risky tools wait for an approval, which costs nothing while waiting.
- **Visibility.** Every turn, tool call, retry and approval is on the run's timeline.

## Define tools

Tools are plain Python functions. The schema the model sees is generated from type hints, and the
descriptions come from the docstring:

```python
from ninelives import RetryPolicy, step_info
from ninelives.agents import tool

@tool
def lookup_order(order_id: str) -> dict:
    """Look up an order by id.

    order_id: the order id, like A-1001
    """
    return orders.get(order_id)

@tool(requires_approval=True, retry=RetryPolicy(max_attempts=1))
def refund(order_id: str, amount_cents: int, reason: str) -> dict:
    """Refund an order. A person approves every refund.

    order_id: the order to refund
    amount_cents: how much, in cents
    reason: shown to the reviewer
    """
    return payments.refund(order_id, amount_cents, idempotency_key=step_info().idempotency_key)
```

Supported parameter types: `str`, `int`, `float`, `bool`, `list[...]`, `dict[...]`, `Literal[...]`,
enums, and optional versions of these.

## Choose a model provider

| Provider | Use it for |
| --- | --- |
| `OpenAICompatibleLLM(model, base_url=..., api_key_env=...)` | any chat-completions API in the OpenAI format: OpenAI, Groq, Together, OpenRouter, DeepSeek, Mistral, Fireworks, and local servers like Ollama, vLLM or LM Studio |
| `AnthropicLLM(model=..., effort=...)` | Anthropic's API through its official SDK (reads `ANTHROPIC_API_KEY`) |
| `ScriptedLLM(replies or function)` | tests and offline demos: no network, deterministic |
| your own class | anything else: implement one method, `complete(system, messages, tools)` |

```python
from ninelives.agents import OpenAICompatibleLLM

local = OpenAICompatibleLLM("llama3.1", base_url="http://localhost:11434/v1")      # Ollama, no key
hosted = OpenAICompatibleLLM("my-model", base_url="https://api.example.com/v1",
                             api_key_env="EXAMPLE_API_KEY")
```

**Errors.** Rate limits, server errors and network failures are retried with backoff (the model
step's retry policy). Rejected requests, such as a bad key, an unknown model or an invalid request,
fail at once.

**Writing your own provider.** Conversations are kept in one neutral shape: messages whose content
is text or a list of `text`, `tool_use` and `tool_result` blocks. `complete` receives that shape
and returns `{"content": [...blocks...], "stop_reason": "end_turn" | "tool_use" | "refusal" | ..., "usage": {...}}`.
`ninelives/agents/openai_compat.py` is a complete example of the conversion.

## Create and run the agent

```python
from ninelives.agents import Agent

support = Agent(
    app, "support",
    llm=local,
    tools=[lookup_order, refund],
    system="You are the support agent for a small shop.",
    max_turns=10,                 # stop runaway loops
    max_total_tokens=200_000,     # and runaway spend
    approval_timeout=24 * 3600,   # an unanswered approval counts as "no"
    publish_progress=True,        # an "agent" event after every turn
    parallel_tools=True,          # run one turn's tool calls concurrently
)

run_id = await support.start("My teapot arrived broken, order A-1001")
result = await support.result(run_id, timeout=600)
# {"status": "completed", "answer": "...", "turns": 3, "usage": {...}, "actions": [...]}
```

`status` is `completed`, `refused` (the model declined), `max_turns` or `budget_exhausted`.
`actions` lists every tool call with its input, outcome, output and any approval decision.

## Approvals

A tool with `requires_approval=True` pauses the run until someone answers:

```python
await support.approve(run_id, call_id, approved=True, note="photo checked")
```

You can also answer from the dashboard, which shows the tool and its arguments with Approve and Deny
buttons, or by signalling `approval:<call_id>` through the API. A denial, an unknown tool, invalid
arguments and a tool that fails after its retries are all reported back to the model as error
results, so the model can recover or explain.

## Try it

`examples/shop.py` has a support agent with a lookup tool and an approval-gated refund tool. It runs
offline with a scripted model, or with a real one:

```bash
ninelives serve examples/shop.py:app --worker
LLM_MODEL=llama3.1 LLM_BASE_URL=http://localhost:11434/v1 ninelives serve examples/shop.py:app --worker
```
