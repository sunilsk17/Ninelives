# Research: what makes a durable-execution library usable by other people

Written while planning v0.2 (November 2026). The question: v0.1 has a correct, well-tested
engine. What do people who adopt Temporal, DBOS, Inngest, Hatchet or Absurd actually rely on day to
day, what hurts them, and which of those things does Ninelives lack?

**Method.** I used web searches over vendor documentation, release notes and practitioner write-ups.
Direct page fetches to several of these sites (for example docs.dbos.dev and lucumr.pocoo.org) were
blocked by the network policy of the environment this was written in. So the notes below rely on
search-result summaries of those pages, and vendor claims are marked as such. Facts are kept
separate from my conclusions, which are in the last two sections.

## 1. The landscape

| System | Shape | Notes |
| --- | --- | --- |
| Temporal | Separate server cluster plus SDKs | The reference design: event history and replay, signals/queries/updates, child workflows, schedules, worker versioning, OpenTelemetry interceptors. [Versioning](https://docs.temporal.io/develop/python/workflows/versioning), [message passing](https://docs.temporal.io/encyclopedia/application-message-passing), [schedules](https://docs.temporal.io/schedule) |
| DBOS Transact | Library plus Postgres | Closest to Ninelives. Queues with concurrency, rate limits, deduplication and priority; cron-scheduled workflows; `send`/`recv`, `set_event`/`get_event`, streams; workflows tagged with an application version. [Repo](https://github.com/DBOS-project/dbos-transact-py), [May 2026 release notes](https://dbos.dev/blog/new-in-dbos-may-2026), [workflow communication](https://docs.dbos.dev/python/tutorials/workflow-communication), [recovery](https://docs.dbos.dev/production/workflow-recovery) |
| Absurd (Armin Ronacher, Nov 2025) | One SQL file plus thin SDKs | Tasks, steps, checkpoints, events, sleeps, all in Postgres. A five-month production report says the core design held up. [Overview](https://thenote.app/post/en/armin-ronacher-absurd-workflows-durable-execution-with-just-postgres-cn7qkushpk), [production report](https://app.daily.dev/posts/absurd-in-production-2sbxsxfzn) |
| Inngest | Hosted or self-hosted server, event-driven | Strongest flow control: concurrency with keys and scopes, throttle, debounce, batching, `cancelOn`; waiting runs don't count against concurrency. [Function reference](https://www.inngest.com/docs/reference/typescript/v4/functions/create) |
| Hatchet | Server plus SDKs | Durable tasks, concurrency limits per dynamic key, global and dynamic rate limits, durable slots, sticky workers. [Python SDK](https://docs.hatchet.run/sdks/python-sdk) |
| Restate | Server plus SDKs | Durable timers, idempotency keys on calls, awakeables (an ID plus a promise that external systems resolve), virtual objects. [Python agents guide](https://docs.restate.dev/develop/python/agents.md) |
| LangGraph | Agent framework with checkpointers | Durability modes (`async`, `sync`, `exit`); resumes at node boundaries, not mid-function; human-in-the-loop via `interrupt`. [Durable execution docs](https://docs.langchain.com/oss/python/langgraph/durable-execution) |
| Procrastinate | Postgres task queue (not durable execution) | LISTEN/NOTIFY with polling as a fallback, periodic tasks, locks. [Docs](https://procrastinate.readthedocs.io) |
| Dapr Workflow | Sidecar runtime | Child workflows, concurrency limits, retention policy and purge API. [Features](https://docs.dapr.io/developing-applications/building-blocks/workflow/workflow-features-concepts/) |

**Agents are becoming the main use case.** Pydantic AI ships durable-execution integrations for
Temporal, DBOS and Prefect, and lists Restate, Absurd, pgtask and others as backends: each model
request and tool call becomes a step. [Pydantic AI durable execution](https://pydantic.dev/docs/ai/capabilities/durable_execution/overview/).
Temporal and OpenAI shipped an Agents SDK integration in 2025. Streaming is still the least settled
part of it, and none of the sources discussed cost tracking.
[Temporal blog](https://temporal.io/blog/announcing-openai-agents-sdk-integration),
[InfoQ](https://www.infoq.com/news/2025/09/temporal-aiagent).

## 2. What users depend on (features that recur across systems)

| Capability | Temporal | DBOS | Inngest | Hatchet | Absurd | Ninelives v0.1 |
| --- | --- | --- | --- | --- | --- | --- |
| Steps, retries, durable sleep, signals | yes | yes | yes | yes | yes | yes |
| Child workflows / spawn and await other work | yes | yes (start a workflow from a workflow) | invoke | yes | spawn | **no** |
| Publish progress or results while running (events, queries, streams) | queries, updates | `set_event`, streams | — | — | events | **no** |
| Workflow-to-workflow messages | signals | `send`/`recv` | events | — | events | signals only from clients |
| Cron schedules | Schedules (overlap policy, catch-up window) | `@scheduled` with crontab | cron triggers | cron | — | **no** |
| Concurrency limits (global and per key) | via task queues | per queue | per key and scope | per dynamic key | — | **no** |
| Rate limits, priority, deduplication | — | yes | throttle, debounce | yes | — | dedup by id only |
| Versioning running code | `patched()`, worker versioning | app-version tagging | — | — | — | detection only |
| Workflow timeouts | yes | yes | yes | yes | — | **no** |
| Retention / purge | retention period | yes | yes | yes | — | **no** |
| Observability | OTel interceptors, web UI | dashboard (Conductor) | dashboard | dashboard | — | dashboard only |
| Low-latency pickup | push | polling + notifications | push | push | polling | polling |

Sources: the documentation linked in section 1.

## 3. Pain points that come up repeatedly

1. **Determinism and versioning are the main production failure.** Practitioner write-ups call broken determinism the most common production issue for new Temporal users. Deploying changed workflow code makes in-flight runs fail on their next replay, and teams such as Stripe wrapped the SDK to enforce versioning discipline. [Arpit Bhayani](https://arpitbhayani.me/blogs/temporal-primer), [DEV Community](https://dev.to/pickuma/temporal-deep-dive-durable-execution-that-survives-process-death-and-network-outages-h9e). DBOS sidesteps this by recovering a run only with the code version that started it; its FAQ lists version mismatch as the usual reason a workflow does not recover. [DBOS FAQ](https://docs.dbos.dev/faq).
2. **Debugging replay is hard without good tooling.** Non-determinism errors are described as cryptic without a history viewer. [DEV Community](https://dev.to/pickuma/temporal-deep-dive-durable-execution-that-survives-process-death-and-network-outages-h9e). Ninelives' dashboard timeline already helps here; error messages that name the step and the mismatch matter as much.
3. **Postgres-as-a-queue operational issues.** Practitioners report three: status churn bloats tables and indexes, long transactions elsewhere block vacuum, and claim transactions must stay short and atomic. DBOS cut index maintenance by making the hot index partial (only enqueued rows). [DBOS: making Postgres queues scale](https://dbos.dev/blog/making-postgres-queues-scale), [production notes](https://artur.bearblog.dev/background-jobs-without-redis-postgres-skip-locked-in-production/).
4. **Flow control is needed early.** As soon as workflows call rate-limited APIs (LLMs above all), teams need concurrency caps per tenant or user and rate limits. Every server product above has them.
5. **Agents need resume, approvals and visibility.** Pydantic AI warns that approval listeners re-run during replay, so their effects must be idempotent. LangGraph resumes only at node boundaries. Streaming partial output while staying durable is unresolved in most integrations. [Pydantic AI](https://pydantic.dev/docs/ai/capabilities/durable_execution/backends/), [LangGraph](https://docs.langchain.com/oss/python/langgraph/durable-execution).
6. **Observability is expected to plug into existing tools.** Temporal's OpenTelemetry interceptor is the reference. Trace context must cross workers, because steps retry on different machines. [Temporal + OTel guide](https://oneuptime.com/blog/post/2026-02-06-instrument-temporal-io-workflows-opentelemetry/view).

## 4. What "others can just use it" means for a library

These come from general open-source practice rather than from the sources above:

- Install with one command, from PyPI with extras, and ship type hints (`py.typed`).
- A docs folder that answers "how do I…" for every feature, plus an API reference, a changelog, a contribution guide and a security policy.
- Schema migrations, so upgrading the library never requires hand-editing the database.
- Safe defaults: the server binds to localhost, and token authentication is available when exposed.
- Testing helpers, so users can unit-test their workflows with a fake clock and no worker processes.
- A sync API for Django/Flask apps that are not async.

## 5. Conclusions for Ninelives v0.2

The engine's core (journal, leases, fencing, replay, chaos-tested recovery) is on par with the
category. What's missing is what users reach for in week two:

1. **Composition:** child workflows, workflow-to-workflow messages and published events. Without them, real systems can't be structured.
2. **Flow control:** concurrency limits (global and per key), priority and workflow timeouts.
3. **Time-based work:** cron schedules with a clear catch-up and overlap policy.
4. **Change management:** `ctx.patched()` for code changes, plus schema migrations for library upgrades.
5. **Operations:** retention/purge, API tokens, Postgres LISTEN/NOTIFY for low latency, metrics and OpenTelemetry, and leaner hot-path transactions.
6. **Developer experience:** testing helpers, a sync client, a full docs set, packaging metadata and a release workflow.

Not planned for v0.2, with reasons:

- **Separate server process or gRPC protocol:** the library-plus-database shape is the point.
- **Streams of LLM tokens through the journal:** that's high write amplification. Events cover progress, and token streaming belongs in the transport layer.
- **Worker versioning and deployment pinning:** needs orchestration beyond a library. `patched()` covers the common case.
- **Multi-tenant hosted control plane:** out of scope.
