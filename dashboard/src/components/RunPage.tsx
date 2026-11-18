import { useState, type FormEvent, type ReactNode } from "react";
import { api, TERMINAL, type Approval } from "../api";
import { ago, clock, duration, runHref, usePoll } from "../util";
import { Timeline } from "./Timeline";
import { Banner, JsonView, StatusBadge } from "./ui";

export function RunPage({ id }: { id: string }) {
  const { data, error, refresh } = usePoll(() => api.describe(id), [id]);
  const [notice, setNotice] = useState<{ kind: "error" | "info"; text: string } | null>(null);

  const act = async (fn: () => Promise<unknown>, ok?: string) => {
    setNotice(null);
    try {
      await fn();
      if (ok) setNotice({ kind: "info", text: ok });
      await refresh();
    } catch (e) {
      setNotice({ kind: "error", text: (e as Error).message });
    }
  };

  const fork = (seq: number) =>
    act(async () => {
      const res = await api.fork(id, seq);
      window.location.hash = runHref(res.id);
    });

  if (error && !data) {
    return (
      <div className="space-y-4">
        <BackLink />
        <Banner kind="error">{error.message}</Banner>
      </div>
    );
  }
  if (!data) return <p className="text-sm text-zinc-500">Loading…</p>;

  const { workflow: wf } = data;
  const finished = TERMINAL.includes(wf.status);
  const waitingFor = data.journal.filter((e) => e.kind === "signal" && e.status === "pending").map((e) => e.name);

  return (
    <div className="space-y-6">
      <BackLink />
      <header className="flex flex-wrap items-start gap-4">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-3">
            <h1 className="font-mono text-lg font-semibold break-all" data-testid="run-id">
              {wf.id}
            </h1>
            <StatusBadge status={wf.status} />
            {wf.cancel_requested && !finished && <span className="text-xs text-rose-600">cancel requested</span>}
          </div>
          <p className="mt-1 text-sm text-zinc-500">
            {wf.name}
            {wf.forked_from && (
              <>
                {" · forked from "}
                <a className="font-mono text-violet-700 hover:underline dark:text-violet-300" href={runHref(wf.forked_from)}>
                  {wf.forked_from}
                </a>{" "}
                at #{wf.fork_seq}
              </>
            )}
            {wf.parent_id && (
              <>
                {" · child of "}
                <a className="font-mono text-violet-700 hover:underline dark:text-violet-300" href={runHref(wf.parent_id)}>
                  {wf.parent_id}
                </a>
              </>
            )}
            {wf.schedule_id && (
              <>
                {" · started by schedule "}
                <a className="font-mono text-violet-700 hover:underline dark:text-violet-300" href="#/schedules">
                  {wf.schedule_id}
                </a>
              </>
            )}
          </p>
          {(wf.priority !== 0 || wf.concurrency_key || wf.deadline) && (
            <p className="mt-2 flex flex-wrap gap-2 text-xs" data-testid="run-options">
              {wf.priority !== 0 && <Chip>priority {wf.priority}</Chip>}
              {wf.concurrency_key && <Chip>key {wf.concurrency_key}</Chip>}
              {wf.deadline && (
                <Chip>
                  {finished ? "deadline" : "times out"} {finished ? clock(wf.deadline) : ago(wf.deadline)}
                </Chip>
              )}
            </p>
          )}
        </div>
        {!finished && (
          <button type="button" className="btn-danger" onClick={() => act(() => api.cancel(id), "Cancellation requested.")}>
            Cancel run
          </button>
        )}
      </header>

      {notice && <Banner kind={notice.kind}>{notice.text}</Banner>}

      <dl className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        <Fact label="Started">{clock(wf.created_at)}</Fact>
        <Fact label={finished ? "Took" : "Running for"}>
          {duration((finished ? wf.updated_at : Date.now() / 1000) - wf.created_at)}
        </Fact>
        <Fact label="Executions" hint="Times a worker picked this run up: the first start, each wake-up after a sleep or signal, and each crash recovery">
          {wf.runs}
        </Fact>
        <Fact label="Crash recoveries" hint="Times a worker resumed this run after the previous worker died mid-execution">
          <span className={wf.recoveries ? "text-amber-600 dark:text-amber-400" : ""}>{wf.recoveries}</span>
        </Fact>
        <Fact label="Steps journaled">{data.journal.length}</Fact>
        <Fact label={wf.status === "RUNNING" ? "Worker" : wf.status === "WAITING" ? "Wakes" : "Updated"}>
          {wf.status === "RUNNING" ? (
            <span className="font-mono text-xs">{wf.lease_owner ?? "—"}</span>
          ) : wf.status === "WAITING" ? (
            wf.wake_at ? ago(wf.wake_at) : "on a signal"
          ) : (
            ago(wf.updated_at)
          )}
        </Fact>
      </dl>

      {data.approvals.length > 0 && (
        <section className="space-y-3" aria-label="Pending approvals">
          {data.approvals.map((a) => (
            <ApprovalCard key={a.signal} approval={a} onAnswer={(approved, note) =>
              act(() => api.signal(id, a.signal, { approved, note }), approved ? "Approved." : "Denied.")
            } />
          ))}
        </section>
      )}

      <div className="grid gap-6 lg:grid-cols-3">
        <section className="card lg:col-span-2" aria-label="Timeline">
          <h2 className="border-b border-zinc-200 px-4 py-3 font-semibold dark:border-zinc-800">Timeline</h2>
          <Timeline workflow={wf} journal={data.journal} attempts={data.attempts} onFork={fork} />
        </section>

        <div className="space-y-6">
          {data.events.length > 0 && (
            <section className="card space-y-2 p-4" aria-label="Progress">
              <h2 className="font-semibold">Progress</h2>
              <dl className="space-y-2">
                {data.events.map((e) => (
                  <div key={e.key}>
                    <dt className="flex items-baseline justify-between gap-2">
                      <span className="font-mono text-xs font-medium">{e.key}</span>
                      <span className="text-xs text-zinc-500">{ago(e.updated_at)}</span>
                    </dt>
                    <dd>
                      <JsonView value={e.value} className="mt-1 max-h-40" />
                    </dd>
                  </div>
                ))}
              </dl>
            </section>
          )}
          {data.children.length > 0 && (
            <section className="card p-4" aria-label="Child runs">
              <h2 className="mb-2 font-semibold">Child runs</h2>
              <ul className="space-y-1.5 text-sm">
                {data.children.map((c) => (
                  <li key={c.id} className="flex flex-wrap items-center gap-2">
                    <StatusBadge status={c.status} />
                    <a className="font-mono text-xs text-violet-700 hover:underline dark:text-violet-300" href={runHref(c.id)}>
                      {c.id}
                    </a>
                    <span className="text-xs text-zinc-500">
                      {c.name}
                      {c.parent_seq ? ` · from #${c.parent_seq}` : ""}
                    </span>
                  </li>
                ))}
              </ul>
            </section>
          )}
          {wf.error && (
            <section className="card space-y-2 p-4" aria-label="Error">
              <h2 className="font-semibold text-rose-700 dark:text-rose-300">{wf.error.type}</h2>
              <p className="text-sm">{wf.error.message}</p>
              {wf.error.traceback && <JsonView value={wf.error.traceback} className="max-h-64 whitespace-pre-wrap" />}
            </section>
          )}
          {wf.status === "SUCCEEDED" && (
            <section className="card space-y-2 p-4" aria-label="Output">
              <h2 className="font-semibold">Output</h2>
              <JsonView value={wf.output} className="max-h-96" />
            </section>
          )}
          <section className="card space-y-2 p-4" aria-label="Input">
            <h2 className="font-semibold">Input</h2>
            <JsonView value={wf.input} className="max-h-64" />
          </section>
          {!finished && <SignalForm suggestions={waitingFor} onSend={(name, payload) => act(() => api.signal(id, name, payload), `Sent "${name}".`)} />}
          {data.signals.length > 0 && (
            <section className="card p-4" aria-label="Signals">
              <h2 className="mb-2 font-semibold">Signals</h2>
              <ul className="space-y-1 text-sm">
                {data.signals.map((s) => (
                  <li key={s.id} className="flex flex-wrap gap-x-2">
                    <span className="font-mono text-xs">{s.name}</span>
                    <span className="text-xs text-zinc-500">{ago(s.created_at)}</span>
                    <span className="text-xs text-zinc-500">{s.consumed_seq ? `→ #${s.consumed_seq}` : "unread"}</span>
                  </li>
                ))}
              </ul>
            </section>
          )}
        </div>
      </div>
    </div>
  );
}

function Chip({ children }: { children: ReactNode }) {
  return <span className="rounded-full bg-zinc-100 px-2 py-0.5 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300">{children}</span>;
}

function BackLink() {
  return (
    <a href="#/" className="text-sm text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100">
      ← All runs
    </a>
  );
}

function Fact({ label, hint, children }: { label: string; hint?: string; children: ReactNode }) {
  return (
    <div className="card px-3 py-2" title={hint}>
      <dt className="label">{label}</dt>
      <dd className="mt-0.5 text-sm font-medium tabular-nums">{children}</dd>
    </div>
  );
}

function ApprovalCard({ approval, onAnswer }: { approval: Approval; onAnswer: (approved: boolean, note: string) => void }) {
  const [note, setNote] = useState("");
  return (
    <div className="card border-amber-300 p-4 dark:border-amber-800" data-testid="approval">
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="rounded bg-amber-100 px-1.5 py-0.5 text-xs font-semibold text-amber-800 dark:bg-amber-950 dark:text-amber-300">
          needs approval
        </span>
        <h3 className="font-semibold">
          The agent wants to call <span className="font-mono">{approval.tool ?? approval.call_id}</span>
        </h3>
        {approval.deadline && <span className="text-xs text-zinc-500">auto-denies {ago(approval.deadline)}</span>}
      </div>
      {approval.input !== undefined && <JsonView value={approval.input} className="mt-3" />}
      <div className="mt-3 flex flex-wrap gap-2">
        <input className="input flex-1" placeholder="Note for the agent (optional)" value={note} onChange={(e) => setNote(e.target.value)} />
        <button type="button" className="btn-primary" onClick={() => onAnswer(true, note)}>
          Approve
        </button>
        <button type="button" className="btn-danger" onClick={() => onAnswer(false, note)}>
          Deny
        </button>
      </div>
    </div>
  );
}

function SignalForm({ suggestions, onSend }: { suggestions: string[]; onSend: (name: string, payload: unknown) => void }) {
  const [name, setName] = useState("");
  const [payload, setPayload] = useState("");
  const [error, setError] = useState<string | null>(null);
  const target = name || suggestions.find((s) => !s.startsWith("approval:")) || "";

  const submit = (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    let value: unknown = null;
    if (payload.trim()) {
      try {
        value = JSON.parse(payload);
      } catch {
        setError("Payload is not valid JSON");
        return;
      }
    }
    onSend(target, value);
    setPayload("");
  };

  return (
    <form className="card space-y-2 p-4" onSubmit={submit} aria-label="Send a signal">
      <h2 className="font-semibold">Send a signal</h2>
      <input
        className="input font-mono"
        placeholder={target || "signal name"}
        value={name}
        onChange={(e) => setName(e.target.value)}
        aria-label="Signal name"
      />
      <textarea
        className="input font-mono"
        rows={2}
        placeholder='payload as JSON, e.g. {"ok": true}'
        value={payload}
        onChange={(e) => setPayload(e.target.value)}
        aria-label="Signal payload"
      />
      {error && <Banner kind="error">{error}</Banner>}
      <button type="submit" className="btn-ghost w-full" disabled={!target}>
        Send{target ? ` "${target}"` : ""}
      </button>
    </form>
  );
}


