import type { Attempt, JournalEntry, Workflow } from "../api";
import { clock, compactJson, duration } from "../util";
import { Disclosure, JsonView } from "./ui";

const KIND_ICON: Record<JournalEntry["kind"], string> = { step: "ƒ", sleep: "◷", signal: "✉", value: "≡" };

const ENTRY_STYLE: Record<JournalEntry["status"], string> = {
  completed: "border-emerald-400 bg-emerald-50 text-emerald-700 dark:border-emerald-700 dark:bg-emerald-950 dark:text-emerald-300",
  failed: "border-rose-400 bg-rose-50 text-rose-700 dark:border-rose-700 dark:bg-rose-950 dark:text-rose-300",
  pending: "border-amber-400 bg-amber-50 text-amber-700 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-300",
};

function describe(entry: JournalEntry): string {
  if (entry.kind === "sleep") return entry.status === "pending" ? `sleeping until ${clock(entry.wake_at)}` : "slept";
  if (entry.kind === "signal") {
    if (entry.status === "pending") return entry.wake_at ? `waiting, times out ${clock(entry.wake_at)}` : "waiting";
    const out = entry.output as { timeout?: boolean } | null;
    return out?.timeout ? "timed out" : "received";
  }
  if (entry.kind === "value") return "recorded";
  return entry.status;
}

export function Timeline({
  workflow,
  journal,
  attempts,
  onFork,
}: {
  workflow: Workflow;
  journal: JournalEntry[];
  attempts: Attempt[];
  onFork: (seq: number) => void;
}) {
  const bySeq = new Map<number, Attempt[]>();
  for (const a of attempts) bySeq.set(a.seq, [...(bySeq.get(a.seq) ?? []), a]);
  const running = workflow.status === "RUNNING" && workflow.lease_owner;
  const nextSeq = (journal.at(-1)?.seq ?? 0) + 1;
  const inFlight = running ? bySeq.get(nextSeq)?.filter((a) => a.finished_at === null) : undefined;

  if (journal.length === 0 && !inFlight?.length) {
    return (
      <p className="px-4 py-8 text-center text-sm text-zinc-500">
        {workflow.status === "PENDING" ? "Waiting for a worker to pick this run up." : "No steps recorded."}
      </p>
    );
  }

  return (
    <ol className="relative space-y-1 px-4 py-3" data-testid="timeline">
      {journal.map((entry, i) => {
        const tries = bySeq.get(entry.seq) ?? [];
        const workers = new Set(tries.map((t) => t.worker));
        const copied = workflow.forked_from !== null && workflow.fork_seq !== null && entry.seq < workflow.fork_seq;
        const took = entry.completed_at && entry.started_at ? entry.completed_at - entry.started_at : null;
        return (
          <li key={entry.seq} className="relative flex gap-3" data-testid="entry">
            {i < journal.length - 1 && <span className="absolute top-8 bottom-[-0.25rem] left-[0.9375rem] w-px bg-zinc-200 dark:bg-zinc-800" />}
            <span
              className={`z-10 mt-1 flex size-8 shrink-0 items-center justify-center rounded-full border text-sm font-semibold ${ENTRY_STYLE[entry.status]}`}
              title={entry.kind}
            >
              {KIND_ICON[entry.kind]}
            </span>
            <div className="group min-w-0 flex-1 rounded-lg px-2 py-1.5 hover:bg-zinc-50 dark:hover:bg-zinc-800/40">
              <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
                <span className="font-mono text-xs text-zinc-400">#{entry.seq}</span>
                <span className="font-medium break-all">{entry.name}</span>
                <span className="text-xs text-zinc-500">{describe(entry)}</span>
                {entry.attempts > 1 && (
                  <span className="rounded bg-amber-100 px-1.5 text-xs text-amber-800 dark:bg-amber-950 dark:text-amber-300">
                    {entry.attempts} attempts
                  </span>
                )}
                {workers.size > 1 && (
                  <span
                    className="rounded bg-violet-100 px-1.5 text-xs text-violet-800 dark:bg-violet-950 dark:text-violet-300"
                    title="Attempts ran on more than one worker: a worker died and another took over"
                  >
                    {workers.size} workers
                  </span>
                )}
                {copied && <span className="text-xs text-zinc-400">copied from parent run</span>}
                <span className="ml-auto flex items-center gap-3 text-xs text-zinc-500 tabular-nums">
                  {took !== null && entry.kind === "step" && duration(took)}
                  {entry.status !== "pending" && workflow.status !== "RUNNING" && (
                    <button
                      type="button"
                      onClick={() => onFork(entry.seq)}
                      className="text-violet-700 opacity-0 group-hover:opacity-100 hover:underline focus:opacity-100 dark:text-violet-300"
                      title={`Start a new run that reuses steps before #${entry.seq} and executes everything from #${entry.seq} again`}
                    >
                      re-run from here
                    </button>
                  )}
                </span>
              </div>
              {entry.status !== "pending" && (
                <div className="mt-1 space-y-2">
                  {entry.error ? (
                    <div className="text-sm text-rose-700 dark:text-rose-300">
                      <span className="font-mono text-xs">{entry.error.type}</span>: {entry.error.message}
                    </div>
                  ) : entry.kind === "sleep" ? null : (
                    <Disclosure summary={<Summary entry={entry} />}>
                      <JsonView value={entry.output} className="max-h-96" />
                    </Disclosure>
                  )}
                  {tries.length > 0 && (entry.attempts > 1 || workers.size > 1 || tries.some((t) => t.error)) && (
                    <Attempts tries={tries} />
                  )}
                </div>
              )}
            </div>
          </li>
        );
      })}
      {inFlight?.map((a) => (
        <li key={`live-${a.attempt}`} className="relative flex gap-3">
          <span className="z-10 mt-1 flex size-8 shrink-0 items-center justify-center rounded-full border border-sky-400 bg-sky-50 dark:border-sky-700 dark:bg-sky-950">
            <span className="size-2 animate-pulse rounded-full bg-sky-500" />
          </span>
          <div className="px-2 py-2 text-sm text-zinc-500">
            <span className="font-mono text-xs text-zinc-400">#{a.seq}</span> running attempt {a.attempt} on{" "}
            <span className="font-mono text-xs">{a.worker}</span>
          </div>
        </li>
      ))}
    </ol>
  );
}

interface Block {
  type: string;
  text?: string;
  name?: string;
  input?: unknown;
}

/** One-line preview of an entry's output; LLM turns show their text and tool calls. */
function Summary({ entry }: { entry: JournalEntry }) {
  const out = entry.output as { content?: Block[]; usage?: { output_tokens?: number } } | null;
  if (entry.kind === "step" && entry.name.endsWith(":llm") && Array.isArray(out?.content)) {
    const text = out.content.filter((b) => b.type === "text").map((b) => b.text).join(" ");
    const calls = out.content.filter((b) => b.type === "tool_use");
    return (
      <span className="text-left">
        {text && <span className="text-zinc-700 dark:text-zinc-300">“{text.length > 140 ? `${text.slice(0, 139)}…` : text}”</span>}
        {calls.map((c, i) => (
          <span key={i} className="ml-2 rounded bg-violet-100 px-1.5 font-mono text-violet-800 dark:bg-violet-950 dark:text-violet-300">
            → {c.name}({compactJson(c.input, 50)})
          </span>
        ))}
      </span>
    );
  }
  if (entry.kind === "signal") {
    const payload = (entry.output as { payload?: unknown; timeout?: boolean } | null)?.payload;
    return <span className="font-mono">payload {compactJson(payload, 80)}</span>;
  }
  return <span className="font-mono">{compactJson(entry.output, 90)}</span>;
}

function Attempts({ tries }: { tries: Attempt[] }) {
  return (
    <ul className="space-y-0.5 rounded-lg border border-zinc-200 p-2 text-xs dark:border-zinc-800" aria-label="Attempts">
      {tries.map((t) => (
        <li key={t.attempt} className="flex flex-wrap gap-x-2">
          <span className="font-medium">attempt {t.attempt}</span>
          <span className="font-mono text-zinc-500">{t.worker}</span>
          <span className="text-zinc-500">{clock(t.started_at)}</span>
          {t.finished_at === null ? (
            <span className="text-amber-700 dark:text-amber-400">never finished (worker lost)</span>
          ) : t.error ? (
            <span className="break-all text-rose-700 dark:text-rose-300">{t.error}</span>
          ) : (
            <span className="text-emerald-700 dark:text-emerald-400">ok in {duration(t.finished_at - t.started_at)}</span>
          )}
        </li>
      ))}
    </ul>
  );
}
