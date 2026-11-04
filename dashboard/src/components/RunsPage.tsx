import { useState } from "react";
import { api, STATUSES, type Stats, type Status } from "../api";
import { ago, compactJson, duration, runHref, usePoll } from "../util";
import { StartForm } from "./StartForm";
import { Banner, Empty, STATUS_DOT, StatusBadge } from "./ui";

const PAGE = 50;

export function RunsPage({ stats }: { stats: Stats | null }) {
  const [status, setStatus] = useState<Status | undefined>();
  const [name, setName] = useState("");
  const [page, setPage] = useState(0);
  const [starting, setStarting] = useState(false);
  const { data, error } = usePoll(
    () => api.list({ status, name: name || undefined, limit: PAGE + 1, offset: page * PAGE }),
    [status, name, page],
  );
  const rows = data?.workflows.slice(0, PAGE) ?? [];
  const hasMore = (data?.workflows.length ?? 0) > PAGE;
  const total = stats ? Object.values(stats.counts).reduce((a, b) => a + b, 0) : 0;

  const choose = (s: Status | undefined) => {
    setStatus(s);
    setPage(0);
  };

  return (
    <div className="space-y-6">
      <section aria-label="Runs by status" className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        {STATUSES.map((s) => (
          <button
            key={s}
            type="button"
            onClick={() => choose(status === s ? undefined : s)}
            aria-pressed={status === s}
            className={`card px-4 py-3 text-left transition hover:border-violet-400 ${
              status === s ? "border-violet-500 ring-2 ring-violet-500/20 dark:border-violet-500" : ""
            }`}
          >
            <div className="flex items-center gap-2 text-xs font-medium text-zinc-500 dark:text-zinc-400">
              <span className={`size-2 rounded-full ${STATUS_DOT[s]}`} />
              {s.toLowerCase()}
            </div>
            <div className="mt-1 text-2xl font-semibold tabular-nums" data-testid={`count-${s}`}>
              {stats?.counts[s] ?? "–"}
            </div>
          </button>
        ))}
      </section>

      <section className="card">
        <div className="flex flex-wrap items-center gap-3 border-b border-zinc-200 px-4 py-3 dark:border-zinc-800">
          <h2 className="mr-auto font-semibold">
            Runs{" "}
            <span className="ml-1 text-sm font-normal text-zinc-500">
              {status ? `${status.toLowerCase()} · ` : ""}
              {total} total
            </span>
          </h2>
          <select
            aria-label="Workflow"
            className="input w-auto"
            value={name}
            onChange={(e) => {
              setName(e.target.value);
              setPage(0);
            }}
          >
            <option value="">All workflows</option>
            {stats?.workflows.map((w) => (
              <option key={w} value={w}>
                {w}
              </option>
            ))}
          </select>
          {status && (
            <button type="button" className="btn-ghost" onClick={() => choose(undefined)}>
              Clear filter
            </button>
          )}
          <button type="button" className="btn-primary" onClick={() => setStarting(true)}>
            Start a run
          </button>
        </div>

        {error && (
          <div className="p-4">
            <Banner kind="error">Could not load runs: {error.message}</Banner>
          </div>
        )}

        {data && rows.length === 0 ? (
          <Empty title={status || name ? "No runs match this filter" : "No runs yet"}>
            Start one here, from code with <code className="font-mono">app.start(...)</code>, or with{" "}
            <code className="font-mono">ninelives start</code>.
          </Empty>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="label text-left">
                  <th className="px-4 py-2 font-medium">Run</th>
                  <th className="px-4 py-2 font-medium">Status</th>
                  <th className="hidden px-4 py-2 font-medium md:table-cell">Input</th>
                  <th className="px-4 py-2 text-right font-medium" title="Times a worker picked the run up">Executions</th>
                  <th className="px-4 py-2 text-right font-medium">Took</th>
                  <th className="px-4 py-2 text-right font-medium">Updated</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-100 dark:divide-zinc-800">
                {rows.map((w) => (
                  <tr key={w.id} className="hover:bg-zinc-50 dark:hover:bg-zinc-800/40" data-testid="run-row">
                    <td className="max-w-xs px-4 py-2.5">
                      <a href={runHref(w.id)} className="block truncate font-mono text-[13px] text-violet-700 hover:underline dark:text-violet-300">
                        {w.id}
                      </a>
                      <div className="truncate text-xs text-zinc-500">
                        {w.name}
                        {w.forked_from && <span className="ml-1 text-violet-600 dark:text-violet-400">· fork</span>}
                      </div>
                    </td>
                    <td className="px-4 py-2.5">
                      <StatusBadge status={w.status} />
                    </td>
                    <td className="hidden max-w-sm truncate px-4 py-2.5 font-mono text-xs text-zinc-500 md:table-cell">
                      {compactJson(w.input.args.length || !Object.keys(w.input.kwargs).length ? w.input.args : w.input.kwargs, 60)}
                    </td>
                    <td className="px-4 py-2.5 text-right tabular-nums">
                      {w.runs}
                      {w.recoveries > 0 && (
                        <span title="Recovered after a worker crash" className="ml-1.5 text-xs text-amber-600 dark:text-amber-400">
                          ↻{w.recoveries}
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-2.5 text-right text-zinc-500 tabular-nums">
                      {["SUCCEEDED", "FAILED", "CANCELLED"].includes(w.status) ? duration(w.updated_at - w.created_at) : "…"}
                    </td>
                    <td className="px-4 py-2.5 text-right text-zinc-500 whitespace-nowrap">{ago(w.updated_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {(page > 0 || hasMore) && (
          <div className="flex items-center justify-end gap-2 border-t border-zinc-200 px-4 py-2 dark:border-zinc-800">
            <button type="button" className="btn-ghost" disabled={page === 0} onClick={() => setPage(page - 1)}>
              Previous
            </button>
            <span className="text-xs text-zinc-500">Page {page + 1}</span>
            <button type="button" className="btn-ghost" disabled={!hasMore} onClick={() => setPage(page + 1)}>
              Next
            </button>
          </div>
        )}
      </section>

      {starting && stats && <StartForm workflows={stats.workflows} onClose={() => setStarting(false)} />}
    </div>
  );
}
