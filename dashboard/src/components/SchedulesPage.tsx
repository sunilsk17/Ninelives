import { useState } from "react";
import { api, type Schedule } from "../api";
import { ago, clock, compactJson, runHref, usePoll } from "../util";
import { Banner, Empty } from "./ui";

export function SchedulesPage() {
  const { data, error, refresh } = usePoll(() => api.schedules(), [], 2000);
  const [notice, setNotice] = useState<{ kind: "error" | "info"; text: string } | null>(null);

  const act = async (fn: () => Promise<unknown>, ok: string) => {
    setNotice(null);
    try {
      await fn();
      setNotice({ kind: "info", text: ok });
      await refresh();
    } catch (e) {
      setNotice({ kind: "error", text: (e as Error).message });
    }
  };

  const schedules = data?.schedules ?? [];

  return (
    <section className="card" aria-label="Schedules">
      <div className="flex flex-wrap items-center gap-3 border-b border-zinc-200 px-4 py-3 dark:border-zinc-800">
        <h2 className="mr-auto font-semibold">
          Schedules <span className="ml-1 text-sm font-normal text-zinc-500">{schedules.length} total</span>
        </h2>
      </div>
      {(error || notice) && (
        <div className="space-y-2 p-4">
          {error && <Banner kind="error">Could not load schedules: {error.message}</Banner>}
          {notice && <Banner kind={notice.kind}>{notice.text}</Banner>}
        </div>
      )}
      {data && schedules.length === 0 ? (
        <Empty title="No schedules yet">
          Declare one in code with <code className="font-mono">app.schedule("nightly", report, "0 2 * * *")</code>; a worker
          registers it when it starts.
        </Empty>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="label text-left">
                <th className="px-4 py-2 font-medium">Schedule</th>
                <th className="px-4 py-2 font-medium">When</th>
                <th className="hidden px-4 py-2 font-medium md:table-cell">Input</th>
                <th className="px-4 py-2 font-medium">Next run</th>
                <th className="px-4 py-2 font-medium">Last run</th>
                <th className="px-4 py-2 text-right font-medium">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-100 dark:divide-zinc-800">
              {schedules.map((s) => (
                <Row key={s.id} schedule={s} onAct={act} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function Row({
  schedule: s,
  onAct,
}: {
  schedule: Schedule;
  onAct: (fn: () => Promise<unknown>, ok: string) => Promise<void>;
}) {
  return (
    <tr data-testid="schedule-row">
      <td className="px-4 py-2.5">
        <div className="font-mono text-[13px]">{s.id}</div>
        <div className="text-xs text-zinc-500">{s.workflow}</div>
      </td>
      <td className="px-4 py-2.5">
        <div className="font-mono text-xs">{s.cron}</div>
        <div className="text-xs text-zinc-500">
          {s.tz} · overlap {s.overlap}
        </div>
      </td>
      <td className="hidden max-w-xs truncate px-4 py-2.5 font-mono text-xs text-zinc-500 md:table-cell">
        {compactJson(s.input.args.length || !Object.keys(s.input.kwargs).length ? s.input.args : s.input.kwargs, 50)}
      </td>
      <td className="px-4 py-2.5 whitespace-nowrap">
        {s.enabled ? (
          <span title={clock(s.next_at)}>{ago(s.next_at)}</span>
        ) : (
          <span className="rounded bg-zinc-100 px-1.5 py-0.5 text-xs text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300">paused</span>
        )}
      </td>
      <td className="px-4 py-2.5">
        {s.last_run_id ? (
          <a className="font-mono text-xs text-violet-700 hover:underline dark:text-violet-300" href={runHref(s.last_run_id)}>
            {s.last_run_id}
          </a>
        ) : (
          <span className="text-xs text-zinc-400">never</span>
        )}
      </td>
      <td className="px-4 py-2.5 text-right whitespace-nowrap">
        <span className="inline-flex gap-2">
          <button
            type="button"
            className="btn-ghost px-2 py-1 text-xs"
            onClick={() =>
              onAct(
                () => (s.enabled ? api.pauseSchedule(s.id) : api.resumeSchedule(s.id)),
                s.enabled ? `Paused ${s.id}.` : `Resumed ${s.id}.`,
              )
            }
          >
            {s.enabled ? "Pause" : "Resume"}
          </button>
          <button
            type="button"
            className="btn-primary px-2 py-1 text-xs"
            onClick={() =>
              onAct(async () => {
                const run = await api.triggerSchedule(s.id);
                window.location.hash = runHref(run.id);
              }, `Started ${s.id}.`)
            }
          >
            Run now
          </button>
        </span>
      </td>
    </tr>
  );
}
