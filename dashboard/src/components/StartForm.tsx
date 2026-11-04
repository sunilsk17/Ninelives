import { useEffect, useRef, useState, type FormEvent } from "react";
import { api } from "../api";
import { runHref } from "../util";
import { Banner } from "./ui";

function parse(label: string, text: string, kind: "array" | "object"): unknown {
  if (!text.trim()) return kind === "array" ? [] : {};
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    throw new Error(`${label} is not valid JSON`);
  }
  const ok = kind === "array" ? Array.isArray(value) : value !== null && typeof value === "object" && !Array.isArray(value);
  if (!ok) throw new Error(`${label} must be a JSON ${kind}`);
  return value;
}

export function StartForm({ workflows, onClose }: { workflows: string[]; onClose: () => void }) {
  const [name, setName] = useState(workflows[0] ?? "");
  const [args, setArgs] = useState("[]");
  const [kwargs, setKwargs] = useState("{}");
  const [id, setId] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const dialog = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    dialog.current?.showModal();
  }, []);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      const body = {
        args: parse("Arguments", args, "array") as unknown[],
        kwargs: parse("Keyword arguments", kwargs, "object") as Record<string, unknown>,
        id: id.trim() || undefined,
      };
      setBusy(true);
      const res = await api.start(name, body);
      window.location.hash = runHref(res.id);
      onClose();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <dialog
      ref={dialog}
      onClose={onClose}
      className="m-auto w-[min(32rem,calc(100vw-2rem))] rounded-xl border border-zinc-200 bg-white p-0 text-zinc-900 shadow-xl backdrop:bg-zinc-950/40 dark:border-zinc-800 dark:bg-zinc-900 dark:text-zinc-100"
    >
      <form onSubmit={submit} className="space-y-4 p-5">
        <div className="flex items-center justify-between">
          <h2 className="text-lg font-semibold">Start a run</h2>
          <button type="button" aria-label="Close" className="text-zinc-400 hover:text-zinc-700" onClick={onClose}>
            ✕
          </button>
        </div>
        {workflows.length === 0 ? (
          <Banner kind="info">This server has no workflows registered.</Banner>
        ) : (
          <>
            <label className="block space-y-1">
              <span className="label">Workflow</span>
              <select className="input" value={name} onChange={(e) => setName(e.target.value)}>
                {workflows.map((w) => (
                  <option key={w}>{w}</option>
                ))}
              </select>
            </label>
            <label className="block space-y-1">
              <span className="label">Arguments (JSON list)</span>
              <textarea className="input font-mono" rows={2} value={args} onChange={(e) => setArgs(e.target.value)} />
            </label>
            <label className="block space-y-1">
              <span className="label">Keyword arguments (JSON object)</span>
              <textarea className="input font-mono" rows={2} value={kwargs} onChange={(e) => setKwargs(e.target.value)} />
            </label>
            <label className="block space-y-1">
              <span className="label">Run id (optional, makes the start idempotent)</span>
              <input className="input font-mono" value={id} onChange={(e) => setId(e.target.value)} placeholder="order-1042" />
            </label>
          </>
        )}
        {error && <Banner kind="error">{error}</Banner>}
        <div className="flex justify-end gap-2">
          <button type="button" className="btn-ghost" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="btn-primary" disabled={busy || !name}>
            {busy ? "Starting…" : "Start"}
          </button>
        </div>
      </form>
    </dialog>
  );
}
