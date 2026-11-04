import { useState, type ReactNode } from "react";
import type { Status } from "../api";

const STATUS_STYLE: Record<Status, string> = {
  PENDING: "bg-zinc-100 text-zinc-700 ring-zinc-300 dark:bg-zinc-800 dark:text-zinc-300 dark:ring-zinc-700",
  RUNNING: "bg-sky-50 text-sky-700 ring-sky-300 dark:bg-sky-950 dark:text-sky-300 dark:ring-sky-800",
  WAITING: "bg-amber-50 text-amber-800 ring-amber-300 dark:bg-amber-950 dark:text-amber-300 dark:ring-amber-800",
  SUCCEEDED: "bg-emerald-50 text-emerald-700 ring-emerald-300 dark:bg-emerald-950 dark:text-emerald-300 dark:ring-emerald-800",
  FAILED: "bg-rose-50 text-rose-700 ring-rose-300 dark:bg-rose-950 dark:text-rose-300 dark:ring-rose-800",
  CANCELLED: "bg-zinc-100 text-zinc-500 ring-zinc-300 line-through dark:bg-zinc-900 dark:text-zinc-500 dark:ring-zinc-700",
};

export const STATUS_DOT: Record<Status, string> = {
  PENDING: "bg-zinc-400",
  RUNNING: "bg-sky-500",
  WAITING: "bg-amber-500",
  SUCCEEDED: "bg-emerald-500",
  FAILED: "bg-rose-500",
  CANCELLED: "bg-zinc-400",
};

export function StatusBadge({ status }: { status: Status }) {
  return (
    <span
      data-testid="status"
      className={`inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-semibold ring-1 ring-inset ${STATUS_STYLE[status]}`}
    >
      <span className={`size-1.5 rounded-full ${STATUS_DOT[status]} ${status === "RUNNING" ? "animate-pulse" : ""}`} />
      {status.toLowerCase()}
    </span>
  );
}

export function JsonView({ value, className = "" }: { value: unknown; className?: string }) {
  const text = value === undefined ? "undefined" : JSON.stringify(value, null, 2);
  return (
    <pre
      className={`overflow-auto rounded-lg bg-zinc-100 p-3 font-mono text-xs whitespace-pre-wrap wrap-anywhere leading-relaxed text-zinc-800 dark:bg-zinc-950 dark:text-zinc-300 ${className}`}
    >
      {text}
    </pre>
  );
}

export function Disclosure({ summary, children, open = false }: { summary: ReactNode; children: ReactNode; open?: boolean }) {
  const [isOpen, setOpen] = useState(open);
  return (
    <div>
      <button
        type="button"
        onClick={() => setOpen(!isOpen)}
        className="flex max-w-full items-start gap-1 text-left text-xs text-zinc-500 hover:text-zinc-800 dark:hover:text-zinc-200"
        aria-expanded={isOpen}
      >
        <span className={`inline-block shrink-0 transition-transform ${isOpen ? "rotate-90" : ""}`}>▸</span>
        <span className="min-w-0 wrap-anywhere">{summary}</span>
      </button>
      {isOpen && <div className="mt-2">{children}</div>}
    </div>
  );
}

export function Banner({ kind, children }: { kind: "error" | "info"; children: ReactNode }) {
  const style =
    kind === "error"
      ? "border-rose-200 bg-rose-50 text-rose-800 dark:border-rose-900 dark:bg-rose-950/50 dark:text-rose-200"
      : "border-violet-200 bg-violet-50 text-violet-900 dark:border-violet-900 dark:bg-violet-950/50 dark:text-violet-200";
  return (
    <div role={kind === "error" ? "alert" : "status"} className={`rounded-lg border px-3 py-2 text-sm ${style}`}>
      {children}
    </div>
  );
}

export function Empty({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="px-6 py-14 text-center">
      <p className="font-medium text-zinc-700 dark:text-zinc-300">{title}</p>
      {children && <div className="mt-1 text-sm text-zinc-500">{children}</div>}
    </div>
  );
}
