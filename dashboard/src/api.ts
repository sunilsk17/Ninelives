export type Status = "PENDING" | "RUNNING" | "WAITING" | "SUCCEEDED" | "FAILED" | "CANCELLED";
export const STATUSES: Status[] = ["PENDING", "RUNNING", "WAITING", "SUCCEEDED", "FAILED", "CANCELLED"];
export const TERMINAL: Status[] = ["SUCCEEDED", "FAILED", "CANCELLED"];

export interface ErrorInfo {
  type: string;
  message: string;
  traceback?: string;
}

export interface Workflow {
  id: string;
  name: string;
  status: Status;
  input: { args: unknown[]; kwargs: Record<string, unknown> };
  output: unknown;
  error: ErrorInfo | null;
  created_at: number;
  updated_at: number;
  wake_at: number | null;
  lease_owner: string | null;
  lease_expires_at: number | null;
  runs: number;
  recoveries: number;
  cancel_requested: boolean;
  forked_from: string | null;
  fork_seq: number | null;
  parent_id: string | null;
  parent_seq: number | null;
  priority: number;
  concurrency_key: string | null;
  deadline: number | null;
  schedule_id: string | null;
}

export interface JournalEntry {
  seq: number;
  kind: "step" | "sleep" | "signal" | "value" | "event" | "child" | "send" | "patch";
  name: string;
  status: "completed" | "failed" | "pending";
  output: unknown;
  error: ErrorInfo | null;
  wake_at: number | null;
  attempts: number;
  started_at: number | null;
  completed_at: number | null;
}

export interface Attempt {
  seq: number;
  attempt: number;
  worker: string;
  started_at: number;
  finished_at: number | null;
  error: string | null;
}

export interface Signal {
  id: number;
  name: string;
  payload: unknown;
  created_at: number;
  consumed_seq: number | null;
}

export interface Approval {
  signal: string;
  call_id: string;
  seq: number;
  deadline: number | null;
  tool?: string;
  input?: unknown;
}

export interface RunEvent {
  key: string;
  value: unknown;
  updated_at: number;
}

export interface Child {
  id: string;
  name: string;
  status: Status;
  parent_seq: number | null;
}

export interface Detail {
  workflow: Workflow;
  journal: JournalEntry[];
  attempts: Attempt[];
  signals: Signal[];
  approvals: Approval[];
  events: RunEvent[];
  children: Child[];
}

export interface Schedule {
  id: string;
  workflow: string;
  cron: string;
  tz: string;
  input: { args: unknown[]; kwargs: Record<string, unknown> };
  overlap: "skip" | "allow";
  enabled: boolean;
  next_at: number | null;
  last_at: number | null;
  last_run_id: string | null;
  seen_at: number | null;
}

export interface Stats {
  version: string;
  counts: Record<Status, number>;
  workflows: string[];
  schedules: number;
}

const TOKEN_KEY = "ninelives-token";

export function getToken(): string | null {
  try {
    return localStorage.getItem(TOKEN_KEY);
  } catch {
    return null;
  }
}

export function setToken(token: string | null): void {
  try {
    if (token) localStorage.setItem(TOKEN_KEY, token);
    else localStorage.removeItem(TOKEN_KEY);
  } catch {
    /* storage unavailable: the token lasts for this page only */
  }
  memoryToken = token;
}

let memoryToken: string | null = getToken();

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers: Record<string, string> = {};
  if (init?.body) headers["content-type"] = "application/json";
  if (memoryToken) headers.authorization = `Bearer ${memoryToken}`;
  const res = await fetch(path, { ...init, headers });
  if (!res.ok) {
    let message = res.statusText;
    try {
      const body = await res.json();
      message = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
    } catch {
      /* not JSON */
    }
    throw new ApiError(res.status, message);
  }
  return res.json() as Promise<T>;
}

const post = <T>(path: string, body?: unknown) =>
  request<T>(path, { method: "POST", body: body === undefined ? undefined : JSON.stringify(body) });

export const api = {
  stats: () => request<Stats>("/api/stats"),
  list: (params: {
    status?: Status;
    name?: string;
    q?: string;
    parent_id?: string;
    limit?: number;
    offset?: number;
  }) => {
    const q = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => v !== undefined && v !== "" && q.set(k, String(v)));
    return request<{ workflows: Workflow[] }>(`/api/workflows?${q}`);
  },
  describe: (id: string) => request<Detail>(`/api/workflows/${encodeURIComponent(id)}`),
  start: (
    name: string,
    body: {
      args: unknown[];
      kwargs: Record<string, unknown>;
      id?: string;
      priority?: number;
      concurrency_key?: string;
      timeout?: number;
    },
  ) =>
    post<{ id: string }>(`/api/workflows/${encodeURIComponent(name)}`, body),
  signal: (id: string, name: string, payload: unknown) =>
    post<{ ok: boolean }>(`/api/workflows/${encodeURIComponent(id)}/signals/${encodeURIComponent(name)}`, payload),
  cancel: (id: string) => post<Workflow>(`/api/workflows/${encodeURIComponent(id)}/cancel`),
  fork: (id: string, fromSeq: number) =>
    post<{ id: string }>(`/api/workflows/${encodeURIComponent(id)}/fork`, { from_seq: fromSeq }),
  schedules: () => request<{ schedules: Schedule[] }>("/api/schedules"),
  pauseSchedule: (id: string) => post<Schedule>(`/api/schedules/${encodeURIComponent(id)}/pause`),
  resumeSchedule: (id: string) => post<Schedule>(`/api/schedules/${encodeURIComponent(id)}/resume`),
  triggerSchedule: (id: string) => post<{ id: string }>(`/api/schedules/${encodeURIComponent(id)}/trigger`),
};
