import { useCallback, useEffect, useRef, useState } from "react";

/** Poll an async loader every `interval` ms while the tab is visible. */
export function usePoll<T>(load: () => Promise<T>, deps: unknown[], interval = 1000) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const loadRef = useRef(load);
  loadRef.current = load;

  const refresh = useCallback(async () => {
    try {
      setData(await loadRef.current());
      setError(null);
    } catch (e) {
      setError(e as Error);
    }
  }, []);

  useEffect(() => {
    setData(null);
    let alive = true;
    let timer: number | undefined;
    const tick = async () => {
      if (document.visibilityState === "visible") await refresh();
      if (alive) timer = window.setTimeout(tick, interval);
    };
    tick();
    return () => {
      alive = false;
      window.clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, interval, refresh]);

  return { data, error, refresh };
}

/** The current hash route, e.g. "/runs/abc". */
export function useRoute(): string {
  const read = () => decodeURIComponent(window.location.hash.replace(/^#/, "")) || "/";
  const [route, setRoute] = useState(read);
  useEffect(() => {
    const on = () => setRoute(read());
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  return route;
}

export const runHref = (id: string) => `#/runs/${encodeURIComponent(id)}`;

export function ago(ts: number | null | undefined, now = Date.now() / 1000): string {
  if (!ts) return "—";
  const s = Math.round(now - ts);
  if (s < -1) return `in ${duration(-s)}`;
  if (s < 2) return "just now";
  return `${duration(s)} ago`;
}

export function duration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || Number.isNaN(seconds)) return "—";
  if (seconds < 1) return `${Math.max(0, Math.round(seconds * 1000))}ms`;
  if (seconds < 60) return `${seconds < 10 ? seconds.toFixed(1) : Math.round(seconds)}s`;
  const m = Math.floor(seconds / 60);
  if (m < 60) return `${m}m ${Math.round(seconds % 60)}s`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

export function clock(ts: number | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function compactJson(value: unknown, max = 80): string {
  const text = JSON.stringify(value);
  if (text === undefined) return "undefined";
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}
