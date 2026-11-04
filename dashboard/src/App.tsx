import { useEffect, useState } from "react";
import { api } from "./api";
import { RunPage } from "./components/RunPage";
import { RunsPage } from "./components/RunsPage";
import { Banner } from "./components/ui";
import { usePoll, useRoute } from "./util";

type Theme = "light" | "dark";

function initialTheme(): Theme {
  try {
    const saved = localStorage.getItem("ninelives-theme");
    if (saved === "light" || saved === "dark") return saved;
  } catch {
    /* storage unavailable */
  }
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

export function App() {
  const route = useRoute();
  const { data: stats, error } = usePoll(() => api.stats(), [], 2000);
  const [theme, setTheme] = useState<Theme>(initialTheme);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark");
    try {
      localStorage.setItem("ninelives-theme", theme);
    } catch {
      /* storage unavailable */
    }
  }, [theme]);

  const runId = route.startsWith("/runs/") ? route.slice("/runs/".length) : null;

  useEffect(() => {
    document.title = runId ? `${runId} · Ninelives` : "Ninelives";
  }, [runId]);

  return (
    <div className="min-h-screen">
      <header className="sticky top-0 z-20 border-b border-zinc-200 bg-white/80 backdrop-blur dark:border-zinc-800 dark:bg-zinc-950/80">
        <div className="mx-auto flex max-w-7xl items-center gap-3 px-4 py-3">
          <a href="#/" className="flex items-center gap-2 font-semibold tracking-tight">
            <span className="flex size-7 items-center justify-center rounded-lg bg-violet-700 text-sm font-bold text-white">9</span>
            Ninelives
          </a>
          <span className="hidden text-sm text-zinc-500 sm:inline">durable workflows &amp; agents</span>
          <span className="ml-auto flex items-center gap-2 text-xs text-zinc-500">
            <span className={`size-2 rounded-full ${error ? "bg-rose-500" : "bg-emerald-500"}`} />
            {error ? "API unreachable" : "live"}
          </span>
          <a className="text-sm text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100" href="/docs" target="_blank" rel="noreferrer">
            API
          </a>
          <button
            type="button"
            className="btn-ghost px-2 py-1"
            aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} theme`}
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
          >
            {theme === "dark" ? "☀" : "☾"}
          </button>
        </div>
      </header>
      <main className="mx-auto max-w-7xl px-4 py-6">
        {error && !stats && (
          <div className="mb-4">
            <Banner kind="error">Cannot reach the Ninelives API: {error.message}</Banner>
          </div>
        )}
        {runId ? <RunPage key={runId} id={runId} /> : <RunsPage stats={stats} />}
      </main>
    </div>
  );
}
