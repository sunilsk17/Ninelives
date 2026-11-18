import { useEffect, useState, type FormEvent } from "react";
import { api, ApiError, getToken, setToken } from "./api";
import { RunPage } from "./components/RunPage";
import { RunsPage } from "./components/RunsPage";
import { SchedulesPage } from "./components/SchedulesPage";
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
  const [authVersion, setAuthVersion] = useState(0);
  const { data: stats, error } = usePoll(() => api.stats(), [authVersion], 2000);
  const [theme, setTheme] = useState<Theme>(initialTheme);
  const needsToken = error instanceof ApiError && error.status === 401;

  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark");
    try {
      localStorage.setItem("ninelives-theme", theme);
    } catch {
      /* storage unavailable */
    }
  }, [theme]);

  const runId = route.startsWith("/runs/") ? route.slice("/runs/".length) : null;
  const page = runId ? "run" : route.startsWith("/schedules") ? "schedules" : "runs";

  useEffect(() => {
    document.title = runId ? `${runId} · Ninelives` : page === "schedules" ? "Schedules · Ninelives" : "Ninelives";
  }, [runId, page]);

  const navLink = (href: string, label: string, active: boolean) => (
    <a
      href={href}
      aria-current={active ? "page" : undefined}
      className={`rounded-md px-2.5 py-1 text-sm ${
        active
          ? "bg-zinc-100 font-medium text-zinc-900 dark:bg-zinc-800 dark:text-zinc-100"
          : "text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100"
      }`}
    >
      {label}
    </a>
  );

  return (
    <div className="min-h-screen">
      <header className="sticky top-0 z-20 border-b border-zinc-200 bg-white/80 backdrop-blur dark:border-zinc-800 dark:bg-zinc-950/80">
        <div className="mx-auto flex max-w-7xl items-center gap-3 px-4 py-3">
          <a href="#/" className="flex items-center gap-2 font-semibold tracking-tight">
            <span className="flex size-7 items-center justify-center rounded-lg bg-violet-700 text-sm font-bold text-white">9</span>
            <span className="hidden sm:inline">Ninelives</span>
          </a>
          <nav className="flex items-center gap-1" aria-label="Main">
            {navLink("#/", "Runs", page !== "schedules")}
            {navLink("#/schedules", "Schedules", page === "schedules")}
          </nav>
          <span className="ml-auto flex items-center gap-2 text-xs text-zinc-500">
            <span className={`size-2 rounded-full ${error ? "bg-rose-500" : "bg-emerald-500"}`} />
            {needsToken ? "sign-in needed" : error ? "API unreachable" : "live"}
          </span>
          {stats && <span className="hidden text-xs text-zinc-400 md:inline">v{stats.version}</span>}
          <a className="text-sm text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100" href="/docs" target="_blank" rel="noreferrer">
            API
          </a>
          {getToken() && (
            <button
              type="button"
              className="text-sm text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100"
              onClick={() => {
                setToken(null);
                setAuthVersion((v) => v + 1);
              }}
            >
              Sign out
            </button>
          )}
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
        {needsToken ? (
          <SignIn
            onSignedIn={() => {
              setAuthVersion((v) => v + 1);
            }}
          />
        ) : (
          <>
            {error && !stats && (
              <div className="mb-4">
                <Banner kind="error">Cannot reach the Ninelives API: {error.message}</Banner>
              </div>
            )}
            {page === "run" && runId ? (
              <RunPage key={`${runId}:${authVersion}`} id={runId} />
            ) : page === "schedules" ? (
              <SchedulesPage key={authVersion} />
            ) : (
              <RunsPage key={authVersion} stats={stats} />
            )}
          </>
        )}
      </main>
    </div>
  );
}

function SignIn({ onSignedIn }: { onSignedIn: () => void }) {
  const [value, setValue] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setBusy(true);
    setToken(value.trim());
    try {
      await api.stats();
      onSignedIn();
    } catch (err) {
      setToken(null);
      setError(err instanceof ApiError && err.status === 401 ? "That token was not accepted." : (err as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <form onSubmit={submit} className="card mx-auto mt-16 max-w-md space-y-4 p-6" aria-label="Sign in">
      <div>
        <h1 className="text-lg font-semibold">Sign in</h1>
        <p className="mt-1 text-sm text-zinc-500">
          This server requires an API token. It is the value of <code className="font-mono">NINELIVES_API_TOKEN</code> (or{" "}
          <code className="font-mono">--token</code>) on the server.
        </p>
      </div>
      <label className="block space-y-1">
        <span className="label">API token</span>
        <input
          className="input font-mono"
          type="password"
          autoComplete="current-password"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          required
        />
      </label>
      {error && <Banner kind="error">{error}</Banner>}
      <button type="submit" className="btn-primary w-full" disabled={busy || !value.trim()}>
        {busy ? "Checking…" : "Sign in"}
      </button>
    </form>
  );
}
