# Operations

## Processes

A deployment has three kinds of process. All of them import the same workflow code and point at the
same database:

- **Your application** starts runs and reads results, through `app.start(...)` / `app.result(...)`,
  the `SyncClient`, the HTTP API or the CLI.
- **Workers** (`ninelives worker app.py:app`) execute runs. Run as many as you like, on as many
  machines as your database allows (SQLite: one machine; Postgres: any number).
- **The server** (`ninelives serve app.py:app`), optionally, for the dashboard and HTTP API. With
  `--worker` it also runs a worker in the same process, which is fine for small setups.

`docker-compose.yml` in the repository runs Postgres, the server and three workers:

```bash
docker compose up --build                     # dashboard on http://localhost:8000
docker compose kill -s SIGKILL worker         # crash every worker...
docker compose up -d worker                   # ...and watch the runs resume
```

To run your own code, build an image that adds your package on top of this one (`FROM` it, `COPY` your code) and set `APP` to your `module:variable`.

## Worker settings

| Option (CLI flag) | Default | What it does |
| --- | --- | --- |
| `concurrency` (`--concurrency`) | 16 | runs this worker executes at once |
| `lease_ttl` (`--lease-ttl`) | 15 s | how long a silent worker keeps its runs; **crash recovery takes about this long** |
| `poll_interval` (`--poll`) | 0.2 s | idle polling; Postgres also wakes workers instantly through LISTEN/NOTIFY |
| `shutdown_grace` | 10 s | on SIGTERM, how long running work may finish before it is handed over |
| `retention` (`--retention`) | off | purge finished runs older than this, hourly |

Workers heartbeat every `lease_ttl / 3`. A worker that stops heartbeating (crash, freeze, network
partition) loses its runs after `lease_ttl`, and another worker resumes them. Writes are fenced: a
worker that comes back after losing its lease cannot overwrite anything.

Stop workers with SIGTERM (or Ctrl+C). They finish or hand over their work cleanly. SIGKILL is also
safe; recovery then just waits for the lease to expire.

## Calling from synchronous code

```python
from ninelives import SyncClient

client = SyncClient(app)                         # e.g. at Django/Flask start-up
run_id = client.start(checkout, "A-1", 4999)
client.signal(run_id, "review", {"stars": 5})
print(client.result(run_id, timeout=30))
```

`SyncClient` runs its own event loop in a background thread and is safe to share between threads.

## The HTTP API and dashboard

`ninelives serve app.py:app` serves the dashboard at `/`, interactive API docs at `/docs`, and:

| Method | Path | |
| --- | --- | --- |
| `GET` | `/api/health` | liveness and version (never needs a token) |
| `GET` | `/api/stats` | counts by status, registered workflows, number of schedules |
| `GET` | `/api/workflows?status=&name=&q=&parent_id=&schedule_id=&limit=&offset=` | list runs, newest first (`q` matches part of the id) |
| `POST` | `/api/workflows/{name}` | start: `{"args": [], "kwargs": {}, "id": null, "priority": 0, "concurrency_key": null, "timeout": null}` |
| `GET` | `/api/workflows/{id}` | the run with its journal, attempts, signals, events, children and pending approvals |
| `GET` | `/api/workflows/{id}/events` | published events |
| `POST` | `/api/workflows/{id}/signals/{name}` | send a signal; the JSON body is the payload |
| `POST` | `/api/workflows/{id}/cancel` | cancel the run and its unfinished children |
| `POST` | `/api/workflows/{id}/fork` | `{"from_seq": N}`: re-run from step N |
| `GET` | `/api/schedules` | list schedules |
| `POST` | `/api/schedules/{id}/pause` · `/resume` · `/trigger` | manage a schedule |
| `POST` | `/api/purge` | `{"older_than": seconds}` |
| `GET` | `/metrics` | Prometheus metrics |

Run ids may contain `/` (child runs). URL-encode them or send them as-is; both work.

## Security

The server has **no authentication by default** and binds to `127.0.0.1`. Before exposing it:

```bash
NINELIVES_API_TOKEN=$(openssl rand -hex 32) ninelives serve app.py:app --host 0.0.0.0
```

With a token set, every route except `/api/health` requires `Authorization: Bearer <token>`. The
dashboard asks for the token once and remembers it in that browser. Put the server behind HTTPS
(a reverse proxy) when it leaves a private network. Workers do not serve anything; they only
need database access.

## Metrics

`GET /metrics` returns Prometheus text:

```text
ninelives_runs{workflow="checkout",status="SUCCEEDED"} 1204
ninelives_runs{workflow="checkout",status="WAITING"} 7
ninelives_schedules{enabled="true"} 3
ninelives_info{version="0.2.0"} 1
```

With a token configured, scrape it with `authorization: {credentials: <token>}`.

## Tracing

Install the `otel` extra and configure OpenTelemetry in your app as usual (an SDK `TracerProvider`
with an exporter). Workers then emit:

- `ninelives.run <workflow>` for each execution of a run, with its outcome (succeeded, failed,
  cancelled, waiting, abandoned) and whether it was a crash recovery;
- `ninelives.step <step>` for each step attempt, with its position, attempt number and any error.
  Steps replayed from the journal don't execute and produce no span.

## The CLI

```text
ninelives worker   APP [--concurrency N] [--lease-ttl S] [--poll S] [--retention 30d]
ninelives serve    APP [--host H] [--port P] [--worker] [--token T]
ninelives start    APP WORKFLOW [--args JSON] [--kwargs JSON] [--id ID] [--priority N]
                       [--concurrency-key K] [--run-timeout 1h] [--wait] [--timeout S]
ninelives status   APP RUN_ID           # everything about a run, as JSON
ninelives events   APP RUN_ID
ninelives signal   APP RUN_ID NAME [--payload JSON]
ninelives cancel   APP RUN_ID
ninelives ls       APP [--status S] [--name WORKFLOW] [--search TEXT] [--limit N]
ninelives purge    APP --older-than 30d
ninelives schedules APP [list | pause ID | resume ID | trigger ID]
ninelives --version
```

Durations accept `90`, `90s`, `15m`, `12h`, `30d` or `2w`. Errors print one line and exit with status 1.

## Database notes

- **Postgres.** Any recent version works (CI runs 16). Ninelives creates its tables (`workflows`,
  `journal`, `attempts`, `signals`, `events`, `schedules`, `ninelives_schema`) on first start, in
  whichever schema the connection uses. Give it its own database or schema if you prefer. Size the
  pool for your worker concurrency; each claim, heartbeat and journal write is a short transaction.
- **SQLite.** Uses WAL mode and is safe for several processes on one machine. Keep the file on a local
  disk, not a network share.
- **Backups.** Use your database's normal backup tools. Everything Ninelives knows is in those tables.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Runs stay PENDING | no worker is running the code that registers that workflow name |
| A crashed run takes a while to resume | it waits for the lease to expire (`lease_ttl`) |
| `NonDeterminismError` after a deploy | the code changed under a running instance; see [Changing code safely](versioning.md) |
| A step ran twice | a worker died after the step's side effect; pass `step_info().idempotency_key` downstream |
| `SchemaTooNew` on start | an older Ninelives is pointed at a database a newer one has upgraded |
