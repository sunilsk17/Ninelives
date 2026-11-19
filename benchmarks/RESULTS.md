# Benchmark results

Measured with `benchmarks/bench.py` on a 4-vCPU cloud container (Linux, Python 3.13), with
Postgres 16 on the same machine. Workers are separate OS processes started with `ninelives worker`.
These are indicative numbers for one small, shared machine, not a tuned deployment. Repeated runs
vary by roughly ±15%.

Throughput: 500 workflows of 10 steps each, 4 worker processes.

| Store | Workflows/s | Steps/s | Per-step latency | Start-to-finish, idle worker | Recovery after `kill -9` (lease TTL 2 s) |
|---|---|---|---|---|---|
| SQLite (WAL) | 127 | 1273 | 0.82 ms | 152 ms | 2.40–2.43 s |
| Postgres 16 | 168 | 1679 | 3.01 ms | 11 ms | 2.41–2.42 s |

What each column measures:

- **Per-step latency.** One workflow of 300 sequential steps: the cost of making a step durable (an
  attempt record, the journal write and the lease check). Since v0.2, a finished step is one
  transaction instead of two.
- **Start-to-finish, idle worker.** `app.start()` to a finished one-step run, with an idle worker in
  another process and the default 200 ms poll interval. Postgres workers are woken immediately by
  `LISTEN/NOTIFY`; SQLite workers find new work on their next poll.
- **Recovery.** From `kill -9` of the worker in the middle of a step until another worker has finished
  the run. This is the lease TTL (how long a silent worker keeps its claim) plus one poll and the
  replay. A lower TTL gives faster failover at the cost of more heartbeat traffic.

Reproduce:

```bash
uv run python benchmarks/bench.py --postgres postgresql://postgres@localhost:5432/postgres
```
