# Benchmark results

Measured with `benchmarks/bench.py` on a 4-vCPU cloud container (Linux, Python 3.13), Postgres 16
running on the same machine. Workers are separate OS processes started with `ninelives worker`.
These are indicative numbers for one small machine, not a tuned deployment.

Throughput: 500 workflows x 10 steps, 4 worker processes.

| Store | Workflows/s | Steps/s | Per-step latency (1 workflow, sequential) | Recovery after SIGKILL (lease TTL 2s) |
|---|---|---|---|---|
| SQLite (WAL) | 125 | 1254 | 0.96 ms | 2.41–2.43 s |
| Postgres | 137 | 1367 | 3.71 ms | 2.43–2.44 s |

* **Per-step latency** is the cost of making a step durable: the attempt record, the journal write
  and the lease check, each in its own fenced transaction.
* **Recovery** is the time from `kill -9` of the worker in the middle of a step until another worker
  has finished the run. It is the lease TTL (how long a silent worker keeps its claim) plus one poll
  interval and the replay. Lower the TTL for faster failover at the cost of more heartbeat traffic.

Reproduce:

```bash
uv run python benchmarks/bench.py --postgres postgresql://postgres@localhost:5432/postgres
```
