# Trailox.Agent.Bench: the MongoDB throughput gate

A console program, not part of the agent image and not run by `dotnet test`. It answers one
question before any MongoDB engine code is written: **can the agent read Atlas log files at
100,000 operations a second, inside the chart's default limits (500m CPU, 512Mi), without
losing a line?**

It does two things:

1. **`generate`** writes gzip log files for one task window of a 3-node replica set: a
   server log (`mongodb`) and an audit log (`audit`) per node, one gzip member per chunk as
   Atlas publishes them. Lines come from templates in `Fixtures/`, which are real Atlas lines
   (MongoDB 8.0, an M10 under load) with every address, host name, UUID, key and signature
   replaced. Each file starts before the window and ends after it, as Atlas's files do, and a
   few lines go back in time, as measured.
2. **`run`** reads those files through a prototype of the engine's pipeline and measures it.

## The prototype pipeline (`Pipeline/`)

| Step | Class | What it does |
|---|---|---|
| gunzip | `GzipLineReader` | Streams every gzip member and cuts lines into one pooled buffer. No string or array per line. |
| prefilter | `LineScan` | On the raw bytes: the line's own time against the window, then the server-log `id` or the audit `atype`. Lines no stream needs stop here. |
| extract | `FileProcessor` | `Utf8JsonReader` over the lines kept, reading only the fields the rows need. |
| shape | `ShapeNormalizer` | Replaces every value of the command by its type (`?string`, `?number`, `?array<string>`...), drops the driver's envelope, and hashes the result. No value byte is ever copied: this is the PII boundary. |
| aggregate | `MinuteAggregator` | One row per (minute, part, source, process, user, IP, connection, command, namespace, shape, result) with a count and summed metrics. A minute ships as soon as the file is 2 s past it. Past the group cap, a minute's new keys go to one `__other__` row with real totals. |
| ship | `RowSink` | NDJSON, gzip at `CompressionLevel.Fastest`, as `GatewayClient` uploads. The bytes are counted and discarded. |

Streams produced: `mongo_ops` (audit `authCheck` and DDL lines), `mongo_slow_ops` (server-log
`51803`), `mongo_connections` (`22943`, `22944`, `5286306`, `4615611`, audit `authenticate` and
`clientMetadata`, one row per line) and `mongo_signin_failures` (`5286307` and failed audit
`authenticate`, one group per connection, so attempts are counted and not lines).

This is a prototype to be measured, not the engine. The engine is written after its plan is
approved, and the bench then runs against the engine's own classes.

## What a run checks

Every pass is verified against the generator's `manifest.json`. A run fails (exit 1) unless:

- lines in = lines out of the window + filtered + malformed (0) + aggregated, per kind;
- the `eventCount` shipped per stream = the lines aggregated into it;
- the slow-operation metrics shipped (`nreturned`, `durationMillis`) = the generator's sums.

`selftest` generates two small corpora (typical and hostile), runs one pass of each with a small
group cap, keeps every shipped byte, and fails if a literal value written by the generator (every
literal string starts with `lit-`) appears in any row.

## Profiles

| | typical | hostile |
|---|---|---|
| Clients | 6 applications on 200 pooled connections from 20 addresses | every operation a new connection and address, 1 of 1,000 users |
| Shapes | 50, fixed per application | every operation a new shape |
| Connection churn | 1 a second | 1,000 a second |
| Failed sign-ins | 1 every 10 s | 100 a second |

Both: 100,000 operations a second (`--ops`), 90% on the primary, Full audit (one `authCheck` line per
operation), 0.05% of operations slow (measured), 10 background server-log lines per node a second.

## Running it

```bash
dotnet run --project tests/Trailox.Agent.Bench -c Release -- selftest
dotnet run --project tests/Trailox.Agent.Bench -c Release -- generate --out /tmp/typical --profile typical --ops 100000 --window-minutes 2
dotnet run --project tests/Trailox.Agent.Bench -c Release -- run --corpus /tmp/typical --passes 2
```

`run` options: `--seconds N` (repeat passes for N seconds), `--rate N` (hold the readers to N lines a
second, for a sustained run), `--group-cap N` (default 20,000 per minute per file), `--readers N`
(default: the processor count), `--stage gunzip|prefilter|parse|full` (stop each line after a stage,
to split the cost; only `full` is verified), `--report FILE`.

**At the chart's limits.** The numbers that count are taken under the limits the agent runs with:
CPU quota 50% and memory 512 MiB, no swap. On Linux with systemd (WSL Ubuntu 24.04, the release the
agent's `noble-chiseled` image is built on):

```bash
dotnet publish tests/Trailox.Agent.Bench -c Release -r linux-x64 --self-contained -o out
systemd-run --user --scope -p CPUQuota=50% -p MemoryMax=512M -p MemorySwapMax=0 \
  ./out/trailox-agent-bench run --corpus typical --passes 2
```

.NET reads the cgroup's limits as it does in a container: `processorCount` 1 and a GC heap limit of
75% of 512 MiB, both in the report. `peakResidentMiB` is the process's own peak; the cgroup's peak
also counts the page cache of the corpus files, which the agent, streaming from HTTP, does not have.
