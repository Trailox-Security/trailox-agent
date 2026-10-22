# Trailox.Agent.Bench: the MongoDB throughput gate

A console program, not part of the agent image and not run by `dotnet test`. It answers one
question: **can the agent read Atlas log files at 100,000 operations a second, inside the chart's
default limits (500m CPU, 512Mi), without losing a line?** It reads through the agent's own MongoDB
engine classes (`src/Trailox.Agent/Engines/MongoDb`), not a copy of them.

It does three things:

1. **`generate`** writes gzip log files for one task window of a 3-node replica set: a
   server log (`mongodb`) and an audit log (`audit`) per node, one gzip member per chunk as
   Atlas publishes them. Lines come from templates in `Fixtures/`, which are real Atlas lines
   (MongoDB 8.0, an M10 under load) with every address, host name, UUID, key and signature
   replaced. Each file starts before the window and ends after it, as Atlas's files do, and a
   few lines go back in time, as measured.
2. **`s3`** rewrites a generated corpus line for line as Atlas's S3 log export delivers it (below).
3. **`run`** reads a corpus through the engine and measures it.

## What a pass does

One pass is one task window, read the way the agent reads it:

| Step | Class | What it does |
|---|---|---|
| gunzip | `LogLines` | Streams every gzip member and cuts lines into one pooled buffer. No string or array per line. |
| prefilter | `LineScan` (in `MongoLogProcessor`) | On the raw bytes: the line's own time against the window, then the server-log `id` or the audit `atype`. Lines the stream does not need stop here. |
| extract | `MongoLogProcessor` | `Utf8JsonReader` over the lines kept, reading only the fields the rows need. |
| shape | `MongoShape` | Replaces every value of the command by its type, in sorted key order, and hashes the result. No value byte is ever copied: this is the PII boundary. |
| aggregate | `MinuteAggregator` | One row per minute and key, with a count and summed metrics. A minute ships once the file is 2 s past it. Past the group cap, a minute's new keys go to one `__other__` row with real totals. |
| ship | `RowSink`, then the bench's `Upload` | NDJSON, gzip at `CompressionLevel.Fastest`, as `GatewayClient` uploads. The bytes are counted and discarded. |

The four streams run one after the other, as the agent runs a window's tasks:

- `mongo_ops` reads every node's audit file (`authCheck`, DDL and the `startup` line) and streams its
  rows to the upload as it goes, as `MongoSource`'s pipe does;
- `mongo_slow_ops`, `mongo_connections` and `mongo_signin_failures` each read every node's server log
  whole (the agent downloads it once per stream), and hold all their rows until the upload, as
  `MongoSource.ServerLogAsync` does. Their peak is in the report (`peakRowsHeldMiB`).

Not reproduced: the HTTP download (TLS, sockets), and the 3-minute server-log tail `mongo_ops` reads per
node to check that a recent window is published.

## The S3 export form (`s3`)

The export form times what the agent's S3 reader (1.10.0) costs on the export's lines, and checks that it ships
the same rows as from the API's lines. It reads each object with the engine's own classes: `LogLines` over the
plain file, `S3Envelope` for each record's inner line, and the processor in `LineForm.Rewritten`. Only listing and
fetching from S3 are left out: objects are read from disk in name order. The form is the one measured on a test
cluster's export:

- objects are plain JSON lines, not gzip: at most 10,000 records, and an object over 10 MiB is split into
  `partNNNNN-of-NNNNN` files at line boundaries;
- each record is the documented envelope (`service.name`, `log`, `host.name`, `mongodb.group.id`,
  `mongodb.cluster.name`, `mongodb.customer.cluster.name`, `mongodb.log.type`, `timestamp`), with `log`
  the line as a JSON string; the reader takes the line's own time, never the envelope's;
- the line is re-encoded: no padding, keys in a new random order at every depth, every array an object
  keyed `"1".."n"`, null members dropped, and an integer outside the 32-bit range written as a double of 15
  significant digits. Which fields the export treats as int64 is not measured, so smaller integers stay.

## What a run checks

Every pass is verified against the generator's `manifest.json`. A run fails (exit 1) unless, for every
stream:

- lines in = the generator's lines of the files it reads;
- lines out of the window + filtered + malformed (0) + kept = lines in;
- kept = the generator's count for the stream;
- the `event_count` shipped (rows, for `mongo_connections`) = kept;
- and the slow-operation metrics shipped (`nreturned`, `durationMillis`) = the generator's sums.

`--verify false` skips the row tally, to measure what the check itself costs.

`selftest` generates two small corpora (typical and hostile) and their export form, runs one pass of
each form with a small group cap, keeps every shipped byte, and fails unless both forms pass the checks
above, both ship the same rows, and no literal value written by the generator (every literal string
starts with `lit-`) appears in any row. Until 1.10.0 a slow `findAndModify` could ship as `update` from the export form
(its `update` field came first) and was tolerated as a known difference; now any difference fails.

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
dotnet run --project tests/Trailox.Agent.Bench -c Release -- s3 --from /tmp/typical --out /tmp/typical-s3
dotnet run --project tests/Trailox.Agent.Bench -c Release -- run --corpus /tmp/typical --passes 2
```

`run` options: `--seconds N` (repeat passes for N seconds), `--rate N` (hold the reader to N lines a
second, for a sustained run), `--group-cap N` (default 20,000 per minute per file), `--verify false`,
`--neighbour-cgroup DIR` (sample another cgroup's CPU over the run, such as an agent on the same machine),
`--report FILE`. A pass processes the audit lines once and the server-log lines three times, and the report's
`linesNeededPerSecond` counts them the same way.

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
