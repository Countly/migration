# MongoDB to ClickHouse Migration Service

Migrates Countly `drill_events*` collections from MongoDB into a single ClickHouse table. Supports multi-pod horizontal scaling, range-parallel processing, async writes, pause/resume, crash recovery, backpressure monitoring, and a real-time dashboard.

## Quick Start

```bash
cp .env.example .env   # edit with your connection details
docker compose up --build
curl http://localhost:8080/healthz
```

Open the dashboard at [http://localhost:8080/viz](http://localhost:8080/viz).

## Running

**Docker Compose (recommended):**

```bash
docker compose up --build
```

**From source (Node 25+):**

```bash
npm install
node --experimental-strip-types --expose-gc --max-old-space-size=2048 src/main.ts
```

Required env vars: `SERVICE_NAME`, `MONGO_URI`, `CLICKHOUSE_URL`. No Redis.

## Architecture

Work is cut into cd-bounded **chunks** tracked in a MongoDB ledger
(`mig_ranges`) — the only progress state, and it is verified, never blindly
trusted. Per chunk: claim (atomic, leased, newest-data-first) → stream-copy
into a per-chunk **staging table** (one long-lived cursor; synchronous inserts
with a concurrent window) → **verify** (read tally vs exact ClickHouse
`count()`) → **promote** into the live table via verify-then-`ATTACH PARTITION`
(`INSERT SELECT` fallback) → drop staging. A dedicated chunk sweeps documents
that have no `cd` value.

Failure handling: permanent insert errors are bisected down to the offending
documents, which land in the DLQ (`mig_dlq_docs`) **with their full raw source
doc** — replayable via `POST /control/replay-dlq` after a transform fix,
without ever re-reading the source. Every unmigratable doc (invalid ts,
missing fields) is captured the same way. A circuit breaker pauses the engine
on systematic failure rates; ClickHouse parts pressure is respected via a
TTL-cached sampler; a background invariant monitor spot-checks done chunks
against live-table counts. Crash recovery: in-flight chunks are dropped and
redone; completed chunks are recounted — a stale or lost ledger cannot cause
wrong data. Multi-pod: pods claim chunks via leases; expired leases are
reclaimed automatically.

Endpoints: `/healthz`, `/stats` (incl. per-stage timings), `/report`
(skips, coercions per key, DLQ summary), `/control/pause|resume|replay-dlq`,
and `/viz` — a live dashboard fed by the ledger.

### Engine env vars

| Variable | Default | Description |
|----------|---------|-------------|
| `LEDGER_RUN_ID` | `ledger-v1` | Stable run identity (resume key) |
| `LEDGER_CHUNK_DOCS_TARGET` | `2000000` | Docs per chunk (sizes crash-redo cost) |
| `LEDGER_INSERT_INFLIGHT` | `3` | Concurrent insert window per chunk |
| `LEDGER_LEASE_SEC` | `600` | Chunk claim lease (multi-pod reclaim) |
| `LEDGER_BREAKER_PCT` | `5` | Pause when >pct% of a chunk's docs fail |
| `LEDGER_BREAKER_CONSECUTIVE` | `3` | Pause after N consecutive failed chunks |
| `LEDGER_MONITOR_INTERVAL_MS` | `900000` | Invariant spot-check interval (0 = off) |
| `LEDGER_CAPTURE_TRANSFORM_ERRORS` | `true` | DLQ every unmigratable doc with its raw doc |
| `DRY_RUN` | `false` | Sampled rehearsal against a Null-engine clone |
| `DRY_RUN_SAMPLE_PCT` | `2` | Dry-run sample size (hard cap 5) |
| `LEDGER_MAX_CHUNK_DAYS` | `7` | Max chunk time span — guards sizing against bad doc estimates |
| `MONGO_READ_PREFERENCE` | `primary` | Set `secondaryPreferred` on replica sets — offloads the primary; exact reads since the source is frozen after cutover |

### Sizing knobs — when to change them

- `MONGO_PAGE_SIZE` (10,000): lower it (1,000 or less) when documents are
  large (hundreds of KB+) — a page is held in memory whole.
- `LEDGER_CHUNK_DOCS_TARGET` (2M): a chunk is the unit of crash-redo and of
  pod parallelism. Smaller chunks = cheaper redo + finer progress, more
  per-chunk overhead. Lower it on unstable infrastructure.
- `LEDGER_INSERT_INFLIGHT` (3): raise for a high-latency ClickHouse (more
  hidden wait), set 1 for a memory-tight one.

### Scaling with pods

Start more instances with the same env and a unique `POD_ID` each
(`MULTI_POD_ENABLED=true`, default). Pods coordinate ONLY through the chunk
ledger: an atomic claim hands each pending chunk to exactly one pod; chunk
cd-ranges are disjoint, so no overlap and no gaps; a dead pod's lease expires
and survivors reclaim its chunk (drop staging, redo). Verified: 3 pods, one
killed mid-run, exact final counts.

**Pods scale across machines, not on one box** — a single pod is CPU-bound
(BSON decode), so extra pods on the same host fight for the same cores
(measured slower locally). Find your ceiling empirically: add a pod at a
time on separate hosts and watch per-pod docs/s in the dashboard's Pods
panel; when adding a pod no longer raises the total (source Mongo or target
ClickHouse saturated — read time share and backpressure waits rise in
/stats stageMs), you've found it.

Validation harness (seed + SIGKILL crash drill): see [`bench/README.md`](bench/README.md).

## Configuration