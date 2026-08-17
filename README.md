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

Validation harness (seed + SIGKILL crash drill): see [`bench/README.md`](bench/README.md).

## Configuration