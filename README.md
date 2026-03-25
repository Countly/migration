# MongoDB to ClickHouse Migration Service

Migrates Countly `drill_events*` collections from MongoDB into a single ClickHouse table. Supports pause/resume, crash recovery, backpressure, and multi-collection orchestration.

## Quick Start

```bash
cp .env.example .env   # edit with your connection details
docker compose up --build
curl http://localhost:8080/healthz
```

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

Required env vars: `SERVICE_NAME`, `MONGO_URI`, `CLICKHOUSE_URL`, `REDIS_URL`.

## Configuration

Copy `.env.example` and adjust. All values below show defaults where applicable.

### Service

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVICE_NAME` | *(required)* | Service identifier |
| `SERVICE_PORT` | `8080` | HTTP server port |
| `SERVICE_HOST` | `0.0.0.0` | HTTP server bind address |
| `GRACEFUL_SHUTDOWN_TIMEOUT_MS` | `60000` | Max wait for graceful shutdown |
| `RERUN_MODE` | `resume` | `resume`, `new-run`, or `clone-run` |
| `LOG_LEVEL` | `info` | `fatal`, `error`, `warn`, `info`, `debug`, `trace` |

### MongoDB Source

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | *(required)* | MongoDB connection string |
| `MONGO_DB` | `countly_drill` | Source database |
| `MONGO_COLLECTION_PREFIX` | `drill_events` | Prefix to discover collections |
| `MONGO_READ_PREFERENCE` | `primary` | Read preference |
| `MONGO_READ_CONCERN` | `majority` | Read concern level |
| `MONGO_RETRY_READS` | `true` | Enable retry reads |
| `MONGO_APP_NAME` | *(optional)* | Connection app name |
| `MONGO_BATCH_ROWS_TARGET` | `10000` | Target docs per batch |
| `MONGO_CURSOR_BATCH_SIZE` | `2000` | MongoDB cursor batch size |
| `MONGO_MAX_TIME_MS` | `120000` | Cursor timeout (ms) |

### Transform

| Variable | Default | Description |
|----------|---------|-------------|
| `TRANSFORM_VERSION` | `v1` | Data transform version tag |

### ClickHouse Target

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_URL` | *(required)* | ClickHouse HTTP endpoint |
| `CLICKHOUSE_DB` | `countly_drill` | Target database |
| `CLICKHOUSE_TABLE` | `drill_events` | Target table |
| `CLICKHOUSE_USERNAME` | `default` | Username |
| `CLICKHOUSE_PASSWORD` | *(empty)* | Password |
| `CLICKHOUSE_COMPRESSION` | `lz4` | Compression codec |
| `CLICKHOUSE_QUERY_TIMEOUT_MS` | `120000` | Query timeout (ms) |
| `CLICKHOUSE_MAX_RETRIES` | `8` | Max insert retry attempts |
| `CLICKHOUSE_RETRY_BASE_DELAY_MS` | `1000` | Backoff base delay (ms) |
| `CLICKHOUSE_RETRY_MAX_DELAY_MS` | `30000` | Backoff max delay (ms) |
| `CLICKHOUSE_USE_DEDUP_TOKEN` | `true` | Enable insert dedup tokens |

### Backpressure

| Variable | Default | Description |
|----------|---------|-------------|
| `BACKPRESSURE_ENABLED` | `true` | Enable CH parts monitoring |
| `BACKPRESSURE_PARTS_TO_THROW_INSERT` | `300` | Parts threshold to pause |
| `BACKPRESSURE_MAX_PARTS_IN_TOTAL` | `500` | Max total parts allowed |
| `BACKPRESSURE_PARTITION_PCT_HIGH` | `0.50` | Partition high watermark |
| `BACKPRESSURE_PARTITION_PCT_LOW` | `0.35` | Partition low watermark |
| `BACKPRESSURE_TOTAL_PCT_HIGH` | `0.50` | Total high watermark |
| `BACKPRESSURE_TOTAL_PCT_LOW` | `0.40` | Total low watermark |
| `BACKPRESSURE_POLL_INTERVAL_MS` | `15000` | Pressure polling interval (ms) |
| `BACKPRESSURE_MAX_PAUSE_EPISODE_MS` | `180000` | Max pause before force resume (ms) |

### State

| Variable | Default | Description |
|----------|---------|-------------|
| `MANIFEST_DB` | `countly_drill` | MongoDB database for run manifests |
| `REDIS_URL` | *(required)* | Redis connection URL |
| `REDIS_KEY_PREFIX` | `mig` | Redis key namespace |

### Memory / GC

| Variable | Default | Description |
|----------|---------|-------------|
| `GC_ENABLED` | `true` | Enable manual GC |
| `GC_RSS_SOFT_LIMIT_MB` | `1536` | RSS soft limit to trigger GC |
| `GC_RSS_HARD_LIMIT_MB` | `2048` | RSS hard limit warning |
| `GC_HEAP_USED_RATIO` | `0.70` | Heap usage ratio trigger |
| `GC_EVERY_N_BATCHES` | `10` | Run GC every N batches |

## Multi-Collection Migration

The service automatically discovers all MongoDB collections matching `MONGO_COLLECTION_PREFIX*` (e.g. `drill_events`, `drill_events5a2b3c4d...`).

- Collections are processed **sequentially** in alphabetical order
- Each collection gets its own run ID and isolated Redis key prefix
- Already-completed collections are **skipped** on restart
- Missing `{cd: 1, _id: 1}` compound index is created automatically
- If a collection fails, the service logs the error and continues to the next

## API Endpoints

### Health

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/healthz` | Liveness probe (always 200 if server is up) |
| `GET` | `/readyz` | Readiness check (mongo, clickhouse, redis, manifest, runner) |

### Stats

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/stats` | Full dashboard: service status, orchestrator progress, throughput, integrity, connections, memory, GC |

### Control

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/control/pause` | Pause after current batch |
| `POST` | `/control/resume` | Resume from pause |
| `POST` | `/control/stop-after-batch` | Stop cleanly after current batch |
| `POST` | `/control/gc` | Trigger manual GC (body: `{"mode":"now"}`) |

### Runs

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/runs` | List all runs (`?status=`, `?limit=`, `?offset=`) |
| `GET` | `/runs/current` | Current active run with coverage |
| `GET` | `/runs/:id` | Run details by ID |
| `GET` | `/runs/:id/batches` | Batch list (`?status=`, `?limit=`) |
| `GET` | `/runs/:id/failures` | Failed batches, retry errors, digest mismatches |
| `GET` | `/runs/:id/timeline` | Performance snapshots over time |
| `GET` | `/runs/:id/coverage` | Document range coverage and completion % |
| `DELETE` | `/runs/:id/cache` | Clean up Redis cache for a run |

## Operations

### Pause and Resume

```bash
curl -X POST http://localhost:8080/control/pause
# Migration pauses after current batch completes
curl -X POST http://localhost:8080/control/resume
# Migration continues from where it left off
```

### Graceful Stop

```bash
curl -X POST http://localhost:8080/control/stop-after-batch
```

Or send `SIGTERM` / `SIGINT` to the process. The service will:
1. Finish the current batch
2. Persist state to manifest
3. Close all connections
4. Exit cleanly

### Crash Recovery

On restart after a crash (e.g. `kill -9`), the service automatically:
1. Finds the interrupted run in the manifest
2. Replays any inflight batches with SHA-256 digest verification
3. Resumes from the last committed cursor
4. ClickHouse dedup tokens (`mig:{runId}:{batchSeq}`) prevent duplicate inserts

### Monitor Progress

```bash
curl -s http://localhost:8080/stats | jq '.orchestrator'
```

```json
{
  "totalCollections": 8,
  "completedCollections": 5,
  "failedCollections": 0,
  "skippedCollections": 0,
  "currentCollection": "drill_events7c8d9e0f...",
  "collections": ["drill_events", "drill_events5a2b...", "..."]
}
```

## Architecture

```
MongoDB (drill_events*)
    │
    │  cursor pagination on (cd, _id) compound index
    ▼
┌──────────────────────┐
│  CollectionOrchestrator  │  discovers collections, processes sequentially
│  └─ BatchRunner      │  reads → transforms → inserts per batch
│     └─ RetryPolicy   │  exponential backoff (1s base, 30s cap, 8 attempts)
└──────────────────────┘
    │                          │
    │  batch inserts           │  state tracking
    │  (LZ4 + dedup tokens)   │
    ▼                          ▼
ClickHouse              MongoDB manifest (authoritative)
(drill_events table)    + Redis hot state (rebuildable)
```

- **Manifest (MongoDB)**: Authoritative state — runs, batches, cursors, digests, error history
- **Redis**: Hot state — bitmap tracking, timeline snapshots, recent errors, command flags
- **Backpressure**: Monitors ClickHouse active parts count; pauses inserts when thresholds exceeded
- **Dedup tokens**: Each batch insert carries `mig:{runId}:{batchSeq}` for idempotent retries
