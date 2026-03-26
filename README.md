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

Required env vars: `SERVICE_NAME`, `MONGO_URI`, `CLICKHOUSE_URL`, `REDIS_URL`.

## Configuration

Copy `.env.example` and adjust. All values below show defaults where applicable.

### Service

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVICE_NAME` | *(required)* | Service identifier |
| `SERVICE_PORT` | `8080` | HTTP server port |
| `SERVICE_HOST` | `0.0.0.0` | HTTP server bind address |
| `GRACEFUL_SHUTDOWN_TIMEOUT_MS` | `60000` | Max wait for graceful shutdown (ms) |
| `RERUN_MODE` | `resume` | `resume`, `new-run`, or `clone-run` |
| `LOG_LEVEL` | `info` | `fatal`, `error`, `warn`, `info`, `debug`, `trace` |

### MongoDB Source

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | *(required)* | MongoDB connection string |
| `MONGO_DB` | `countly_drill` | Source database |
| `MONGO_COUNTLY_DB` | `countly` | Countly database (for hash resolution) |
| `MONGO_COLLECTION_PREFIX` | `drill_events` | Prefix to discover collections |
| `MONGO_READ_PREFERENCE` | `primary` | Read preference |
| `MONGO_READ_CONCERN` | `majority` | Read concern level |
| `MONGO_RETRY_READS` | `true` | Enable retry reads |
| `MONGO_APP_NAME` | *(optional)* | Connection app name |
| `MONGO_BATCH_ROWS_TARGET` | `10000` | Target docs per ClickHouse write batch |
| `MONGO_PAGE_SIZE` | `10000` | MongoDB page size per cursor read |
| `MONGO_CURSOR_BATCH_SIZE` | `10000` | MongoDB cursor batch size |
| `MONGO_MAX_TIME_MS` | `600000` | Cursor timeout (ms) |

### Range-Parallel Processing

| Variable | Default | Description |
|----------|---------|-------------|
| `RANGE_PARALLEL_THRESHOLD` | `500000` | Estimated doc count to trigger range splitting |
| `RANGE_COUNT` | `100` | Number of time-ranges to split a collection into |
| `RANGE_LEASE_TTL_SEC` | `300` | Range lease TTL for dead-pod reclaim (s) |

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
| `CLICKHOUSE_QUERY_TIMEOUT_MS` | `120000` | Query timeout (ms) |
| `CLICKHOUSE_MAX_RETRIES` | `8` | Max insert retry attempts |
| `CLICKHOUSE_RETRY_BASE_DELAY_MS` | `1000` | Backoff base delay (ms) |
| `CLICKHOUSE_RETRY_MAX_DELAY_MS` | `30000` | Backoff max delay (ms) |
| `CLICKHOUSE_USE_DEDUP_TOKEN` | `true` | Enable insert dedup tokens |

### Backpressure

| Variable | Default | Description |
|----------|---------|-------------|
| `BACKPRESSURE_ENABLED` | `true` | Enable ClickHouse parts monitoring |
| `BACKPRESSURE_PARTS_TO_THROW_INSERT` | `300` | Parts threshold to pause inserts |
| `BACKPRESSURE_MAX_PARTS_IN_TOTAL` | `500` | Max total parts allowed |
| `BACKPRESSURE_PARTITION_PCT_HIGH` | `0.70` | Partition high watermark |
| `BACKPRESSURE_PARTITION_PCT_LOW` | `0.55` | Partition low watermark |
| `BACKPRESSURE_TOTAL_PCT_HIGH` | `0.70` | Total high watermark |
| `BACKPRESSURE_TOTAL_PCT_LOW` | `0.55` | Total low watermark |
| `BACKPRESSURE_POLL_INTERVAL_MS` | `5000` | Pressure polling interval (ms) |
| `BACKPRESSURE_MAX_PAUSE_EPISODE_MS` | `180000` | Max pause before force resume (ms) |

### State

| Variable | Default | Description |
|----------|---------|-------------|
| `MANIFEST_DB` | `countly_drill` | MongoDB database for run manifests |
| `REDIS_URL` | *(required)* | Redis connection URL |
| `REDIS_KEY_PREFIX` | `mig` | Redis key namespace |
| `TIMELINE_SNAPSHOT_INTERVAL` | `10` | Timeline snapshot every N batches |

### Memory / GC

| Variable | Default | Description |
|----------|---------|-------------|
| `GC_ENABLED` | `true` | Enable manual GC |
| `GC_RSS_SOFT_LIMIT_MB` | `3072` | RSS soft limit to trigger GC |
| `GC_RSS_HARD_LIMIT_MB` | `6144` | RSS hard limit warning |
| `GC_HEAP_USED_RATIO` | `0.70` | Heap usage ratio trigger |
| `GC_EVERY_N_BATCHES` | `50` | Run GC every N batches |

### Worker / Multi-Pod

| Variable | Default | Description |
|----------|---------|-------------|
| `POD_ID` | `hostname()` | Unique pod identifier |
| `MULTI_POD_ENABLED` | `true` | Enable distributed locking |
| `LOCK_TTL_SECONDS` | `300` | Collection lock TTL — crash safety net (s) |
| `LOCK_RENEW_MS` | `60000` | Lock renewal interval (ms) |
| `PROGRESS_UPDATE_MS` | `5000` | Progress reporting interval to Redis (ms) |
| `POD_HEARTBEAT_MS` | `30000` | Pod heartbeat interval (ms) |
| `POD_DEAD_AFTER_SEC` | `180` | Pod considered dead after this silence (s) |

### Async Write

| Variable | Default | Description |
|----------|---------|-------------|
| `ASYNC_WRITE_FLUSH_INTERVAL_MS` | `5000` | Flush queued batch records to MongoDB every N ms |
| `ASYNC_WRITE_FLUSH_BATCH_SIZE` | `10` | Flush after N batch records queued |

## Multi-Collection Migration

The service automatically discovers all MongoDB collections matching `MONGO_COLLECTION_PREFIX*` (e.g. `drill_events`, `drill_events5a2b3c4d...`).

- Collections are processed in priority order (base collection first, then alphabetical)
- Each collection gets its own run ID and isolated Redis key prefix
- Already-completed collections are skipped on restart
- Missing `{cd: 1, _id: 1}` compound index is created automatically in background
- If a collection fails, the service logs the error and continues to the next

## Multi-Pod Mode

Multiple pods can process collections in parallel using Redis-based distributed locking. Each pod autonomously discovers collections, acquires locks, and processes work.

### How It Works

1. **Pod heartbeat**: Each pod writes a TTL-based heartbeat key (`mig:pod:{podId}`) to Redis every 30s
2. **Collection locking**: Before processing a collection, a pod acquires a lock via atomic Lua script. If the lock is held by another live pod, it moves to the next collection
3. **Dead pod detection**: If a pod's heartbeat expires (180s default), other pods can steal its locks
4. **Global progress**: Each pod reports per-collection progress to Redis. The `/stats` endpoint and dashboard merge data from all pods

### Range-Parallel Processing

Collections exceeding `RANGE_PARALLEL_THRESHOLD` (default 500K docs) are split into `RANGE_COUNT` equal time-ranges based on `[min(cd), max(cd)]`. Multiple pods can process different ranges of the same collection concurrently.

- **Range initialization**: First pod to reach a large collection divides the time span into N ranges via Redis SETNX
- **Atomic claiming**: Pods claim ranges via a Redis Lua script that atomically marks a pending range as processing
- **Exclusive boundaries**: Ranges use `[start, end)` (exclusive upper bound) to prevent duplicate processing. The final range uses `[start, max]`
- **Stale reclaim**: If a pod dies mid-range, other pods reclaim its ranges after the lease TTL expires
- **Batch sequence isolation**: Each range gets 10,000 batch sequence slots to prevent collisions

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: migration
spec:
  replicas: 3
  selector:
    matchLabels:
      app: migration
  template:
    metadata:
      labels:
        app: migration
    spec:
      terminationGracePeriodSeconds: 120
      containers:
        - name: migration
          image: countly-migration:latest
          ports:
            - containerPort: 8080
          env:
            - name: POD_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8080
            initialDelaySeconds: 10
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8080
            initialDelaySeconds: 15
          lifecycle:
            preStop:
              httpGet:
                path: /control/drain
                port: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: migration
spec:
  selector:
    app: migration
  ports:
    - port: 8080
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: migration
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: migration
  minReplicas: 1
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

K8s lifecycle:
- **Scale up**: New pods start, discover collections, acquire locks on unclaimed work
- **Scale down**: `preStop` hook calls `/control/drain`, pod finishes current batch, releases locks, exits
- **Pod crash**: SIGTERM triggers graceful shutdown. On `kill -9`, heartbeat expires after 180s, locks are stolen by live pods

## Dashboard

The `/viz` endpoint serves a self-contained real-time HTML dashboard. No external dependencies.

**Features:**
- Overall migration progress bar with ETA
- Current collection progress and batch sequence
- Per-pod progress bars with docs/rows/throughput stats
- Live batches panel showing batch phase (READING, TRANSFORMING, WRITING, COMMITTING)
- Range heatmap for range-parallel collections
- Active locks table with release buttons
- Stale pod detection with remove action
- Global cluster controls (pause/resume/stop all pods)
- Index build status
- Skip reason breakdown
- Memory and GC metrics
- Infrastructure connection status

## API Endpoints

### Health

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/healthz` | Liveness probe (always 200) |
| `GET` | `/readyz` | Readiness check (mongo, clickhouse, redis, manifest, runner) |

### Stats

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/stats` | Full dashboard JSON: progress, throughput, integrity, cluster, live batches |
| `GET` | `/viz` | Real-time HTML dashboard |

### Control (Single Pod)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/control/pause` | Pause after current batch |
| `POST` | `/control/resume` | Resume from pause |
| `POST` | `/control/stop-after-batch` | Stop cleanly after current batch |
| `POST` | `/control/gc` | Trigger manual GC (body: `{"mode":"now"}`) |
| `POST` | `/control/reindex/:collection` | Trigger index creation for a collection |
| `POST` | `/control/retry-collection/:collection` | Re-queue a failed/skipped collection |
| `POST` | `/control/retry-batch/:runId/:batchSeq` | Retry a specific failed/skipped batch |
| `POST` | `/control/retry-skipped-batches/:runId` | Retry all skipped_empty batches in a run |

### Control (Multi-Pod)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/control/global/pause` | Pause all pods |
| `POST` | `/control/global/resume` | Resume all pods |
| `POST` | `/control/global/stop` | Stop all pods after current batch |
| `GET` | `/control/locks` | List all active collection locks |
| `POST` | `/control/locks/release/:collection` | Force-release a lock (admin) |
| `GET` | `/control/pods` | List all pods with status and locks held |
| `POST` | `/control/pods/remove/:podId` | Remove dead pod and release its locks |
| `POST` | `/control/drain` | Graceful drain for K8s scale-down |

### Runs

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/runs` | List all runs (`?status=`, `?limit=`, `?offset=`) |
| `GET` | `/runs/current` | Current active run with coverage |
| `GET` | `/runs/current/batches` | Batches for current run (`?status=`, `?limit=`) |
| `GET` | `/runs/:id` | Run details by ID |
| `GET` | `/runs/:id/batches` | Batch list (`?status=`, `?limit=`) |
| `GET` | `/runs/:id/failures` | Failed batches, retry errors, digest mismatches |
| `GET` | `/runs/:id/timeline` | Performance snapshots over time |
| `GET` | `/runs/:id/coverage` | Document range coverage and completion % |
| `DELETE` | `/runs/:id/cache` | Clean up Redis cache for a run |

## Operations

### Pause and Resume

```bash
# Single pod
curl -X POST http://localhost:8080/control/pause
curl -X POST http://localhost:8080/control/resume

# All pods (multi-pod mode)
curl -X POST http://localhost:8080/control/global/pause
curl -X POST http://localhost:8080/control/global/resume
```

### Graceful Stop

```bash
curl -X POST http://localhost:8080/control/stop-after-batch
```

Or send `SIGTERM` / `SIGINT`. The service will:
1. Finish the current batch
2. Flush async write queue to MongoDB
3. Release all collection locks
4. Close all connections
5. Exit cleanly

### Lock Management

```bash
# List all active locks
curl http://localhost:8080/control/locks

# Force-release a stuck lock
curl -X POST http://localhost:8080/control/locks/release/drill_events5a2b...

# List pods and their status
curl http://localhost:8080/control/pods

# Remove a dead pod and release its locks
curl -X POST http://localhost:8080/control/pods/remove/pod-3
```

### Retry Failed Work

```bash
# Re-queue a failed collection
curl -X POST http://localhost:8080/control/retry-collection/drill_events5a2b...

# Retry a specific failed batch
curl -X POST http://localhost:8080/control/retry-batch/{runId}/{batchSeq}

# Retry all skipped batches in a run
curl -X POST http://localhost:8080/control/retry-skipped-batches/{runId}
```

### Crash Recovery

On restart after a crash, the service automatically:
1. Checks Redis for the last committed cursor (hot-path authority)
2. Falls back to MongoDB manifest if Redis is empty
3. Replays any inflight batches with digest verification
4. Resumes from the last committed position
5. ClickHouse dedup tokens (`mig:{runId}:{batchSeq}`) prevent duplicate inserts

### Monitor Progress

```bash
# JSON stats
curl -s http://localhost:8080/stats | jq '.summary'

# Cluster overview
curl -s http://localhost:8080/stats | jq '.cluster.pods'

# Or use the dashboard
open http://localhost:8080/viz
```

## Architecture

```
MongoDB (drill_events*)
    |
    |  cursor pagination on (cd, _id) compound index
    v
+------------------------------------------------------+
|  CollectionOrchestrator                              |
|  |-- discovers collections, index-aware scheduling   |
|  |-- multi-pod: Redis lock per collection            |
|  |                                                    |
|  +-- Small collections (< 500K docs):               |
|  |   BatchRunner --> ClickHouse                      |
|  |                                                    |
|  +-- Large collections (>= 500K docs):               |
|      RangeCoordinator                                |
|      |-- splits [min(cd), max(cd)] into N ranges     |
|      |-- atomic range claiming via Redis Lua script   |
|      +-- per-range BatchRunner --> ClickHouse         |
+------------------------------------------------------+
    |                    |                    |
    |  batch inserts     |  hot-path commit   |  async flush
    |  (dedup tokens)    |                    |
    v                    v                    v
 ClickHouse          Redis                MongoDB manifest
 (drill_events)      (cursor, bitmap,     (batch records,
                      live phases,         run metadata,
                      range stats,         audit trail)
                      pod heartbeats,
                      collection locks)
```

- **Redis (hot-path authority)**: Committed cursor, batch completion bitmap, live batch phases, range stats, pod heartbeats, collection locks, global commands
- **MongoDB manifest (async, authoritative for audit)**: Batch records flushed asynchronously every 5s or 10 batches. Run metadata, skip samples, error history
- **Async batch writer**: Redis is the commit point for each batch. MongoDB writes are queued and bulk-flushed in the background. On graceful shutdown, the queue is drained
- **Backpressure**: Monitors ClickHouse active parts count; pauses inserts when thresholds exceeded (hysteresis with high/low watermarks)
- **Dedup tokens**: Each batch carries `mig:{runId}:{batchSeq}` for idempotent retries

## Redis Key Schema

| Key Pattern | Type | TTL | Description |
|-------------|------|-----|-------------|
| `mig:active_run` | STRING | - | Current active run ID |
| `mig:run:{runId}:state` | STRING | - | Run state JSON blob |
| `mig:run:{runId}:cursor` | STRING | - | Last committed cursor (hot-path authority) |
| `mig:run:{runId}:done_bitmap` | STRING | - | Batch completion bitmap (SETBIT/GETBIT) |
| `mig:run:{runId}:stats:latest` | STRING | - | Latest run stats snapshot |
| `mig:run:{runId}:commands` | HASH | - | Operator command flags (pause, abort, etc.) |
| `mig:run:{runId}:recent_errors` | LIST | - | Last 100 batch errors |
| `mig:run:{runId}:timeline` | LIST | - | Last 1000 timeline snapshots |
| `mig:run:{runId}:batch:{seq}:errors` | LIST | - | Verbose errors per batch (last 20) |
| `mig:liveBatch:{collection}` | STRING | 30s | Live batch phase data (heartbeat-refreshed) |
| `mig:rangeLive:{collection}:{idx}` | STRING | 60s | Per-range live stats (heartbeat-refreshed) |
| `mig:lock:{collection}` | STRING | 300s | Collection lock with pod owner |
| `mig:pod:{podId}` | STRING | 180s | Pod heartbeat/liveness key |
| `mig:progress:{collection}` | STRING | 300s | Per-collection progress for cluster aggregation |
| `mig:cmd:global` | HASH | - | Global commands (pause, stop) |
| `mig:ranges:{collection}` | HASH | - | Range entries (idx, status, podId, claimedAt) |
| `mig:ranges:{collection}:init` | STRING | 60s | SETNX coordinator election for range init |
| `mig:ranges:{collection}:runId` | STRING | - | Shared run ID for range-parallel processing |
| `mig:ranges:{collection}:meta` | STRING | - | Range metadata (minCd, maxCd, count) |
| `mig:ranges:{collection}:finalized` | STRING | 60s | SETNX for run finalization (one pod only) |

## Project Structure

```
src/
  main.ts                              # Entry point
  config/
    schema.ts                          # Zod config schema with defaults
    loader.ts                          # Env var loader and validator
  http/
    health-route.ts                    # /healthz, /readyz
    stats-route.ts                     # /stats (JSON dashboard)
    control-route.ts                   # /control/* (pause, resume, locks, pods)
    run-route.ts                       # /runs/* (history, batches, coverage)
    viz-route.ts                       # /viz (HTML dashboard)
  source/
    mongo-reader.ts                    # Cursor pagination on (cd, _id)
    discover-collections.ts            # Collection discovery
  target/
    clickhouse-writer.ts               # Batch inserts with dedup
    clickhouse-pressure.ts             # Backpressure monitoring
  transform/
    normalize.ts                       # Document normalization
    skip-reasons.ts                    # Skip tracking
    validators.ts                      # Data validation
    hash-resolver.ts                   # Collection hash defaults
  runtime/
    collection-orchestrator.ts         # Multi-collection + multi-pod orchestration
    batch-runner.ts                    # Core batch lifecycle (read, transform, write)
    range-coordinator.ts               # Range-parallel processing
    retry-policy.ts                    # Exponential backoff
    gc-controller.ts                   # Manual GC management
    process-metrics.ts                 # Memory/CPU metrics
    resolve-run.ts                     # Run resolution on startup
  state/
    manifest-store.ts                  # MongoDB manifest (authoritative audit trail)
    redis-hot-state.ts                 # Redis hot state (bitmap, timeline, phases)
    async-batch-writer.ts              # Async MongoDB write queue
    global-progress.ts                 # Multi-pod progress aggregation
    collection-lock.ts                 # Distributed collection locking
    coverage.ts                        # Coverage interval tracking
  types/
    cursor.ts                          # Cursor serialization
```

## CI/CD

A GitHub Actions workflow automatically builds and pushes the Docker image to Docker Hub when a GitHub Release is published.

### Tag Behavior

| Release Tag | Docker Tags | `latest`? |
|-------------|-------------|-----------|
| `v1.0.0` | `1.0.0`, `1.0`, `1`, `latest` | Yes |
| `v2.3.1` | `2.3.1`, `2.3`, `2`, `latest` | Yes |
| `v1.0.0-rc.1` | `1.0.0-rc.1` | No |
| `v2.0.0-beta` | `2.0.0-beta` | No |

### Setting Up Docker Hub Credentials

1. Go to [Docker Hub](https://hub.docker.com) > Account Settings > Security > New Access Token
2. Add `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` as GitHub repo secrets
3. Create a release (e.g. tag `v1.0.0`) and the workflow builds and pushes

### Using a Pre-Built Image

```yaml
services:
  migration:
    image: <your-username>/countly-migration:1.0.0
    # ... rest of config unchanged
```
