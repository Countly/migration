# Multi-Pod Migration Guide

Run N parallel pods to migrate MongoDB collections concurrently. Each pod claims different collections via Redis distributed locks. For large collections (500K+ docs), multiple pods share work via range-parallel processing. No central coordinator — pods are autonomous and self-organizing.

## Architecture

```
                     +-----------+
                     |   Redis   |  locks, heartbeats, progress, ranges,
                     |           |  live batch phases, async cursor
                     +-----+-----+
                           |
          +----------------+----------------+
          |                |                |
     +----+----+      +---+-----+     +----+----+
     |  Pod 1  |      |  Pod 2  |     |  Pod 3  |
     | port 80 |      | port 80 |     | port 80 |
     +---------+      +---------+     +---------+
          |                |  \             |
     Small colls      Small colls   \   Small colls
     (locked,         (locked,       \  (locked,
      sequential)      sequential)    \ sequential)
          |                |           \
          |                +-----+------+
          |                      |
          |              Large collection
          |              (range-parallel)
          |              Ranges split across
          |              pods via Lua atomics
          |                      |
     +----+----+          +------+------+
     | MongoDB |          |  ClickHouse |
     | (reads) |          |  (writes)   |
     +---------+          +-------------+
          |
   +------+------+
   |  Manifest   |
   |  (MongoDB)  |  <-- async flush from Redis
   +-------------+
```

Each pod:
1. Discovers all collections (same deterministic list)
2. Checks estimated document count for each collection
3. **Small collections** (< threshold): acquires Redis lock, processes exclusively
4. **Large collections** (>= threshold): enters range-parallel mode, claims ranges atomically via Redis Lua script — no collection lock needed
5. Explicitly releases locks on completion
6. Picks up the next available work

## Range-Parallel Processing

Collections exceeding `RANGE_PARALLEL_THRESHOLD` (default 500K docs) are split into `RANGE_COUNT` time-ranges based on `[min(cd), max(cd)]`. Multiple pods process different ranges concurrently.

### How It Works

1. **Range initialization**: First pod reaching a large collection becomes the coordinator (via SETNX). It queries min/max `cd`, divides the time span into N equal ranges, and writes them to Redis
2. **Atomic claiming**: Each pod calls a Redis Lua script that atomically:
   - Reclaims stale ranges from dead pods (checks heartbeat)
   - Claims the first pending range
3. **Processing**: Each claimed range gets its own BatchRunner with cursor bounds `[startCd, endCd)`
4. **Exclusive boundaries**: Ranges use `[start, end)` (non-final) and `[start, max]` (final range) — zero duplicates
5. **Batch sequence isolation**: Range N uses batch sequences `N*10000` to `(N+1)*10000-1`, preventing collisions
6. **Run finalization**: The last pod to see all ranges terminal finalizes the run status (via SETNX)

### Example

With 3 pods and a 100M-doc collection split into 100 ranges:

```
Pod 1: claims ranges 0,3,6,9,12,...  (33 ranges)
Pod 2: claims ranges 1,4,7,10,13,... (33 ranges)
Pod 3: claims ranges 2,5,8,11,14,... (34 ranges)
```

Each pod processes its claimed ranges sequentially. If Pod 2 dies, Pods 1 and 3 reclaim its stale ranges after the lease TTL expires.

## Async Batch Writer

Batch completions use Redis as the hot-path commit point instead of blocking on MongoDB:

1. On batch completion: write cursor + bitmap to Redis (sync, ~1ms)
2. Queue MongoDB batch record for async flush
3. Background loop flushes every 5s or 10 batches (configurable)
4. Queue is bounded (default 1000) to prevent memory growth on MongoDB outages

This removes MongoDB from the critical path. Crash recovery checks Redis cursor first, falls back to MongoDB.

## Live Batch Phase Tracking

Each active batch reports its current phase to Redis with a 30s TTL:

- **READING** — fetching pages from MongoDB
- **TRANSFORMING** — normalizing documents
- **WRITING** — inserting into ClickHouse (may take minutes for large batches)
- **COMMITTING** — writing cursor to Redis

A 10-second heartbeat refreshes the TTL during long ClickHouse writes, so batches never disappear from the dashboard mid-write.

## Configuration

### Collection Locking

| Env Var | Default | Description |
|---------|---------|-------------|
| `MULTI_POD_ENABLED` | `true` | Enable distributed locking and cluster coordination |
| `POD_ID` | `os.hostname()` | Unique pod identifier. In K8s, use `metadata.name` |
| `LOCK_TTL_SECONDS` | `300` | Lock TTL (crash safety net). Normal path uses explicit release |
| `LOCK_RENEW_MS` | `60000` | Lock TTL renewal interval |
| `PROGRESS_UPDATE_MS` | `5000` | Progress reporting interval to Redis |
| `POD_HEARTBEAT_MS` | `30000` | Pod liveness heartbeat interval |
| `POD_DEAD_AFTER_SEC` | `180` | Pod considered dead after this silence |

### Range-Parallel

| Env Var | Default | Description |
|---------|---------|-------------|
| `RANGE_PARALLEL_THRESHOLD` | `500000` | Doc count to trigger range splitting |
| `RANGE_COUNT` | `100` | Number of time-ranges to split into |
| `RANGE_LEASE_TTL_SEC` | `300` | Range lease TTL for dead-pod reclaim |

### Async Write

| Env Var | Default | Description |
|---------|---------|-------------|
| `ASYNC_WRITE_FLUSH_INTERVAL_MS` | `5000` | Flush batch records to MongoDB every N ms |
| `ASYNC_WRITE_FLUSH_BATCH_SIZE` | `10` | Flush after N records queued |

All other env vars (MongoDB, ClickHouse, Redis, backpressure, etc.) are shared across pods.

## Kubernetes Deployment

### Deployment

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
          image: your-registry/migration:latest
          ports:
            - containerPort: 8080
          env:
            - name: POD_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: MONGO_URI
              value: "mongodb://mongo-svc:27017/?directConnection=true"
            - name: CLICKHOUSE_URL
              value: "http://clickhouse-svc:8123"
            - name: REDIS_URL
              value: "redis://redis-svc:6379"
            # ... other env vars from .env
          resources:
            requests:
              memory: "2Gi"
              cpu: "500m"
            limits:
              memory: "6Gi"
              cpu: "2000m"
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8080
            initialDelaySeconds: 15
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8080
            initialDelaySeconds: 10
            periodSeconds: 10
          lifecycle:
            preStop:
              httpGet:
                path: /control/drain
                port: 8080
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: migration-svc
spec:
  selector:
    app: migration
  ports:
    - port: 8080
      targetPort: 8080
```

### HorizontalPodAutoscaler (optional)

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: migration-hpa
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

## K8s Lifecycle

### Scale Up
New pods start, discover collections, and acquire locks on unclaimed work. Large collections automatically share ranges with existing pods. No coordination needed — just increase `replicas`.

### Scale Down
K8s sends `preStop` hook (`/control/drain`) then `SIGTERM`:
1. `preStop` triggers `stopAfterBatch()` — pod finishes current batch
2. `SIGTERM` triggers graceful shutdown:
   - Flushes async write queue to MongoDB
   - Stops lock heartbeat
   - Releases all held locks
   - Closes connections
3. Released locks and unclaimed ranges are immediately available for other pods

`terminationGracePeriodSeconds: 120` gives the pod 2 minutes to finish its current batch before K8s sends SIGKILL.

### Pod Crash (SIGKILL / OOM)
If a pod is killed without graceful shutdown:
1. Lock heartbeat stops
2. Pod heartbeat key expires after 3 minutes (default)
3. Lock TTL expires after 5 minutes (default)
4. Other pods detect the dead pod and steal its locks
5. Range-parallel: stale ranges are reclaimed by the next pod that runs the claim Lua script
6. Dashboard shows the pod as "stale" with a "Remove Pod" button

For faster recovery:
```bash
curl -X POST http://migration-svc:8080/control/pods/remove/crashed-pod-name
```

## Lock Management API

### List Locks
```bash
GET /control/locks
# { "ok": true, "locks": [{ "collectionName": "...", "podId": "...", "acquiredAt": "..." }] }
```

### Force-Release a Lock
```bash
POST /control/locks/release/:collectionName
# { "ok": true, "collection": "...", "message": "Lock force-released" }
```

### List Pods
```bash
GET /control/pods
# { "ok": true, "pods": [{ "podId": "...", "alive": true, "locks": [...], "lockCount": 2 }] }
```

### Remove Dead Pod
```bash
POST /control/pods/remove/:podId
# { "ok": true, "podId": "...", "releasedLocks": ["coll1", "coll2"], "message": "..." }
```

### Drain (Scale-Down)
```bash
POST /control/drain
# { "ok": true, "message": "Drain initiated..." }
```

## Global Control

These commands affect ALL pods in the cluster:

```bash
POST /control/global/pause    # Pause all pods
POST /control/global/resume   # Resume all pods
POST /control/global/stop     # Stop all pods after current batch
```

## Dashboard

The `/viz` dashboard on any pod shows the complete cluster picture:

- **Cluster progress bar**: Aggregated completion across all pods
- **Per-pod progress bars**: Each pod shows its own progress with docs/rows/throughput
- **Live Batches panel**: Active batches with phase tags (READING/WRITING/COMMITTING), elapsed time, docs/rows
- **Active Ranges panel**: Range heatmap for range-parallel collections (green=done, blue=processing, gray=pending)
- **Active Locks table**: All locks with pod owner, acquired time, and "Release" button
- **Stale Pods section**: Dead pods with orphaned locks and "Remove Pod" button
- **Collections table**: All collections with assigned pod, status, progress, and retry actions
- **Global controls**: Pause/Resume/Stop buttons for the entire cluster

## Redis Key Schema

| Key Pattern | Type | TTL | Description |
|-------------|------|-----|-------------|
| `mig:lock:{collection}` | STRING | 300s | Collection lock (podId + acquiredAt) |
| `mig:pod:{podId}` | STRING | 180s | Pod heartbeat/liveness |
| `mig:progress:{collection}` | STRING | 300s | Per-collection progress for cluster view |
| `mig:cmd:global` | HASH | - | Global commands (pause, stop) |
| `mig:liveBatch:{collection}` | STRING | 30s | Live batch phase (heartbeat-refreshed) |
| `mig:rangeLive:{collection}:{idx}` | STRING | 60s | Per-range live stats (heartbeat-refreshed) |
| `mig:ranges:{collection}` | HASH | - | Range entries (idx, status, podId, claimedAt) |
| `mig:ranges:{collection}:init` | STRING | 60s | SETNX coordinator election |
| `mig:ranges:{collection}:runId` | STRING | - | Shared run ID for range-parallel |
| `mig:ranges:{collection}:meta` | STRING | - | Range metadata (minCd, maxCd, count) |
| `mig:ranges:{collection}:finalized` | STRING | 60s | SETNX run finalization (one pod) |
| `mig:run:{runId}:cursor` | STRING | - | Last committed cursor (hot-path authority) |
| `mig:run:{runId}:state` | STRING | - | Run state blob |
| `mig:run:{runId}:done_bitmap` | STRING | - | Batch completion bitmap |
| `mig:run:{runId}:stats:latest` | STRING | - | Latest stats snapshot |

## Troubleshooting

### Pod stuck with lock after crash
Use the dashboard "Remove Pod" button or:
```bash
curl -X POST http://migration-svc:8080/control/pods/remove/<dead-pod-id>
```

### Collections not being picked up
Check if all collections are locked:
```bash
curl http://migration-svc:8080/control/locks
```
Force-release specific locks:
```bash
curl -X POST http://migration-svc:8080/control/locks/release/<collection-name>
```

### Large collection only processed by one pod
If the collection exceeds `RANGE_PARALLEL_THRESHOLD`, multiple pods should share ranges. Check:
1. Is the estimated doc count above the threshold? (Check `/stats` for `estimatedCounts`)
2. Are other pods available? (`GET /control/pods`)
3. Did the first pod finish all ranges before others started? (Expected with small data or few ranges)

Increase `RANGE_COUNT` for more granular splitting, or reduce `RANGE_PARALLEL_THRESHOLD` for testing.

### Two pods processing the same small collection
This should not happen — collection locks prevent it. If it does (Redis split-brain), ClickHouse dedup tokens prevent data corruption. Check Redis connectivity.

### Ranges stuck in "processing" after pod crash
Ranges have a lease TTL (`RANGE_LEASE_TTL_SEC`, default 300s). After the TTL expires AND the pod's heartbeat is gone, the next claim Lua script automatically reclaims stale ranges. For faster recovery, remove the dead pod:
```bash
curl -X POST http://migration-svc:8080/control/pods/remove/<dead-pod-id>
```

### Async write queue warnings
If you see "Async write queue at max depth" errors, MongoDB is unreachable or slow. The queue is bounded (default 1000 records) to prevent OOM. Redis still has the authoritative cursor, so no data is lost from ClickHouse — only MongoDB audit trail records may be delayed or dropped until MongoDB recovers.

### Migration completed but locks still held
Locks are released on collection completion. If the process crashes after ClickHouse write but before lock release, the lock expires after TTL (5 min). Use "Remove Pod" to accelerate cleanup.
