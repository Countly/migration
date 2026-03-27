import type { FastifyInstance } from 'fastify';
import { hostname } from 'node:os';
import type { Run } from '../state/manifest-store.ts';
import type { RunnerStatus } from '../runtime/batch-runner.ts';
import type { GcTelemetry } from '../runtime/gc-controller.ts';
import type { ProcessMetricsSnapshot } from '../runtime/process-metrics.ts';
import type { CommandFlags, LiveBatchData, RangeLiveStats } from '../state/redis-hot-state.ts';
import type { Config } from '../config/schema.ts';
import type { CollectionOrchestrator, OrchestratorProgress } from '../runtime/collection-orchestrator.ts';
import type { GlobalProgress, CollectionProgress, PodInfo } from '../state/global-progress.ts';
import type { CollectionLock, LockInfo } from '../state/collection-lock.ts';

// ─────────────────────────────────────────────────────────────────────────────
// Formatting helpers
// ─────────────────────────────────────────────────────────────────────────────

function progressBar(pct: number, width = 30): string {
  const filled = Math.round((pct / 100) * width);
  const arrow = filled < width ? '>' : '';
  const empty = Math.max(0, width - filled - (arrow ? 1 : 0));
  return '[' + '='.repeat(filled) + arrow + ' '.repeat(empty) + '] ' + pct + '%';
}

function fmtDuration(ms: number): string {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}h ${m}m ${sec}s`;
  if (m > 0) return `${m}m ${sec}s`;
  return `${sec}s`;
}

function fmtNum(n: number): string {
  return n.toLocaleString('en-US');
}

// ─────────────────────────────────────────────────────────────────────────────
// Route
// ─────────────────────────────────────────────────────────────────────────────

export interface StatsDeps {
  orchestrator: CollectionOrchestrator;
  mongoReader: { isConnected(): boolean };
  chWriter: { isConnected(): boolean };
  redisState: {
    isHealthy(): Promise<boolean>;
    getBitmapCount(runId: string): Promise<number>;
    getCommands(runId: string): Promise<CommandFlags>;
    getMetrics(): { lastStateWriteMs: number; lastError: string | null };
    getAllLiveBatches(): Promise<LiveBatchData[]>;
    getRangeLiveStats(collection: string): Promise<RangeLiveStats[]>;
    getThroughputWindow(runId: string): Promise<Array<{ ts: number; docsRead: number }>>;
    getAllCollectionCompleted(): Promise<Map<string, { docsRead: number; rowsInserted: number; runId: string; completedAt: string }>>;
  };
  gcController: { getTelemetry(): GcTelemetry };
  processMetrics: { snapshot(): ProcessMetricsSnapshot };
  manifestStore: { getRun(runId: string): Promise<Run | undefined> };
  config: Config;
  startedAt: Date;
  version: string;
  globalProgress?: GlobalProgress;
  collectionLock?: CollectionLock;
}

export function registerStatsRoute(app: FastifyInstance, deps: StatsDeps): void {
  const {
    orchestrator,
    redisState,
    gcController,
    processMetrics,
    manifestStore,
    config,
    startedAt,
    version,
  } = deps;

  let frozenElapsedMs: number | null = null;

  app.get('/stats', async (_request, reply) => {
    const now = new Date();
    const uptimeSec = Math.floor((now.getTime() - startedAt.getTime()) / 1000);

    const progress = orchestrator.getProgress();
    const batchStats = orchestrator.getStats();
    const runnerStatus = orchestrator.getStatus();
    const currentBatchSeq = orchestrator.getCurrentBatchSeq();
    const estimatedCounts = orchestrator.getEstimatedCounts();
    const indexStatus = orchestrator.getIndexStatus();

    // Find the current run ID from the orchestrator's progress
    const currentResult = progress.results.find(
      (r) => r.collection === progress.currentCollection,
    );
    const runId = currentResult?.runId ?? null;

    let redisConnected = false;
    try {
      redisConnected = await redisState.isHealthy();
    } catch {
      redisConnected = false;
    }

    let bitmapBitsSet = 0;
    if (runId) {
      try {
        bitmapBitsSet = await redisState.getBitmapCount(runId);
      } catch {
        bitmapBitsSet = -1;
      }
    }

    const runRecord = runId ? await manifestStore.getRun(runId) : undefined;

    // Throughput calculations
    const totalDocsRead = batchStats?.totalDocsRead ?? 0;
    const totalDocsSkipped = batchStats?.totalDocsSkipped ?? 0;
    const totalRowsInserted = batchStats?.totalRowsInserted ?? 0;
    const elapsedMs = batchStats?.elapsedMs ?? 0;
    const elapsedSec = elapsedMs / 1000;

    // ── Progress calculations ───────────────────────────────────────────
    const currentCollEstimate = progress.currentCollection
      ? estimatedCounts.get(progress.currentCollection) ?? 0 : 0;
    const currentCollDocsRead = totalDocsRead;
    const currentCollPct = currentCollEstimate > 0
      ? Math.min(100, Math.round((currentCollDocsRead / currentCollEstimate) * 100)) : 0;

    // Overall progress — recomputed after mergedCollectionProgress is built
    const totalEstimated = Array.from(estimatedCounts.values()).reduce((a, b) => a + b, 0);
    let overallDocsRead = 0;
    let overallPct = 0;

    // ETA computed later using cluster-wide data
    let etaMs: number | null = null;

    // Commands from Redis (best-effort)
    let commands: CommandFlags = {};
    if (runId) {
      try {
        commands = await redisState.getCommands(runId);
      } catch {
        // default all false
      }
    }

    const redisMetrics = redisState.getMetrics();

    // ── Cluster data (multi-pod mode) ─────────────────────────────────
    let clusterData: {
      podCount: number;
      pods: PodInfo[];
      locks: LockInfo[];
      globalCommands: { pause: boolean; stop: boolean };
      collectionProgress: CollectionProgress[];
    } | null = null;

    if (deps.globalProgress) {
      try {
        const [allProgress, allPods, allLocks, globalCmds] = await Promise.all([
          deps.globalProgress.getAllCollectionProgress(),
          deps.globalProgress.getAllPods(),
          deps.collectionLock?.listAllLocks() ?? Promise.resolve([]),
          deps.globalProgress.getGlobalCommands(),
        ]);
        clusterData = {
          podCount: allPods.length,
          pods: allPods,
          locks: allLocks,
          globalCommands: globalCmds,
          collectionProgress: allProgress,
        };
      } catch {
        // best-effort
      }
    }

    // ── Persistent completion aggregates (no TTL) ───────────────────
    let completedAggregates = new Map<string, { docsRead: number; rowsInserted: number; runId: string; completedAt: string }>();
    try {
      completedAggregates = await redisState.getAllCollectionCompleted();
    } catch { /* best-effort */ }

    // ── Sliding window throughput (best-effort) ──────────────────────
    let slidingThroughput: number | null = null;
    if (runId) {
      try {
        const throughputWindow = await redisState.getThroughputWindow(runId);
        if (throughputWindow.length >= 2) {
          const newest = throughputWindow[0];
          const oldest = throughputWindow[throughputWindow.length - 1];
          const durationSec = (newest.ts - oldest.ts) / 1000;
          const delta = newest.docsRead - oldest.docsRead;
          slidingThroughput = durationSec > 0 ? Math.round(delta / durationSec) : null;
        }
      } catch {
        // best-effort
      }
    }

    // ── Live batches from Redis ──────────────────────────────────────
    let liveBatches: LiveBatchData[] = [];
    try {
      liveBatches = await redisState.getAllLiveBatches();
    } catch {
      // best-effort
    }

    // Merge cluster progress with local data for comprehensive collection status
    const mergedCollectionProgress = progress.collections.map(collection => {
      const localResult = progress.results.find(r => r.collection === collection);
      const remote = clusterData?.collectionProgress.find(p => p.collectionName === collection);

      // Local "skipped" means another pod completed it — use remote data for attribution
      if (localResult && localResult.status === 'skipped' && remote) {
        return {
          collection,
          status: remote.status as string,
          runId: remote.runId || null,
          estimated: remote.estimatedTotal ?? estimatedCounts.get(collection) ?? null,
          docsRead: remote.docsRead ?? null,
          rowsInserted: remote.rowsInserted ?? null,
          podId: remote.podId,
        };
      }
      // Local result takes priority — overlay persistent completion data for best counts
      if (localResult) {
        const completedAgg = completedAggregates.get(collection);
        const remoteCompleted = remote && remote.status === 'completed';
        const isCompleted = remoteCompleted || !!completedAgg || localResult.status === 'completed';
        const bestDocsRead = Math.max(localResult.docsRead ?? 0, remote?.docsRead ?? 0, completedAgg?.docsRead ?? 0);
        const bestRowsInserted = Math.max(localResult.rowsInserted ?? 0, remote?.rowsInserted ?? 0, completedAgg?.rowsInserted ?? 0);
        return {
          collection,
          status: isCompleted ? 'completed' : localResult.status,
          runId: localResult.runId || completedAgg?.runId || remote?.runId || null,
          estimated: estimatedCounts.get(collection) ?? null,
          docsRead: isCompleted ? (bestDocsRead || null) : (localResult.docsRead ?? null),
          rowsInserted: isCompleted ? (bestRowsInserted || null) : (localResult.rowsInserted ?? null),
          podId: remoteCompleted ? remote.podId : config.worker.podId,
        };
      }
      // Check cluster progress from other pods
      if (remote) {
        return {
          collection,
          status: remote.status,
          runId: remote.runId || null,
          estimated: remote.estimatedTotal ?? null,
          docsRead: remote.docsRead ?? null,
          rowsInserted: remote.rowsInserted ?? null,
          podId: remote.podId,
        };
      }
      // Check persistent completion data (survives TTL expiry of progress:* keys)
      const completedAgg = completedAggregates.get(collection);
      if (completedAgg) {
        return {
          collection,
          status: "completed" as const,
          runId: completedAgg.runId || null,
          estimated: estimatedCounts.get(collection) ?? null,
          docsRead: completedAgg.docsRead,
          rowsInserted: completedAgg.rowsInserted,
          podId: null,
        };
      }
      // No data yet
      return {
        collection,
        status: "pending" as const,
        runId: null,
        estimated: estimatedCounts.get(collection) ?? null,
        docsRead: null,
        rowsInserted: null,
        podId: null,
      };
    });

    // ── Recompute overall progress from merged data ─────────────────
    {
      const completedDocsRead = mergedCollectionProgress
        .filter(c => c.status === 'completed' || c.status === 'skipped')
        .reduce((sum, c) => sum + (c.docsRead ?? 0), 0);
      const processingDocsRead = progress.currentCollection
        ? (mergedCollectionProgress.find(c => c.collection === progress.currentCollection && c.status === 'processing')?.docsRead
            ?? currentCollDocsRead)
        : 0;
      overallDocsRead = completedDocsRead + processingDocsRead;
      overallPct = totalEstimated > 0
        ? Math.min(100, Math.round((overallDocsRead / totalEstimated) * 100)) : 0;
    }

    // ── Compute cluster-wide aggregates for summary ─────────────────
    // Exclude collections with null docsRead (not yet started) from both numerator AND denominator
    const activeMerged = clusterData
      ? mergedCollectionProgress.filter(c => c.docsRead !== null)
      : [];
    const clusterDocsRead = clusterData
      ? activeMerged.reduce((s, c) => s + (c.docsRead ?? 0), 0)
      : overallDocsRead;
    const clusterEstimated = clusterData
      ? (activeMerged.reduce((s, c) => s + (c.estimated ?? 0), 0) || totalEstimated)
      : totalEstimated;
    const clusterPct = clusterEstimated > 0
      ? Math.min(100, Math.round((clusterDocsRead / clusterEstimated) * 100))
      : overallPct;
    const clusterDone = clusterData
      ? mergedCollectionProgress.filter(c => c.status === 'completed' || c.status === 'skipped').length
      : progress.completedCollections + progress.skippedCollections;
    const clusterFailed = clusterData
      ? mergedCollectionProgress.filter(c => c.status === 'failed').length
      : progress.failedCollections;
    const clusterProcessing = clusterData
      ? mergedCollectionProgress.filter(c => c.status === 'processing').length
      : (progress.currentCollection ? 1 : 0);
    const clusterTotal = clusterData
      ? (progress.totalCollections || mergedCollectionProgress.length)
      : progress.totalCollections;

    // ETA based on cluster-wide progress — use earliest collection start for accuracy
    const earliestProcessingStart = clusterData
      ? activeMerged.reduce((earliest, c) => {
          const started = (c as Record<string, unknown>)['startedAt'];
          if (typeof started === 'string') {
            const ms = new Date(started).getTime();
            return ms > 0 && ms < earliest ? ms : earliest;
          }
          return earliest;
        }, startedAt.getTime())
      : (orchestrator.getFirstCollectionStartedAt() ?? startedAt.getTime());
    const processingElapsedMs = Date.now() - earliestProcessingStart;
    etaMs = clusterPct > 0 && clusterPct < 100 && processingElapsedMs > 0
      ? Math.round((processingElapsedMs / clusterPct) * (100 - clusterPct))
      : null;

    // ── Range live stats for current collection ────────────────────
    let rangeLiveStats: RangeLiveStats[] = [];
    if (progress.currentCollection) {
      try {
        rangeLiveStats = await redisState.getRangeLiveStats(progress.currentCollection);
      } catch { /* best-effort */ }
    }

    const payload = {
      // ── Quick-glance summary (cluster-wide when multi-pod) ──────────
      summary: {
        overall: progressBar(clusterPct),
        overallPct: clusterPct,
        currentCollection: progress.currentCollection
          ? `${progress.currentCollection}  ${progressBar(currentCollPct)}`
          : 'idle',
        currentCollectionPct: currentCollPct,
        collections: `${clusterDone}/${clusterTotal} done`
          + (clusterFailed > 0 ? `, ${clusterFailed} failed` : '')
          + (clusterProcessing > 0 ? `, ${clusterProcessing} processing` : ''),
        docsProgress: `${fmtNum(clusterDocsRead)} / ~${fmtNum(clusterEstimated)} docs`,
        throughput: (() => {
          // Prefer sliding window throughput when available
          if (slidingThroughput !== null && slidingThroughput > 0) {
            return `${fmtNum(slidingThroughput)} docs/s`;
          }
          // Fallback: cluster-wide lifetime throughput
          const clusterDocs = clusterData
            ? activeMerged.reduce((s, c) => s + (c.docsRead ?? 0), 0)
            : 0;
          if (clusterDocs > 0 && uptimeSec > 0) {
            return `${fmtNum(Math.round(clusterDocs / uptimeSec))} docs/s`;
          }
          return `${fmtNum(Math.round(batchStats?.docsPerSecond ?? 0))} docs/s`;
        })(),
        elapsed: (() => {
          const processingStart = orchestrator.getFirstCollectionStartedAt() ?? startedAt.getTime();
          const isTerminal = runnerStatus === 'completed' || runnerStatus === 'failed' || runnerStatus === 'stopped';
          if (isTerminal && frozenElapsedMs === null) {
            frozenElapsedMs = Date.now() - processingStart;
          }
          return fmtDuration(frozenElapsedMs ?? (Date.now() - processingStart));
        })(),
        eta: (runnerStatus === 'completed' || runnerStatus === 'failed' || runnerStatus === 'stopped')
          ? 'done'
          : (etaMs !== null ? `~${fmtDuration(etaMs)}` : 'calculating...'),
        status: (() => {
          if (!clusterData) return runnerStatus;
          if (clusterProcessing > 0) return "running";
          const clusterPending = clusterTotal - clusterDone - clusterFailed;
          if (clusterPending > 0) return "running";
          if (clusterTotal > 0 && clusterDone + clusterFailed >= clusterTotal) {
            return clusterFailed > 0 ? "completed_with_errors" : "completed";
          }
          return runnerStatus;
        })(),
      },

      // ── Current collection detail ─────────────────────────────────────
      currentCollectionProgress: progress.currentCollection ? {
        collection: progress.currentCollection,
        estimated: currentCollEstimate,
        docsRead: currentCollDocsRead,
        rowsInserted: totalRowsInserted,
        pct: currentCollPct,
        bar: progressBar(currentCollPct),
        batchSeq: currentBatchSeq,
        skipRate: currentCollDocsRead > 0
          ? `${((totalDocsSkipped / currentCollDocsRead) * 100).toFixed(1)}%` : '0%',
        rangeBreakdown: rangeLiveStats.length > 0 ? rangeLiveStats : undefined,
      } : null,

      indexStatus,

      service: {
        name: config.service.name,
        version,
        runId,
        status: runnerStatus,
        uptimeSec,
        pid: process.pid,
        hostname: hostname(),
      },
      orchestrator: {
        totalCollections: progress.totalCollections,
        completedCollections: progress.completedCollections,
        failedCollections: progress.failedCollections,
        skippedCollections: progress.skippedCollections,
        currentCollection: progress.currentCollection,
        collections: progress.collections,
        collectionProgress: mergedCollectionProgress,
      },
      run: {
        sourceNs: runRecord?.source_ns ?? null,
        targetTable: runRecord?.target_table ?? null,
        upperBoundCursor: runRecord?.upper_bound_cursor ?? null,
        lastCommittedCursor: runRecord?.last_committed_cursor ?? null,
        batchSeqCommitted: currentBatchSeq,
        batchSeqInFlight: null,
        transformVersion: runRecord?.transform_version ?? null,
        pauseReason: null,
        stopAfterBatch: false,
        catchupMode: false,
      },
      throughput: {
        sourceDocsReadTotal: totalDocsRead,
        docsSkippedTotal: totalDocsSkipped,
        rowsInsertedTotal: totalRowsInserted,
        avgSourceDocsPerSec: elapsedSec > 0 ? Math.round((totalDocsRead / elapsedSec) * 100) / 100 : 0,
        avgRowsInsertedPerSec: elapsedSec > 0 ? Math.round((totalRowsInserted / elapsedSec) * 100) / 100 : 0,
      },
      skipReasons: batchStats?.skipsByReason ?? {},
      integrity: {
        digestMismatches: batchStats?.digestMismatches ?? 0,
        estimatedDuplicateRows: batchStats?.estimatedDuplicateRows ?? 0,
        batchesFailed: batchStats?.batchesFailed ?? 0,
      },
      batch: null,
      mongo: {
        connected: deps.mongoReader.isConnected(),
        readPreference: config.source.readPreference,
        readConcern: config.source.readConcern,
        batchRowsTarget: config.source.batchRowsTarget,
        cursorBatchSize: config.source.cursorBatchSize,
      },
      clickhouse: {
        connected: deps.chWriter.isConnected(),
        target: `${config.target.db}.${config.target.table}`,
        compression: 'gzip',
        partsToThrowInsert: config.backpressure.partsToThrowInsert,
        maxPartsInTotal: config.backpressure.maxPartsInTotal,
      },
      redis: {
        connected: redisConnected,
        lastStateWriteMs: redisMetrics.lastStateWriteMs,
        bitmapBitsSet,
        lastError: redisMetrics.lastError,
      },
      manifest: {
        db: config.state.manifestDb,
        lastCheckpointTime: runRecord?.updated_at ?? null,
      },
      gc: gcController.getTelemetry(),
      process: processMetrics.snapshot(),
      commands: {
        pauseRequested: !!(commands.pause),
        resumeRequested: !!(commands.resume),
        stopAfterBatchRequested: !!(commands.abort ?? commands.stopAfterBatch),
        gcRequested: !!(commands.gc),
      },
      // ── Live batches + range progress ───────────────────────────────
      liveBatches,
      cluster: clusterData ? {
        ...clusterData,
        pods: clusterData.pods.map(pod => {
          // Aggregate per-pod stats from merged collection progress
          const podCollections = mergedCollectionProgress.filter(c => c.podId === pod.podId);
          return {
            ...pod,
            stats: {
              collectionsCompleted: podCollections.filter(c => c.status === 'completed' || c.status === 'skipped').length,
              docsRead: podCollections.reduce((s, c) => s + (c.docsRead ?? 0), 0),
              rowsInserted: podCollections.reduce((s, c) => s + (c.rowsInserted ?? 0), 0),
            },
          };
        }),
        stalePods: clusterData.locks
          .map(l => l.podId)
          .filter(podId => !clusterData!.pods.some(p => p.podId === podId))
          .filter((v, i, a) => a.indexOf(v) === i),
        lockSummary: {
          total: clusterData.locks.length,
          byPod: Object.fromEntries(
            [...new Set(clusterData.locks.map(l => l.podId))].map(pid =>
              [pid, clusterData!.locks.filter(l => l.podId === pid).length]
            )
          ),
          stale: clusterData.locks.filter(l => {
            const pod = clusterData!.pods.find(p => p.podId === l.podId);
            if (!pod) return true;
            // Pod key exists but heartbeat is stale (>180s old)
            const heartbeatAge = Date.now() - new Date(pod.lastHeartbeat).getTime();
            return heartbeatAge > 180_000;
          }).length,
        },
      } : null,
      clusterProgress: clusterData ? (() => {
        const done = mergedCollectionProgress.filter(c => c.status === 'completed' || c.status === 'skipped').length;
        const failed = mergedCollectionProgress.filter(c => c.status === 'failed').length;
        const processing = mergedCollectionProgress.filter(c => c.status === 'processing').length;
        const pending = mergedCollectionProgress.filter(c => c.status === 'pending').length;
        const total = progress.totalCollections || mergedCollectionProgress.length;
        const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;
        const docsRead = mergedCollectionProgress.reduce((s, c) => s + (c.docsRead ?? 0), 0);
        const rowsInserted = mergedCollectionProgress.reduce((s, c) => s + (c.rowsInserted ?? 0), 0);
        const estimated = mergedCollectionProgress.reduce((s, c) => s + (c.estimated ?? 0), 0);
        return { total, done, failed, processing, pending, pct, docsRead, rowsInserted, estimated };
      })() : null,
    };

    return reply.status(200).send(payload);
  });
}
