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

    // Overall: completed collections use actual docsRead, fallback to estimate
    const totalEstimated = Array.from(estimatedCounts.values()).reduce((a, b) => a + b, 0);
    const completedDocsRead = progress.results
      .filter(r => r.status === 'completed' || r.status === 'skipped')
      .reduce((sum, r) => sum + (r.docsRead ?? estimatedCounts.get(r.collection) ?? 0), 0);
    const overallDocsRead = completedDocsRead + currentCollDocsRead;
    const overallPct = totalEstimated > 0
      ? Math.min(100, Math.round((overallDocsRead / totalEstimated) * 100)) : 0;

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
      // Local result takes priority for non-skipped statuses
      if (localResult) {
        return {
          collection,
          status: localResult.status,
          runId: localResult.runId || null,
          estimated: estimatedCounts.get(collection) ?? null,
          docsRead: localResult.docsRead ?? null,
          rowsInserted: localResult.rowsInserted ?? null,
          podId: config.worker.podId,
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

    // ── Compute cluster-wide aggregates for summary ─────────────────
    const clusterDocsRead = clusterData
      ? mergedCollectionProgress.reduce((s, c) => s + (c.docsRead ?? 0), 0)
      : overallDocsRead;
    const clusterEstimated = clusterData
      ? mergedCollectionProgress.reduce((s, c) => s + (c.estimated ?? 0), 0) || totalEstimated
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

    // ETA based on cluster-wide progress
    const serviceElapsedMs = Date.now() - startedAt.getTime();
    etaMs = clusterPct > 0 && clusterPct < 100 && serviceElapsedMs > 0
      ? Math.round((serviceElapsedMs / clusterPct) * (100 - clusterPct))
      : null;

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
          // In multi-pod mode, compute cluster-wide throughput from total docs / uptime
          const clusterDocs = clusterData
            ? mergedCollectionProgress.reduce((s, c) => s + (c.docsRead ?? 0), 0)
            : 0;
          if (clusterDocs > 0 && uptimeSec > 0) {
            return `${fmtNum(Math.round(clusterDocs / uptimeSec))} docs/s`;
          }
          return `${fmtNum(Math.round(batchStats?.docsPerSecond ?? 0))} docs/s`;
        })(),
        elapsed: (() => {
          const isTerminal = runnerStatus === 'completed' || runnerStatus === 'failed' || runnerStatus === 'stopped';
          if (isTerminal && frozenElapsedMs === null) {
            frozenElapsedMs = Date.now() - startedAt.getTime();
          }
          return fmtDuration(frozenElapsedMs ?? (Date.now() - startedAt.getTime()));
        })(),
        eta: (runnerStatus === 'completed' || runnerStatus === 'failed' || runnerStatus === 'stopped')
          ? 'done'
          : (etaMs !== null ? `~${fmtDuration(etaMs)}` : 'calculating...'),
        status: runnerStatus,
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
