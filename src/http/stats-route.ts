import type { FastifyInstance } from 'fastify';
import { hostname } from 'node:os';
import type { Run } from '../state/manifest-store.ts';
import type { RunnerStatus } from '../runtime/batch-runner.ts';
import type { GcTelemetry } from '../runtime/gc-controller.ts';
import type { ProcessMetricsSnapshot } from '../runtime/process-metrics.ts';
import type { CommandFlags } from '../state/redis-hot-state.ts';
import type { Config } from '../config/schema.ts';
import type { CollectionOrchestrator, OrchestratorProgress } from '../runtime/collection-orchestrator.ts';

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
  };
  gcController: { getTelemetry(): GcTelemetry };
  processMetrics: { snapshot(): ProcessMetricsSnapshot };
  manifestStore: { getRun(runId: string): Promise<Run | undefined> };
  config: Config;
  startedAt: Date;
  version: string;
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

  app.get('/stats', async (_request, reply) => {
    const now = new Date();
    const uptimeSec = Math.floor((now.getTime() - startedAt.getTime()) / 1000);

    const progress = orchestrator.getProgress();
    const batchStats = orchestrator.getStats();
    const runnerStatus = orchestrator.getStatus();
    const currentBatchSeq = orchestrator.getCurrentBatchSeq();
    const estimatedCounts = orchestrator.getEstimatedCounts();

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

    // ETA based on elapsed time and progress percentage
    const etaMs = overallPct > 0 && overallPct < 100 && elapsedMs > 0
      ? Math.round(((Date.now() - startedAt.getTime()) / overallPct) * (100 - overallPct))
      : null;

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

    const payload = {
      // ── Quick-glance summary ──────────────────────────────────────────
      summary: {
        overall: progressBar(overallPct),
        overallPct,
        currentCollection: progress.currentCollection
          ? `${progress.currentCollection}  ${progressBar(currentCollPct)}`
          : 'idle',
        currentCollectionPct: currentCollPct,
        collections: `${progress.completedCollections}/${progress.totalCollections} done`
          + (progress.failedCollections > 0 ? `, ${progress.failedCollections} failed` : '')
          + (progress.skippedCollections > 0 ? `, ${progress.skippedCollections} skipped` : ''),
        docsProgress: `${fmtNum(overallDocsRead)} / ~${fmtNum(totalEstimated)} docs`,
        throughput: `${fmtNum(Math.round(batchStats?.docsPerSecond ?? 0))} docs/s`,
        elapsed: fmtDuration(Date.now() - startedAt.getTime()),
        eta: etaMs !== null ? `~${fmtDuration(etaMs)}` : 'calculating...',
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
        collectionProgress: progress.results.map(r => ({
          collection: r.collection,
          status: r.status,
          estimated: estimatedCounts.get(r.collection) ?? null,
          docsRead: r.docsRead ?? null,
          rowsInserted: r.rowsInserted ?? null,
        })),
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
    };

    return reply.status(200).send(payload);
  });
}
