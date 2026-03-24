import type { FastifyInstance } from 'fastify';
import { hostname } from 'node:os';
import type { Run } from '../state/manifest-store.ts';
import type { RunnerStatus } from '../runtime/batch-runner.ts';
import type { GcTelemetry } from '../runtime/gc-controller.ts';
import type { ProcessMetricsSnapshot } from '../runtime/process-metrics.ts';
import type { CommandFlags } from '../state/redis-hot-state.ts';
import type { Config } from '../config/schema.ts';
import type { CollectionOrchestrator, OrchestratorProgress } from '../runtime/collection-orchestrator.ts';

export interface StatsDeps {
  orchestrator: CollectionOrchestrator;
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
        connected: true,
        readPreference: config.source.readPreference,
        readConcern: config.source.readConcern,
        batchRowsTarget: config.source.batchRowsTarget,
        cursorBatchSize: config.source.cursorBatchSize,
      },
      clickhouse: {
        connected: true,
        target: `${config.target.db}.${config.target.table}`,
        compression: config.target.compression,
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
