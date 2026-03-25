import type { FastifyInstance } from 'fastify';
import { buildCoverageFromBatches, compactIntervals } from '../state/coverage.ts';
import type { Run, Batch, BatchStatus, RunStatus, GetBatchesOptions } from '../state/manifest-store.ts';

export interface RunDeps {
  manifestStore: {
    getActiveRun(sourceNs?: string, targetTable?: string): Promise<Run | undefined>;
    getRun(runId: string): Promise<Run | undefined>;
    listRuns(opts: { status?: RunStatus; limit?: number; offset?: number }): Promise<{ runs: Run[]; total: number }>;
    getBatches(runId: string, opts: GetBatchesOptions): Promise<Batch[]>;
    getFailedBatches(runId: string): Promise<Batch[]>;
    countEvents(runId: string, eventType?: string): Promise<number>;
  };
  redisState: {
    getTimeline(runId: string): Promise<import('../state/redis-hot-state.ts').TimelineSnapshot[]>;
    getRecentErrors(runId: string): Promise<import('../state/redis-hot-state.ts').RecentError[]>;
    getVerboseErrors(runId: string, batchSeq: number): Promise<import('../state/redis-hot-state.ts').VerboseError[]>;
    cleanupRun(runId: string): Promise<number>;
    isHealthy(): Promise<boolean>;
  };
  orchestrator?: {
    getCurrentRunId(): string | null;
  };
}

interface BatchesQuerystring {
  status?: string;
  limit?: string;
}

export function registerRunRoutes(app: FastifyInstance, deps: RunDeps): void {
  const { manifestStore, redisState } = deps;

  // GET /runs/current - returns current run header from manifest store
  app.get('/runs/current', async (_request, reply) => {
    const activeRun = await manifestStore.getActiveRun();

    if (activeRun == null) {
      return reply.status(404).send({
        error: 'No active run found',
      });
    }

    const runId = activeRun.run_id;
    const doneBatches = await manifestStore.getBatches(runId, { status: 'done' });
    const coverage = buildCoverageFromBatches(doneBatches);
    const compactedIntervals = compactIntervals(coverage);

    return reply.status(200).send({ ...activeRun, coverage: compactedIntervals });
  });

  // GET /runs/current/batches?status=inflight|failed|done&limit=N
  app.get<{ Querystring: BatchesQuerystring }>(
    '/runs/current/batches',
    async (request, reply) => {
      const activeRun = await manifestStore.getActiveRun();

      if (activeRun == null) {
        return reply.status(404).send({
          error: 'No active run found',
        });
      }

      const runId = activeRun.run_id;
      const { status, limit } = request.query;
      const parsedLimit = limit != null ? Math.max(1, parseInt(limit, 10) || 50) : 50;

      const batches = await manifestStore.getBatches(runId, {
        status: status as BatchStatus | undefined,
        limit: parsedLimit,
      });

      return reply.status(200).send({
        runId,
        status: status ?? 'all',
        limit: parsedLimit,
        count: batches.length,
        batches,
      });
    },
  );

  // GET /runs - list runs
  app.get<{ Querystring: { status?: string; limit?: string; offset?: string } }>(
    '/runs',
    async (request, reply) => {
      const { status, limit, offset } = request.query;
      const parsedLimit = limit != null ? Math.max(1, parseInt(limit, 10) || 20) : 20;
      const parsedOffset = offset != null ? Math.max(0, parseInt(offset, 10) || 0) : 0;

      const result = await manifestStore.listRuns({
        status: status as RunStatus | undefined,
        limit: parsedLimit,
        offset: parsedOffset,
      });

      return reply.status(200).send({
        ...result,
        limit: parsedLimit,
        offset: parsedOffset,
      });
    },
  );

  // GET /runs/:runId
  app.get<{ Params: { runId: string } }>(
    '/runs/:runId',
    async (request, reply) => {
      const run = await manifestStore.getRun(request.params.runId);
      if (!run) {
        return reply.status(404).send({ error: 'Run not found' });
      }
      return reply.status(200).send(run);
    },
  );

  // GET /runs/:runId/batches
  app.get<{ Params: { runId: string }; Querystring: BatchesQuerystring }>(
    '/runs/:runId/batches',
    async (request, reply) => {
      const { runId } = request.params;
      const { status, limit } = request.query;
      const parsedLimit = limit != null ? Math.max(1, parseInt(limit, 10) || 50) : 50;

      const batches = await manifestStore.getBatches(runId, {
        status: status as BatchStatus | undefined,
        limit: parsedLimit,
      });

      return reply.status(200).send({
        runId,
        status: status ?? 'all',
        limit: parsedLimit,
        count: batches.length,
        batches,
      });
    },
  );

  // GET /runs/:runId/failures
  app.get<{ Params: { runId: string } }>(
    '/runs/:runId/failures',
    async (request, reply) => {
      const { runId } = request.params;

      const [run, failedBatches, totalRetryErrors, digestMismatches] = await Promise.all([
        manifestStore.getRun(runId),
        manifestStore.getFailedBatches(runId),
        manifestStore.countEvents(runId, 'batch_retry_error'),
        manifestStore.countEvents(runId, 'digest_mismatch'),
      ]);

      if (!run) {
        return reply.status(404).send({ error: 'Run not found' });
      }

      let recentErrors: unknown[] = [];
      let verboseErrors: Record<number, unknown[]> = {};
      let redisDataAvailable = false;

      try {
        const healthy = await redisState.isHealthy();
        if (healthy) {
          redisDataAvailable = true;
          recentErrors = await redisState.getRecentErrors(runId);
          const verboseResults = await Promise.all(
            failedBatches.map(batch =>
              redisState.getVerboseErrors(runId, batch.batch_seq)
                .then(v => ({ seq: batch.batch_seq, errors: v }))
            )
          );
          const errorEntries: Record<number, unknown[]> = {};
          for (const { seq, errors } of verboseResults) {
            if (errors.length > 0) {
              errorEntries[seq] = errors;
            }
          }
          verboseErrors = errorEntries;
        }
      } catch {
        redisDataAvailable = false;
      }

      return reply.status(200).send({
        runId,
        total_failed_batches: failedBatches.length,
        total_retry_errors: totalRetryErrors,
        digest_mismatches: digestMismatches,
        estimated_duplicate_rows: run.summary?.estimated_duplicate_rows ?? 0,
        failed_batches: failedBatches,
        recent_errors: recentErrors,
        verbose_errors: verboseErrors,
        redis_data_available: redisDataAvailable,
      });
    },
  );

  // GET /runs/:runId/timeline
  app.get<{ Params: { runId: string } }>(
    '/runs/:runId/timeline',
    async (request, reply) => {
      const { runId } = request.params;
      try {
        const snapshots = await redisState.getTimeline(runId);
        return reply.status(200).send({ runId, snapshots });
      } catch {
        return reply.status(200).send({ runId, snapshots: [], redis_available: false });
      }
    },
  );

  // GET /runs/:runId/coverage
  app.get<{ Params: { runId: string } }>(
    '/runs/:runId/coverage',
    async (request, reply) => {
      const { runId } = request.params;

      const run = await manifestStore.getRun(runId);
      if (!run) {
        return reply.status(404).send({ error: 'Run not found' });
      }

      const doneBatches = await manifestStore.getBatches(runId, { status: 'done' });
      const allBatches = await manifestStore.getBatches(runId, {});
      const coverage = buildCoverageFromBatches(doneBatches);
      const compactedIntervals = compactIntervals(coverage);

      return reply.status(200).send({
        runId,
        intervals: compactedIntervals,
        total_batches_done: doneBatches.length,
        total_batches: allBatches.length,
        coverage_pct: allBatches.length > 0
          ? (doneBatches.length / allBatches.length) * 100
          : 0,
      });
    },
  );

  // DELETE /runs/:runId/cache
  app.delete<{ Params: { runId: string } }>(
    '/runs/:runId/cache',
    async (request, reply) => {
      const { runId } = request.params;
      const currentRunId = deps.orchestrator?.getCurrentRunId?.();
      if (currentRunId && currentRunId === runId) {
        return reply.status(409).send({
          error: 'Cannot delete cache for the currently active run',
        });
      }
      const keysDeleted = await redisState.cleanupRun(runId);
      return reply.status(200).send({ runId, keys_deleted: keysDeleted });
    },
  );
}
