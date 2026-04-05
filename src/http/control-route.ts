import type { FastifyInstance } from 'fastify';
import type { RunnerStatus } from '../runtime/batch-runner.ts';
import type { GcMode } from '../runtime/gc-controller.ts';
import type { Batch, ManifestStore } from '../state/manifest-store.ts';
import type { MongoReader, PageResult } from '../source/mongo-reader.ts';
import type { ClickHouseWriter } from '../target/clickhouse-writer.ts';
import type { GlobalProgress } from '../state/global-progress.ts';
import type { CollectionLock } from '../state/collection-lock.ts';
import type { RedisHotState } from '../state/redis-hot-state.ts';
import { transformBatch } from '../transform/normalize.ts';
import { SkipCounter } from '../transform/skip-reasons.ts';
import { deserializeCursor } from '../types/cursor.ts';

export interface ControlDeps {
  orchestrator: {
    pause(): void;
    resume(): void;
    stopAfterBatch(): void;
    getStatus(): RunnerStatus;
    triggerReindex(collectionName: string): void;
    retryCollection(collectionName: string): void;
  };
  gcController: {
    runGc(mode: GcMode, reason: string): Promise<boolean>;
    isAvailable: boolean;
  };
  manifestStore: ManifestStore;
  mongoReader: MongoReader;
  chWriter: ClickHouseWriter;
  globalProgress?: GlobalProgress;
  collectionLock?: CollectionLock;
  redisState?: RedisHotState;
}

interface GcRequestBody {
  mode: GcMode;
}

export function registerControlRoutes(app: FastifyInstance, deps: ControlDeps): void {
  const { orchestrator, gcController, manifestStore, mongoReader, chWriter } = deps;

  // POST /control/pause - pause after current batch
  app.post('/control/pause', async (_request, reply) => {
    orchestrator.pause();
    return reply.status(200).send({
      ok: true,
      status: orchestrator.getStatus(),
    });
  });

  // POST /control/resume - resume batch scheduling
  app.post('/control/resume', async (_request, reply) => {
    orchestrator.resume();
    return reply.status(200).send({
      ok: true,
      status: orchestrator.getStatus(),
    });
  });

  // POST /control/stop-after-batch - stop cleanly after current batch
  app.post('/control/stop-after-batch', async (_request, reply) => {
    orchestrator.stopAfterBatch();
    return reply.status(200).send({
      ok: true,
      status: orchestrator.getStatus(),
    });
  });

  // POST /control/reindex/:collection - trigger index creation for a collection
  app.post('/control/reindex/:collection', async (request, reply) => {
    const { collection } = request.params as { collection: string };
    orchestrator.triggerReindex(collection);
    return reply.status(200).send({
      ok: true,
      collection,
      message: 'Index build triggered',
    });
  });

  // POST /control/retry-collection/:collection - re-queue a failed/skipped collection
  app.post('/control/retry-collection/:collection', async (request, reply) => {
    const { collection } = request.params as { collection: string };
    orchestrator.retryCollection(collection);
    return reply.status(200).send({
      ok: true,
      collection,
      message: 'Collection queued for retry',
    });
  });

  // POST /control/retry-batch/:runId/:batchSeq - re-read, re-transform, re-insert a skipped/failed batch
  app.post('/control/retry-batch/:runId/:batchSeq', async (request, reply) => {
    const { runId, batchSeq } = request.params as { runId: string; batchSeq: string };
    const seq = Number(batchSeq);

    try {
      const batches = await manifestStore.getBatches(runId, {});
      const batch = batches.find(b => b.batch_seq === seq);
      if (!batch) {
        return reply.status(404).send({ ok: false, error: 'Batch not found' });
      }
      if (batch.status !== 'skipped_empty' && batch.status !== 'failed') {
        return reply.status(400).send({ ok: false, error: `Batch status is "${batch.status}", only skipped_empty/failed can be retried` });
      }

      // Re-read the exact MongoDB range
      const lowerCursor = batch.lower_exclusive_cursor ? deserializeCursor(batch.lower_exclusive_cursor) : null;
      const upperCursor = deserializeCursor(batch.upper_inclusive_cursor);
      const batchPhase = (batch as any).phase ?? "cursor";
      let page: PageResult;
      if (batchPhase === "null_cd") {
        const lowerId = lowerCursor ? lowerCursor.id : null;
        const upperId = upperCursor.id;
        page = await mongoReader.readNullCdPage(lowerId, upperId);
      } else {
        page = await mongoReader.readPage(lowerCursor, upperCursor);
      }

      if (page.docs.length === 0) {
        return reply.status(200).send({ ok: true, message: 'No documents in range', docsRead: 0, rowsInserted: 0 });
      }

      // Re-transform
      const skipCounter = new SkipCounter();
      const { rows } = transformBatch(page.docs, skipCounter);

      if (rows.length === 0) {
        return reply.status(200).send({ ok: true, message: 'All documents skipped again', docsRead: page.docs.length, rowsInserted: 0 });
      }

      // Insert into ClickHouse
      const result = await chWriter.insertBatch({ runId, batchSeq: seq, rows });

      // Update batch status in manifest
      await manifestStore.updateBatchStatus(runId, seq, 'done');

      return reply.status(200).send({
        ok: true,
        message: 'Batch retried successfully',
        docsRead: page.docs.length,
        rowsInserted: result.rowsInserted,
      });
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      return reply.status(500).send({ ok: false, error });
    }
  });

  // POST /control/retry-skipped-batches/:runId - retry all skipped_empty batches in a run
  app.post('/control/retry-skipped-batches/:runId', async (request, reply) => {
    const { runId } = request.params as { runId: string };

    try {
      const batches = await manifestStore.getBatches(runId, { status: 'skipped_empty' as any });
      if (batches.length === 0) {
        return reply.status(200).send({ ok: true, message: 'No skipped batches found', retried: 0 });
      }

      let retried = 0;
      let totalInserted = 0;

      for (const batch of batches) {
        const lowerCursor = batch.lower_exclusive_cursor ? deserializeCursor(batch.lower_exclusive_cursor) : null;
        const upperCursor = deserializeCursor(batch.upper_inclusive_cursor);
        const batchPhase = (batch as any).phase ?? "cursor";
        let page: PageResult;
        if (batchPhase === "null_cd") {
          const lowerId = lowerCursor ? lowerCursor.id : null;
          const upperId = upperCursor.id;
          page = await mongoReader.readNullCdPage(lowerId, upperId);
        } else {
          page = await mongoReader.readPage(lowerCursor, upperCursor);
        }
        if (page.docs.length === 0) continue;

        const skipCounter = new SkipCounter();
        const { rows } = transformBatch(page.docs, skipCounter);
        if (rows.length === 0) continue;

        const result = await chWriter.insertBatch({ runId, batchSeq: batch.batch_seq, rows });
        await manifestStore.updateBatchStatus(runId, batch.batch_seq, 'done');
        retried++;
        totalInserted += result.rowsInserted;
      }

      return reply.status(200).send({ ok: true, message: `Retried ${retried} batches`, retried, totalInserted });
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      return reply.status(500).send({ ok: false, error });
    }
  });

  // POST /control/gc - trigger manual GC
  app.post<{ Body: GcRequestBody }>('/control/gc', async (request, reply) => {
    if (!gcController.isAvailable) {
      return reply.status(400).send({
        ok: false,
        error: 'GC not available (--expose-gc not set)',
      });
    }

    const { mode } = request.body ?? { mode: 'now' };
    const triggered = await gcController.runGc(mode, 'manual-http-request');

    return reply.status(200).send({
      ok: true,
      mode,
      triggered,
    });
  });

  // ── Global control endpoints (multi-pod mode) ─────────────────────────
  const { globalProgress } = deps;

  // POST /control/global/pause - pause all pods
  app.post('/control/global/pause', async (_request, reply) => {
    if (!globalProgress) {
      return reply.status(400).send({ ok: false, error: 'Multi-pod mode not enabled' });
    }
    await globalProgress.setGlobalCommand('pause', true);
    orchestrator.pause(); // Also pause this pod
    return reply.status(200).send({ ok: true, message: 'Global pause issued' });
  });

  // POST /control/global/resume - resume all pods
  app.post('/control/global/resume', async (_request, reply) => {
    if (!globalProgress) {
      return reply.status(400).send({ ok: false, error: 'Multi-pod mode not enabled' });
    }
    await globalProgress.setGlobalCommand('pause', false);
    orchestrator.resume();
    return reply.status(200).send({ ok: true, message: 'Global resume issued' });
  });

  // POST /control/global/stop - stop all pods
  app.post('/control/global/stop', async (_request, reply) => {
    if (!globalProgress) {
      return reply.status(400).send({ ok: false, error: 'Multi-pod mode not enabled' });
    }
    await globalProgress.setGlobalCommand('stop', true);
    orchestrator.stopAfterBatch();
    return reply.status(200).send({ ok: true, message: 'Global stop issued' });
  });

  // ── Lock + Pod management endpoints ───────────────────────────────────
  const { collectionLock } = deps;

  // GET /control/locks - list all active locks
  app.get('/control/locks', async (_request, reply) => {
    if (!collectionLock) {
      return reply.status(400).send({ ok: false, error: 'Multi-pod mode not enabled' });
    }
    const locks = await collectionLock.listAllLocks();
    return reply.status(200).send({ ok: true, locks });
  });

  // POST /control/locks/release/:collection - force-release a lock (admin)
  app.post('/control/locks/release/:collection', async (request, reply) => {
    if (!collectionLock) {
      return reply.status(400).send({ ok: false, error: 'Multi-pod mode not enabled' });
    }
    const { collection } = request.params as { collection: string };
    await collectionLock.forceRelease(collection);
    return reply.status(200).send({ ok: true, collection, message: 'Lock force-released' });
  });

  // GET /control/pods - list all pods with alive status and locks
  app.get('/control/pods', async (_request, reply) => {
    if (!collectionLock) {
      return reply.status(400).send({ ok: false, error: 'Multi-pod mode not enabled' });
    }
    const [podKeys, locks] = await Promise.all([
      collectionLock.listAllPodKeys(),
      collectionLock.listAllLocks(),
    ]);
    const alivePodIds = new Set(podKeys.map(p => p.podId));
    // Find pods referenced in locks but not alive
    const lockPodIds = [...new Set(locks.map(l => l.podId))];
    const pods = lockPodIds.map(podId => {
      const alive = alivePodIds.has(podId);
      const podLocks = locks.filter(l => l.podId === podId);
      const podInfo = podKeys.find(p => p.podId === podId);
      return {
        podId,
        alive,
        lastHeartbeat: podInfo?.lastHeartbeat ?? null,
        collectionsActive: podInfo?.collectionsActive ?? [],
        locks: podLocks.map(l => l.collectionName),
        lockCount: podLocks.length,
      };
    });
    // Add alive pods with no locks
    for (const pk of podKeys) {
      if (!lockPodIds.includes(pk.podId)) {
        pods.push({
          podId: pk.podId,
          alive: true,
          lastHeartbeat: pk.lastHeartbeat,
          collectionsActive: pk.collectionsActive,
          locks: [],
          lockCount: 0,
        });
      }
    }
    return reply.status(200).send({ ok: true, pods });
  });

  // POST /control/pods/remove/:podId - remove a dead pod's keys and release its locks
  app.post('/control/pods/remove/:podId', async (request, reply) => {
    if (!collectionLock) {
      return reply.status(400).send({ ok: false, error: 'Multi-pod mode not enabled' });
    }
    const { podId } = request.params as { podId: string };
    await collectionLock.deletePodKey(podId);
    const released = await collectionLock.releaseLocksForPod(podId);
    return reply.status(200).send({ ok: true, podId, releasedLocks: released, message: `Pod removed, ${released.length} locks released` });
  });

  // POST /control/drain - graceful drain for K8s scale-down
  app.post('/control/drain', async (_request, reply) => {
    orchestrator.stopAfterBatch();
    return reply.status(200).send({ ok: true, message: 'Drain initiated — finishing current batch then releasing locks' });
  });

  // ── DANGER ZONE ────────────────────────────────────────────────────

  // POST /control/danger/clear-mongodb - drop all migration state from MongoDB
  app.post('/control/danger/clear-mongodb', async (_request, reply) => {
    const { manifestStore: ms } = deps;
    try {
      const runs = await ms.listRuns({ limit: 1000 });
      let totalDeleted = 0;
      for (const run of runs.runs) {
        totalDeleted += await ms.deleteRunData(run.run_id);
      }
      // Also delete the run records themselves
      const db = (ms as any).client.db((ms as any).dbName);
      const runResult = await db.collection('mig_runs').deleteMany({});
      totalDeleted += runResult.deletedCount ?? 0;
      return reply.status(200).send({
        ok: true,
        message: `Cleared MongoDB migration state: ${totalDeleted} records deleted`,
        deletedRecords: totalDeleted,
      });
    } catch (err) {
      return reply.status(500).send({ ok: false, error: err instanceof Error ? err.message : String(err) });
    }
  });

  // POST /control/danger/clear-redis - flush all migration keys from Redis
  app.post('/control/danger/clear-redis', async (_request, reply) => {
    const { redisState: rs } = deps;
    if (!rs) {
      return reply.status(400).send({ ok: false, error: 'Redis not available' });
    }
    try {
      const redis = rs.getRedisClient();
      const keys = await (async () => {
        const found: string[] = [];
        const stream = redis.scanStream({ match: 'mig:*', count: 500 });
        for await (const batch of stream) found.push(...(batch as string[]));
        return found;
      })();
      if (keys.length > 0) {
        await redis.unlink(...keys);
      }
      return reply.status(200).send({
        ok: true,
        message: `Cleared Redis migration state: ${keys.length} keys deleted`,
        deletedKeys: keys.length,
      });
    } catch (err) {
      return reply.status(500).send({ ok: false, error: err instanceof Error ? err.message : String(err) });
    }
  });
}
