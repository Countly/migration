import type { FastifyInstance } from 'fastify';
import type { RunnerStatus } from '../runtime/batch-runner.ts';
import type { GcMode } from '../runtime/gc-controller.ts';
import type { Batch, ManifestStore } from '../state/manifest-store.ts';
import type { MongoReader } from '../source/mongo-reader.ts';
import type { ClickHouseWriter } from '../target/clickhouse-writer.ts';
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
      const page = await mongoReader.readPage(lowerCursor, upperCursor);

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
        const page = await mongoReader.readPage(lowerCursor, upperCursor);
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
}
