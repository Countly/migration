import type { Logger } from "pino";
import type { ManifestStore, Batch, BatchStatus } from "./manifest-store.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface QueuedBatchWrite {
    batch: Batch;
    upperInclusiveCursor: string;
    queuedAt: number;
}

export interface AsyncBatchWriterConfig {
    flushIntervalMs: number;
    flushBatchSize: number;
    maxQueueDepth?: number;
}

// ---------------------------------------------------------------------------
// AsyncBatchWriter
// ---------------------------------------------------------------------------

/**
 * Async write queue that makes Redis the hot-path commit point.
 *
 * On batch completion:
 *   1. Write cursor + bitmap to Redis (sync — this is the commit point)
 *   2. Queue MongoDB batch record for async bulk flush
 *
 * Background loop flushes queued records to MongoDB periodically.
 */
export class AsyncBatchWriter {
    private queue: QueuedBatchWrite[] = [];
    private flushTimer: ReturnType<typeof setInterval> | null = null;
    private flushing = false;
    private stopped = false;
    private readonly manifestStore: ManifestStore;
    private readonly config: AsyncBatchWriterConfig;
    private readonly logger: Logger;

    constructor(
        manifestStore: ManifestStore,
        config: AsyncBatchWriterConfig,
        logger: Logger,
    ) {
        this.manifestStore = manifestStore;
        this.config = config;
        this.logger = logger.child({ component: "AsyncBatchWriter" });
    }

    /**
     * Queue a completed batch for async MongoDB flush.
     *
     * Caller (BatchRunner) is responsible for writing cursor + bitmap to
     * the correct per-collection/per-range RedisHotState BEFORE calling this.
     * This method only handles the MongoDB write queue.
     */
    async queueBatch(
        batch: Batch,
        upperInclusiveCursor: string,
    ): Promise<void> {
        // Queue MongoDB write (bounded)
        const maxDepth = this.config.maxQueueDepth ?? 1000;
        if (this.queue.length >= maxDepth) {
            this.logger.error(
                { queueDepth: this.queue.length, maxQueueDepth: maxDepth },
                "Async write queue at max depth — MongoDB may be unreachable. Forcing flush.",
            );
            await this.flush();
        }
        const doc: Batch = {
            ...batch,
            status: "done" as BatchStatus,
            finished_at: new Date().toISOString(),
            error_history: batch.error_history ?? [],
            digest_match: batch.digest_match ?? null,
        };
        this.queue.push({ batch: doc, upperInclusiveCursor, queuedAt: Date.now() });

        // 3. Flush if queue is large enough
        if (this.queue.length >= this.config.flushBatchSize) {
            this.triggerFlush();
        }
    }

    /** Start the periodic flush timer. */
    startPeriodicFlush(): void {
        if (this.flushTimer) return;
        this.flushTimer = setInterval(() => {
            this.triggerFlush();
        }, this.config.flushIntervalMs);
    }

    /** Flush all queued writes and stop the timer. */
    async drainAndStop(): Promise<void> {
        this.stopped = true;
        if (this.flushTimer) {
            clearInterval(this.flushTimer);
            this.flushTimer = null;
        }
        if (this.queue.length > 0) {
            this.logger.info({ pending: this.queue.length }, "Draining async write queue on shutdown");
        }
        try {
            await this.flush();
        } catch (err) {
            this.logger.error(
                { error: err instanceof Error ? err.message : String(err), lostRecords: this.queue.length },
                "Failed to drain async write queue on shutdown — batch records may be missing from MongoDB",
            );
        }
    }

    /** Number of writes waiting to be flushed. */
    getPendingCount(): number {
        return this.queue.length;
    }

    // -----------------------------------------------------------------------
    // Private
    // -----------------------------------------------------------------------

    private triggerFlush(): void {
        if (this.flushing || this.queue.length === 0) return;
        this.flush().catch(err => {
            this.logger.warn({ error: err instanceof Error ? err.message : String(err) }, "Async flush failed, will retry");
        });
    }

    private async flush(): Promise<void> {
        if (this.flushing || this.queue.length === 0) return;
        this.flushing = true;

        // Drain the current queue
        const items = this.queue.splice(0);
        const batchDocs = items.map(i => i.batch);

        // Find the cursor with the highest batch_seq (monotonically increasing)
        let latestCursor = items[0].upperInclusiveCursor;
        let latestRunId = items[0].batch.run_id;
        let maxBatchSeq = items[0].batch.batch_seq;
        for (const item of items) {
            if (item.batch.batch_seq > maxBatchSeq) {
                maxBatchSeq = item.batch.batch_seq;
                latestCursor = item.upperInclusiveCursor;
                latestRunId = item.batch.run_id;
            }
        }

        try {
            // Bulk insert batch records
            await this.manifestStore.bulkInsertBatches(batchDocs);

            // Advance cursor to latest position
            await this.manifestStore.advanceCursor(latestRunId, latestCursor);

            this.logger.debug(
                { flushed: batchDocs.length, latestBatchSeq: batchDocs[batchDocs.length - 1].batch_seq },
                "Async flush completed",
            );
        } catch (err) {
            // Re-queue failed items (capped to prevent unbounded growth)
            const maxDepth = this.config.maxQueueDepth ?? 1000;
            const totalAfterRequeue = this.queue.length + items.length;
            if (totalAfterRequeue <= maxDepth) {
                this.queue.unshift(...items);
            } else {
                const dropped = items.length - (maxDepth - this.queue.length);
                this.queue.unshift(...items.slice(0, Math.max(0, maxDepth - this.queue.length)));
                this.logger.error(
                    { dropped, queueDepth: this.queue.length },
                    "Dropped batch records exceeding max queue depth — MongoDB writes lost",
                );
            }
            this.logger.warn(
                { error: err instanceof Error ? err.message : String(err), count: items.length, queueDepth: this.queue.length },
                "Async flush to MongoDB failed, re-queued",
            );
        } finally {
            this.flushing = false;
        }
    }
}
