import { MongoClient, type Collection, type Db } from "mongodb";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type RunStatus = "active" | "completed" | "failed" | "paused" | "stopped";

export interface RunSummary {
    finished_at: string;
    duration_ms: number;
    total_docs_read: number;
    total_rows_inserted: number;
    total_docs_skipped: number;
    avg_docs_per_second: number;
    avg_rows_per_second: number;
    total_batches: number;
    batches_done: number;
    batches_failed: number;
    batches_skipped_empty: number;
    skip_reasons: Record<string, number>;
    total_errors: number;
    failed_batch_seqs: number[];
    digest_mismatches: number;
    estimated_duplicate_rows: number;
    coverage_pct: number;
}

export type BatchStatus =
    | "prepared"
    | "inflight"
    | "done"
    | "failed"
    | "skipped_empty";

export interface Run {
    run_id: string;
    status: RunStatus;
    source_ns: string;
    target_table: string;
    upper_bound_cursor: string;
    last_committed_cursor: string | null;
    transform_version: string;
    created_at: string;
    updated_at: string;
    summary: RunSummary | null;
}

export interface Batch {
    run_id: string;
    batch_seq: number;
    lower_exclusive_cursor: string;
    upper_inclusive_cursor: string;
    source_docs_read: number;
    docs_skipped: number;
    rows_to_insert: number;
    payload_digest: string;
    insert_dedup_token: string;
    query_id: string;
    status: BatchStatus;
    retry_count: number;
    last_error: string | null;
    started_at: string | null;
    finished_at: string | null;
    error_history?: CompactError[];
    digest_match?: boolean | null;
}

export interface SkipSample {
    run_id: string;
    batch_seq: number;
    doc_id: string;
    reason: string;
    captured_at: string;
}

export interface CompactError {
    attempt: number;
    error: string;
    timestamp: string;
}

export interface EventRecord {
    run_id: string;
    event_type: string;
    message: string;
    metadata: Record<string, unknown> | null;
    created_at: string;
}

export interface GetBatchesOptions {
    status?: BatchStatus;
    limit?: number;
}

// ---------------------------------------------------------------------------
// ManifestStore (MongoDB)
// ---------------------------------------------------------------------------

interface Collections {
    runs: Collection<Run>;
    batches: Collection<Batch>;
    skipSamples: Collection<SkipSample>;
    events: Collection<EventRecord>;
}

export class ManifestStore {
    private client: MongoClient;
    private readonly dbName: string;
    private collections: Collections | null = null;
    private _lastWriteLatencyMs = 0;

    constructor(uri: string, dbName: string) {
        this.dbName = dbName;
        this.client = new MongoClient(uri, {
            writeConcern: { w: "majority", journal: true },
        });
    }

    async connect(): Promise<void> {
        await this.client.connect();
        const db = this.client.db(this.dbName);

        this.collections = {
            runs: db.collection<Run>("mig_runs"),
            batches: db.collection<Batch>("mig_batches"),
            skipSamples: db.collection<SkipSample>("mig_skip_samples"),
            events: db.collection<EventRecord>("mig_events"),
        };

        // Ensure indexes
        await this.collections.runs.createIndex({ run_id: 1 }, { unique: true });
        await this.collections.runs.createIndex({ status: 1 });
        await this.collections.batches.createIndex({ run_id: 1, batch_seq: 1 }, { unique: true });
        await this.collections.batches.createIndex({ run_id: 1, status: 1 });
        await this.collections.skipSamples.createIndex({ run_id: 1, batch_seq: 1 });
        await this.collections.events.createIndex({ run_id: 1, created_at: 1 });
    }

    private ensureConnected(): Collections {
        if (!this.collections) {
            throw new Error("ManifestStore is not connected. Call connect() first.");
        }
        return this.collections;
    }

    // -----------------------------------------------------------------------
    // Runs
    // -----------------------------------------------------------------------

    async createRun(run: Omit<Run, "last_committed_cursor" | "updated_at" | "summary"> & {
        last_committed_cursor?: string | null;
        updated_at?: string;
    }): Promise<void> {
        const { runs } = this.ensureConnected();
        const now = new Date().toISOString();
        await runs.insertOne({
            ...run,
            last_committed_cursor: run.last_committed_cursor ?? null,
            updated_at: run.updated_at ?? now,
            summary: null,
        } as Run);
    }

    async getRun(runId: string): Promise<Run | undefined> {
        const { runs } = this.ensureConnected();
        const doc = await runs.findOne({ run_id: runId }, { projection: { _id: 0 } });
        return doc ?? undefined;
    }

    async listRuns(opts: {
        status?: RunStatus;
        limit?: number;
        offset?: number;
    } = {}): Promise<{ runs: Run[]; total: number }> {
        const { runs } = this.ensureConnected();
        const filter: { status?: RunStatus } = {};
        if (opts.status !== undefined) {
            filter.status = opts.status;
        }
        const limit = opts.limit ?? 20;
        const offset = opts.offset ?? 0;

        const [docs, total] = await Promise.all([
            runs.find(filter, { projection: { _id: 0 } })
                .sort({ updated_at: -1 })
                .skip(offset)
                .limit(limit)
                .toArray() as Promise<Run[]>,
            runs.countDocuments(filter),
        ]);

        return { runs: docs, total };
    }

    async getActiveRun(sourceNs?: string, targetTable?: string): Promise<Run | undefined> {
        const { runs } = this.ensureConnected();
        const filter: Record<string, unknown> = { status: "active" };
        if (sourceNs) filter.source_ns = sourceNs;
        if (targetTable) filter.target_table = targetTable;
        const doc = await runs.findOne(filter, { projection: { _id: 0 } });
        return doc ?? undefined;
    }

    async getResumableRun(
        sourceNs: string,
        targetTable: string,
        transformVersion: string,
    ): Promise<Run | undefined> {
        const { runs } = this.ensureConnected();
        const doc = await runs.findOne(
            {
                status: { $in: ["paused", "stopped"] as RunStatus[] },
                source_ns: sourceNs,
                target_table: targetTable,
                transform_version: transformVersion,
            },
            { sort: { updated_at: -1 }, projection: { _id: 0 } },
        );
        return doc ?? undefined;
    }

    async updateRunLastCommittedCursor(runId: string, lastCommittedCursor: string): Promise<void> {
        const { runs } = this.ensureConnected();
        const now = new Date().toISOString();
        const start = performance.now();
        await runs.updateOne(
            { run_id: runId },
            { $set: { last_committed_cursor: lastCommittedCursor, updated_at: now } },
        );
        this._lastWriteLatencyMs = Math.round(performance.now() - start);
    }

    async updateRunStatus(runId: string, status: RunStatus): Promise<void> {
        const { runs } = this.ensureConnected();
        const now = new Date().toISOString();
        await runs.updateOne(
            { run_id: runId },
            { $set: { status, updated_at: now } },
        );
    }

    async writeSummary(runId: string, status: RunStatus, summary: RunSummary): Promise<void> {
        const { runs } = this.ensureConnected();
        const now = new Date().toISOString();
        await runs.updateOne(
            { run_id: runId },
            { $set: { status, summary, updated_at: now } },
        );
    }

    // -----------------------------------------------------------------------
    // Batches
    // -----------------------------------------------------------------------

    async insertBatch(batch: Omit<Batch, "error_history" | "digest_match"> & {
        error_history?: CompactError[];
        digest_match?: boolean | null;
    }): Promise<void> {
        const { batches } = this.ensureConnected();
        await batches.insertOne({
            ...batch,
            error_history: batch.error_history ?? [],
            digest_match: batch.digest_match ?? null,
        } as Batch);
    }

    async updateBatchStatus(
        runId: string,
        batchSeq: number,
        status: BatchStatus,
        error?: string,
    ): Promise<void> {
        const { batches } = this.ensureConnected();
        const now = new Date().toISOString();
        const $set: Record<string, unknown> = { status };

        // Only set finished_at for terminal statuses
        if (status === "done" || status === "failed" || status === "skipped_empty") {
            $set.finished_at = now;
        }

        if (error !== undefined) {
            $set.last_error = error;
        }

        await batches.updateOne(
            { run_id: runId, batch_seq: batchSeq },
            error !== undefined
                ? { $set, $inc: { retry_count: 1 } }
                : { $set },
        );
    }

    /**
     * Mark a batch as done and advance the run's last_committed_cursor.
     * Two-step write (not transactional) — the resume path's `getLastDoneBatch`
     * fallback compensates if the process crashes between the two writes.
     */
    async completeBatch(
        runId: string,
        batchSeq: number,
        lastCommittedCursor: string,
    ): Promise<void> {
        const { batches, runs } = this.ensureConnected();
        const now = new Date().toISOString();
        const start = performance.now();

        await batches.updateOne(
            { run_id: runId, batch_seq: batchSeq },
            { $set: { status: "done" as BatchStatus, finished_at: now } },
        );

        await runs.updateOne(
            { run_id: runId },
            { $set: { last_committed_cursor: lastCommittedCursor, updated_at: now } },
        );

        this._lastWriteLatencyMs = Math.round(performance.now() - start);
    }

    /**
     * Insert a completed batch and advance the run cursor in one go.
     * Skips the intermediate "prepared"/"inflight" states — the batch record
     * is written only after successful ClickHouse insertion.
     */
    async insertCompletedBatch(
        batch: Omit<Batch, "error_history" | "digest_match">,
        lastCommittedCursor: string,
    ): Promise<void> {
        const { batches, runs } = this.ensureConnected();
        const now = new Date().toISOString();
        const start = performance.now();

        const doc = {
            ...batch,
            status: "done" as BatchStatus,
            finished_at: now,
            error_history: [],
            digest_match: null,
        };

        await batches.insertOne(doc as Batch);

        await runs.updateOne(
            { run_id: batch.run_id },
            { $set: { last_committed_cursor: lastCommittedCursor, updated_at: now } },
        );

        this._lastWriteLatencyMs = Math.round(performance.now() - start);
    }

    /**
     * Bulk insert completed batch records (used by async batch writer).
     */
    async bulkInsertBatches(batchDocs: Batch[]): Promise<void> {
        if (batchDocs.length === 0) return;
        const { batches } = this.ensureConnected();
        const start = performance.now();
        // Use upsert to handle duplicates — if (run_id, batch_seq) exists, update it
        const ops = batchDocs.map(doc => ({
            updateOne: {
                filter: { run_id: doc.run_id, batch_seq: doc.batch_seq },
                update: { $set: doc },
                upsert: true,
            },
        }));
        await batches.bulkWrite(ops, { ordered: false });
        this._lastWriteLatencyMs = Math.round(performance.now() - start);
    }

    /**
     * Advance the run cursor to the latest position (used by async batch writer).
     */
    async advanceCursor(runId: string, cursor: string): Promise<void> {
        const { runs } = this.ensureConnected();
        const now = new Date().toISOString();
        await runs.updateOne(
            { run_id: runId },
            { $set: { last_committed_cursor: cursor, updated_at: now } },
        );
    }

    async updateBatchDigestMatch(
        runId: string,
        batchSeq: number,
        digestMatch: boolean,
    ): Promise<void> {
        const { batches } = this.ensureConnected();
        await batches.updateOne(
            { run_id: runId, batch_seq: batchSeq },
            { $set: { digest_match: digestMatch } },
        );
    }

    async pushBatchError(
        runId: string,
        batchSeq: number,
        error: CompactError,
    ): Promise<void> {
        const { batches } = this.ensureConnected();
        await batches.updateOne(
            { run_id: runId, batch_seq: batchSeq },
            {
                $push: { error_history: { $each: [error], $slice: -50 } as any },
                $set: { last_error: error.error },
            },
        );
    }

    async getLastBatch(runId: string): Promise<Batch | null> {
        const { batches } = this.ensureConnected();
        const doc = await batches.findOne(
            { run_id: runId },
            { sort: { batch_seq: -1 }, projection: { _id: 0 } },
        );
        return doc ?? null;
    }

    async getLastDoneBatch(runId: string): Promise<Batch | null> {
        const { batches } = this.ensureConnected();
        const doc = await batches.findOne(
            { run_id: runId, status: "done" },
            { sort: { batch_seq: -1 }, projection: { _id: 0 } },
        );
        return doc ?? null;
    }

    async existsCompletedRun(sourceNs: string, targetTable: string): Promise<boolean> {
        const { runs } = this.ensureConnected();
        const count = await runs.countDocuments(
            { status: "completed", source_ns: sourceNs, target_table: targetTable },
            { limit: 1 },
        );
        return count > 0;
    }

    /** Get the most recent completed run for a given sourceNs/targetTable. */
    async getCompletedRun(sourceNs: string, targetTable: string): Promise<Run | null> {
        const { runs } = this.ensureConnected();
        const doc = await runs.findOne(
            { status: "completed", source_ns: sourceNs, target_table: targetTable },
            { sort: { created_at: -1 }, projection: { _id: 0 } },
        );
        return (doc as Run | null) ?? null;
    }

    /** Sum source_docs_read and rows_to_insert from all done batches for a run. */
    async sumCompletedBatchStats(runId: string): Promise<{ docsRead: number; rowsInserted: number }> {
        const { batches } = this.ensureConnected();
        const pipeline = [
            { $match: { run_id: runId, status: "done" } },
            { $group: { _id: null, docsRead: { $sum: "$source_docs_read" }, rowsInserted: { $sum: "$rows_to_insert" } } },
        ];
        const result = await batches.aggregate(pipeline).toArray();
        if (result.length === 0) return { docsRead: 0, rowsInserted: 0 };
        return { docsRead: result[0].docsRead ?? 0, rowsInserted: result[0].rowsInserted ?? 0 };
    }

    async getBatches(runId: string, opts: GetBatchesOptions = {}): Promise<Batch[]> {
        const { batches } = this.ensureConnected();
        const filter: { run_id: string; status?: BatchStatus } = { run_id: runId };
        if (opts.status !== undefined) {
            filter.status = opts.status;
        }
        let cursor = batches.find(filter, { projection: { _id: 0 } }).sort({ batch_seq: 1 });
        if (opts.limit !== undefined) {
            cursor = cursor.limit(opts.limit);
        }
        return cursor.toArray() as Promise<Batch[]>;
    }

    async getFailedBatches(runId: string): Promise<Batch[]> {
        const { batches } = this.ensureConnected();
        return batches.find(
            { run_id: runId, status: "failed" },
            { projection: { _id: 0 } },
        ).sort({ batch_seq: 1 }).toArray() as Promise<Batch[]>;
    }

    // -----------------------------------------------------------------------
    // Skip samples
    // -----------------------------------------------------------------------

    async insertSkipSample(sample: SkipSample): Promise<void> {
        const { skipSamples } = this.ensureConnected();
        await skipSamples.insertOne({ ...sample });
    }

    async insertSkipSamples(samples: SkipSample[]): Promise<void> {
        if (samples.length === 0) return;
        const { skipSamples } = this.ensureConnected();
        await skipSamples.insertMany(samples);
    }

    /**
     * Delete all batch records and skip samples for a given run.
     * Used when starting a fresh (non-resume) run to prevent stale data.
     */
    async deleteRunData(runId: string): Promise<number> {
        const { batches, skipSamples, events } = this.ensureConnected();
        const [bResult, sResult, eResult] = await Promise.all([
            batches.deleteMany({ run_id: runId }),
            skipSamples.deleteMany({ run_id: runId }),
            events.deleteMany({ run_id: runId }),
        ]);
        return (bResult.deletedCount ?? 0) + (sResult.deletedCount ?? 0) + (eResult.deletedCount ?? 0);
    }

    // -----------------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------------

    async insertEvent(event: EventRecord): Promise<void> {
        const { events } = this.ensureConnected();
        await events.insertOne({ ...event });
    }

    async countEvents(runId: string, eventType?: string): Promise<number> {
        const { events } = this.ensureConnected();
        const filter: { run_id: string; event_type?: string } = { run_id: runId };
        if (eventType !== undefined) {
            filter.event_type = eventType;
        }
        return events.countDocuments(filter);
    }

    // -----------------------------------------------------------------------
    // Telemetry
    // -----------------------------------------------------------------------

    getWriteLatency(): number {
        return this._lastWriteLatencyMs;
    }

    // -----------------------------------------------------------------------
    // Health
    // -----------------------------------------------------------------------

    async isWritable(): Promise<boolean> {
        try {
            const db = this.client.db(this.dbName);
            await db.command({ ping: 1 });
            return true;
        } catch {
            return false;
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    async close(): Promise<void> {
        await this.client.close();
        this.collections = null;
    }
}
