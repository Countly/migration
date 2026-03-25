import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { setTimeout as sleep } from "node:timers/promises";
import type { Logger } from "pino";

import type { ManifestStore, Batch, BatchStatus, RunSummary, CompactError } from "../state/manifest-store.ts";
import type { RedisHotState, VerboseError } from "../state/redis-hot-state.ts";
import type { MongoReader } from "../source/mongo-reader.ts";
import type { ClickHouseWriter } from "../target/clickhouse-writer.ts";
import type { ClickHousePressure, BackpressureConfig } from "../target/clickhouse-pressure.ts";
import type { GcController } from "./gc-controller.ts";
import type { RetryPolicy } from "./retry-policy.ts";
import { SkipCounter, type SkipReason } from "../transform/skip-reasons.ts";
import { transformBatch, type OutputRow } from "../transform/normalize.ts";
import { type Cursor, deserializeCursor, serializeCursor } from "../types/cursor.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface BatchRunnerConfig {
  runId: string;
  transformVersion: string;
  sourceNs: string;
  targetTable: string;
  upperBoundId: string;
  batchRowsTarget: number;
  backpressure: BackpressureConfig;
  useDedupToken: boolean;
  database: string;
  table: string;
  snapshotInterval: number;
}

export interface BatchRunnerDeps {
  manifestStore: ManifestStore;
  redisState: RedisHotState;
  mongoReader: MongoReader;
  chWriter: ClickHouseWriter;
  chPressure: ClickHousePressure;
  gcController: GcController;
  retryPolicy: RetryPolicy;
  logger: Logger;
  config: BatchRunnerConfig;
}

export type RunnerStatus =
  | "idle"
  | "running"
  | "paused"
  | "stopping"
  | "stopped"
  | "completed"
  | "failed";

export interface BatchRunnerStats {
  status: RunnerStatus;
  batchSeq: number;
  lastCommittedId: string | null;
  totalDocsRead: number;
  totalRowsInserted: number;
  totalDocsSkipped: number;
  skipsByReason: Record<SkipReason, number>;
  elapsedMs: number;
  docsPerSecond: number;
  rowsPerSecond: number;
  batchesFailed: number;
  digestMismatches: number;
  estimatedDuplicateRows: number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Normalize unknown catch values to Error instances. */
function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

/** Build a Batch record with common defaults, overridden by specifics. */
function buildBatch(
  runId: string,
  batchSeq: number,
  lowerExclusiveId: string,
  upperInclusiveId: string,
  docsRead: number,
  docsSkipped: number,
  overrides: Partial<Batch>,
): Batch {
  return {
    run_id: runId,
    batch_seq: batchSeq,
    lower_exclusive_cursor: lowerExclusiveId,
    upper_inclusive_cursor: upperInclusiveId,
    source_docs_read: docsRead,
    docs_skipped: docsSkipped,
    rows_to_insert: 0,
    payload_digest: "",
    insert_dedup_token: "",
    query_id: "",
    status: "prepared",
    retry_count: 0,
    last_error: null,
    started_at: new Date().toISOString(),
    finished_at: null,
    error_history: [],
    digest_match: null,
    ...overrides,
  };
}

/**
 * Compute a SHA-256 hex digest over an array of rows.
 *
 * Each row is serialised with deterministic key ordering (JSON.stringify
 * with sorted keys) and rows are delimited by newlines, giving a
 * reproducible digest regardless of JS object insertion order.
 */
function computePayloadDigest(rows: OutputRow[]): string {
  const hash = createHash("sha256");
  for (const row of rows) {
    hash.update(canonicalJson(row));
    hash.update("\n");
  }
  return hash.digest("hex");
}

/**
 * Produce a canonical (deterministic) JSON string by sorting keys
 * recursively.  This ensures the digest is stable across JS engines
 * and object-creation paths.
 */
function canonicalJson(obj: unknown): string {
  return JSON.stringify(obj, (_key, value) => {
    if (value !== null && typeof value === "object" && !Array.isArray(value)) {
      const sorted: Record<string, unknown> = {};
      for (const k of Object.keys(value as Record<string, unknown>).sort()) {
        sorted[k] = (value as Record<string, unknown>)[k];
      }
      return sorted;
    }
    return value;
  });
}

// ---------------------------------------------------------------------------
// BatchRunner
// ---------------------------------------------------------------------------

/**
 * Core batch lifecycle orchestrator.
 *
 * The runner implements an idempotent, crash-safe processing loop:
 *
 *   1. Read a page from MongoDB.
 *   2. Transform the documents.
 *   3. Persist a batch manifest (MongoDB) in `prepared` state.
 *   4. Mark `inflight`, insert into ClickHouse.
 *   5. On success: mark `done` in manifest first, then update Redis.
 *
 * MongoDB manifest is authoritative; Redis is rebuildable.
 */
export class BatchRunner {
  private status: RunnerStatus = "idle";
  private batchSeq = 0;
  private lastCommittedId: string | null = null;
  private skipCounter: SkipCounter;
  private totalRowsInserted = 0;
  private totalDocsRead = 0;
  private startedAt: number = 0;
  private readonly emitter = new EventEmitter();

  private batchesFailed = 0;
  private batchesSkippedEmpty = 0;
  private digestMismatches = 0;
  private estimatedDuplicateRows = 0;

  private readonly deps: BatchRunnerDeps;
  private readonly logger: Logger;

  constructor(deps: BatchRunnerDeps) {
    this.deps = deps;
    this.logger = deps.logger.child({ component: "BatchRunner" });
    this.skipCounter = new SkipCounter();
  }

  // -----------------------------------------------------------------------
  // Public API
  // -----------------------------------------------------------------------

  /** Start the batch processing loop. */
  async run(): Promise<void> {
    if (this.status === "running") {
      throw new Error("BatchRunner is already running");
    }

    this.status = "running";
    this.startedAt = Date.now();
    const { runId, upperBoundId } = this.deps.config;

    this.logger.info({ runId, upperBoundId }, "Batch runner starting");

    try {
      // ------------------------------------------------------------------
      // 1. Resume check: look for interrupted batches
      // ------------------------------------------------------------------
      await this.resumeFromInterruption();

      // ------------------------------------------------------------------
      // 2. Main loop
      // ------------------------------------------------------------------
      while ((this.status as RunnerStatus) === "running" || (this.status as RunnerStatus) === "paused") {
        // (a) Check commands (pause / stop-after-batch)
        await this.checkCommands();

        const currentStatus = this.status as RunnerStatus;
        if (currentStatus === "paused") {
          this.logger.info("Runner paused, waiting for resume");
          await this.waitForResume();
          if ((this.status as RunnerStatus) !== "running") break;
        }

        if (currentStatus === "stopping" || currentStatus === "stopped") {
          break;
        }

        // (b) Sample backpressure; wait if pressured
        if (this.deps.config.backpressure.enabled) {
          await this.waitForBackpressure();
          if (this.status !== "running") break;
        }

        // (c) Read next Mongo page
        const page = await this.deps.retryPolicy.execute(
          () =>
            this.deps.mongoReader.readPage(
              this.lastCommittedId ? deserializeCursor(this.lastCommittedId) : null,
              deserializeCursor(upperBoundId),
            ),
          `mongo-read-batch-${this.batchSeq}`,
          this.logger,
        );

        // (d) Empty page = run complete
        if (page.docs.length === 0) {
          this.logger.info(
            {
              totalBatches: this.batchSeq,
              totalDocsRead: this.totalDocsRead,
              totalRowsInserted: this.totalRowsInserted,
            },
            "All documents processed, run complete",
          );

          const summary = this.buildSummary("completed");
          await this.deps.manifestStore.writeSummary(runId, "completed", summary);

          await this.bestEffortRedis(
            () => this.deps.redisState.setState(runId, {
              runId,
              status: "completed",
              sourceNs: this.deps.config.sourceNs,
              targetTable: this.deps.config.targetTable,
              upperBoundCursor: upperBoundId,
              lastCommittedCursor: this.lastCommittedId,
              transformVersion: this.deps.config.transformVersion,
              totalBatches: this.batchSeq,
              completedBatches: this.batchSeq - this.batchesFailed,
              startedAt: new Date(this.startedAt).toISOString(),
            }),
            "Redis setState failed on completion",
          );

          this.setTerminalStatus("completed");
          break;
        }

        this.totalDocsRead += page.docs.length;
        this.batchSeq++;

        // (e) Transform docs
        const { rows, skippedSamples } = transformBatch(
          page.docs,
          this.skipCounter,
        );

        // Record skip samples in manifest
        for (const sample of skippedSamples) {
          await this.deps.manifestStore.insertSkipSample({
            run_id: runId,
            batch_seq: this.batchSeq,
            doc_id: sample._id,
            reason: sample.reason,
            captured_at: new Date().toISOString(),
          });
        }

        const lowerExclusiveId = this.lastCommittedId ?? "";
        const upperInclusiveId = serializeCursor(page.lastCursor!);
        const docsSkipped = page.docs.length - rows.length;

        // (f) All docs skipped -> record skipped_empty batch, advance
        if (rows.length === 0) {
          this.logger.info(
            { batchSeq: this.batchSeq, docsRead: page.docs.length, docsSkipped },
            "All documents in batch were skipped",
          );

          const batch = buildBatch(runId, this.batchSeq, lowerExclusiveId, upperInclusiveId, page.docs.length, docsSkipped, {
            status: "skipped_empty",
            finished_at: new Date().toISOString(),
          });

          await this.deps.manifestStore.insertBatch(batch);
          this.lastCommittedId = upperInclusiveId;

          // Advance run cursor
          await this.deps.manifestStore.updateRunLastCommittedCursor(runId, upperInclusiveId);
          await this.bestEffortRedis(
            () => this.deps.redisState.markBatchDone(runId, this.batchSeq),
            "Redis markBatchDone failed (continuing)",
          );

          this.batchesSkippedEmpty++;
          continue;
        }

        // (g) Build batch manifest
        const payloadDigest = computePayloadDigest(rows);
        const queryId = `mig__${runId}__${this.batchSeq}`;
        const dedupToken = this.deps.config.useDedupToken
          ? `mig:${runId}:${this.batchSeq}`
          : "";

        const batch = buildBatch(runId, this.batchSeq, lowerExclusiveId, upperInclusiveId, page.docs.length, docsSkipped, {
          rows_to_insert: rows.length,
          payload_digest: payloadDigest,
          insert_dedup_token: dedupToken,
          query_id: queryId,
        });

        // (h) Persist manifest as 'prepared'
        await this.deps.manifestStore.insertBatch(batch);

        // (i) Mark 'inflight'
        await this.deps.manifestStore.updateBatchStatus(
          runId,
          this.batchSeq,
          "inflight",
        );

        // (j) Insert into ClickHouse
        try {
          const currentBatchSeq = this.batchSeq;
          const result = await this.deps.retryPolicy.execute(
            () =>
              this.deps.chWriter.insertBatch({
                runId,
                batchSeq: currentBatchSeq,
                rows,
              }),
            `ch-insert-batch-${currentBatchSeq}`,
            this.logger,
            async (attempt, err) => {
              const now = new Date().toISOString();

              // Tier 1: Compact error on batch document
              await this.deps.manifestStore.pushBatchError(runId, currentBatchSeq, {
                attempt,
                error: err.message.slice(0, 200),
                timestamp: now,
              }).catch(() => {});

              // Tier 2: Audit event
              await this.deps.manifestStore.insertEvent({
                run_id: runId,
                event_type: "batch_retry_error",
                message: `Batch ${currentBatchSeq} attempt ${attempt} failed: ${err.message.slice(0, 200)}`,
                metadata: { batch_seq: currentBatchSeq, attempt },
                created_at: now,
              }).catch(() => {});

              // Tier 3: Verbose error to Redis
              await this.bestEffortRedis(
                () => this.deps.redisState.pushVerboseError(runId, currentBatchSeq, {
                  attempt,
                  error: err.message,
                  stack: err.stack ?? null,
                  timestamp: now,
                  context: { queryId: `mig__${runId}__${currentBatchSeq}`, rowCount: rows.length },
                }),
                "Redis pushVerboseError failed",
              );
            },
          );

          // (k) On success: atomically mark batch done + advance cursor
          await this.deps.manifestStore.completeBatch(runId, this.batchSeq, upperInclusiveId);
          this.lastCommittedId = upperInclusiveId;
          this.totalRowsInserted += result.rowsInserted;

          // Redis is rebuildable (continue if Redis fails)
          await this.bestEffortRedis(
            async () => {
              await this.deps.redisState.markBatchDone(runId, this.batchSeq);
              await this.deps.redisState.updateStats(runId, {
                docsRead: this.totalDocsRead,
                docsSkipped: this.skipCounter.getTotal(),
                rowsInserted: this.totalRowsInserted,
                batchesDone: this.batchSeq - this.batchesFailed,
                batchesFailed: this.batchesFailed,
                batchesInflight: 0,
                elapsedMs: Date.now() - this.startedAt,
                docsPerSecond: ((Date.now() - this.startedAt) / 1000) > 0
                  ? this.totalDocsRead / ((Date.now() - this.startedAt) / 1000)
                  : 0,
                lastBatchSeq: this.batchSeq,
                lastBatchFinishedAt: new Date().toISOString(),
              });
            },
            "Redis update failed after batch success (batch is durably committed, continuing)",
            { batchSeq: this.batchSeq },
          );

          // Timeline snapshot (every N batches)
          if (this.batchSeq % this.deps.config.snapshotInterval === 0) {
            const elapsedSec = (Date.now() - this.startedAt) / 1000;
            await this.bestEffortRedis(
              () => this.deps.redisState.pushTimelineSnapshot(runId, {
                timestamp: new Date().toISOString(),
                batch_seq: this.batchSeq,
                docs_read: this.totalDocsRead,
                rows_inserted: this.totalRowsInserted,
                docs_skipped: this.skipCounter.getTotal(),
                docs_per_second: elapsedSec > 0 ? this.totalDocsRead / elapsedSec : 0,
                rows_per_second: elapsedSec > 0 ? this.totalRowsInserted / elapsedSec : 0,
                skip_reasons: this.skipCounter.getCounts(),
                digest_mismatches: this.digestMismatches,
                estimated_duplicate_rows: this.estimatedDuplicateRows,
                batches_failed: this.batchesFailed,
                heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
                rss_mb: Math.round(process.memoryUsage().rss / 1024 / 1024),
              }),
              "Redis timeline snapshot failed (continuing)",
            );
          }

          this.logger.info(
            {
              batchSeq: this.batchSeq,
              rowsInserted: result.rowsInserted,
              insertMs: Math.round(result.insertMs),
              lastId: upperInclusiveId,
            },
            "Batch completed",
          );
        } catch (err) {
          // Insert failed after all retries
          const error = toError(err);
          await this.deps.manifestStore.updateBatchStatus(runId, this.batchSeq, "failed", error.message);

          await this.bestEffortRedis(
            () => this.deps.redisState.pushError(runId, {
              batchSeq: this.batchSeq,
              error: error.message,
              timestamp: new Date().toISOString(),
              retryCount: this.deps.retryPolicy.maxRetries,
            }),
            "Redis pushError failed (continuing)",
          );

          this.logger.error(
            { batchSeq: this.batchSeq, error: error.message },
            "Batch failed after all retries",
          );

          this.batchesFailed++;
          const summary = this.buildSummary("failed");
          await this.deps.manifestStore.writeSummary(runId, "failed", summary);
          this.setTerminalStatus("failed");
          break;
        }

        // (l) Release batch data references (let V8 collect them)
        // Rows array and page docs go out of scope naturally here.

        // (m) Conditional GC check
        if (this.deps.gcController.shouldRunAfterBatch(this.batchSeq)) {
          await this.deps.gcController.runGc(
            "after-batch",
            `post-batch-${this.batchSeq}`,
          );
        } else if (this.deps.gcController.isPending) {
          await this.deps.gcController.runGc(
            "after-batch",
            `pending-gc-after-batch-${this.batchSeq}`,
          );
        }

        // (n) Next batch (loop continues)
      }

      // Handle graceful stop
      if ((this.status as RunnerStatus) === "stopping") {
        const summary = this.buildSummary("stopped");
        await this.deps.manifestStore.writeSummary(runId, "stopped", summary);
        await this.bestEffortRedis(
          () => this.deps.redisState.setState(runId, {
            runId,
            status: "stopped",
            sourceNs: this.deps.config.sourceNs,
            targetTable: this.deps.config.targetTable,
            upperBoundCursor: upperBoundId,
            lastCommittedCursor: this.lastCommittedId,
            transformVersion: this.deps.config.transformVersion,
            totalBatches: this.batchSeq,
            completedBatches: this.batchSeq - this.batchesFailed,
            startedAt: new Date(this.startedAt).toISOString(),
          }),
          "Redis setState failed on stop",
        );
        this.setTerminalStatus("stopped");
      }
    } catch (err) {
      const error = toError(err);
      this.logger.error({ error: error.message }, "BatchRunner fatal error");
      this.setTerminalStatus("failed");

      try {
        const summary = this.buildSummary("failed");
        await this.deps.manifestStore.writeSummary(
          this.deps.config.runId, "failed", summary,
        );
      } catch (summaryErr) {
        this.logger.warn(
          { error: toError(summaryErr).message },
          "Failed to write run summary on fatal error (continuing with throw)",
        );
      }

      try {
        await this.deps.manifestStore.insertEvent({
          run_id: this.deps.config.runId,
          event_type: "fatal_error",
          message: error.message,
          metadata: { stack: error.stack },
          created_at: new Date().toISOString(),
        });
      } catch (eventErr) {
        this.logger.warn(
          { error: toError(eventErr).message },
          "Failed to insert fatal error event (continuing with throw)",
        );
      }

      throw error;
    }
  }

  /** Pause after current batch completes. */
  pause(): void {
    if (this.status === "running") {
      this.logger.info("Pause requested");
      this.status = "paused";
      this.deps.manifestStore.updateRunStatus(this.deps.config.runId, "paused").catch(() => {});
    }
  }

  /** Resume from pause. */
  resume(): void {
    if (this.status === "paused") {
      this.logger.info("Resume requested");
      this.status = "running";
      this.emitter.emit("resumed");
      this.deps.manifestStore.updateRunStatus(this.deps.config.runId, "active").catch(() => {});
    }
  }

  /** Stop after current batch completes. */
  stopAfterBatch(): void {
    if (this.status === "running" || this.status === "paused") {
      this.logger.info("Stop-after-batch requested");
      this.status = "stopping";
      this.emitter.emit("resumed"); // unblock waitForResume if paused
    }
  }

  /** Get current status. */
  getStatus(): RunnerStatus {
    return this.status;
  }

  /** Returns a promise that resolves when the runner reaches stopped or failed state. */
  waitForStop(): Promise<void> {
    if (this.status === "stopped" || this.status === "failed" || this.status === "completed") {
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      this.emitter.once("stopped", resolve);
    });
  }

  /** Get current batch sequence number. */
  getCurrentBatchSeq(): number {
    return this.batchSeq;
  }

  /** Get throughput stats. */
  getStats(): BatchRunnerStats {
    const elapsedMs = this.startedAt > 0 ? Date.now() - this.startedAt : 0;
    const elapsedSec = elapsedMs / 1000;

    return {
      status: this.status,
      batchSeq: this.batchSeq,
      lastCommittedId: this.lastCommittedId,
      totalDocsRead: this.totalDocsRead,
      totalRowsInserted: this.totalRowsInserted,
      totalDocsSkipped: this.skipCounter.getTotal(),
      skipsByReason: this.skipCounter.getCounts(),
      elapsedMs,
      docsPerSecond: elapsedSec > 0 ? this.totalDocsRead / elapsedSec : 0,
      rowsPerSecond:
        elapsedSec > 0 ? this.totalRowsInserted / elapsedSec : 0,
      batchesFailed: this.batchesFailed,
      digestMismatches: this.digestMismatches,
      estimatedDuplicateRows: this.estimatedDuplicateRows,
    };
  }

  // -----------------------------------------------------------------------
  // Private helpers
  // -----------------------------------------------------------------------

  /** Set terminal status and notify waiters. */
  private setTerminalStatus(status: "stopped" | "failed" | "completed"): void {
    this.status = status as RunnerStatus;
    this.emitter.emit("stopped");
  }

  /** Best-effort Redis call — logs warning and continues on failure. */
  private async bestEffortRedis(
    fn: () => Promise<unknown>,
    msg: string,
    ctx?: Record<string, unknown>,
  ): Promise<void> {
    try { await fn(); }
    catch (err) { this.logger.warn({ error: toError(err).message, ...ctx }, msg); }
  }

  private buildSummary(
    _status: "completed" | "failed" | "stopped",
  ): RunSummary {
    const elapsedMs = this.startedAt > 0 ? Date.now() - this.startedAt : 0;
    const elapsedSec = elapsedMs / 1000;
    const batchesDone = this.batchSeq - this.batchesFailed;

    return {
      finished_at: new Date().toISOString(),
      duration_ms: elapsedMs,
      total_docs_read: this.totalDocsRead,
      total_rows_inserted: this.totalRowsInserted,
      total_docs_skipped: this.skipCounter.getTotal(),
      avg_docs_per_second: elapsedSec > 0 ? this.totalDocsRead / elapsedSec : 0,
      avg_rows_per_second: elapsedSec > 0 ? this.totalRowsInserted / elapsedSec : 0,
      total_batches: this.batchSeq,
      batches_done: batchesDone,
      batches_failed: this.batchesFailed,
      batches_skipped_empty: this.batchesSkippedEmpty,
      skip_reasons: this.skipCounter.getCounts(),
      total_errors: this.batchesFailed,
      failed_batch_seqs: [],
      digest_mismatches: this.digestMismatches,
      estimated_duplicate_rows: this.estimatedDuplicateRows,
      coverage_pct: this.batchSeq > 0 ? (batchesDone / this.batchSeq) * 100 : 0,
    };
  }

  /**
   * Resume from an interrupted run (spec §15.1, §15.2).
   *
   * For each non-done batch (inflight or prepared):
   *  1. Re-read the exact _id range from MongoDB
   *  2. Re-transform and recompute SHA-256 digest
   *  3. If digest matches manifest → retry insert (CH dedup handles idempotency)
   *  4. If digest doesn't match → log warning and continue (lenient mode)
   *
   * This handles both:
   *  - §15.1: Crash before insert (batch is prepared or inflight, insert never ack'd)
   *  - §15.2: Crash after CH success but before checkpoint (CH dedup ignores the retry)
   */
  private async resumeFromInterruption(): Promise<void> {
    const { runId, upperBoundId } = this.deps.config;
    const run = await this.deps.manifestStore.getRun(runId);

    if (!run) {
      this.logger.info({ runId }, "No existing run found, starting fresh");
      return;
    }

    this.lastCommittedId = run.last_committed_cursor;

    // Derive cursor from highest done batch as fallback (handles crash between
    // updateBatchStatus("done") and updateRunLastCommittedCursor)
    const lastDoneBatch = await this.deps.manifestStore.getLastDoneBatch(runId);
    if (lastDoneBatch?.upper_inclusive_cursor) {
      this.lastCommittedId = lastDoneBatch.upper_inclusive_cursor;
    }

    // Find batches that need recovery (inflight or prepared)
    const inflightBatches = await this.deps.manifestStore.getBatches(runId, {
      status: "inflight",
    });
    const preparedBatches = await this.deps.manifestStore.getBatches(runId, {
      status: "prepared",
    });
    const recoverableBatches = [...preparedBatches, ...inflightBatches]
      .sort((a, b) => a.batch_seq - b.batch_seq);

    for (const batch of recoverableBatches) {
      this.logger.info(
        { batchSeq: batch.batch_seq, status: batch.status },
        "Attempting to recover interrupted batch",
      );

      try {
        // Step 1: Re-read the exact source range
        const page = await this.deps.mongoReader.readPage(
          batch.lower_exclusive_cursor ? deserializeCursor(batch.lower_exclusive_cursor) : null,
          deserializeCursor(batch.upper_inclusive_cursor),
        );

        // Step 2: Re-transform
        const { rows } = transformBatch(page.docs, this.skipCounter);

        // Step 3: Recompute digest
        const newDigest = computePayloadDigest(rows);

        // Step 4: Compare with stored digest (lenient mode)
        const digestMatched = newDigest === batch.payload_digest;

        if (newDigest !== batch.payload_digest) {
          this.digestMismatches++;
          this.estimatedDuplicateRows += batch.rows_to_insert;

          await this.deps.manifestStore.insertEvent({
            run_id: runId,
            event_type: "digest_mismatch",
            message: `Batch ${batch.batch_seq}: source data changed, continuing (lenient mode)`,
            metadata: {
              batch_seq: batch.batch_seq,
              stored_digest: batch.payload_digest,
              computed_digest: newDigest,
              original_rows: batch.rows_to_insert,
              new_rows: rows.length,
              estimated_duplicates: batch.rows_to_insert,
            },
            created_at: new Date().toISOString(),
          });

          await this.bestEffortRedis(
            () => this.deps.redisState.pushVerboseError(runId, batch.batch_seq, {
              attempt: 0,
              error: `Digest mismatch: stored=${batch.payload_digest} computed=${newDigest}`,
              stack: null,
              timestamp: new Date().toISOString(),
              context: {
                stored_digest: batch.payload_digest,
                computed_digest: newDigest,
                original_rows: batch.rows_to_insert,
                new_rows: rows.length,
              },
            }),
            "Redis pushVerboseError failed on digest mismatch",
          );

          this.logger.warn(
            {
              batchSeq: batch.batch_seq,
              storedDigest: batch.payload_digest,
              computedDigest: newDigest,
              estimatedDuplicates: batch.rows_to_insert,
            },
            "Digest mismatch on recovery — continuing (lenient mode, CH dedup will handle)",
          );
          // Continue — do NOT return or fail the run
        }

        // Step 5: Retry the insert with the same dedup token (CH dedup ignores duplicates)
        if (rows.length > 0) {
          await this.deps.manifestStore.updateBatchStatus(runId, batch.batch_seq, "inflight");

          await this.deps.retryPolicy.execute(
            () =>
              this.deps.chWriter.insertBatch({
                runId,
                batchSeq: batch.batch_seq,
                rows,
              }),
            `ch-recovery-batch-${batch.batch_seq}`,
            this.logger,
          );
        }

        // Step 6: Mark done + advance cursor, then set digest_match
        await this.deps.manifestStore.completeBatch(runId, batch.batch_seq, batch.upper_inclusive_cursor);
        await this.deps.manifestStore.updateBatchDigestMatch(runId, batch.batch_seq, digestMatched);
        this.lastCommittedId = batch.upper_inclusive_cursor;
        this.totalDocsRead += page.docs.length;
        this.totalRowsInserted += rows.length;

        try {
          await this.deps.redisState.markBatchDone(runId, batch.batch_seq);
        } catch {
          this.logger.warn("Redis markBatchDone failed during recovery (continuing)");
        }

        this.logger.info(
          { batchSeq: batch.batch_seq },
          "Successfully recovered interrupted batch",
        );

        await this.deps.manifestStore.insertEvent({
          run_id: runId,
          event_type: "batch_recovered",
          message: `Batch ${batch.batch_seq} recovered from ${batch.status}`,
          metadata: {
            batch_seq: batch.batch_seq,
            prior_status: batch.status,
            digest_matched: digestMatched,
          },
          created_at: new Date().toISOString(),
        });
      } catch (err) {
        const error = toError(err);
        this.logger.error(
          { batchSeq: batch.batch_seq, error: error.message },
          "Failed to recover interrupted batch — aborting recovery to prevent data gap",
        );
        await this.deps.manifestStore.updateBatchStatus(runId, batch.batch_seq, "failed", error.message);
        throw error;
      }
    }

    // Determine the last completed batch sequence
    const lastBatch = await this.deps.manifestStore.getLastBatch(runId);
    if (lastBatch) {
      this.batchSeq = lastBatch.batch_seq;
    }

    this.logger.info(
      {
        runId,
        resumeFromId: this.lastCommittedId,
        resumeFromBatchSeq: this.batchSeq,
        inflightRecovered: inflightBatches.length,
      },
      "Resuming from last committed position",
    );
  }

  /**
   * Poll Redis for operator commands (pause, stop-after-batch).
   */
  private async checkCommands(): Promise<void> {
    const { runId } = this.deps.config;

    try {
      const commands = await this.deps.redisState.getCommands(runId);

      if (commands.pause) {
        this.pause();
        // Clear the command flag
        await this.deps.redisState.setCommand(runId, "pause", false);
      }

      if (commands.abort) {
        this.stopAfterBatch();
        await this.deps.redisState.setCommand(runId, "abort", false);
      }
    } catch (err) {
      this.logger.warn({ error: toError(err).message }, "Failed to check Redis commands, continuing");
    }
  }

  /**
   * Block until the runner is resumed via the public API or Redis command.
   * Uses event-driven wait with periodic Redis command polling.
   */
  private async waitForResume(): Promise<void> {
    while (this.status === "paused") {
      await new Promise<void>((resolve) => {
        const onResume = () => { clearTimeout(timer); resolve(); };
        const timer = setTimeout(() => {
          this.emitter.off("resumed", onResume);
          resolve();
        }, 2_000);
        this.emitter.once("resumed", onResume);
      });
      if (this.status === "paused") {
        await this.checkCommands();
      }
    }
  }

  /**
   * Poll ClickHouse backpressure and wait until pressure subsides.
   *
   * Polls at `backpressure.pollIntervalMs`, up to `maxPauseEpisodeMs`.
   * If the deadline is reached the runner continues regardless (operator
   * must intervene if merges are truly stuck).
   */
  private async waitForBackpressure(): Promise<void> {
    const { backpressure, database, table } = this.deps.config;

    const pressure = await this.deps.chPressure.sample(database, table);

    if (!pressure.shouldPause) return;

    this.logger.warn(
      { reason: pressure.pauseReason },
      "Backpressure detected, pausing inserts",
    );

    const deadline = Date.now() + backpressure.maxPauseEpisodeMs;

    while (Date.now() < deadline && this.status === "running") {
      await sleep(backpressure.pollIntervalMs);

      const sample = await this.deps.chPressure.sample(database, table);
      if (sample.canResume) {
        this.logger.info("Backpressure cleared, resuming inserts");
        return;
      }
    }

    if (this.status === "running") {
      this.logger.warn(
        { maxPauseEpisodeMs: backpressure.maxPauseEpisodeMs },
        "Backpressure wait deadline reached, resuming anyway",
      );
    }
  }
}
