import { EventEmitter } from "node:events";
import { setTimeout as sleep } from "node:timers/promises";
import type { Logger } from "pino";

import type { ManifestStore, Batch, BatchStatus, RunSummary, CompactError, BatchSeqRange } from "../state/manifest-store.ts";
import type { RedisHotState, VerboseError, BatchPhase, LiveBatchData } from "../state/redis-hot-state.ts";
import type { AsyncBatchWriter } from "../state/async-batch-writer.ts";
import type { MongoReader } from "../source/mongo-reader.ts";
import type { ClickHouseWriter } from "../target/clickhouse-writer.ts";
import type { ClickHousePressure, BackpressureConfig } from "../target/clickhouse-pressure.ts";
import type { GcController } from "./gc-controller.ts";
import type { RetryPolicy } from "./retry-policy.ts";
import { SkipCounter, type SkipReason } from "../transform/skip-reasons.ts";
import { transformBatch, type OutputRow } from "../transform/normalize.ts";
import type { CollectionDefaults } from "../transform/hash-resolver.ts";
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
  mongoPageSize: number;
  backpressure: BackpressureConfig;
  useDedupToken: boolean;
  database: string;
  table: string;
  snapshotInterval: number;
  collectionDefaults?: CollectionDefaults;
  batchSeqOffset?: number;
  collectionName?: string;
  podId?: string;
  rangeIdx?: number;
  batchSeqMax?: number;
}

export interface BatchRunnerDeps {
  manifestStore: ManifestStore;
  redisState: RedisHotState;
  globalRedisState?: RedisHotState;
  asyncBatchWriter?: AsyncBatchWriter;
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
  | "waiting_for_index"
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

/** Lightweight digest: row count as string. ClickHouse dedup tokens handle actual deduplication. */
function computePayloadDigest(rows: OutputRow[]): string {
  return String(rows.length);
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
  private batchSeq: number;
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
  private readonly isRangeMode: boolean;
  private phaseHeartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private currentPhaseData: LiveBatchData | null = null;

  constructor(deps: BatchRunnerDeps) {
    this.deps = deps;
    this.logger = deps.logger.child({ component: "BatchRunner" });
    this.skipCounter = new SkipCounter();
    this.batchSeq = deps.config.batchSeqOffset ?? 0;
    this.isRangeMode = deps.config.rangeIdx !== undefined;
  }

  // -----------------------------------------------------------------------
  // Public API
  // -----------------------------------------------------------------------

  /** Start the batch processing loop. Optional startCursor for range-parallel mode. */
  async run(startCursor?: string): Promise<void> {
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
      await this.resumeFromInterruption(startCursor);

      // If a startCursor was provided (range mode) and no resume state exists, use it
      if (startCursor && !this.lastCommittedId) {
        this.lastCommittedId = startCursor;
      }

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

        // (c) Accumulate multiple MongoDB pages into one ClickHouse write batch
        await this.setPhase("READING", { docsRead: 0, rowsToInsert: 0 });
        const { batchRowsTarget, mongoPageSize } = this.deps.config;
        const accRows: OutputRow[] = [];
        const accSkipSamples: Array<{ _id: string; reason: SkipReason }> = [];
        let accDocsRead = 0;
        let pageCursor: Cursor | null = this.lastCommittedId
          ? deserializeCursor(this.lastCommittedId) : null;
        const upperBoundCursor = deserializeCursor(upperBoundId);
        let lastPageCursor: Cursor | null = null;

        while (accRows.length < batchRowsTarget) {
          const page = await this.deps.retryPolicy.execute(
            () => this.deps.mongoReader.readPage(pageCursor, upperBoundCursor, mongoPageSize),
            `mongo-read-batch-${this.batchSeq + 1}-page`,
            this.logger,
          );

          if (page.docs.length === 0) break;

          accDocsRead += page.docs.length;
          lastPageCursor = page.lastCursor;
          pageCursor = page.lastCursor;

          const { rows: pageRows, skippedSamples } = transformBatch(
            page.docs,
            this.skipCounter,
            this.deps.config.collectionDefaults,
          );
          accRows.push(...pageRows);
          accSkipSamples.push(...skippedSamples);

          // Stop if MongoDB returned fewer docs than requested (last page)
          if (page.docs.length < mongoPageSize) break;
        }

        // (d) Empty accumulation = run complete
        if (accDocsRead === 0) {
          this.logger.info(
            {
              totalBatches: this.batchSeq,
              totalDocsRead: this.totalDocsRead,
              totalRowsInserted: this.totalRowsInserted,
            },
            "All documents processed, run complete",
          );

          // In range mode, only the RangeCoordinator finalizes the shared run status
          if (!this.isRangeMode) {
            const summary = this.buildSummary("completed");
            await this.deps.manifestStore.writeSummary(runId, "completed", summary);
          }

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

        await this.setPhase("TRANSFORMING", { docsRead: accDocsRead, rowsToInsert: 0 });
        this.totalDocsRead += accDocsRead;
        this.batchSeq++;

        // Record skip samples in manifest (single batch write)
        if (accSkipSamples.length > 0) {
          const now = new Date().toISOString();
          await this.deps.manifestStore.insertSkipSamples(
            accSkipSamples.map(s => ({
              run_id: runId,
              batch_seq: this.batchSeq,
              doc_id: s._id,
              reason: s.reason,
              captured_at: now,
            })),
          );
        }

        const lowerExclusiveId = this.lastCommittedId ?? "";
        const upperInclusiveId = serializeCursor(lastPageCursor!);
        const docsSkipped = accDocsRead - accRows.length;
        const rows = accRows;

        // (f) All docs skipped -> record skipped_empty batch, advance
        if (rows.length === 0) {
          this.logger.info(
            { batchSeq: this.batchSeq, docsRead: accDocsRead, docsSkipped },
            "All documents in batch were skipped",
          );

          const batch = buildBatch(runId, this.batchSeq, lowerExclusiveId, upperInclusiveId, accDocsRead, docsSkipped, {
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

        // (g) Build batch record (not persisted yet — only written on success or failure)
        const payloadDigest = computePayloadDigest(rows);
        const queryId = `mig__${runId}__${this.batchSeq}`;
        const dedupToken = this.deps.config.useDedupToken
          ? `mig:${runId}:${this.batchSeq}`
          : "";

        const batch = buildBatch(runId, this.batchSeq, lowerExclusiveId, upperInclusiveId, accDocsRead, docsSkipped, {
          rows_to_insert: rows.length,
          payload_digest: payloadDigest,
          insert_dedup_token: dedupToken,
          query_id: queryId,
        });

        // (h) Insert into ClickHouse (no pre-write to manifest — single write on success)
        try {
          await this.setPhase("WRITING", { docsRead: accDocsRead, rowsToInsert: rows.length });
          this.startPhaseHeartbeat();
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

              // Audit event for retry
              await this.deps.manifestStore.insertEvent({
                run_id: runId,
                event_type: "batch_retry_error",
                message: `Batch ${currentBatchSeq} attempt ${attempt} failed: ${err.message.slice(0, 200)}`,
                metadata: { batch_seq: currentBatchSeq, attempt },
                created_at: now,
              }).catch(() => {});

              // Verbose error to Redis
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

          // (i) On success: write cursor + bitmap to Redis (commit point), then queue MongoDB
          this.stopPhaseHeartbeat();
          await this.setPhase("COMMITTING", { docsRead: accDocsRead, rowsToInsert: rows.length });

          // Redis commit point — atomic MULTI/EXEC for cursor + bitmap
          await this.bestEffortRedis(
            () => this.deps.redisState.commitBatch(runId, upperInclusiveId, this.batchSeq),
            "Redis cursor/bitmap commit failed",
          );

          // MongoDB: async queue or direct write
          if (this.deps.asyncBatchWriter) {
            await this.deps.asyncBatchWriter.queueBatch(batch, upperInclusiveId);
          } else {
            await this.deps.manifestStore.insertCompletedBatch(batch, upperInclusiveId);
          }
          this.lastCommittedId = upperInclusiveId;
          this.totalRowsInserted += result.rowsInserted;

          const batchSeqOffset = this.deps.config.batchSeqOffset ?? 0;

          // Redis stats (rebuildable, continue if fails)
          await this.bestEffortRedis(
            async () => {
              await this.deps.redisState.updateStats(runId, {
                docsRead: this.totalDocsRead,
                docsSkipped: this.skipCounter.getTotal(),
                rowsInserted: this.totalRowsInserted,
                batchesDone: (this.batchSeq - batchSeqOffset) - this.batchesFailed,
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

          // Throughput sliding window sample (lightweight LPUSH per batch)
          await this.bestEffortRedis(
            () => this.deps.redisState.pushThroughputSample(runId, {
              ts: Date.now(),
              docsRead: this.totalDocsRead,
            }),
            "Redis throughput sample failed",
          );

          // Clear live batch phase
          if (this.deps.config.collectionName) {
            const liveRedis = this.deps.globalRedisState ?? this.deps.redisState;
            await this.bestEffortRedis(
              () => liveRedis.clearLiveBatch(this.deps.config.collectionName!),
              "Redis clearLiveBatch failed",
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
          // Insert failed after all retries — write batch record as failed
          this.stopPhaseHeartbeat();
          const error = toError(err);
          batch.status = "failed" as any;
          batch.last_error = error.message;
          batch.finished_at = new Date().toISOString();
          await this.deps.manifestStore.insertBatch(batch);

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
          if (!this.isRangeMode) {
            const summary = this.buildSummary("failed");
            await this.deps.manifestStore.writeSummary(runId, "failed", summary);
          }
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
        if (!this.isRangeMode) {
          const summary = this.buildSummary("stopped");
          await this.deps.manifestStore.writeSummary(runId, "stopped", summary);
        }
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
      this.stopPhaseHeartbeat();
      const error = toError(err);
      this.logger.error({ error: error.message }, "BatchRunner fatal error");
      this.setTerminalStatus("failed");

      if (!this.isRangeMode) {
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
  private async resumeFromInterruption(startCursor?: string): Promise<void> {
    const { runId, upperBoundId } = this.deps.config;
    const run = await this.deps.manifestStore.getRun(runId);

    if (!run) {
      this.logger.info({ runId }, "No existing run found, starting fresh");
      return;
    }

    // Compute range-scoped batch filter (undefined in standard mode)
    const { rangeIdx, batchSeqOffset, batchSeqMax } = this.deps.config;
    const batchSeqRange: BatchSeqRange | undefined =
      rangeIdx !== undefined && batchSeqOffset !== undefined && batchSeqMax !== undefined
        ? { min: batchSeqOffset, max: batchSeqMax }
        : undefined;

    // ── Cursor recovery ──────────────────────────────────────────────────
    // Redis per-range cursor (hot-path authority, already isolated by prefix)
    const redisCursor = await this.deps.redisState.getLastCommittedCursor(runId).catch(() => null);

    if (redisCursor) {
      this.lastCommittedId = redisCursor;
    } else if (rangeIdx === undefined) {
      // Standard (non-range) mode: fall back to run-level cursor
      this.lastCommittedId = run.last_committed_cursor;
    }
    // Range mode with no Redis cursor: lastCommittedId stays null
    // (will be set from batch-derived cursor or startCursor in run())

    // ── Batch-derived cursor (scoped to this range's slot) ───────────────
    const lastDoneBatch = await this.deps.manifestStore.getLastDoneBatch(runId, batchSeqRange);
    if (lastDoneBatch?.upper_inclusive_cursor) {
      this.lastCommittedId = lastDoneBatch.upper_inclusive_cursor;
    }

    // ── Bounds guard (Layer 2) ───────────────────────────────────────────
    if (this.lastCommittedId && rangeIdx !== undefined && startCursor) {
      const recovered = deserializeCursor(this.lastCommittedId);
      const rangeStart = deserializeCursor(startCursor);
      const rangeEnd = deserializeCursor(upperBoundId);

      if (recovered.cd < rangeStart.cd || recovered.cd > rangeEnd.cd) {
        this.logger.warn(
          { recoveredCd: recovered.cd, rangeStartCd: rangeStart.cd, rangeEndCd: rangeEnd.cd, rangeIdx },
          "Recovered cursor outside range bounds — discarding, will use startCursor",
        );
        this.lastCommittedId = null;
      }
    }

    // ── Recover interrupted batches (scoped to range slot) ───────────────
    const inflightBatches = await this.deps.manifestStore.getBatches(runId, {
      status: "inflight",
      batchSeqRange,
    });
    const preparedBatches = await this.deps.manifestStore.getBatches(runId, {
      status: "prepared",
      batchSeqRange,
    });
    const recoverableBatches = [...preparedBatches, ...inflightBatches]
      .sort((a, b) => a.batch_seq - b.batch_seq);

    for (const batch of recoverableBatches) {
      this.logger.info(
        { batchSeq: batch.batch_seq, status: batch.status },
        "Attempting to recover interrupted batch",
      );

      try {
        // Re-read the exact source range
        const page = await this.deps.mongoReader.readPage(
          batch.lower_exclusive_cursor ? deserializeCursor(batch.lower_exclusive_cursor) : null,
          deserializeCursor(batch.upper_inclusive_cursor),
          this.deps.config.batchRowsTarget,
        );

        // Re-transform
        const { rows } = transformBatch(page.docs, this.skipCounter, this.deps.config.collectionDefaults);

        // Recompute digest
        const newDigest = computePayloadDigest(rows);

        // Compare with stored digest (lenient mode)
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
        }

        // Retry the insert with the same dedup token (CH dedup ignores duplicates)
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

        // Mark done + advance cursor, then set digest_match
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

    // ── Restore batchSeq (scoped to this range's slot) ───────────────────
    const lastBatch = await this.deps.manifestStore.getLastBatch(runId, batchSeqRange);
    if (lastBatch) {
      this.batchSeq = lastBatch.batch_seq;
    }

    // ── Recover accumulated counters ─────────────────────────────────────
    const lastStats = await this.deps.redisState.getStats(runId).catch(() => null);
    if (lastStats && lastStats.docsRead > 0) {
      this.totalDocsRead = lastStats.docsRead;
      this.totalRowsInserted = lastStats.rowsInserted;
      this.logger.info(
        { docsRead: lastStats.docsRead, rowsInserted: lastStats.rowsInserted, source: "redis" },
        "Recovered accumulated counters from Redis",
      );
    } else {
      // Redis stats missing (flushed) — fallback to manifest aggregate (scoped)
      const aggregate = await this.deps.manifestStore.sumCompletedBatchStats(runId, batchSeqRange);
      if (aggregate.docsRead > 0) {
        this.totalDocsRead = aggregate.docsRead;
        this.totalRowsInserted = aggregate.rowsInserted;
        this.logger.info(
          { docsRead: aggregate.docsRead, rowsInserted: aggregate.rowsInserted, source: "manifest" },
          "Recovered accumulated counters from manifest",
        );
      }
    }

    this.logger.info(
      {
        runId,
        resumeFromId: this.lastCommittedId,
        resumeFromBatchSeq: this.batchSeq,
        inflightRecovered: inflightBatches.length,
        rangeIdx,
        batchSeqRange: batchSeqRange ? `[${batchSeqRange.min}, ${batchSeqRange.max})` : "global",
      },
      "Resuming from last committed position",
    );
  }

  /** Update the live batch phase in Redis with heartbeat. */
  private async setPhase(phase: BatchPhase, stats: { docsRead: number; rowsToInsert: number }): Promise<void> {
    const { collectionName, podId, rangeIdx } = this.deps.config;
    if (!collectionName) return;

    this.currentPhaseData = {
      collection: collectionName,
      podId: podId ?? "unknown",
      batchSeq: this.batchSeq,
      phase,
      docsRead: stats.docsRead,
      rowsToInsert: stats.rowsToInsert,
      startedAt: Date.now(),
      rangeIdx,
    };

    const liveRedis = this.deps.globalRedisState ?? this.deps.redisState;
    await this.bestEffortRedis(
      () => liveRedis.setLiveBatch(collectionName, this.currentPhaseData!),
      "Redis setLiveBatch failed",
    );
  }

  private startPhaseHeartbeat(): void {
    this.stopPhaseHeartbeat();
    const liveRedis = this.deps.globalRedisState ?? this.deps.redisState;
    this.phaseHeartbeatTimer = setInterval(() => {
      if (this.currentPhaseData && this.deps.config.collectionName) {
        liveRedis.setLiveBatch(this.deps.config.collectionName, this.currentPhaseData).catch(() => {});
      }
    }, 10_000);
  }

  private stopPhaseHeartbeat(): void {
    if (this.phaseHeartbeatTimer) {
      clearInterval(this.phaseHeartbeatTimer);
      this.phaseHeartbeatTimer = null;
    }
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
