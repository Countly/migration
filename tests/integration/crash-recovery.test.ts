/**
 * Integration test: crash recovery scenarios for BatchRunner.
 *
 * Validates that resumeFromInterruption() correctly handles:
 *  1. Inflight batches left after a crash (CH write done, manifest not marked done)
 *  2. Redis cursor loss with manifest fallback
 *  3. Counter recovery from manifest aggregate when Redis stats are lost
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { randomUUID } from "node:crypto";
import pino from "pino";
import { createClient, type ClickHouseClient } from "@clickhouse/client";

import { MongoReader } from "../../src/source/mongo-reader.ts";
import { ClickHouseWriter } from "../../src/target/clickhouse-writer.ts";
import { ManifestStore, type Batch, type BatchStatus } from "../../src/state/manifest-store.ts";
import { RedisHotState } from "../../src/state/redis-hot-state.ts";
import { BatchRunner, type BatchRunnerDeps } from "../../src/runtime/batch-runner.ts";
import { ClickHousePressure, type BackpressureConfig } from "../../src/target/clickhouse-pressure.ts";
import { GcController } from "../../src/runtime/gc-controller.ts";
import { RetryPolicy } from "../../src/runtime/retry-policy.ts";
import { serializeCursor } from "../../src/types/cursor.ts";

import {
  setupClickHouse,
  teardownClickHouse,
  teardownMongo,
  teardownRedis,
  closeAll,
  chRowCount,
  chQuery,
  getRedis,
  TEST_MONGO_URI,
  TEST_MONGO_DB,
  TEST_CH_URL,
  TEST_CH_DB,
  TEST_CH_TABLE,
  TEST_REDIS_URL,
  TEST_REDIS_PREFIX,
} from "../helpers/setup.ts";
import { seedCollection } from "../helpers/seed-mongo.ts";

// ---------------------------------------------------------------------------
// Shared config
// ---------------------------------------------------------------------------

const logger = pino({ level: "warn" });

const APP_ID = "aaaaaaaaaaaaaaaaaaaaaaaa";
const EVENT_NAME = "crash_recovery_event";

const BACKPRESSURE_OFF: BackpressureConfig = {
  enabled: false,
  partsToThrowInsert: 300,
  maxPartsInTotal: 500,
  partitionPctHigh: 0.7,
  partitionPctLow: 0.55,
  totalPctHigh: 0.7,
  totalPctLow: 0.55,
  pollIntervalMs: 5000,
  maxPauseEpisodeMs: 180_000,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let chClientForPressure: ClickHouseClient | null = null;

function getChClientForPressure(): ClickHouseClient {
  if (!chClientForPressure) {
    chClientForPressure = createClient({
      url: TEST_CH_URL,
      database: TEST_CH_DB,
      username: "default",
      password: "",
    });
  }
  return chClientForPressure;
}

interface BuildDepsResult {
  deps: BatchRunnerDeps;
  mongoReader: MongoReader;
  chWriter: ClickHouseWriter;
  manifestStore: ManifestStore;
  redisState: RedisHotState;
  gcController: GcController;
  runId: string;
  upperBoundId: string;
  sourceNs: string;
  targetTable: string;
}

/**
 * Build a full set of BatchRunner dependencies for a given collection.
 * Optionally reuse an existing runId and upperBoundId (for resume scenarios).
 */
async function buildDeps(opts: {
  collName: string;
  appId?: string;
  eventName?: string;
  batchRowsTarget?: number;
  existingRunId?: string;
  existingUpperBoundId?: string;
  skipRunCreation?: boolean;
}): Promise<BuildDepsResult> {
  const appId = opts.appId ?? APP_ID;
  const eventName = opts.eventName ?? EVENT_NAME;
  const collName = opts.collName;
  const batchRowsTarget = opts.batchRowsTarget ?? 500;

  // MongoReader
  const mongoReader = new MongoReader(
    {
      uri: TEST_MONGO_URI,
      database: TEST_MONGO_DB,
      readPreference: "primary",
      readConcern: "local",
      retryReads: true,
      appName: "crash-recovery-test",
      batchRowsTarget,
      cursorBatchSize: 500,
      maxTimeMs: 30_000,
    },
    logger,
  );
  await mongoReader.connect();
  await mongoReader.switchCollection(collName);

  // ClickHouseWriter
  const chWriter = new ClickHouseWriter(
    {
      url: TEST_CH_URL,
      database: TEST_CH_DB,
      table: TEST_CH_TABLE,
      username: "default",
      password: "",
      queryTimeoutMs: 30_000,
      useDedupToken: true,
    },
    logger,
  );
  await chWriter.connect();

  // ManifestStore
  const manifestStore = new ManifestStore(TEST_MONGO_URI, TEST_MONGO_DB);
  await manifestStore.connect();

  // RedisHotState
  const redisState = new RedisHotState(TEST_REDIS_URL, TEST_REDIS_PREFIX);
  await redisState.connect();

  // ClickHousePressure (disabled)
  const chPressure = new ClickHousePressure(
    getChClientForPressure(),
    BACKPRESSURE_OFF,
    logger,
  );

  // GcController (no-op)
  const gcController = new GcController(
    {
      enabled: false,
      rssSoftLimitBytes: 2 * 1024 * 1024 * 1024,
      rssHardLimitBytes: 3 * 1024 * 1024 * 1024,
      heapUsedRatio: 0.9,
      everyNBatches: 999_999,
    },
    logger,
  );

  // RetryPolicy
  const retryPolicy = new RetryPolicy({
    maxRetries: 3,
    baseDelayMs: 100,
    maxDelayMs: 1000,
  });

  // Determine upper bound (or reuse existing)
  let upperBoundId: string;
  if (opts.existingUpperBoundId) {
    upperBoundId = opts.existingUpperBoundId;
  } else {
    const upperBound = await mongoReader.getUpperBound();
    upperBoundId = upperBound ? serializeCursor(upperBound) : "";
  }

  const runId = opts.existingRunId ?? randomUUID();
  const now = new Date().toISOString();
  const sourceNs = `${TEST_MONGO_DB}.${collName}`;
  const targetTable = `${TEST_CH_DB}.${TEST_CH_TABLE}`;

  if (!opts.skipRunCreation) {
    await manifestStore.createRun({
      run_id: runId,
      status: "active",
      source_ns: sourceNs,
      target_table: targetTable,
      upper_bound_cursor: upperBoundId,
      transform_version: "v1",
      created_at: now,
      updated_at: now,
    });

    await redisState.setActiveRun(runId);
    await redisState.setState(runId, {
      runId,
      status: "active",
      sourceNs,
      targetTable,
      upperBoundCursor: upperBoundId,
      lastCommittedCursor: null,
      transformVersion: "v1",
      totalBatches: 0,
      completedBatches: 0,
      startedAt: now,
    });
  }

  const deps: BatchRunnerDeps = {
    manifestStore,
    redisState,
    mongoReader,
    chWriter,
    chPressure,
    gcController,
    retryPolicy,
    logger,
    config: {
      runId,
      transformVersion: "v1",
      sourceNs,
      targetTable,
      upperBoundId,
      batchRowsTarget,
      mongoPageSize: batchRowsTarget,
      backpressure: BACKPRESSURE_OFF,
      useDedupToken: true,
      database: TEST_CH_DB,
      table: TEST_CH_TABLE,
      snapshotInterval: 10,
      collectionDefaults: { a: appId, e: eventName },
    },
  };

  return {
    deps,
    mongoReader,
    chWriter,
    manifestStore,
    redisState,
    gcController,
    runId,
    upperBoundId,
    sourceNs,
    targetTable,
  };
}

async function cleanupDeps(parts: {
  mongoReader: MongoReader;
  chWriter: ClickHouseWriter;
  manifestStore: ManifestStore;
  redisState: RedisHotState;
  gcController: GcController;
}): Promise<void> {
  await parts.mongoReader.close().catch(() => {});
  await parts.chWriter.close().catch(() => {});
  await parts.manifestStore.close().catch(() => {});
  await parts.redisState.close().catch(() => {});
  parts.gcController.dispose();
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe("crash-recovery", () => {
  beforeAll(async () => {
    await teardownMongo();
    await teardownClickHouse();
    await teardownRedis();
    await setupClickHouse();
  });

  beforeEach(async () => {
    // Clean CH table + Redis between tests to avoid cross-contamination
    await teardownClickHouse();
    await teardownRedis();
    await setupClickHouse();
  });

  afterAll(async () => {
    if (chClientForPressure) {
      await chClientForPressure.close();
      chClientForPressure = null;
    }
    await closeAll();
  });

  // -----------------------------------------------------------------------
  // Test 1: recovers inflight batch after crash
  // -----------------------------------------------------------------------

  it("recovers inflight batch after crash", async () => {
    // Seed 2000 docs
    const { collName, expectedRows } = await seedCollection({
      count: 2000,
      appId: APP_ID,
      eventName: "crash_inflight",
    });
    expect(expectedRows).toBe(2000);

    // --- Phase 1: Run first batch, then simulate crash ---
    const parts1 = await buildDeps({
      collName,
      eventName: "crash_inflight",
      batchRowsTarget: 500,
    });

    const runner1 = new BatchRunner(parts1.deps);

    // Use a wrapping CH writer that stops after 1 successful insert
    let insertCount = 0;
    const origInsertBatch = parts1.chWriter.insertBatch.bind(parts1.chWriter);
    parts1.chWriter.insertBatch = async (params) => {
      const result = await origInsertBatch(params);
      insertCount++;
      if (insertCount >= 1) {
        // Stop after first batch completes
        runner1.stopAfterBatch();
      }
      return result;
    };

    await runner1.run();
    await new Promise((r) => setTimeout(r, 1500));

    // Confirm some rows were written
    const rowsAfterPhase1 = await chRowCount();
    expect(rowsAfterPhase1).toBeGreaterThan(0);

    // Get the last done batch to create a simulated inflight batch after it
    const lastDone = await parts1.manifestStore.getLastDoneBatch(parts1.runId);
    expect(lastDone).not.toBeNull();

    // Now simulate an "inflight" batch: create a batch record in manifest
    // as if the process crashed after CH write but before marking done.
    // Read the next page to build a realistic batch record.
    const lastCursorStr = lastDone!.upper_inclusive_cursor;
    const nextBatchSeq = lastDone!.batch_seq + 1;

    // Read the next page from Mongo to get realistic cursor values
    const { deserializeCursor } = await import("../../src/types/cursor.ts");
    const { transformBatch } = await import("../../src/transform/normalize.ts");
    const { SkipCounter } = await import("../../src/transform/skip-reasons.ts");

    const lastCursor = deserializeCursor(lastCursorStr);
    const upperBound = deserializeCursor(parts1.upperBoundId);
    const page = await parts1.deps.mongoReader.readPage(lastCursor, upperBound, 500);

    let inflightUpperCursor: string;
    if (page.docs.length > 0 && page.lastCursor) {
      inflightUpperCursor = serializeCursor(page.lastCursor);

      const skipCounter = new SkipCounter();
      const { rows } = transformBatch(page.docs, skipCounter, { a: APP_ID, e: "crash_inflight" });

      // Insert as inflight batch in manifest
      await parts1.manifestStore.insertBatch({
        run_id: parts1.runId,
        batch_seq: nextBatchSeq,
        lower_exclusive_cursor: lastCursorStr,
        upper_inclusive_cursor: inflightUpperCursor,
        source_docs_read: page.docs.length,
        docs_skipped: page.docs.length - rows.length,
        rows_to_insert: rows.length,
        payload_digest: String(rows.length),
        insert_dedup_token: `mig:${parts1.runId}:${nextBatchSeq}`,
        query_id: `mig__${parts1.runId}__${nextBatchSeq}`,
        status: "inflight" as BatchStatus,
        retry_count: 0,
        last_error: null,
        started_at: new Date().toISOString(),
        finished_at: null,
      });
    }

    await cleanupDeps(parts1);

    // --- Phase 2: Create NEW BatchRunner and resume ---
    const parts2 = await buildDeps({
      collName,
      eventName: "crash_inflight",
      batchRowsTarget: 500,
      existingRunId: parts1.runId,
      existingUpperBoundId: parts1.upperBoundId,
      skipRunCreation: true,
    });

    // Re-activate the run for the new runner
    await parts2.manifestStore.updateRunStatus(parts1.runId, "active");

    const runner2 = new BatchRunner(parts2.deps);
    await runner2.run();

    // Allow async inserts to flush
    await new Promise((r) => setTimeout(r, 2000));

    // Verify: total CH rows = all expected docs (no gaps)
    const totalRows = await chRowCount();
    expect(totalRows).toBe(expectedRows);

    // Verify: inflight batch was recovered (check events for batch_recovered)
    const recoveryEvents = await parts2.manifestStore.countEvents(parts1.runId, "batch_recovered");
    expect(recoveryEvents).toBeGreaterThanOrEqual(1);

    await cleanupDeps(parts2);
  });

  // -----------------------------------------------------------------------
  // Test 2: recovers from Redis cursor loss (manifest fallback)
  // -----------------------------------------------------------------------

  it("recovers from Redis cursor loss (manifest fallback)", async () => {
    // Seed 3000 docs
    const { collName, expectedRows } = await seedCollection({
      count: 3000,
      appId: APP_ID,
      eventName: "redis_loss",
    });
    expect(expectedRows).toBe(3000);

    // --- Phase 1: Run a few batches ---
    const parts1 = await buildDeps({
      collName,
      eventName: "redis_loss",
      batchRowsTarget: 500,
    });

    const runner1 = new BatchRunner(parts1.deps);

    // Stop after 2 batches (~1000 docs)
    let batchCount = 0;
    const origInsert1 = parts1.chWriter.insertBatch.bind(parts1.chWriter);
    parts1.chWriter.insertBatch = async (params) => {
      const result = await origInsert1(params);
      batchCount++;
      if (batchCount >= 2) {
        runner1.stopAfterBatch();
      }
      return result;
    };

    await runner1.run();
    await new Promise((r) => setTimeout(r, 1500));

    const rowsAfterPhase1 = await chRowCount();
    expect(rowsAfterPhase1).toBeGreaterThan(0);
    expect(rowsAfterPhase1).toBeLessThan(expectedRows);

    // Verify manifest has committed cursor
    const runDoc = await parts1.manifestStore.getRun(parts1.runId);
    expect(runDoc).toBeDefined();
    expect(runDoc!.last_committed_cursor).not.toBeNull();

    const savedRunId = parts1.runId;
    const savedUpperBoundId = parts1.upperBoundId;
    await cleanupDeps(parts1);

    // --- Delete all Redis keys (simulate Redis flush) ---
    await teardownRedis();

    // --- Phase 2: Create NEW BatchRunner and resume ---
    const parts2 = await buildDeps({
      collName,
      eventName: "redis_loss",
      batchRowsTarget: 500,
      existingRunId: savedRunId,
      existingUpperBoundId: savedUpperBoundId,
      skipRunCreation: true,
    });

    // Re-activate the run
    await parts2.manifestStore.updateRunStatus(savedRunId, "active");

    const runner2 = new BatchRunner(parts2.deps);
    await runner2.run();

    // Allow async inserts to flush
    await new Promise((r) => setTimeout(r, 2000));

    // Verify: all docs end up in ClickHouse
    const totalRows = await chRowCount();
    expect(totalRows).toBe(expectedRows);

    await cleanupDeps(parts2);
  });

  // -----------------------------------------------------------------------
  // Test 3: counter recovery from manifest aggregate
  // -----------------------------------------------------------------------

  it("counter recovery from manifest aggregate", async () => {
    // Seed 2000 docs
    const { collName, expectedRows } = await seedCollection({
      count: 2000,
      appId: APP_ID,
      eventName: "counter_recovery",
    });
    expect(expectedRows).toBe(2000);

    // --- Phase 1: Run some batches to create done batch records ---
    const parts1 = await buildDeps({
      collName,
      eventName: "counter_recovery",
      batchRowsTarget: 500,
    });

    const runner1 = new BatchRunner(parts1.deps);

    // Stop after 2 batches
    let batchCount = 0;
    const origInsert = parts1.chWriter.insertBatch.bind(parts1.chWriter);
    parts1.chWriter.insertBatch = async (params) => {
      const result = await origInsert(params);
      batchCount++;
      if (batchCount >= 2) {
        runner1.stopAfterBatch();
      }
      return result;
    };

    await runner1.run();
    await new Promise((r) => setTimeout(r, 1500));

    // Check that we have some done batches in the manifest
    const doneBatches = await parts1.manifestStore.getBatches(parts1.runId, { status: "done" });
    expect(doneBatches.length).toBeGreaterThanOrEqual(2);

    // Sum up what the manifest says we should have
    const manifestAggregate = await parts1.manifestStore.sumCompletedBatchStats(parts1.runId);
    expect(manifestAggregate.docsRead).toBeGreaterThan(0);

    const savedRunId = parts1.runId;
    const savedUpperBoundId = parts1.upperBoundId;
    await cleanupDeps(parts1);

    // --- Delete all Redis keys (simulate Redis stats loss) ---
    await teardownRedis();

    // --- Phase 2: Create new BatchRunner and verify counter recovery ---
    const parts2 = await buildDeps({
      collName,
      eventName: "counter_recovery",
      batchRowsTarget: 500,
      existingRunId: savedRunId,
      existingUpperBoundId: savedUpperBoundId,
      skipRunCreation: true,
    });

    // Re-activate the run
    await parts2.manifestStore.updateRunStatus(savedRunId, "active");

    const runner2 = new BatchRunner(parts2.deps);
    await runner2.run();

    // Allow async inserts to flush
    await new Promise((r) => setTimeout(r, 2000));

    // Verify: the BatchRunner recovered counters from manifest aggregate
    const stats = runner2.getStats();

    // totalDocsRead should be the full count (recovered portion + newly read)
    expect(stats.totalDocsRead).toBe(expectedRows);

    // totalDocsRead should not be less than the manifest aggregate
    // (it should start from the aggregate, not 0)
    expect(stats.totalDocsRead).toBeGreaterThanOrEqual(manifestAggregate.docsRead);

    // All rows should be in ClickHouse
    const totalRows = await chRowCount();
    expect(totalRows).toBe(expectedRows);

    await cleanupDeps(parts2);
  });
});
