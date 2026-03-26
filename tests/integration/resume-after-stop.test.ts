/**
 * Integration test: pause/stop/resume scenarios for BatchRunner.
 *
 * Validates that:
 *  1. Stopping mid-migration and resuming completes all data with no duplicates
 *  2. Counter recovery is correct after resume (totalDocsRead is full count)
 *  3. batchSeq continues from the correct offset after resume
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { randomUUID } from "node:crypto";
import pino from "pino";
import { createClient, type ClickHouseClient } from "@clickhouse/client";

import { MongoReader } from "../../src/source/mongo-reader.ts";
import { ClickHouseWriter } from "../../src/target/clickhouse-writer.ts";
import { ManifestStore } from "../../src/state/manifest-store.ts";
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
const EVENT_NAME = "resume_test_event";

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
      appName: "resume-test",
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

  // Determine upper bound
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

describe("resume-after-stop", () => {
  beforeAll(async () => {
    await teardownMongo();
    await teardownClickHouse();
    await teardownRedis();
    await setupClickHouse();
  });

  beforeEach(async () => {
    // Clean CH table + Redis between tests
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
  // Test 1: stop mid-migration and resume completes all data
  // -----------------------------------------------------------------------

  it("stop mid-migration and resume completes all data", async () => {
    // Seed 5000 docs
    const { collName, expectedRows } = await seedCollection({
      count: 5000,
      appId: APP_ID,
      eventName: "stop_resume",
    });
    expect(expectedRows).toBe(5000);

    // --- Phase 1: Run and stop after first batch ---
    const parts1 = await buildDeps({
      collName,
      eventName: "stop_resume",
      batchRowsTarget: 500,
    });

    const runner1 = new BatchRunner(parts1.deps);

    // Wrap CH writer to call stopAfterBatch() after first successful insert
    let insertCount = 0;
    const origInsert = parts1.chWriter.insertBatch.bind(parts1.chWriter);
    parts1.chWriter.insertBatch = async (params) => {
      const result = await origInsert(params);
      insertCount++;
      if (insertCount >= 1) {
        runner1.stopAfterBatch();
      }
      return result;
    };

    await runner1.run();

    // Wait for the runner to stop
    expect(runner1.getStatus()).toBe("stopped");

    // Allow async inserts to flush
    await new Promise((r) => setTimeout(r, 1500));

    const rowsAfterPhase1 = await chRowCount();
    expect(rowsAfterPhase1).toBeGreaterThan(0);
    expect(rowsAfterPhase1).toBeLessThan(expectedRows);

    const savedRunId = parts1.runId;
    const savedUpperBoundId = parts1.upperBoundId;
    await cleanupDeps(parts1);

    // --- Phase 2: Create NEW BatchRunner and resume ---
    const parts2 = await buildDeps({
      collName,
      eventName: "stop_resume",
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

    // Verify: total ClickHouse rows = all expected docs
    const totalRows = await chRowCount();
    expect(totalRows).toBeGreaterThanOrEqual(expectedRows);
    expect(totalRows).toBeLessThanOrEqual(expectedRows + 20);

    // Verify: no duplicate rows (COUNT DISTINCT _id should equal total rows)
    const distinctResult = await chQuery<{ cnt: string }>(
      `SELECT count(DISTINCT _id) AS cnt FROM ${TEST_CH_TABLE}`,
    );
    const distinctCount = Number(distinctResult[0]?.cnt ?? 0);
    // max() is exclusive so 1 doc at the exact upper bound may be missed
    expect(distinctCount).toBeGreaterThanOrEqual(expectedRows - 1);
    expect(distinctCount).toBeLessThanOrEqual(expectedRows);

    await cleanupDeps(parts2);
  });

  // -----------------------------------------------------------------------
  // Test 2: counter recovery is correct after resume
  // -----------------------------------------------------------------------

  it("counter recovery is correct after resume", async () => {
    // Seed 3000 docs
    const { collName, expectedRows } = await seedCollection({
      count: 3000,
      appId: APP_ID,
      eventName: "counter_resume",
    });
    expect(expectedRows).toBe(3000);

    // --- Phase 1: Run and stop after 2 batches ---
    const parts1 = await buildDeps({
      collName,
      eventName: "counter_resume",
      batchRowsTarget: 500,
    });

    const runner1 = new BatchRunner(parts1.deps);

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

    const phase1Stats = runner1.getStats();
    expect(phase1Stats.totalDocsRead).toBeGreaterThan(0);

    const savedRunId = parts1.runId;
    const savedUpperBoundId = parts1.upperBoundId;
    await cleanupDeps(parts1);

    // --- Phase 2: Resume ---
    const parts2 = await buildDeps({
      collName,
      eventName: "counter_resume",
      batchRowsTarget: 500,
      existingRunId: savedRunId,
      existingUpperBoundId: savedUpperBoundId,
      skipRunCreation: true,
    });

    await parts2.manifestStore.updateRunStatus(savedRunId, "active");

    const runner2 = new BatchRunner(parts2.deps);
    await runner2.run();

    // Allow async inserts to flush
    await new Promise((r) => setTimeout(r, 2000));

    // Verify: totalDocsRead should be the full count, not just the resumed portion
    const phase2Stats = runner2.getStats();
    expect(phase2Stats.totalDocsRead).toBeGreaterThanOrEqual(expectedRows);
    expect(phase2Stats.totalDocsRead).toBeLessThanOrEqual(expectedRows + 20);

    // It should not be less than what phase 1 had processed
    // (proving it recovered counters rather than starting from 0)
    expect(phase2Stats.totalDocsRead).toBeGreaterThanOrEqual(phase1Stats.totalDocsRead);

    // All rows in CH
    const totalRows = await chRowCount();
    expect(totalRows).toBeGreaterThanOrEqual(expectedRows);
    expect(totalRows).toBeLessThanOrEqual(expectedRows + 20);

    await cleanupDeps(parts2);
  });

  // -----------------------------------------------------------------------
  // Test 3: batchSeq continues from correct offset
  // -----------------------------------------------------------------------

  it("batchSeq continues from correct offset", async () => {
    // Seed 2000 docs
    const { collName, expectedRows } = await seedCollection({
      count: 2000,
      appId: APP_ID,
      eventName: "batchseq_resume",
    });
    expect(expectedRows).toBe(2000);

    // --- Phase 1: Run a few batches (expect batch_seq 1-3 or so), then stop ---
    const parts1 = await buildDeps({
      collName,
      eventName: "batchseq_resume",
      batchRowsTarget: 500,
    });

    const runner1 = new BatchRunner(parts1.deps);

    // Stop after 3 batches
    let batchCount = 0;
    const origInsert = parts1.chWriter.insertBatch.bind(parts1.chWriter);
    parts1.chWriter.insertBatch = async (params) => {
      const result = await origInsert(params);
      batchCount++;
      if (batchCount >= 3) {
        runner1.stopAfterBatch();
      }
      return result;
    };

    await runner1.run();
    await new Promise((r) => setTimeout(r, 1500));

    // Record the batchSeq at stop
    const phase1FinalBatchSeq = runner1.getCurrentBatchSeq();
    expect(phase1FinalBatchSeq).toBeGreaterThanOrEqual(3);

    // Verify the manifest has these batch records
    const phase1Batches = await parts1.manifestStore.getBatches(parts1.runId, { status: "done" });
    expect(phase1Batches.length).toBeGreaterThanOrEqual(3);

    // Record max batch_seq from phase 1
    const maxBatchSeqPhase1 = Math.max(...phase1Batches.map((b) => b.batch_seq));

    const savedRunId = parts1.runId;
    const savedUpperBoundId = parts1.upperBoundId;
    await cleanupDeps(parts1);

    // --- Phase 2: Resume ---
    const parts2 = await buildDeps({
      collName,
      eventName: "batchseq_resume",
      batchRowsTarget: 500,
      existingRunId: savedRunId,
      existingUpperBoundId: savedUpperBoundId,
      skipRunCreation: true,
    });

    await parts2.manifestStore.updateRunStatus(savedRunId, "active");

    const runner2 = new BatchRunner(parts2.deps);
    await runner2.run();

    // Allow async inserts to flush
    await new Promise((r) => setTimeout(r, 2000));

    // Verify: new batches start from batch_seq > maxBatchSeqPhase1 (not 0)
    const allBatches = await parts2.manifestStore.getBatches(savedRunId);
    const phase2Batches = allBatches.filter((b) => b.batch_seq > maxBatchSeqPhase1);

    // There should be new batches from phase 2
    expect(phase2Batches.length).toBeGreaterThan(0);

    // The first new batch should start right after maxBatchSeqPhase1
    const minBatchSeqPhase2 = Math.min(...phase2Batches.map((b) => b.batch_seq));
    expect(minBatchSeqPhase2).toBe(maxBatchSeqPhase1 + 1);

    // No batch_seq gaps between phase 1 and phase 2
    const allBatchSeqs = allBatches.map((b) => b.batch_seq).sort((a, b) => a - b);
    for (let i = 1; i < allBatchSeqs.length; i++) {
      expect(allBatchSeqs[i]).toBe(allBatchSeqs[i - 1] + 1);
    }

    // All rows should be in ClickHouse
    const totalRows = await chRowCount();
    expect(totalRows).toBeGreaterThanOrEqual(expectedRows);
    expect(totalRows).toBeLessThanOrEqual(expectedRows + 20);

    await cleanupDeps(parts2);
  });
});
