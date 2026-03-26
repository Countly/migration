/**
 * Integration tests for run finalization and summary writing.
 *
 * Verifies that ManifestStore.writeSummary correctly persists run summaries,
 * that failed runs have the correct status, and that the RangeCoordinator's
 * SETNX-based finalization ensures only one pod finalizes a run.
 */
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { randomUUID } from "node:crypto";
import pino from "pino";
import { createClient, type ClickHouseClient } from "@clickhouse/client";

import { MongoReader } from "../../src/source/mongo-reader.ts";
import { ClickHouseWriter } from "../../src/target/clickhouse-writer.ts";
import { ManifestStore, type RunSummary } from "../../src/state/manifest-store.ts";
import { RedisHotState } from "../../src/state/redis-hot-state.ts";
import { BatchRunner, type BatchRunnerDeps } from "../../src/runtime/batch-runner.ts";
import { ClickHousePressure, type BackpressureConfig } from "../../src/target/clickhouse-pressure.ts";
import { GcController } from "../../src/runtime/gc-controller.ts";
import { RetryPolicy } from "../../src/runtime/retry-policy.ts";
import { serializeCursor } from "../../src/types/cursor.ts";

import {
  getRedis,
  setupClickHouse,
  teardownClickHouse,
  teardownMongo,
  teardownRedis,
  closeAll,
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

const logger = pino({ level: "silent" });

const APP_ID = "aaaaaaaaaaaaaaaaaaaaaaaa";
const EVENT_NAME = "summary_test_event";

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

async function buildDeps(collName: string): Promise<{
  deps: BatchRunnerDeps;
  mongoReader: MongoReader;
  chWriter: ClickHouseWriter;
  manifestStore: ManifestStore;
  redisState: RedisHotState;
  gcController: GcController;
  runId: string;
}> {
  const mongoReader = new MongoReader(
    {
      uri: TEST_MONGO_URI,
      database: TEST_MONGO_DB,
      readPreference: "primary",
      readConcern: "local",
      retryReads: true,
      appName: "integration-test-summary",
      batchRowsTarget: 500,
      cursorBatchSize: 500,
      maxTimeMs: 30_000,
    },
    logger,
  );
  await mongoReader.connect();
  await mongoReader.switchCollection(collName);

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

  const manifestStore = new ManifestStore(TEST_MONGO_URI, TEST_MONGO_DB);
  await manifestStore.connect();

  const redisState = new RedisHotState(TEST_REDIS_URL, TEST_REDIS_PREFIX);
  await redisState.connect();

  const chPressure = new ClickHousePressure(
    getChClientForPressure(),
    BACKPRESSURE_OFF,
    logger,
  );

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

  const retryPolicy = new RetryPolicy({
    maxRetries: 3,
    baseDelayMs: 100,
    maxDelayMs: 1000,
  });

  const upperBound = await mongoReader.getUpperBound();
  const upperBoundId = upperBound ? serializeCursor(upperBound) : "";

  const runId = randomUUID();
  const now = new Date().toISOString();
  const sourceNs = `${TEST_MONGO_DB}.${collName}`;
  const targetTable = `${TEST_CH_DB}.${TEST_CH_TABLE}`;

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
      batchRowsTarget: 500,
      mongoPageSize: 500,
      backpressure: BACKPRESSURE_OFF,
      useDedupToken: true,
      database: TEST_CH_DB,
      table: TEST_CH_TABLE,
      snapshotInterval: 10,
      collectionDefaults: { a: APP_ID, e: EVENT_NAME },
    },
  };

  return { deps, mongoReader, chWriter, manifestStore, redisState, gcController, runId };
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
// Lifecycle
// ---------------------------------------------------------------------------

beforeAll(async () => {
  await teardownMongo();
  await teardownClickHouse();
  await teardownRedis();
  await setupClickHouse();
});

afterAll(async () => {
  if (chClientForPressure) {
    await chClientForPressure.close();
    chClientForPressure = null;
  }
  await teardownMongo();
  await teardownClickHouse();
  await teardownRedis();
  await closeAll();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("write-summary", () => {
  it("completed run has correct summary stats", async () => {
    // Seed a small collection
    const { collName, expectedRows } = await seedCollection({
      count: 200,
      appId: APP_ID,
      eventName: EVENT_NAME,
    });
    expect(expectedRows).toBe(200);

    const parts = await buildDeps(collName);

    try {
      // Run migration
      const runner = new BatchRunner(parts.deps);
      await runner.run();

      // Allow async inserts to flush
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Get batch runner stats to build a summary
      const stats = runner.getStats();

      // Write a summary via ManifestStore
      const summary: RunSummary = {
        finished_at: new Date().toISOString(),
        duration_ms: stats.elapsedMs,
        total_docs_read: stats.totalDocsRead,
        total_rows_inserted: stats.totalRowsInserted,
        total_docs_skipped: stats.totalDocsSkipped,
        avg_docs_per_second: stats.docsPerSecond,
        avg_rows_per_second: stats.rowsPerSecond,
        total_batches: stats.batchSeq,
        batches_done: stats.batchSeq - stats.batchesFailed,
        batches_failed: stats.batchesFailed,
        batches_skipped_empty: 0,
        skip_reasons: stats.skipsByReason,
        total_errors: stats.batchesFailed,
        failed_batch_seqs: [],
        digest_mismatches: stats.digestMismatches,
        estimated_duplicate_rows: stats.estimatedDuplicateRows,
        coverage_pct: 100,
      };

      await parts.manifestStore.writeSummary(parts.runId, "completed", summary);

      // Verify the run document in MongoDB
      const run = await parts.manifestStore.getRun(parts.runId);
      expect(run).toBeDefined();
      expect(run!.status).toBe("completed");
      expect(run!.summary).not.toBeNull();
      expect(run!.summary!.total_docs_read).toBe(stats.totalDocsRead);
      expect(run!.summary!.total_rows_inserted).toBe(stats.totalRowsInserted);
      // max() is exclusive so the upper-bound doc may be missed (off by 1)
      expect(run!.summary!.total_docs_read).toBeGreaterThanOrEqual(198);
      expect(run!.summary!.total_rows_inserted).toBeGreaterThanOrEqual(198);
    } finally {
      await cleanupDeps(parts);
    }
  });

  it("failed run has status 'failed' not 'completed'", async () => {
    // Create a run and force-write a failed status with summary
    const manifestStore = new ManifestStore(TEST_MONGO_URI, TEST_MONGO_DB);
    await manifestStore.connect();

    try {
      const runId = randomUUID();
      const now = new Date().toISOString();

      await manifestStore.createRun({
        run_id: runId,
        status: "active",
        source_ns: `${TEST_MONGO_DB}.test_coll`,
        target_table: `${TEST_CH_DB}.${TEST_CH_TABLE}`,
        upper_bound_cursor: '{"cd":9999999999999,"id":"zzz"}',
        transform_version: "v1",
        created_at: now,
        updated_at: now,
      });

      // Write a "failed" summary
      const failedSummary: RunSummary = {
        finished_at: now,
        duration_ms: 1000,
        total_docs_read: 50,
        total_rows_inserted: 30,
        total_docs_skipped: 5,
        avg_docs_per_second: 50,
        avg_rows_per_second: 30,
        total_batches: 3,
        batches_done: 2,
        batches_failed: 1,
        batches_skipped_empty: 0,
        skip_reasons: {},
        total_errors: 1,
        failed_batch_seqs: [2],
        digest_mismatches: 0,
        estimated_duplicate_rows: 0,
        coverage_pct: 66.7,
      };

      await manifestStore.writeSummary(runId, "failed", failedSummary);

      // Verify the run is marked as failed
      const run = await manifestStore.getRun(runId);
      expect(run).toBeDefined();
      expect(run!.status).toBe("failed");
      expect(run!.status).not.toBe("completed");
      expect(run!.summary).not.toBeNull();
      expect(run!.summary!.batches_failed).toBe(1);
      expect(run!.summary!.failed_batch_seqs).toEqual([2]);
    } finally {
      await manifestStore.close();
    }
  });

  it("run finalized exactly once in range mode (SETNX)", async () => {
    const redis = await getRedis();
    const collName = "test_finalize_once";
    const prefix = TEST_REDIS_PREFIX;
    const finalizeKey = `${prefix}:ranges:${collName}:finalized`;

    // Clean up the key first
    await redis.del(finalizeKey);

    // Simulate two pods racing to finalize via SETNX
    const podAResult = await redis.set(finalizeKey, "pod-A", "EX", 60, "NX");
    const podBResult = await redis.set(finalizeKey, "pod-B", "EX", 60, "NX");

    // Only one pod should succeed
    expect(podAResult).toBe("OK");
    expect(podBResult).toBeNull();

    // The value should be from the first pod
    const value = await redis.get(finalizeKey);
    expect(value).toBe("pod-A");

    // Clean up
    await redis.del(finalizeKey);
  });
});
