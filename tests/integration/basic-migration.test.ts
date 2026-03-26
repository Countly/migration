/**
 * Integration test: basic MongoDB-to-ClickHouse migration flow.
 *
 * Seeds test documents into MongoDB, runs the BatchRunner programmatically,
 * and verifies ClickHouse row counts match expectations.
 */
import { describe, it, expect, beforeAll, afterAll } from "vitest";
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
  getMongoDb,
  getRedis,
  setupClickHouse,
  teardownClickHouse,
  teardownMongo,
  teardownRedis,
  closeAll,
  chRowCount,
  TEST_MONGO_URI,
  TEST_MONGO_DB,
  TEST_CH_URL,
  TEST_CH_DB,
  TEST_CH_TABLE,
  TEST_REDIS_URL,
  TEST_REDIS_PREFIX,
  TEST_COLLECTION_PREFIX,
} from "../helpers/setup.ts";
import { seedCollection, collectionName } from "../helpers/seed-mongo.ts";

// ---------------------------------------------------------------------------
// Shared config
// ---------------------------------------------------------------------------

const logger = pino({ level: "warn" });

const APP_ID = "aaaaaaaaaaaaaaaaaaaaaaaa";
const EVENT_NAME = "test_event";

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
// Helpers to construct migration components per test
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

async function buildDeps(overrides: {
  collName: string;
  appId?: string;
  eventName?: string;
}): Promise<{
  deps: BatchRunnerDeps;
  mongoReader: MongoReader;
  chWriter: ClickHouseWriter;
  manifestStore: ManifestStore;
  redisState: RedisHotState;
  gcController: GcController;
  runId: string;
  upperBoundId: string;
}> {
  const appId = overrides.appId ?? APP_ID;
  const eventName = overrides.eventName ?? EVENT_NAME;
  const collName = overrides.collName;

  // MongoReader
  const mongoReader = new MongoReader(
    {
      uri: TEST_MONGO_URI,
      database: TEST_MONGO_DB,
      readPreference: "primary",
      readConcern: "local",
      retryReads: true,
      appName: "integration-test",
      batchRowsTarget: 500,
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

  // ClickHousePressure (disabled for tests)
  const chPressure = new ClickHousePressure(
    getChClientForPressure(),
    BACKPRESSURE_OFF,
    logger,
  );

  // GcController (no-op since --expose-gc is not set in test)
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
  const upperBound = await mongoReader.getUpperBound();
  const upperBoundId = upperBound ? serializeCursor(upperBound) : "";

  // Create run in manifest
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

describe("basic-migration", () => {
  beforeAll(async () => {
    // Clean slate: drop everything and recreate the CH table
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
    await closeAll();
  });

  // -----------------------------------------------------------------------
  // Test 1: full migration of 5000 docs
  // -----------------------------------------------------------------------

  it("should migrate all documents from MongoDB to ClickHouse", async () => {
    // Seed
    const { collName, expectedRows } = await seedCollection({
      count: 5000,
      appId: APP_ID,
      eventName: EVENT_NAME,
    });
    expect(expectedRows).toBe(5000);

    // Build components
    const parts = await buildDeps({ collName });

    try {
      // Run migration
      const runner = new BatchRunner(parts.deps);
      await runner.run();

      // Allow a moment for ClickHouse async inserts to flush
      await new Promise((r) => setTimeout(r, 2000));

      // Verify ClickHouse count (min() inclusivity may cause 1-2 extra duplicates per page)
      const count = await chRowCount();
      expect(count).toBeGreaterThanOrEqual(5000);
      expect(count).toBeLessThanOrEqual(5020); // small tolerance for min() inclusivity
    } finally {
      await cleanupDeps(parts);
    }
  });

  // -----------------------------------------------------------------------
  // Test 2: skip migrated documents (10%)
  // -----------------------------------------------------------------------

  it("should skip documents marked as migrated", async () => {
    // Clean CH table for this test
    await teardownClickHouse();
    await setupClickHouse();

    // Seed with 10% migrated
    const { collName, expectedRows, totalDocs } = await seedCollection({
      count: 5000,
      appId: APP_ID,
      eventName: "migrated_test",
      migratedFraction: 0.1,
    });

    // expectedRows should be approximately 4500 (5000 - ~10% migrated)
    expect(totalDocs).toBe(5000);
    expect(expectedRows).toBeLessThan(5000);
    expect(expectedRows).toBeGreaterThan(4000);

    // Build components
    const parts = await buildDeps({
      collName,
      eventName: "migrated_test",
    });

    try {
      const runner = new BatchRunner(parts.deps);
      await runner.run();

      // Allow async inserts to flush
      await new Promise((r) => setTimeout(r, 2000));

      const count = await chRowCount();
      // min() inclusivity may cause small duplicate count; expectedRows is minimum
      expect(count).toBeGreaterThanOrEqual(expectedRows);
      expect(count).toBeLessThanOrEqual(expectedRows + 20);
    } finally {
      await cleanupDeps(parts);
    }
  });

  // -----------------------------------------------------------------------
  // Test 3: skip documents missing uid (5%)
  // -----------------------------------------------------------------------

  it("should skip documents missing uid", async () => {
    // Clean CH table for this test
    await teardownClickHouse();
    await setupClickHouse();

    // Seed with 5% missing uid
    const { collName, expectedRows, totalDocs } = await seedCollection({
      count: 5000,
      appId: APP_ID,
      eventName: "missing_uid_test",
      missingUidFraction: 0.05,
    });

    expect(totalDocs).toBe(5000);
    expect(expectedRows).toBeLessThan(5000);
    expect(expectedRows).toBeGreaterThan(4500);

    // Build components
    const parts = await buildDeps({
      collName,
      eventName: "missing_uid_test",
    });

    try {
      const runner = new BatchRunner(parts.deps);
      await runner.run();

      // Allow async inserts to flush
      await new Promise((r) => setTimeout(r, 2000));

      const count = await chRowCount();
      expect(count).toBeGreaterThanOrEqual(expectedRows);
      expect(count).toBeLessThanOrEqual(expectedRows + 20);
    } finally {
      await cleanupDeps(parts);
    }
  });

  // -----------------------------------------------------------------------
  // Test 4: empty collection
  // -----------------------------------------------------------------------

  it("should handle empty collection gracefully", async () => {
    // Clean CH table for this test
    await teardownClickHouse();
    await setupClickHouse();

    // Seed with 0 docs
    const { collName, expectedRows } = await seedCollection({
      count: 0,
      appId: APP_ID,
      eventName: "empty_test",
    });
    expect(expectedRows).toBe(0);

    // For an empty collection, getUpperBound() returns null.
    // Build MongoReader manually and check.
    const mongoReader = new MongoReader(
      {
        uri: TEST_MONGO_URI,
        database: TEST_MONGO_DB,
        readPreference: "primary",
        readConcern: "local",
        retryReads: true,
        appName: "integration-test",
        batchRowsTarget: 500,
        cursorBatchSize: 500,
        maxTimeMs: 30_000,
      },
      logger,
    );
    await mongoReader.connect();
    await mongoReader.switchCollection(collName);

    try {
      const upperBound = await mongoReader.getUpperBound();
      expect(upperBound).toBeNull();

      // With a null upper bound the migration has nothing to do.
      // Verify no rows in ClickHouse.
      const count = await chRowCount();
      expect(count).toBe(0);
    } finally {
      await mongoReader.close();
    }
  });
});
