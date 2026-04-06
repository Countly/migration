/**
 * End-to-end test: 3 collections with varying null-cd fractions.
 *
 * Seeds 3 collections (1000, 1500, 2000 docs) with 0%, 30%, and 100%
 * null-cd documents, runs the full migration pipeline, and verifies
 * all rows land in ClickHouse — including the null-cd sweep phase.
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
import { seedCollection, seedNullCdCollection } from "../helpers/seed-mongo.ts";

// ---------------------------------------------------------------------------
// Shared config
// ---------------------------------------------------------------------------

const logger = pino({ level: "warn" });

const APP_ID = "aaaaaaaaaaaaaaaaaaaaaaaa";

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

interface MigrationComponents {
  mongoReader: MongoReader;
  chWriter: ClickHouseWriter;
  manifestStore: ManifestStore;
  redisState: RedisHotState;
  gcController: GcController;
  retryPolicy: RetryPolicy;
  chPressure: ClickHousePressure;
}

async function buildComponents(): Promise<MigrationComponents> {
  const mongoReader = new MongoReader(
    {
      uri: TEST_MONGO_URI,
      database: TEST_MONGO_DB,
      readPreference: "primary",
      readConcern: "local",
      retryReads: true,
      appName: "three-coll-e2e-test",
      batchRowsTarget: 500,
      cursorBatchSize: 500,
      maxTimeMs: 30_000,
    },
    logger,
  );
  await mongoReader.connect();

  const chWriter = new ClickHouseWriter(
    {
      url: TEST_CH_URL,
      database: TEST_CH_DB,
      table: TEST_CH_TABLE,
      username: "default",
      password: "",
      queryTimeoutMs: 30_000,
      useDedupToken: false,
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

  return { mongoReader, chWriter, manifestStore, redisState, gcController, retryPolicy, chPressure };
}

async function cleanupComponents(c: MigrationComponents): Promise<void> {
  await c.mongoReader.close().catch(() => {});
  await c.chWriter.close().catch(() => {});
  await c.manifestStore.close().catch(() => {});
  await c.redisState.close().catch(() => {});
  c.gcController.dispose();
}

async function migrateCollection(
  c: MigrationComponents,
  collName: string,
  eventName: string,
  appId: string = APP_ID,
): Promise<{ runId: string; docsRead: number; rowsInserted: number }> {
  const { mongoReader, chWriter, manifestStore, redisState, chPressure, gcController, retryPolicy } = c;

  await mongoReader.switchCollection(collName);

  const upperBound = await mongoReader.getUpperBound();
  if (!upperBound) {
    // Empty collection or all-null-cd — check for null-cd docs
    const hasNullCd = await mongoReader.hasNullCdDocuments();
    if (!hasNullCd) {
      return { runId: "", docsRead: 0, rowsInserted: 0 };
    }

    // All-null-cd collection: create a run with nullCdMode enabled.
    // BatchRunner still deserializes upperBoundId in its loop, so we
    // provide a dummy cursor that won't actually be used for reads.
    const bounds = await mongoReader.getNullCdBounds();
    if (!bounds) {
      return { runId: "", docsRead: 0, rowsInserted: 0 };
    }

    const dummyUpperBound = JSON.stringify({ cd: 0, id: "000000000000000000000000" });

    const runId = randomUUID();
    const now = new Date().toISOString();
    const sourceNs = `${TEST_MONGO_DB}.${collName}`;
    const targetTable = `${TEST_CH_DB}.${TEST_CH_TABLE}`;

    const collRedisState = RedisHotState.fromExistingConnection(
      redisState.getRedisClient(),
      `${TEST_REDIS_PREFIX}:${collName}`,
    );

    await manifestStore.createRun({
      run_id: runId,
      status: "active",
      source_ns: sourceNs,
      target_table: targetTable,
      upper_bound_cursor: dummyUpperBound,
      transform_version: "v1",
      created_at: now,
      updated_at: now,
    });

    await collRedisState.setActiveRun(runId);
    await collRedisState.setState(runId, {
      runId,
      status: "active",
      sourceNs,
      targetTable,
      upperBoundCursor: dummyUpperBound,
      lastCommittedCursor: null,
      transformVersion: "v1",
      totalBatches: 0,
      completedBatches: 0,
      startedAt: now,
    });

    const deps: BatchRunnerDeps = {
      manifestStore,
      redisState: collRedisState,
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
        upperBoundId: dummyUpperBound,
        batchRowsTarget: 500,
        mongoPageSize: 500,
        backpressure: BACKPRESSURE_OFF,
        useDedupToken: false,
        database: TEST_CH_DB,
        table: TEST_CH_TABLE,
        snapshotInterval: 10,
        collectionDefaults: { a: appId, e: eventName },
        collectionName: collName,
        nullCdMode: true,
        nullCdUpperBound: bounds.upper,
      },
    };

    const runner = new BatchRunner(deps);
    await runner.run();

    const stats = runner.getStats();
    return { runId, docsRead: stats.totalDocsRead, rowsInserted: stats.totalRowsInserted };
  }

  // Normal path: cursor-based migration (with auto null-cd sweep if needed)
  const upperBoundId = serializeCursor(upperBound);
  const runId = randomUUID();
  const now = new Date().toISOString();
  const sourceNs = `${TEST_MONGO_DB}.${collName}`;
  const targetTable = `${TEST_CH_DB}.${TEST_CH_TABLE}`;

  const collRedisState = RedisHotState.fromExistingConnection(
    redisState.getRedisClient(),
    `${TEST_REDIS_PREFIX}:${collName}`,
  );

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

  await collRedisState.setActiveRun(runId);
  await collRedisState.setState(runId, {
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
    redisState: collRedisState,
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
      useDedupToken: false,
      database: TEST_CH_DB,
      table: TEST_CH_TABLE,
      snapshotInterval: 10,
      collectionDefaults: { a: appId, e: eventName },
      collectionName: collName,
    },
  };

  const runner = new BatchRunner(deps);
  await runner.run();

  const stats = runner.getStats();
  return { runId, docsRead: stats.totalDocsRead, rowsInserted: stats.totalRowsInserted };
}

/** Count rows in ClickHouse for a specific event name (custom events have n = eventName). */
async function chCountByEvent(eventName: string): Promise<number> {
  const rows = await chQuery<{ cnt: string }>(
    `SELECT count() AS cnt FROM ${TEST_CH_TABLE} WHERE n = '${eventName}'`,
  );
  return Number(rows[0]?.cnt ?? 0);
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe("three-collection-nullcd-e2e", () => {
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
    await closeAll();
  });

  it("migrates 3 collections with 0%, 30%, and 100% null-cd docs to ClickHouse", async () => {
    // ── Seed ────────────────────────────────────────────────────────────

    // Collection 1: 1000 docs, all with valid cd
    const seed1 = await seedCollection({
      count: 1000,
      appId: APP_ID,
      eventName: "e2e_all_valid",
      nullCdFraction: 0,
    });

    // Collection 2: 1500 docs, 30% null-cd
    const seed2 = await seedCollection({
      count: 1500,
      appId: APP_ID,
      eventName: "e2e_mixed_cd",
      nullCdFraction: 0.3,
    });

    // Collection 3: 2000 docs, ALL null-cd
    const seed3 = await seedNullCdCollection({
      count: 2000,
      appId: APP_ID,
      eventName: "e2e_all_null_cd",
    });

    logger.info({
      seed1: { collName: seed1.collName, total: seed1.totalDocs, expected: seed1.expectedRows },
      seed2: { collName: seed2.collName, total: seed2.totalDocs, expected: seed2.expectedRows },
      seed3: { collName: seed3.collName, total: seed3.totalDocs, expected: seed3.expectedRows },
    }, "Seeded 3 collections");

    // ── Migrate ─────────────────────────────────────────────────────────

    const components = await buildComponents();

    try {
      // Collection 1: all-valid (cursor phase only)
      const result1 = await migrateCollection(components, seed1.collName, "e2e_all_valid");
      expect(result1.docsRead).toBeGreaterThanOrEqual(1000);

      // Collection 2: mixed (cursor phase + null-cd sweep)
      const result2 = await migrateCollection(components, seed2.collName, "e2e_mixed_cd");
      expect(result2.docsRead).toBeGreaterThanOrEqual(1);

      // Collection 3: all-null-cd (null-cd mode only)
      const result3 = await migrateCollection(components, seed3.collName, "e2e_all_null_cd");
      expect(result3.docsRead).toBeGreaterThanOrEqual(1);

      // ── Verify per-collection ───────────────────────────────────────

      // Allow ClickHouse async inserts to flush
      await new Promise((r) => setTimeout(r, 3000));

      // Collection 1: 1000 docs, 0% null-cd → all 1000 expected
      const count1 = await chCountByEvent("e2e_all_valid");
      expect(count1).toBeGreaterThanOrEqual(1000);
      expect(count1).toBeLessThanOrEqual(1020);

      // Collection 2: 1500 docs, 30% null-cd → all 1500 expected (cursor + sweep)
      const count2 = await chCountByEvent("e2e_mixed_cd");
      expect(count2).toBeGreaterThanOrEqual(seed2.expectedRows);
      expect(count2).toBeLessThanOrEqual(seed2.expectedRows + 30);

      // Collection 3: 2000 docs, 100% null-cd → all 2000 expected (sweep only)
      const count3 = await chCountByEvent("e2e_all_null_cd");
      expect(count3).toBeGreaterThanOrEqual(2000);
      expect(count3).toBeLessThanOrEqual(2020);

      // ── Verify aggregate total ──────────────────────────────────────

      const totalExpected = seed1.expectedRows + seed2.expectedRows + seed3.expectedRows;
      const totalCh = await chRowCount();
      expect(totalCh).toBeGreaterThanOrEqual(totalExpected);
      expect(totalCh).toBeLessThanOrEqual(totalExpected + 70);

      logger.info({
        count1, count2, count3,
        totalCh, totalExpected,
      }, "Verification complete");
    } finally {
      await cleanupComponents(components);
    }
  }, 180_000);
});
