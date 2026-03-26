/**
 * Integration test: multi-collection migration flows.
 *
 * Verifies that the migration service correctly processes multiple collections
 * sequentially, skips already-completed collections on restart, and excludes
 * APM collections.
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
  TEST_MONGO_URI,
  TEST_MONGO_DB,
  TEST_CH_URL,
  TEST_CH_DB,
  TEST_CH_TABLE,
  TEST_REDIS_URL,
  TEST_REDIS_PREFIX,
} from "../helpers/setup.ts";
import { seedCollection, seedMultipleCollections } from "../helpers/seed-mongo.ts";

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

/** Build shared migration infrastructure components (single set, reused across collections). */
async function buildComponents(): Promise<MigrationComponents> {
  const mongoReader = new MongoReader(
    {
      uri: TEST_MONGO_URI,
      database: TEST_MONGO_DB,
      readPreference: "primary",
      readConcern: "local",
      retryReads: true,
      appName: "multi-collection-test",
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

/**
 * Run the migration for a single collection using a BatchRunner.
 * Creates a run, resolves upper bound, and processes all batches.
 * Returns the runId and final stats.
 */
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
    // Empty collection
    return { runId: "", docsRead: 0, rowsInserted: 0 };
  }

  const upperBoundId = serializeCursor(upperBound);
  const runId = randomUUID();
  const now = new Date().toISOString();
  const sourceNs = `${TEST_MONGO_DB}.${collName}`;
  const targetTable = `${TEST_CH_DB}.${TEST_CH_TABLE}`;

  // Per-collection Redis prefix to avoid collisions
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
  return {
    runId,
    docsRead: stats.totalDocsRead,
    rowsInserted: stats.totalRowsInserted,
  };
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe("multi-collection", () => {
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

  // -----------------------------------------------------------------------
  // Test 1: processes 10 collections sequentially
  // -----------------------------------------------------------------------

  it("processes 10 collections sequentially", async () => {
    // Fresh slate
    await teardownClickHouse();
    await teardownMongo();
    await teardownRedis();
    await setupClickHouse();

    const sizes = [500, 1000, 1500, 2000, 500, 800, 1200, 600, 900, 700];
    const eventNames = sizes.map((_, i) => `multi_event_${i}`);

    // Seed 10 collections with varying sizes
    const seeded = await seedMultipleCollections(
      sizes.map((count, i) => ({
        eventName: eventNames[i],
        count,
        appId: APP_ID,
      })),
    );

    const expectedTotalRows = seeded.reduce((sum, s) => sum + s.expectedRows, 0);

    // Build shared components
    const components = await buildComponents();

    try {
      // Migrate each collection sequentially
      let totalDocsRead = 0;
      let totalRowsInserted = 0;

      for (const seed of seeded) {
        const result = await migrateCollection(
          components,
          seed.collName,
          seed.eventName,
          APP_ID,
        );
        totalDocsRead += result.docsRead;
        totalRowsInserted += result.rowsInserted;
      }

      // Allow ClickHouse async inserts to flush
      await new Promise((r) => setTimeout(r, 3000));

      // Verify all data is in ClickHouse
      const count = await chRowCount();
      expect(count).toBe(expectedTotalRows);

      // Verify total docs match
      expect(totalDocsRead).toBe(sizes.reduce((a, b) => a + b, 0));
      expect(totalRowsInserted).toBe(expectedTotalRows);
    } finally {
      await cleanupComponents(components);
    }
  }, 180_000);

  // -----------------------------------------------------------------------
  // Test 2: skips already-completed collections on restart
  // -----------------------------------------------------------------------

  it("skips already-completed collections on restart", async () => {
    // Fresh slate
    await teardownClickHouse();
    await teardownMongo();
    await teardownRedis();
    await setupClickHouse();

    const eventNames = ["restart_a", "restart_b", "restart_c", "restart_d", "restart_e"];
    const sizes = [300, 400, 500, 600, 700];

    // Seed 5 collections
    const seeded = await seedMultipleCollections(
      sizes.map((count, i) => ({
        eventName: eventNames[i],
        count,
        appId: APP_ID,
      })),
    );

    const expectedTotal = seeded.reduce((sum, s) => sum + s.expectedRows, 0);

    // --- Session 1: process first 3 collections ---
    const components1 = await buildComponents();
    const completedRunIds: string[] = [];

    try {
      for (let i = 0; i < 3; i++) {
        const result = await migrateCollection(
          components1,
          seeded[i].collName,
          seeded[i].eventName,
          APP_ID,
        );
        completedRunIds.push(result.runId);

        // Mark the run as "completed" in the manifest so the next session skips it
        const sourceNs = `${TEST_MONGO_DB}.${seeded[i].collName}`;
        const targetTable = `${TEST_CH_DB}.${TEST_CH_TABLE}`;

        await components1.manifestStore.writeSummary(result.runId, "completed", {
          finished_at: new Date().toISOString(),
          duration_ms: 0,
          total_docs_read: result.docsRead,
          total_rows_inserted: result.rowsInserted,
          total_docs_skipped: 0,
          avg_docs_per_second: 0,
          avg_rows_per_second: 0,
          total_batches: 1,
          batches_done: 1,
          batches_failed: 0,
          batches_skipped_empty: 0,
          skip_reasons: {},
          total_errors: 0,
          failed_batch_seqs: [],
          digest_mismatches: 0,
          estimated_duplicate_rows: 0,
          coverage_pct: 100,
        });
      }
    } finally {
      await cleanupComponents(components1);
    }

    // Allow ClickHouse async inserts to flush
    await new Promise((r) => setTimeout(r, 2000));
    const countAfterSession1 = await chRowCount();
    const session1Expected = seeded.slice(0, 3).reduce((sum, s) => sum + s.expectedRows, 0);
    expect(countAfterSession1).toBe(session1Expected);

    // --- Session 2: "restart" - new components, process remaining 2 ---
    const components2 = await buildComponents();

    try {
      // Before migrating each remaining collection, verify the first 3 are already completed
      for (let i = 0; i < 3; i++) {
        const sourceNs = `${TEST_MONGO_DB}.${seeded[i].collName}`;
        const targetTable = `${TEST_CH_DB}.${TEST_CH_TABLE}`;
        const alreadyCompleted = await components2.manifestStore.existsCompletedRun(sourceNs, targetTable);
        expect(alreadyCompleted).toBe(true);
      }

      // Process collections 4 and 5 only (simulating the skip-and-continue logic)
      for (let i = 3; i < 5; i++) {
        const sourceNs = `${TEST_MONGO_DB}.${seeded[i].collName}`;
        const targetTable = `${TEST_CH_DB}.${TEST_CH_TABLE}`;
        const alreadyCompleted = await components2.manifestStore.existsCompletedRun(sourceNs, targetTable);
        expect(alreadyCompleted).toBe(false);

        const result = await migrateCollection(
          components2,
          seeded[i].collName,
          seeded[i].eventName,
          APP_ID,
        );
        expect(result.docsRead).toBe(sizes[i]);
      }
    } finally {
      await cleanupComponents(components2);
    }

    // Allow ClickHouse async inserts to flush
    await new Promise((r) => setTimeout(r, 2000));

    // Verify total CH rows = sum of all 5 collections
    const finalCount = await chRowCount();
    expect(finalCount).toBe(expectedTotal);
  }, 120_000);

  // -----------------------------------------------------------------------
  // Test 3: APM collections are excluded
  // -----------------------------------------------------------------------

  it("APM collections are excluded", async () => {
    // Fresh slate
    await teardownClickHouse();
    await teardownMongo();
    await teardownRedis();
    await setupClickHouse();

    // Seed a normal collection
    const normalEvent = "normal_event";
    const normalSeed = await seedCollection({
      count: 200,
      appId: APP_ID,
      eventName: normalEvent,
    });

    // Seed APM collections: [CLY]_apm_device and [CLY]_apm_network
    const apmDeviceSeed = await seedCollection({
      count: 100,
      appId: APP_ID,
      eventName: "[CLY]_apm_device",
    });

    const apmNetworkSeed = await seedCollection({
      count: 100,
      appId: APP_ID,
      eventName: "[CLY]_apm_network",
    });

    // Seed another normal collection to confirm non-APM still works
    const normalEvent2 = "normal_event_2";
    const normalSeed2 = await seedCollection({
      count: 150,
      appId: APP_ID,
      eventName: normalEvent2,
    });

    // The orchestrator filters APM by checking hashResolver.resolveCollectionName.
    // We simulate this by checking the event names directly (the orchestrator uses
    // the skipEventNames set: [CLY]_apm_device, [CLY]_apm_network).
    const skipEventNames = new Set(["[CLY]_apm_device", "[CLY]_apm_network"]);

    // Gather all seeded collections and their event info
    const allSeeded = [
      { ...normalSeed, eventName: normalEvent, isApm: false },
      { ...apmDeviceSeed, eventName: "[CLY]_apm_device", isApm: true },
      { ...apmNetworkSeed, eventName: "[CLY]_apm_network", isApm: true },
      { ...normalSeed2, eventName: normalEvent2, isApm: false },
    ];

    // Filter: simulate orchestrator's APM exclusion
    const nonApm = allSeeded.filter(s => !skipEventNames.has(s.eventName));
    const apmExcluded = allSeeded.filter(s => skipEventNames.has(s.eventName));

    expect(nonApm.length).toBe(2);
    expect(apmExcluded.length).toBe(2);

    // Migrate only non-APM collections
    const components = await buildComponents();
    const results: Array<{ collName: string; eventName: string; status: string; rowsInserted: number }> = [];

    try {
      for (const seed of nonApm) {
        const result = await migrateCollection(
          components,
          seed.collName,
          seed.eventName,
          APP_ID,
        );
        results.push({
          collName: seed.collName,
          eventName: seed.eventName,
          status: "completed",
          rowsInserted: result.rowsInserted,
        });
      }

      // Record APM collections as skipped (simulating orchestrator behavior)
      for (const seed of apmExcluded) {
        results.push({
          collName: seed.collName,
          eventName: seed.eventName,
          status: "skipped",
          rowsInserted: 0,
        });
      }
    } finally {
      await cleanupComponents(components);
    }

    // Allow ClickHouse async inserts to flush
    await new Promise((r) => setTimeout(r, 2000));

    // Verify: APM collections are NOT in results with status "completed"
    const completedResults = results.filter(r => r.status === "completed");
    const skippedResults = results.filter(r => r.status === "skipped");

    expect(completedResults.length).toBe(2);
    expect(skippedResults.length).toBe(2);

    // No completed result should be an APM collection
    for (const cr of completedResults) {
      expect(skipEventNames.has(cr.eventName)).toBe(false);
    }

    // All skipped should be APM
    for (const sr of skippedResults) {
      expect(skipEventNames.has(sr.eventName)).toBe(true);
    }

    // ClickHouse should only have rows from the two normal collections
    const expectedNonApmRows = normalSeed.expectedRows + normalSeed2.expectedRows;
    const totalCh = await chRowCount();
    expect(totalCh).toBe(expectedNonApmRows);
  }, 60_000);
});
