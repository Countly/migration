/**
 * Integration test: migration statistics accuracy.
 *
 * Verifies that migration metrics (docsRead, rowsInserted, docsSkipped)
 * match actual data in MongoDB and ClickHouse, and that cluster-aware
 * status derivation logic is correct.
 */
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { randomUUID } from "node:crypto";
import pino from "pino";
import { createClient, type ClickHouseClient } from "@clickhouse/client";

import { MongoReader } from "../../src/source/mongo-reader.ts";
import { ClickHouseWriter } from "../../src/target/clickhouse-writer.ts";
import { ManifestStore } from "../../src/state/manifest-store.ts";
import { RedisHotState } from "../../src/state/redis-hot-state.ts";
import { BatchRunner, type BatchRunnerDeps, type BatchRunnerStats } from "../../src/runtime/batch-runner.ts";
import { ClickHousePressure, type BackpressureConfig } from "../../src/target/clickhouse-pressure.ts";
import { GcController } from "../../src/runtime/gc-controller.ts";
import { RetryPolicy } from "../../src/runtime/retry-policy.ts";
import { serializeCursor } from "../../src/types/cursor.ts";

import {
  getMongoDb,
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

interface MigrationResult {
  runId: string;
  stats: BatchRunnerStats;
}

/**
 * Build deps and run migration for a single collection.
 * Returns the run ID and the BatchRunner stats snapshot.
 */
async function buildAndRunMigration(opts: {
  collName: string;
  appId?: string;
  eventName: string;
}): Promise<{
  result: MigrationResult;
  cleanup: () => Promise<void>;
}> {
  const appId = opts.appId ?? APP_ID;
  const { collName, eventName } = opts;

  const mongoReader = new MongoReader(
    {
      uri: TEST_MONGO_URI,
      database: TEST_MONGO_DB,
      readPreference: "primary",
      readConcern: "local",
      retryReads: true,
      appName: "stats-accuracy-test",
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
      useDedupToken: false,
    },
    logger,
  );
  await chWriter.connect();

  const manifestStore = new ManifestStore(TEST_MONGO_URI, TEST_MONGO_DB);
  await manifestStore.connect();

  const redisState = new RedisHotState(TEST_REDIS_URL, TEST_REDIS_PREFIX);
  await redisState.connect();

  const collRedisState = RedisHotState.fromExistingConnection(
    redisState.getRedisClient(),
    `${TEST_REDIS_PREFIX}:${collName}`,
  );

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

  const cleanup = async () => {
    await mongoReader.close().catch(() => {});
    await chWriter.close().catch(() => {});
    await manifestStore.close().catch(() => {});
    await redisState.close().catch(() => {});
    gcController.dispose();
  };

  return {
    result: { runId, stats },
    cleanup,
  };
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe("stats-accuracy", () => {
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
  // Test 1: estimatedCounts match estimatedDocumentCount
  // -----------------------------------------------------------------------

  it("estimatedCounts match estimatedDocumentCount", async () => {
    // Fresh slate
    await teardownMongo();
    await teardownRedis();

    const sizes = [1000, 2000, 3000];
    const eventNames = ["est_event_a", "est_event_b", "est_event_c"];

    const seeded = await seedMultipleCollections(
      sizes.map((count, i) => ({
        eventName: eventNames[i],
        count,
        appId: APP_ID,
      })),
    );

    // Verify estimatedDocumentCount matches seeded count
    const db = await getMongoDb();
    for (let i = 0; i < seeded.length; i++) {
      const estimated = await db.collection(seeded[i].collName).estimatedDocumentCount();
      // estimatedDocumentCount can be slightly off, allow +/-1 tolerance
      expect(estimated).toBeGreaterThanOrEqual(sizes[i] - 1);
      expect(estimated).toBeLessThanOrEqual(sizes[i] + 1);
    }
  });

  // -----------------------------------------------------------------------
  // Test 2: docsRead matches actual docs processed
  // -----------------------------------------------------------------------

  it("docsRead matches actual docs processed", async () => {
    // Fresh slate for CH
    await teardownClickHouse();
    await teardownMongo();
    await teardownRedis();
    await setupClickHouse();

    // Seed 2000 docs with 10% migrated (should be skipped at transform, not at read)
    const eventName = "docs_read_test";
    const seed = await seedCollection({
      count: 2000,
      appId: APP_ID,
      eventName,
      migratedFraction: 0.1,
    });

    const { result, cleanup } = await buildAndRunMigration({
      collName: seed.collName,
      eventName,
    });

    try {
      const { stats } = result;

      // BatchRunner reads ALL docs including migrated ones (min() inclusivity adds small duplicates)
      expect(stats.totalDocsRead).toBeGreaterThanOrEqual(2000);
      expect(stats.totalDocsRead).toBeLessThanOrEqual(2020);

      // Allow ClickHouse async inserts to flush
      await new Promise((r) => setTimeout(r, 2000));

      // CH rows should only include non-migrated docs (with small min() tolerance)
      const chCount = await chRowCount();
      expect(chCount).toBeGreaterThanOrEqual(seed.expectedRows);
      expect(chCount).toBeLessThanOrEqual(seed.expectedRows + 20);

      // expectedRows is approximately 1800 (2000 - ~10% migrated)
      expect(seed.expectedRows).toBeLessThan(2000);
      expect(seed.expectedRows).toBeGreaterThan(1700);

      // totalDocsSkipped should account for the migrated documents
      // Migrated docs are skipped with reason "already_marked_migrated"
      const migratedSkipped = stats.totalDocsRead - seed.expectedRows;
      expect(stats.totalDocsSkipped).toBeGreaterThanOrEqual(migratedSkipped - 10);
      expect(stats.totalDocsSkipped).toBeLessThanOrEqual(migratedSkipped + 10);
    } finally {
      await cleanup();
    }
  }, 60_000);

  // -----------------------------------------------------------------------
  // Test 3: rowsInserted matches ClickHouse count
  // -----------------------------------------------------------------------

  it("rowsInserted matches ClickHouse count", async () => {
    // Fresh slate
    await teardownClickHouse();
    await teardownMongo();
    await teardownRedis();
    await setupClickHouse();

    const eventName = "rows_inserted_test";
    const seed = await seedCollection({
      count: 3000,
      appId: APP_ID,
      eventName,
    });

    // No migrated or missing uid docs, so expectedRows = 3000
    expect(seed.expectedRows).toBe(3000);

    const { result, cleanup } = await buildAndRunMigration({
      collName: seed.collName,
      eventName,
    });

    try {
      const { stats } = result;

      // Allow ClickHouse async inserts to flush
      await new Promise((r) => setTimeout(r, 2000));

      // stats.totalRowsInserted should match SELECT count() from ClickHouse (with min() tolerance)
      const chCount = await chRowCount();
      expect(chCount).toBeGreaterThanOrEqual(3000);
      expect(chCount).toBeLessThanOrEqual(3020);
      // The runner's count should match CH
      expect(stats.totalRowsInserted).toBe(chCount);
    } finally {
      await cleanup();
    }
  }, 60_000);

  // -----------------------------------------------------------------------
  // Test 4: cluster-aware status shows running while processing
  // -----------------------------------------------------------------------

  it("cluster-aware status shows running while processing", () => {
    // This tests the status derivation IIFE logic from stats-route.ts:
    //
    //   status: (() => {
    //     if (!clusterData) return runnerStatus;
    //     if (clusterProcessing > 0) return "running";
    //     if ((clusterDone + clusterFailed) >= clusterTotal && clusterTotal > 0) return "completed";
    //     return runnerStatus;
    //   })()
    //
    // We test this as a pure unit test of the derivation logic.

    function deriveClusterStatus(opts: {
      clusterData: boolean;
      runnerStatus: string;
      clusterProcessing: number;
      clusterDone: number;
      clusterFailed: number;
      clusterTotal: number;
    }): string {
      if (!opts.clusterData) return opts.runnerStatus;
      if (opts.clusterProcessing > 0) return "running";
      if ((opts.clusterDone + opts.clusterFailed) >= opts.clusterTotal && opts.clusterTotal > 0) return "completed";
      return opts.runnerStatus;
    }

    // Case 1: No cluster data - returns runner status directly
    expect(deriveClusterStatus({
      clusterData: false,
      runnerStatus: "idle",
      clusterProcessing: 0,
      clusterDone: 0,
      clusterFailed: 0,
      clusterTotal: 10,
    })).toBe("idle");

    // Case 2: Cluster processing > 0 - returns "running"
    expect(deriveClusterStatus({
      clusterData: true,
      runnerStatus: "idle",
      clusterProcessing: 3,
      clusterDone: 5,
      clusterFailed: 0,
      clusterTotal: 10,
    })).toBe("running");

    // Case 3: All collections done (no failures) - returns "completed"
    expect(deriveClusterStatus({
      clusterData: true,
      runnerStatus: "idle",
      clusterProcessing: 0,
      clusterDone: 10,
      clusterFailed: 0,
      clusterTotal: 10,
    })).toBe("completed");

    // Case 4: All collections terminal (some failed) - returns "completed"
    expect(deriveClusterStatus({
      clusterData: true,
      runnerStatus: "idle",
      clusterProcessing: 0,
      clusterDone: 8,
      clusterFailed: 2,
      clusterTotal: 10,
    })).toBe("completed");

    // Case 5: Still processing with some failures - returns "running"
    expect(deriveClusterStatus({
      clusterData: true,
      runnerStatus: "idle",
      clusterProcessing: 1,
      clusterDone: 7,
      clusterFailed: 2,
      clusterTotal: 10,
    })).toBe("running");

    // Case 6: Zero total collections (edge case) - falls through to runnerStatus
    expect(deriveClusterStatus({
      clusterData: true,
      runnerStatus: "idle",
      clusterProcessing: 0,
      clusterDone: 0,
      clusterFailed: 0,
      clusterTotal: 0,
    })).toBe("idle");

    // Case 7: Partial completion, no processing - falls through to runnerStatus
    expect(deriveClusterStatus({
      clusterData: true,
      runnerStatus: "waiting_for_index",
      clusterProcessing: 0,
      clusterDone: 5,
      clusterFailed: 0,
      clusterTotal: 10,
    })).toBe("waiting_for_index");
  });
});
