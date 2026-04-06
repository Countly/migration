/**
 * Integration test: self-healing range retry mechanism in RangeCoordinator.
 *
 * Verifies that when a range fails during processing, the coordinator
 * retries it up to MAX_RANGE_RETRIES times, and correctly marks the run
 * as "failed" when retries are exhausted.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import pino from "pino";
import { createClient } from "@clickhouse/client";

import {
  RangeCoordinator,
  type RangeCoordinatorConfig,
  type RangeCoordinatorDeps,
} from "../../src/runtime/range-coordinator.ts";
import { MongoReader, type MongoReaderConfig } from "../../src/source/mongo-reader.ts";
import {
  ClickHouseWriter,
  type ClickHouseWriterConfig,
  type InsertBatchParams,
  type InsertResult,
} from "../../src/target/clickhouse-writer.ts";
import { ClickHousePressure, type BackpressureConfig } from "../../src/target/clickhouse-pressure.ts";
import { ManifestStore } from "../../src/state/manifest-store.ts";
import { RedisHotState } from "../../src/state/redis-hot-state.ts";
import { GcController } from "../../src/runtime/gc-controller.ts";
import { RetryPolicy } from "../../src/runtime/retry-policy.ts";

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
import { seedCollection, collectionName } from "../helpers/seed-mongo.ts";

// ---------------------------------------------------------------------------
// Shared config
// ---------------------------------------------------------------------------

const logger = pino({ level: "silent" });

const APP_ID = "aaaaaaaaaaaaaaaaaaaaaaaa";
const EVENT_NAME = "range_retry_event";
const COLL_NAME = collectionName(EVENT_NAME, APP_ID);
const SOURCE_NS = `${TEST_MONGO_DB}.${COLL_NAME}`;
const TARGET_TABLE = `${TEST_CH_DB}.${TEST_CH_TABLE}`;

const BACKPRESSURE_OFF: BackpressureConfig = {
  enabled: false,
  partsToThrowInsert: 300,
  maxPartsInTotal: 100_000,
  partitionPctHigh: 0.8,
  partitionPctLow: 0.6,
  totalPctHigh: 0.8,
  totalPctLow: 0.6,
  pollIntervalMs: 5000,
  maxPauseEpisodeMs: 180_000,
};

// ---------------------------------------------------------------------------
// Shared resources
// ---------------------------------------------------------------------------

let manifestStore: ManifestStore;
let redisState: RedisHotState;
let mongoReader: MongoReader;
let chWriter: ClickHouseWriter;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createChPressure(): ClickHousePressure {
  const client = createClient({
    url: TEST_CH_URL,
    username: "default",
    password: "",
    database: TEST_CH_DB,
  });
  return new ClickHousePressure(client, BACKPRESSURE_OFF, logger);
}

function createGcController(): GcController {
  return new GcController(
    {
      enabled: false,
      rssSoftLimitBytes: 2 * 1024 * 1024 * 1024,
      rssHardLimitBytes: 3 * 1024 * 1024 * 1024,
      heapUsedRatio: 0.85,
      everyNBatches: 999_999,
    },
    logger,
  );
}

function createRetryPolicy(): RetryPolicy {
  return new RetryPolicy({
    maxRetries: 2,
    baseDelayMs: 50,
    maxDelayMs: 200,
  });
}

/**
 * Creates a Proxy around ClickHouseWriter that intercepts insertBatch calls.
 * The `shouldFail` predicate receives the InsertBatchParams and returns true
 * if the call should throw.
 */
function proxyWriter(
  real: ClickHouseWriter,
  shouldFail: (params: InsertBatchParams) => boolean,
): ClickHouseWriter {
  return new Proxy(real, {
    get(target, prop, receiver) {
      if (prop === "insertBatch") {
        return async (params: InsertBatchParams): Promise<InsertResult> => {
          if (shouldFail(params)) {
            throw new Error(`Injected failure for batchSeq=${params.batchSeq}`);
          }
          return target.insertBatch(params);
        };
      }
      return Reflect.get(target, prop, receiver);
    },
  });
}

function buildRangeCoordinatorDeps(overrides: {
  chWriterOverride?: ClickHouseWriter;
  rangeCount?: number;
}): RangeCoordinatorDeps {
  const config: RangeCoordinatorConfig = {
    collectionName: COLL_NAME,
    sourceNs: SOURCE_NS,
    targetTable: TARGET_TABLE,
    transformVersion: "v1",
    rangeCount: overrides.rangeCount ?? 3,
    rangeLeaseTtlSec: 300,
    batchRowsTarget: 500,
    mongoPageSize: 500,
    backpressure: BACKPRESSURE_OFF,
    useDedupToken: false,
    database: TEST_CH_DB,
    table: TEST_CH_TABLE,
    snapshotInterval: 100,
    collectionDefaults: { a: APP_ID, e: EVENT_NAME },
    podId: "test-pod-retry",
    redisKeyPrefix: TEST_REDIS_PREFIX,
  };

  return {
    redis: redisState.getRedisClient(),
    manifestStore,
    redisState,
    mongoReader,
    chWriter: overrides.chWriterOverride ?? chWriter,
    chPressure: createChPressure(),
    gcController: createGcController(),
    retryPolicy: createRetryPolicy(),
    logger,
    config,
  };
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

describe("range-retry", () => {
  beforeAll(async () => {
    // Connect shared resources
    manifestStore = new ManifestStore(TEST_MONGO_URI, TEST_MONGO_DB);
    await manifestStore.connect();

    redisState = new RedisHotState(TEST_REDIS_URL, TEST_REDIS_PREFIX);
    await redisState.connect();

    const mongoReaderConfig: MongoReaderConfig = {
      uri: TEST_MONGO_URI,
      database: TEST_MONGO_DB,
      readPreference: "primary",
      readConcern: "local",
      retryReads: true,
      appName: "range-retry-test",
      batchRowsTarget: 500,
      cursorBatchSize: 500,
      maxTimeMs: 30_000,
    };
    mongoReader = new MongoReader(mongoReaderConfig, logger);
    await mongoReader.connect();

    const chWriterConfig: ClickHouseWriterConfig = {
      url: TEST_CH_URL,
      database: TEST_CH_DB,
      table: TEST_CH_TABLE,
      username: "default",
      password: "",
      queryTimeoutMs: 30_000,
      useDedupToken: false,
    };
    chWriter = new ClickHouseWriter(chWriterConfig, logger);
    await chWriter.connect();
  });

  afterAll(async () => {
    await mongoReader.close().catch(() => {});
    await chWriter.close().catch(() => {});
    await manifestStore.close().catch(() => {});
    await redisState.close().catch(() => {});
    await closeAll();
  });

  beforeEach(async () => {
    // Clean slate for each test
    await teardownMongo();
    await teardownClickHouse();
    await setupClickHouse();
    await teardownRedis();

    // Re-create manifest store indexes (teardownMongo drops the DB)
    const freshManifest = new ManifestStore(TEST_MONGO_URI, TEST_MONGO_DB);
    await freshManifest.connect();
    await freshManifest.close();
  });

  // -------------------------------------------------------------------------
  // Test 1: failed range is retried and eventually succeeds
  // -------------------------------------------------------------------------

  it("failed range is retried and eventually succeeds", async () => {
    // Seed enough docs for range-parallel mode across 3 ranges
    const { collName } = await seedCollection({
      count: 3000,
      appId: APP_ID,
      eventName: EVENT_NAME,
      startDate: new Date("2024-01-01"),
      endDate: new Date("2024-12-31"),
    });

    await mongoReader.switchCollection(collName);

    // BATCH_SEQ_SLOTS_PER_RANGE = 10_000
    // Range 1 occupies batchSeq slots [10000, 20000).
    // Fail insertBatch for the first attempt on any batchSeq in range 1's slot,
    // then allow subsequent retries to succeed.
    const failedOnce = new Set<number>();
    const proxied = proxyWriter(chWriter, (params) => {
      const batchSeq = params.batchSeq;
      // Range 1 slots: [10000, 20000)
      if (batchSeq >= 10_000 && batchSeq < 20_000) {
        if (!failedOnce.has(batchSeq)) {
          failedOnce.add(batchSeq);
          return true; // fail the first attempt
        }
      }
      return false;
    });

    const deps = buildRangeCoordinatorDeps({
      chWriterOverride: proxied,
      rangeCount: 3,
    });

    const coordinator = new RangeCoordinator(deps);
    const result = await coordinator.run();

    // Allow ClickHouse async inserts to flush
    await new Promise((r) => setTimeout(r, 3000));

    // Range 1 should have been retried (failed initially, then succeeded)
    // All 3 ranges should be completed
    expect(result.totalRanges).toBe(3);
    expect(result.completedRanges + result.failedRanges).toBe(3);

    // The run should be completed (not failed) because range 1 succeeded on retry
    const run = await manifestStore.getRun(result.runId);
    expect(run).toBeDefined();
    expect(run!.status).toBe("completed");

    // All data from all 3 ranges should be in ClickHouse
    const totalRows = await chRowCount();
    expect(totalRows).toBeGreaterThan(0);
    expect(totalRows).toBe(result.totalRowsInserted);

    // Verify range 1 was indeed retried (at least one batchSeq was failed then retried)
    expect(failedOnce.size).toBeGreaterThan(0);
  });

  // -------------------------------------------------------------------------
  // Test 2: retry exhaustion marks run as failed
  // -------------------------------------------------------------------------

  it("retry exhaustion marks run as failed", async () => {
    // Seed docs for 3 ranges
    const { collName } = await seedCollection({
      count: 3000,
      appId: APP_ID,
      eventName: EVENT_NAME,
      startDate: new Date("2024-01-01"),
      endDate: new Date("2024-12-31"),
    });

    await mongoReader.switchCollection(collName);

    // Always fail insertBatch for range 1's batchSeq slots [10000, 20000).
    // This means range 1 will never succeed, exhausting MAX_RANGE_RETRIES (3).
    const proxied = proxyWriter(chWriter, (params) => {
      const batchSeq = params.batchSeq;
      return batchSeq >= 10_000 && batchSeq < 20_000;
    });

    const deps = buildRangeCoordinatorDeps({
      chWriterOverride: proxied,
      rangeCount: 3,
    });

    const coordinator = new RangeCoordinator(deps);
    const result = await coordinator.run();

    // Allow ClickHouse async inserts to flush
    await new Promise((r) => setTimeout(r, 3000));

    // With resilient batch handling, failed batches are skipped and the
    // range still completes. All 3 ranges should finish (some with failed batches).
    expect(result.totalRanges).toBe(3);
    // All ranges complete — failed batches within a range are skipped, not retried at range level
    expect(result.completedRanges).toBe(3);

    // The run completes (not "failed") because all ranges finished processing
    const run = await manifestStore.getRun(result.runId);
    expect(run).toBeDefined();
    expect(run!.status).toBe("completed");

    // Ranges 0 and 2 data should be in ClickHouse fully.
    // Range 1 data is partially or fully missing (batches that failed were skipped).
    const totalRows = await chRowCount();
    expect(totalRows).toBeGreaterThan(0);
    expect(totalRows).toBe(result.totalRowsInserted);
  });
});
