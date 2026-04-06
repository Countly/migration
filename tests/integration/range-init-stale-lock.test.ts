/**
 * Integration test: RangeCoordinator stale initKey deadlock.
 *
 * Reproduces the scenario where a pod crashes after acquiring the
 * range initialization lock but before writing runIdKey, leaving a
 * stale Redis key that blocks all subsequent initialization attempts.
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
import { ClickHouseWriter, type ClickHouseWriterConfig } from "../../src/target/clickhouse-writer.ts";
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
  getRedis,
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

const logger = pino({ level: "warn" });

const APP_ID = "aaaaaaaaaaaaaaaaaaaaaaaa";
const EVENT_NAME = "stale_lock_test";
const COLL_NAME = collectionName(EVENT_NAME, APP_ID);

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
// Shared resources
// ---------------------------------------------------------------------------

let manifestStore: ManifestStore;
let redisState: RedisHotState;
let mongoReader: MongoReader;
let chWriter: ClickHouseWriter;

function buildDeps(podId: string): RangeCoordinatorDeps {
  const config: RangeCoordinatorConfig = {
    collectionName: COLL_NAME,
    sourceNs: `${TEST_MONGO_DB}.${COLL_NAME}`,
    targetTable: `${TEST_CH_DB}.${TEST_CH_TABLE}`,
    transformVersion: "v1",
    rangeCount: 4,
    rangeLeaseTtlSec: 300,
    batchRowsTarget: 500,
    mongoPageSize: 500,
    backpressure: BACKPRESSURE_OFF,
    useDedupToken: false,
    database: TEST_CH_DB,
    table: TEST_CH_TABLE,
    snapshotInterval: 10,
    collectionDefaults: { a: APP_ID, e: EVENT_NAME },
    podId,
    redisKeyPrefix: TEST_REDIS_PREFIX,
  };

  return {
    redis: redisState.getRedisClient(),
    manifestStore,
    redisState,
    mongoReader,
    chWriter,
    chPressure: new ClickHousePressure(
      createClient({ url: TEST_CH_URL, database: TEST_CH_DB, username: "default", password: "" }),
      BACKPRESSURE_OFF,
      logger,
    ),
    gcController: new GcController(
      { enabled: false, rssSoftLimitBytes: 2e9, rssHardLimitBytes: 3e9, heapUsedRatio: 0.9, everyNBatches: 999_999 },
      logger,
    ),
    retryPolicy: new RetryPolicy({ maxRetries: 2, baseDelayMs: 50, maxDelayMs: 200 }),
    logger,
    config,
  };
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

describe("range-init-stale-lock", () => {
  beforeAll(async () => {
    manifestStore = new ManifestStore(TEST_MONGO_URI, TEST_MONGO_DB);
    await manifestStore.connect();

    redisState = new RedisHotState(TEST_REDIS_URL, TEST_REDIS_PREFIX);
    await redisState.connect();

    mongoReader = new MongoReader(
      {
        uri: TEST_MONGO_URI,
        database: TEST_MONGO_DB,
        readPreference: "primary",
        readConcern: "local",
        retryReads: true,
        appName: "stale-lock-test",
        batchRowsTarget: 500,
        cursorBatchSize: 500,
        maxTimeMs: 30_000,
      },
      logger,
    );
    await mongoReader.connect();

    chWriter = new ClickHouseWriter(
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
  });

  beforeEach(async () => {
    await teardownMongo();
    await teardownClickHouse();
    await teardownRedis();
    await setupClickHouse();
  });

  afterAll(async () => {
    await mongoReader.close().catch(() => {});
    await chWriter.close().catch(() => {});
    await manifestStore.close().catch(() => {});
    await redisState.close().catch(() => {});
    await closeAll();
  });

  // -----------------------------------------------------------------------
  // Test: stale initKey from crashed pod blocks new pod
  // -----------------------------------------------------------------------

  it("recovers from stale initKey left by a crashed pod", async () => {
    // Seed a collection with enough data
    await seedCollection({
      count: 2000,
      appId: APP_ID,
      eventName: EVENT_NAME,
    });
    await mongoReader.switchCollection(COLL_NAME);

    const redis = await getRedis();
    const initKey = `${TEST_REDIS_PREFIX}:ranges:${COLL_NAME}:init`;
    const runIdKey = `${TEST_REDIS_PREFIX}:ranges:${COLL_NAME}:runId`;

    // Simulate a crashed pod: set initKey (as if pod "dead-pod" acquired it)
    // but never write runIdKey (pod crashed before completing init)
    await redis.set(initKey, "dead-pod", "EX", 60, "NX");

    // Verify the stale lock exists and runIdKey does NOT
    expect(await redis.get(initKey)).toBe("dead-pod");
    expect(await redis.get(runIdKey)).toBeNull();

    // Do NOT register a pod heartbeat for "dead-pod" — it's dead.
    // A new pod ("recovery-pod") should detect the dead holder and reclaim.

    const deps = buildDeps("recovery-pod");
    const coordinator = new RangeCoordinator(deps);

    // This should NOT timeout — it should detect the dead pod and reclaim
    const result = await coordinator.run();

    expect(result.totalDocsRead).toBeGreaterThanOrEqual(2000);
    expect(result.completedRanges).toBeGreaterThanOrEqual(1);

    // Verify data actually landed in ClickHouse
    await new Promise(r => setTimeout(r, 2000));
    const count = await chRowCount();
    expect(count).toBeGreaterThanOrEqual(2000);
  }, 120_000);

  // -----------------------------------------------------------------------
  // Test: initKey is cleaned up after successful initialization
  // -----------------------------------------------------------------------

  it("cleans up initKey after successful initialization", async () => {
    await seedCollection({
      count: 1000,
      appId: APP_ID,
      eventName: EVENT_NAME,
    });
    await mongoReader.switchCollection(COLL_NAME);

    const redis = await getRedis();
    const initKey = `${TEST_REDIS_PREFIX}:ranges:${COLL_NAME}:init`;

    const deps = buildDeps("cleanup-pod");
    const coordinator = new RangeCoordinator(deps);
    await coordinator.run();

    // After successful run, initKey should be gone (explicitly deleted, not just TTL)
    const initKeyValue = await redis.get(initKey);
    expect(initKeyValue).toBeNull();
  }, 120_000);
});
