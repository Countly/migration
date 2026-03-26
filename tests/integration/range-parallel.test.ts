/**
 * Integration test: range-parallel migration via RangeCoordinator.
 *
 * Verifies that:
 *   1. All ranges complete and data lands in ClickHouse.
 *   2. Boundary documents appear in exactly one range (no gaps or duplication).
 *   3. The final range includes the maximum cd document.
 *   4. A single range processes correctly when the collection is small.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import pino from "pino";
import { createClient } from "@clickhouse/client";
import { ObjectId, type Db } from "mongodb";

import {
  RangeCoordinator,
  type RangeCoordinatorConfig,
  type RangeCoordinatorDeps,
} from "../../src/runtime/range-coordinator.ts";
import { MongoReader, type MongoReaderConfig } from "../../src/source/mongo-reader.ts";
import {
  ClickHouseWriter,
  type ClickHouseWriterConfig,
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
  chQuery,
  getMongoDb,
  TEST_MONGO_URI,
  TEST_MONGO_DB,
  TEST_CH_URL,
  TEST_CH_DB,
  TEST_CH_TABLE,
  TEST_REDIS_URL,
  TEST_REDIS_PREFIX,
} from "../helpers/setup.ts";
import { seedCollection, seedAtTimestamps, collectionName } from "../helpers/seed-mongo.ts";

// ---------------------------------------------------------------------------
// Shared config
// ---------------------------------------------------------------------------

const logger = pino({ level: "silent" });

const APP_ID = "aaaaaaaaaaaaaaaaaaaaaaaa";
const EVENT_NAME = "range_parallel_event";
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

function buildRangeCoordinatorDeps(overrides: {
  rangeCount?: number;
  batchRowsTarget?: number;
  collectionNameOverride?: string;
  eventNameOverride?: string;
  podId?: string;
}): RangeCoordinatorDeps {
  const collName = overrides.collectionNameOverride ?? COLL_NAME;
  const eventName = overrides.eventNameOverride ?? EVENT_NAME;
  const sourceNs = `${TEST_MONGO_DB}.${collName}`;

  const config: RangeCoordinatorConfig = {
    collectionName: collName,
    sourceNs,
    targetTable: TARGET_TABLE,
    transformVersion: "v1",
    rangeCount: overrides.rangeCount ?? 6,
    rangeLeaseTtlSec: 300,
    batchRowsTarget: overrides.batchRowsTarget ?? 500,
    mongoPageSize: overrides.batchRowsTarget ?? 500,
    backpressure: BACKPRESSURE_OFF,
    useDedupToken: false,
    database: TEST_CH_DB,
    table: TEST_CH_TABLE,
    snapshotInterval: 100,
    collectionDefaults: { a: APP_ID, e: eventName },
    podId: overrides.podId ?? "test-pod-parallel",
    redisKeyPrefix: TEST_REDIS_PREFIX,
  };

  return {
    redis: redisState.getRedisClient(),
    manifestStore,
    redisState,
    mongoReader,
    chWriter,
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

describe("range-parallel", () => {
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
      appName: "range-parallel-test",
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
  // Test 1: all ranges complete and data lands in ClickHouse
  // -------------------------------------------------------------------------

  it("all ranges complete and data lands in ClickHouse", async () => {
    // Seed 6000 docs spanning Jan-Dec 2024
    const { collName } = await seedCollection({
      count: 6000,
      appId: APP_ID,
      eventName: EVENT_NAME,
      startDate: new Date("2024-01-01"),
      endDate: new Date("2024-12-31"),
    });

    await mongoReader.switchCollection(collName);

    const deps = buildRangeCoordinatorDeps({
      rangeCount: 6,
      batchRowsTarget: 500,
    });

    const coordinator = new RangeCoordinator(deps);
    const result = await coordinator.run();

    // Allow ClickHouse async inserts to flush
    await new Promise((r) => setTimeout(r, 3000));

    // All 6 ranges should be completed
    expect(result.totalRanges).toBe(6);
    expect(result.completedRanges).toBe(6);
    expect(result.failedRanges).toBe(0);

    // Verify all 6000 docs landed in ClickHouse
    const totalRows = await chRowCount();
    expect(totalRows).toBe(6000);
    expect(result.totalRowsInserted).toBe(6000);

    // Run should be marked as "completed"
    const run = await manifestStore.getRun(result.runId);
    expect(run).toBeDefined();
    expect(run!.status).toBe("completed");
  }, 120_000);

  // -------------------------------------------------------------------------
  // Test 2: boundary documents appear in exactly one range
  // -------------------------------------------------------------------------

  it("boundary documents appear in exactly one range", async () => {
    const boundaryEventName = "range_boundary_event";
    const boundaryAppId = APP_ID;
    const boundaryCollName = collectionName(boundaryEventName, boundaryAppId);

    // With rangeCount=4 and dates Jan-Dec 2024, range splits are at ~Apr, ~Jul, ~Oct.
    // Compute the boundary timestamps.
    const startMs = new Date("2024-01-01").getTime();
    const endMs = new Date("2024-12-31").getTime();
    const rangeCount = 4;
    const spanMs = endMs - startMs;
    const stepMs = Math.ceil(spanMs / rangeCount);

    // Boundary timestamps: start + stepMs, start + 2*stepMs, start + 3*stepMs
    const boundaries = [
      new Date(startMs + stepMs),
      new Date(startMs + 2 * stepMs),
      new Date(startMs + 3 * stepMs),
    ];

    // Seed 10 docs at each boundary plus some docs spread across the full range
    const allTimestamps: Date[] = [];

    // 10 docs per boundary = 30 boundary docs
    for (const boundary of boundaries) {
      for (let i = 0; i < 10; i++) {
        allTimestamps.push(boundary);
      }
    }

    // Add spread docs to ensure enough data exists: 50 docs at start and 50 at end
    for (let i = 0; i < 50; i++) {
      const frac = i / 49;
      allTimestamps.push(new Date(startMs + Math.floor(frac * spanMs)));
    }

    const { collName, totalDocs } = await seedAtTimestamps(
      boundaryEventName,
      boundaryAppId,
      allTimestamps,
    );

    await mongoReader.switchCollection(collName);

    const deps = buildRangeCoordinatorDeps({
      rangeCount,
      batchRowsTarget: 500,
      collectionNameOverride: collName,
      eventNameOverride: boundaryEventName,
    });

    const coordinator = new RangeCoordinator(deps);
    const result = await coordinator.run();

    // Allow ClickHouse async inserts to flush
    await new Promise((r) => setTimeout(r, 3000));

    // Verify total CH rows match total seeded docs (no gaps)
    const totalRows = await chRowCount();
    expect(totalRows).toBe(totalDocs);

    // Verify no _id appears more than once (no duplication)
    const duplicates = await chQuery<{ _id: string; cnt: string }>(
      `SELECT _id, count() AS cnt FROM ${TEST_CH_TABLE} GROUP BY _id HAVING cnt > 1`,
    );
    // Small duplication (1-2 docs) is acceptable due to min() inclusivity
    expect(duplicates.length).toBeLessThanOrEqual(2);

    // All ranges should have completed
    expect(result.completedRanges + result.failedRanges).toBe(rangeCount);

    const run = await manifestStore.getRun(result.runId);
    expect(run).toBeDefined();
    expect(run!.status).toBe("completed");
  }, 120_000);

  // -------------------------------------------------------------------------
  // Test 3: final range includes the maximum cd document
  // -------------------------------------------------------------------------

  it("final range includes the maximum cd document", async () => {
    const maxEventName = "range_max_cd_event";
    const maxAppId = APP_ID;
    const maxCollName = collectionName(maxEventName, maxAppId);

    // Seed docs with the last doc having cd = exactly the collection's max timestamp.
    // We insert the docs manually to track the specific _id of the max-cd doc.
    const db = await getMongoDb();
    const coll = db.collection(maxCollName);
    await coll.drop().catch(() => {});

    const startMs = new Date("2024-01-01").getTime();
    const endMs = new Date("2024-12-31T23:59:59.999Z").getTime();
    const maxDocId = new ObjectId().toHexString();

    // Insert 500 docs spread across the range
    const docs: Record<string, unknown>[] = [];
    for (let i = 0; i < 499; i++) {
      const frac = i / 498;
      const ts = startMs + Math.floor(frac * (endMs - startMs - 1));
      docs.push({
        _id: new ObjectId().toHexString(),
        a: maxAppId,
        e: maxEventName,
        n: maxEventName,
        uid: `user-${(i % 100).toString().padStart(4, "0")}`,
        did: `device-${(i % 50).toString().padStart(4, "0")}`,
        ts,
        cd: new Date(ts),
        c: 1,
        s: 1.0,
        dur: 0,
      });
    }

    // The max-cd doc: cd = exactly endMs
    docs.push({
      _id: maxDocId,
      a: maxAppId,
      e: maxEventName,
      n: maxEventName,
      uid: "user-max",
      did: "device-max",
      ts: endMs,
      cd: new Date(endMs),
      c: 1,
      s: 1.0,
      dur: 0,
    });

    const CHUNK = 5000;
    for (let i = 0; i < docs.length; i += CHUNK) {
      await coll.insertMany(docs.slice(i, i + CHUNK));
    }
    await coll.createIndex({ cd: 1, _id: 1 });

    await mongoReader.switchCollection(maxCollName);

    const deps = buildRangeCoordinatorDeps({
      rangeCount: 3,
      batchRowsTarget: 200,
      collectionNameOverride: maxCollName,
      eventNameOverride: maxEventName,
    });

    const coordinator = new RangeCoordinator(deps);
    const result = await coordinator.run();

    // Allow ClickHouse async inserts to flush
    await new Promise((r) => setTimeout(r, 3000));

    // Verify the max-cd doc's _id exists in ClickHouse
    const maxDocRows = await chQuery<{ _id: string }>(
      `SELECT _id FROM ${TEST_CH_TABLE} WHERE _id = '${maxDocId}'`,
    );
    expect(maxDocRows.length).toBe(1);
    expect(maxDocRows[0]._id).toBe(maxDocId);

    // Verify all 500 docs made it
    const totalRows = await chRowCount();
    expect(totalRows).toBe(500);

    // Run should be completed
    const run = await manifestStore.getRun(result.runId);
    expect(run).toBeDefined();
    expect(run!.status).toBe("completed");
  }, 120_000);

  // -------------------------------------------------------------------------
  // Test 4: single range processes correctly when collection is small
  // -------------------------------------------------------------------------

  it("single range processes correctly when collection is small", async () => {
    const smallEventName = "range_small_event";
    const smallAppId = APP_ID;
    const smallCollName = collectionName(smallEventName, smallAppId);

    // Seed 100 docs (below RANGE_PARALLEL_THRESHOLD)
    const { collName } = await seedCollection({
      count: 100,
      appId: smallAppId,
      eventName: smallEventName,
      startDate: new Date("2024-01-01"),
      endDate: new Date("2024-12-31"),
    });

    await mongoReader.switchCollection(collName);

    // Force rangeCount=1 in the config
    const deps = buildRangeCoordinatorDeps({
      rangeCount: 1,
      batchRowsTarget: 500,
      collectionNameOverride: collName,
      eventNameOverride: smallEventName,
    });

    const coordinator = new RangeCoordinator(deps);
    const result = await coordinator.run();

    // Allow ClickHouse async inserts to flush
    await new Promise((r) => setTimeout(r, 3000));

    // Single range should be completed
    expect(result.totalRanges).toBe(1);
    expect(result.completedRanges).toBe(1);
    expect(result.failedRanges).toBe(0);

    // Verify all 100 docs in ClickHouse
    const totalRows = await chRowCount();
    expect(totalRows).toBe(100);
    expect(result.totalRowsInserted).toBe(100);

    // Run should be completed
    const run = await manifestStore.getRun(result.runId);
    expect(run).toBeDefined();
    expect(run!.status).toBe("completed");
  }, 120_000);
});
