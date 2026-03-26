/**
 * Integration test: Layer 3 completion guard in RangeCoordinator's processRange().
 *
 * The completion guard detects "falsely empty" ranges -- ranges where the
 * BatchRunner returns 0 docs but the actual time window contains data
 * (indicating a cursor bleed or resume bug). It also verifies that
 * genuinely empty ranges (no documents in the time window) pass through
 * without error.
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
import { seedAtTimestamps, collectionName } from "../helpers/seed-mongo.ts";

// ---------------------------------------------------------------------------
// Shared config
// ---------------------------------------------------------------------------

const logger = pino({ level: "silent" });

const APP_ID = "aaaaaaaaaaaaaaaaaaaaaaaa";

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

function buildRangeCoordinatorDeps(
  collName: string,
  eventName: string,
  overrides?: { rangeCount?: number },
): RangeCoordinatorDeps {
  const sourceNs = `${TEST_MONGO_DB}.${collName}`;
  const targetTable = `${TEST_CH_DB}.${TEST_CH_TABLE}`;

  const config: RangeCoordinatorConfig = {
    collectionName: collName,
    sourceNs,
    targetTable,
    transformVersion: "v1",
    rangeCount: overrides?.rangeCount ?? 4,
    rangeLeaseTtlSec: 300,
    batchRowsTarget: 500,
    mongoPageSize: 500,
    backpressure: BACKPRESSURE_OFF,
    useDedupToken: false,
    database: TEST_CH_DB,
    table: TEST_CH_TABLE,
    snapshotInterval: 100,
    collectionDefaults: { a: APP_ID, e: eventName },
    podId: "test-pod-guard",
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

describe("completion-guard", () => {
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
      appName: "completion-guard-test",
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
  // Test 1: empty range detected as genuinely empty
  // -------------------------------------------------------------------------

  it("empty range detected as genuinely empty", async () => {
    const eventName = "guard_empty_range";
    const collName = collectionName(eventName, APP_ID);

    // Seed docs ONLY in the first half of a broad date range, leaving a gap.
    // Date range: 2024-01-01 to 2024-12-31
    // Docs only in Jan-Mar, leaving Apr-Dec empty.
    // With rangeCount=4, ranges will roughly be:
    //   range 0: Jan-Mar (has data)
    //   range 1: Apr-Jun (empty)
    //   range 2: Jul-Sep (empty)
    //   range 3: Oct-Dec (empty -- but endCd is maxCd so it is the final range)
    //
    // We place docs at the very start and very end of the full range to ensure
    // the coordinator can determine min/max cd bounds, then cluster data only
    // in the first quarter.

    const timestamps: Date[] = [];

    // One doc at the very start (bookend for range calculation)
    timestamps.push(new Date("2024-01-01T00:00:00Z"));

    // 200 docs clustered in Jan-Feb
    for (let i = 0; i < 200; i++) {
      const d = new Date("2024-01-02T00:00:00Z");
      d.setHours(d.getHours() + i * 2); // spread over ~400 hours (~17 days)
      timestamps.push(d);
    }

    // One doc at the very end (bookend)
    timestamps.push(new Date("2024-12-31T23:59:59Z"));

    const { collName: seededColl, totalDocs } = await seedAtTimestamps(
      eventName,
      APP_ID,
      timestamps,
    );

    expect(seededColl).toBe(collName);
    expect(totalDocs).toBe(timestamps.length);

    await mongoReader.switchCollection(collName);

    const deps = buildRangeCoordinatorDeps(collName, eventName, { rangeCount: 4 });
    const coordinator = new RangeCoordinator(deps);
    const result = await coordinator.run();

    // Allow ClickHouse async inserts to flush
    await new Promise((r) => setTimeout(r, 3000));

    // The migration should complete without error -- the empty ranges should
    // pass the completion guard (genuinely empty, probe returns 0 docs).
    const run = await manifestStore.getRun(result.runId);
    expect(run).toBeDefined();
    // The run should be "completed" because all ranges finished (some empty, some with data)
    expect(run!.status).toBe("completed");
    expect(result.failedRanges).toBe(0);

    // ClickHouse should contain the non-empty range data
    const totalRows = await chRowCount();
    expect(totalRows).toBeGreaterThan(0);
    expect(totalRows).toBe(result.totalRowsInserted);

    // Verify the total docs processed match what was seeded
    // (all 202 docs should have been read, though some ranges read 0)
    expect(result.totalDocsRead).toBe(totalDocs);
  });

  // -------------------------------------------------------------------------
  // Test 2: completion guard catches falsely empty range
  // -------------------------------------------------------------------------

  it("completion guard catches falsely empty range", async () => {
    const eventName = "guard_false_empty";
    const collName = collectionName(eventName, APP_ID);

    // Seed docs uniformly across the full date range to ensure every range
    // has data. With rangeCount=4, each range should have ~75 docs.
    const timestamps: Date[] = [];
    const start = new Date("2024-01-01T00:00:00Z").getTime();
    const end = new Date("2024-12-31T23:59:59Z").getTime();
    const step = (end - start) / 299; // 300 docs evenly spread

    for (let i = 0; i < 300; i++) {
      timestamps.push(new Date(start + Math.floor(i * step)));
    }

    await seedAtTimestamps(eventName, APP_ID, timestamps);
    await mongoReader.switchCollection(collName);

    // Create a Proxy around the MongoReader that makes readPage return empty
    // results for a specific range's time window. This simulates a cursor bug
    // where a range appears empty even though it has data. The completion
    // guard's probe call goes through the *real* readPage, exposing the lie.
    //
    // Strategy: intercept the BatchRunner's processing by wrapping mongoReader
    // such that the *batch* readPage calls (which use the internal cursor)
    // return empty when the startCd is in range 1's window. However, the
    // guard's probe call uses different cursor arguments, so we need to be
    // more surgical.
    //
    // Simpler approach: wrap the CH writer to silently drop all writes for
    // range 1's batch_seq slots AND make the mongoReader return 0 docs for
    // range 1. This triggers the guard because the range has data but the
    // BatchRunner sees 0 docs.
    //
    // Even simpler: We can directly test the guard by using a Proxy on
    // mongoReader.readPage. For calls during the batch loop where the cursor
    // falls within range 1's time window, return empty results. The guard's
    // subsequent probe will also go through readPage, but we let the probe
    // through (returning real data), which will trigger the guard error.

    const blockRange1 = true;

    // Determine approximate range 1 boundaries
    // With 4 ranges over [start, end], range 1 covers roughly [start + 1/4*span, start + 2/4*span)
    const span = end - start;
    const range1StartApprox = start + Math.floor(span / 4);
    const range1EndApprox = start + Math.floor((2 * span) / 4);

    const proxiedReader = new Proxy(mongoReader, {
      get(target, prop, receiver) {
        if (prop === "readPage") {
          return async (...args: Parameters<MongoReader["readPage"]>) => {
            const [lastCursor, upperBound, limit] = args;

            // Let probe calls through (limit=1 is the guard's probe signature)
            if (limit === 1) {
              return target.readPage(lastCursor, upperBound, limit);
            }

            // Check if this readPage is for range 1's time window
            if (blockRange1 && lastCursor) {
              const cursorCd = lastCursor.cd;
              if (cursorCd >= range1StartApprox && cursorCd < range1EndApprox) {
                // Return empty page -- simulating a cursor bug
                return { docs: [], lastCursor: null, fetchMs: 0 };
              }
            }

            return target.readPage(lastCursor, upperBound, limit);
          };
        }
        return Reflect.get(target, prop, receiver);
      },
    });

    const sourceNs = `${TEST_MONGO_DB}.${collName}`;
    const targetTable = `${TEST_CH_DB}.${TEST_CH_TABLE}`;

    const config: RangeCoordinatorConfig = {
      collectionName: collName,
      sourceNs,
      targetTable,
      transformVersion: "v1",
      rangeCount: 4,
      rangeLeaseTtlSec: 300,
      batchRowsTarget: 500,
      mongoPageSize: 500,
      backpressure: BACKPRESSURE_OFF,
      useDedupToken: false,
      database: TEST_CH_DB,
      table: TEST_CH_TABLE,
      snapshotInterval: 100,
      collectionDefaults: { a: APP_ID, e: eventName },
      podId: "test-pod-guard-false",
      redisKeyPrefix: TEST_REDIS_PREFIX,
    };

    const deps: RangeCoordinatorDeps = {
      redis: redisState.getRedisClient(),
      manifestStore,
      redisState,
      mongoReader: proxiedReader as MongoReader,
      chWriter,
      chPressure: createChPressure(),
      gcController: createGcController(),
      retryPolicy: createRetryPolicy(),
      logger: pino({ level: "silent" }),
      config,
    };

    const coordinator = new RangeCoordinator(deps);
    const result = await coordinator.run();

    // Allow ClickHouse async inserts to flush
    await new Promise((r) => setTimeout(r, 3000));

    // The range that was artificially emptied should have been caught by the
    // completion guard and marked as failed.
    // With MAX_RANGE_RETRIES=3, the coordinator will retry range 1 multiple times,
    // and each time the guard fires because our proxy still returns empty.
    // After exhausting retries, the run should be marked as "failed".
    const run = await manifestStore.getRun(result.runId);
    expect(run).toBeDefined();
    expect(run!.status).toBe("failed");
    expect(result.failedRanges).toBeGreaterThan(0);

    // Ranges 0, 2, and 3 should have their data in ClickHouse
    const totalRows = await chRowCount();
    expect(totalRows).toBeGreaterThan(0);

    // The number of completed ranges should be less than 4
    // (at least range 1 failed)
    expect(result.completedRanges).toBeLessThan(4);
    expect(result.completedRanges).toBeGreaterThanOrEqual(2);
  });
});
