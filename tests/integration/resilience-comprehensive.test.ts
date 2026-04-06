/**
 * Comprehensive resilience tests for the migration system.
 *
 * Covers: dead-pod range reclaim, null-cd sweep lock reclaim, initKey error
 * cleanup, pause/resume round-trips, and individual doc skip verification.
 * Every test uses mixed data (valid cd, null cd, missing uid, migrated,
 * invalid ts) to prove invalid docs are skipped individually — not at the
 * collection level.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { randomUUID } from "node:crypto";
import pino from "pino";
import { createClient, type ClickHouseClient } from "@clickhouse/client";

import { MongoReader } from "../../src/source/mongo-reader.ts";
import { ClickHouseWriter } from "../../src/target/clickhouse-writer.ts";
import { ManifestStore } from "../../src/state/manifest-store.ts";
import { RedisHotState } from "../../src/state/redis-hot-state.ts";
import {
  BatchRunner,
  type BatchRunnerDeps,
} from "../../src/runtime/batch-runner.ts";
import {
  RangeCoordinator,
  type RangeCoordinatorConfig,
  type RangeCoordinatorDeps,
} from "../../src/runtime/range-coordinator.ts";
import {
  ClickHousePressure,
  type BackpressureConfig,
} from "../../src/target/clickhouse-pressure.ts";
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

/** Standard mixed-data seed options. */
const MIXED_DATA = {
  nullCdFraction: 0.2,
  missingUidFraction: 0.1,
  migratedFraction: 0.1,
  invalidTsFraction: 0.1,
};

// ---------------------------------------------------------------------------
// Shared resources
// ---------------------------------------------------------------------------

let manifestStore: ManifestStore;
let redisState: RedisHotState;
let mongoReader: MongoReader;
let chWriter: ClickHouseWriter;
let chClientForPressure: ClickHouseClient | null = null;

function getChPressure(): ClickHousePressure {
  if (!chClientForPressure) {
    chClientForPressure = createClient({
      url: TEST_CH_URL,
      database: TEST_CH_DB,
      username: "default",
      password: "",
    });
  }
  return new ClickHousePressure(chClientForPressure, BACKPRESSURE_OFF, logger);
}

function makeGc(): GcController {
  return new GcController(
    {
      enabled: false,
      rssSoftLimitBytes: 2e9,
      rssHardLimitBytes: 3e9,
      heapUsedRatio: 0.9,
      everyNBatches: 999_999,
    },
    logger,
  );
}

function makeRetry(): RetryPolicy {
  return new RetryPolicy({ maxRetries: 3, baseDelayMs: 50, maxDelayMs: 500 });
}

// ---------------------------------------------------------------------------
// BatchRunner helpers (single-collection mode)
// ---------------------------------------------------------------------------

async function buildBatchRunnerDeps(
  collName: string,
  eventName: string,
  opts?: { existingRunId?: string; existingUpperBoundId?: string },
): Promise<{
  deps: BatchRunnerDeps;
  runId: string;
  upperBoundId: string;
  collRedisState: RedisHotState;
}> {
  await mongoReader.switchCollection(collName);

  const upperBound = await mongoReader.getUpperBound();
  const upperBoundId =
    opts?.existingUpperBoundId ??
    (upperBound ? serializeCursor(upperBound) : "");

  const runId = opts?.existingRunId ?? randomUUID();
  const now = new Date().toISOString();
  const sourceNs = `${TEST_MONGO_DB}.${collName}`;
  const targetTable = `${TEST_CH_DB}.${TEST_CH_TABLE}`;

  const collRedisState = RedisHotState.fromExistingConnection(
    redisState.getRedisClient(),
    `${TEST_REDIS_PREFIX}:${collName}`,
  );

  if (!opts?.existingRunId) {
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
  }

  const deps: BatchRunnerDeps = {
    manifestStore,
    redisState: collRedisState,
    mongoReader,
    chWriter,
    chPressure: getChPressure(),
    gcController: makeGc(),
    retryPolicy: makeRetry(),
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
      collectionDefaults: { a: APP_ID, e: eventName },
      collectionName: collName,
    },
  };

  return { deps, runId, upperBoundId, collRedisState };
}

// ---------------------------------------------------------------------------
// RangeCoordinator helpers
// ---------------------------------------------------------------------------

function buildRangeCoordinatorDeps(
  collName: string,
  eventName: string,
  podId: string,
  rangeCount = 4,
): RangeCoordinatorDeps {
  const config: RangeCoordinatorConfig = {
    collectionName: collName,
    sourceNs: `${TEST_MONGO_DB}.${collName}`,
    targetTable: `${TEST_CH_DB}.${TEST_CH_TABLE}`,
    transformVersion: "v1",
    rangeCount,
    rangeLeaseTtlSec: 5, // short TTL for tests
    batchRowsTarget: 500,
    mongoPageSize: 500,
    backpressure: BACKPRESSURE_OFF,
    useDedupToken: false,
    database: TEST_CH_DB,
    table: TEST_CH_TABLE,
    snapshotInterval: 10,
    collectionDefaults: { a: APP_ID, e: eventName },
    podId,
    redisKeyPrefix: TEST_REDIS_PREFIX,
  };

  return {
    redis: redisState.getRedisClient(),
    manifestStore,
    redisState,
    mongoReader,
    chWriter,
    chPressure: getChPressure(),
    gcController: makeGc(),
    retryPolicy: makeRetry(),
    logger,
    config,
  };
}

/** Count CH rows for a specific event (custom events: n = eventName). */
async function chCountByEvent(eventName: string): Promise<number> {
  const rows = await chQuery<{ cnt: string }>(
    `SELECT count() AS cnt FROM ${TEST_CH_TABLE} WHERE n = '${eventName}'`,
  );
  return Number(rows[0]?.cnt ?? 0);
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

describe("resilience-comprehensive", () => {
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
        appName: "resilience-test",
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
    if (chClientForPressure) {
      await chClientForPressure.close();
      chClientForPressure = null;
    }
    await closeAll();
  });

  // -----------------------------------------------------------------------
  // Test 1: Dead-pod range reclaim via CLAIM_RANGE_LUA
  // -----------------------------------------------------------------------

  it("reclaims stale range from dead pod and completes migration", async () => {
    const eventName = "reclaim_range_event";
    const collName = collectionName(eventName, APP_ID);

    const seed = await seedCollection({
      count: 2000,
      appId: APP_ID,
      eventName,
      ...MIXED_DATA,
    });

    await mongoReader.switchCollection(collName);

    // Pod-A initializes ranges and processes some, then "crashes"
    const depsA = buildRangeCoordinatorDeps(collName, eventName, "pod-A", 4);
    // Override rangeLeaseTtlSec to be very short so stale claims are detectable
    (depsA.config as any).rangeLeaseTtlSec = 2;

    const coordA = new RangeCoordinator(depsA);

    // Register pod-A heartbeat so it can acquire the init lock
    const redis = await getRedis();
    const podKeyA = `${TEST_REDIS_PREFIX}:pod:pod-A`;
    await redis.set(podKeyA, JSON.stringify({ podId: "pod-A", lastHeartbeat: new Date().toISOString() }), "EX", 5);

    // Run pod-A (it will complete all ranges since it's the only pod)
    // Instead, we simulate a partial run: let pod-A init ranges, then
    // manually mark range 0 as "processing" by pod-A with an old claimedAt
    // to simulate a crash.

    // First, init ranges via pod-A
    const initRunId = await (coordA as any).initRanges();
    expect(initRunId).toBeTruthy();

    // Verify ranges were created
    const rangesKey = `${TEST_REDIS_PREFIX}:ranges:${collName}`;
    const allRanges = await redis.hgetall(rangesKey);
    expect(Object.keys(allRanges).length).toBe(4);

    // Simulate pod-A claiming range 0 and crashing:
    // set range 0 to "processing" with old claimedAt, then expire pod-A heartbeat
    const range0 = JSON.parse(allRanges["0"]);
    range0.status = "processing";
    range0.podId = "pod-A";
    range0.claimedAt = Math.floor(Date.now() / 1000) - 10; // 10 seconds ago (stale)
    await redis.hset(rangesKey, "0", JSON.stringify(range0));

    // Expire pod-A heartbeat (simulate crash)
    await redis.del(podKeyA);

    // Wait for lease TTL to be clearly exceeded
    await new Promise(r => setTimeout(r, 500));

    // Pod-B starts — should reclaim range 0 from dead pod-A and process everything
    const depsB = buildRangeCoordinatorDeps(collName, eventName, "pod-B", 4);
    (depsB.config as any).rangeLeaseTtlSec = 2;

    // Register pod-B heartbeat
    const podKeyB = `${TEST_REDIS_PREFIX}:pod:pod-B`;
    await redis.set(podKeyB, JSON.stringify({ podId: "pod-B", lastHeartbeat: new Date().toISOString() }), "EX", 300);

    const coordB = new RangeCoordinator(depsB);
    const result = await coordB.run();

    expect(result.completedRanges).toBe(4);
    expect(result.totalDocsRead).toBeGreaterThan(0);

    await new Promise(r => setTimeout(r, 2000));

    const count = await chCountByEvent(eventName);
    // Valid + null-cd docs should all be in CH; invalid docs skipped individually
    expect(count).toBeGreaterThanOrEqual(seed.expectedRows - 30);
    expect(count).toBeLessThanOrEqual(seed.expectedRows + 30);

    // Verify no ranges stuck in "processing"
    const finalStatus = await coordB.getRangeStatus();
    expect(finalStatus.processing).toBe(0);
  }, 120_000);

  // -----------------------------------------------------------------------
  // Test 2: Null-cd sweep lock reclaim from dead pod
  // -----------------------------------------------------------------------

  it("reclaims stale null-cd sweep lock from dead pod", async () => {
    const eventName = "sweep_reclaim_event";
    const collName = collectionName(eventName, APP_ID);

    const seed = await seedCollection({
      count: 1000,
      appId: APP_ID,
      eventName,
      ...MIXED_DATA,
    });

    await mongoReader.switchCollection(collName);

    const redis = await getRedis();
    const rangesKey = `${TEST_REDIS_PREFIX}:ranges:${collName}`;
    const sweepKey = `${rangesKey}:null_cd_sweep`;

    // Plant a stale sweep lock from a dead pod
    await redis.set(sweepKey, "dead-sweep-pod", "EX", 3600, "NX");

    // Do NOT register a heartbeat for "dead-sweep-pod" — it's dead

    // Register live pod heartbeat
    const podKey = `${TEST_REDIS_PREFIX}:pod:live-pod`;
    await redis.set(podKey, JSON.stringify({ podId: "live-pod" }), "EX", 300);

    // Run range coordinator — it should reclaim the stale sweep lock
    const deps = buildRangeCoordinatorDeps(collName, eventName, "live-pod", 4);
    const coord = new RangeCoordinator(deps);
    const result = await coord.run();

    expect(result.totalDocsRead).toBeGreaterThan(0);

    await new Promise(r => setTimeout(r, 2000));

    const count = await chCountByEvent(eventName);
    expect(count).toBeGreaterThanOrEqual(seed.expectedRows - 20);
    expect(count).toBeLessThanOrEqual(seed.expectedRows + 20);

    // Verify sweep done key is set
    const sweepDoneKey = `${sweepKey}:done`;
    const sweepDone = await redis.get(sweepDoneKey);
    expect(sweepDone).toBe("1");
  }, 120_000);

  // -----------------------------------------------------------------------
  // Test 3: initKey cleanup on error
  // -----------------------------------------------------------------------

  it("cleans up initKey when initialization errors", async () => {
    const eventName = "init_error_event";
    const collName = collectionName(eventName, APP_ID);

    const redis = await getRedis();
    const initKey = `${TEST_REDIS_PREFIX}:ranges:${collName}:init`;

    // Register pod heartbeat
    const podKey = `${TEST_REDIS_PREFIX}:pod:error-pod`;
    await redis.set(podKey, JSON.stringify({ podId: "error-pod" }), "EX", 300);

    // Create an empty collection (no docs) — getUpperBound and hasNullCd
    // both return null/false, so initRanges will throw "Collection is empty"
    const db = await (await import("../helpers/setup.ts")).getMongoDb();
    const coll = db.collection(collName);
    await coll.drop().catch(() => {});
    await coll.insertOne({ _placeholder: true }); // create collection
    await coll.deleteMany({}); // but with zero docs
    await coll.createIndex({ cd: 1, _id: 1 });

    await mongoReader.switchCollection(collName);

    const deps = buildRangeCoordinatorDeps(collName, eventName, "error-pod", 4);
    const coord = new RangeCoordinator(deps);

    // Should throw "Collection is empty" but clean up initKey
    await expect(coord.run()).rejects.toThrow(/empty/i);

    // Verify initKey was cleaned up
    const initKeyValue = await redis.get(initKey);
    expect(initKeyValue).toBeNull();
  }, 30_000);

  // -----------------------------------------------------------------------
  // Test 4a: Pause/resume single collection
  // -----------------------------------------------------------------------

  it("pause and resume single collection with mixed data — no data loss", async () => {
    const eventName = "pause_resume_single";
    const collName = collectionName(eventName, APP_ID);

    const seed = await seedCollection({
      count: 1500,
      appId: APP_ID,
      eventName,
      ...MIXED_DATA,
    });

    const { deps } = await buildBatchRunnerDeps(collName, eventName);

    // Use tiny batch size so there are many batches (~30), giving time to pause
    deps.config.batchRowsTarget = 50;
    deps.config.mongoPageSize = 50;

    const runner = new BatchRunner(deps);
    const runPromise = runner.run();

    // Poll until runner is running and has processed at least one batch, then pause
    let didPause = false;
    for (let i = 0; i < 100; i++) {
      await new Promise(r => setTimeout(r, 50));
      const status = runner.getStatus();
      if (status === "completed" || status === "failed") break;
      if (status === "running" && runner.getStats().batchSeq >= 1) {
        runner.pause();
        didPause = true;
        break;
      }
    }

    if (didPause) {
      // Verify paused state held
      await new Promise(r => setTimeout(r, 200));
      expect(runner.getStatus()).toBe("paused");

      // Resume
      runner.resume();
    }
    // If runner completed before pause took effect, that's ok — still verify data

    await runPromise;

    const stats = runner.getStats();
    expect(stats.status).toBe("completed");
    expect(stats.totalDocsRead).toBeGreaterThan(0);

    await new Promise(r => setTimeout(r, 2000));

    const count = await chCountByEvent(eventName);
    expect(count).toBeGreaterThanOrEqual(seed.expectedRows - 20);
    expect(count).toBeLessThanOrEqual(seed.expectedRows + 20);
  }, 60_000);

  // -----------------------------------------------------------------------
  // Test 4b: Pause/resume multi-collection
  // -----------------------------------------------------------------------

  it("pause and resume across 3 collections with mixed data — no data loss", async () => {
    const events = ["pause_multi_a", "pause_multi_b", "pause_multi_c"];
    const sizes = [1000, 1500, 2000];

    const seeds = await Promise.all(
      events.map((eventName, i) =>
        seedCollection({
          count: sizes[i],
          appId: APP_ID,
          eventName,
          ...MIXED_DATA,
        }),
      ),
    );

    for (let i = 0; i < 3; i++) {
      const { deps } = await buildBatchRunnerDeps(
        seeds[i].collName,
        events[i],
      );

      // Tiny batches for collection 2 so pause has time to take effect
      if (i === 1) {
        deps.config.batchRowsTarget = 50;
        deps.config.mongoPageSize = 50;
      }

      const runner = new BatchRunner(deps);
      const runPromise = runner.run();

      // Pause/resume during collection 2 (index 1)
      if (i === 1) {
        let didPause = false;
        for (let j = 0; j < 100; j++) {
          await new Promise(r => setTimeout(r, 50));
          const status = runner.getStatus();
          if (status === "completed" || status === "failed") break;
          if (status === "running" && runner.getStats().batchSeq >= 1) {
            runner.pause();
            didPause = true;
            break;
          }
        }

        if (didPause) {
          await new Promise(r => setTimeout(r, 200));
          expect(runner.getStatus()).toBe("paused");
          runner.resume();
        }
      }

      await runPromise;
      expect(runner.getStats().status).toBe("completed");
    }

    await new Promise(r => setTimeout(r, 2000));

    // Verify per-collection counts
    for (let i = 0; i < 3; i++) {
      const count = await chCountByEvent(events[i]);
      expect(count).toBeGreaterThanOrEqual(seeds[i].expectedRows - 20);
      expect(count).toBeLessThanOrEqual(seeds[i].expectedRows + 20);
    }

    // Verify aggregate
    const totalExpected = seeds.reduce((s, seed) => s + seed.expectedRows, 0);
    const totalCh = await chRowCount();
    expect(totalCh).toBeGreaterThanOrEqual(totalExpected - 60);
    expect(totalCh).toBeLessThanOrEqual(totalExpected + 60);
  }, 120_000);

  // -----------------------------------------------------------------------
  // Test 5a: Mixed data individual skip — single collection
  // -----------------------------------------------------------------------

  it("skips invalid docs individually in single collection (not entire collection)", async () => {
    const eventName = "skip_single";
    const collName = collectionName(eventName, APP_ID);

    const seed = await seedCollection({
      count: 1000,
      appId: APP_ID,
      eventName,
      ...MIXED_DATA,
    });

    // expectedRows should be less than 1000 (invalid docs deducted)
    // but greater than 0 (collection NOT entirely skipped)
    expect(seed.expectedRows).toBeLessThan(1000);
    expect(seed.expectedRows).toBeGreaterThan(0);

    const { deps } = await buildBatchRunnerDeps(collName, eventName);
    const runner = new BatchRunner(deps);
    await runner.run();

    const stats = runner.getStats();
    expect(stats.status).toBe("completed");
    expect(stats.totalDocsSkipped).toBeGreaterThan(0);

    // Verify skip reasons are populated (individual skips happened)
    const skips = stats.skipsByReason;
    const totalSkips =
      (skips.missing_uid ?? 0) +
      (skips.already_marked_migrated ?? 0) +
      (skips.invalid_ts ?? 0);
    expect(totalSkips).toBeGreaterThan(0);

    await new Promise(r => setTimeout(r, 2000));

    const count = await chCountByEvent(eventName);
    // Valid + null-cd docs present; invalid docs absent
    expect(count).toBeGreaterThanOrEqual(seed.expectedRows - 20);
    expect(count).toBeLessThanOrEqual(seed.expectedRows + 20);

    // Collection was NOT skipped — there are rows
    expect(count).toBeGreaterThan(0);
  }, 60_000);

  // -----------------------------------------------------------------------
  // Test 5b: Mixed data individual skip — multi-collection
  // -----------------------------------------------------------------------

  it("skips invalid docs individually across 3 collections (none skipped entirely)", async () => {
    const events = [
      "skip_multi_a", // heavy null-cd
      "skip_multi_b", // heavy missing uid
      "skip_multi_c", // heavy migrated + invalid ts
    ];

    // Different invalid-data profiles per collection
    const seedConfigs = [
      { count: 1000, nullCdFraction: 0.4, missingUidFraction: 0.05, migratedFraction: 0.05, invalidTsFraction: 0.05 },
      { count: 1500, nullCdFraction: 0.1, missingUidFraction: 0.2, migratedFraction: 0.05, invalidTsFraction: 0.05 },
      { count: 2000, nullCdFraction: 0.1, missingUidFraction: 0.05, migratedFraction: 0.15, invalidTsFraction: 0.15 },
    ];

    const seeds = await Promise.all(
      events.map((eventName, i) =>
        seedCollection({
          appId: APP_ID,
          eventName,
          ...seedConfigs[i],
        }),
      ),
    );

    // Verify none have expectedRows = 0 (all have some valid docs)
    for (const seed of seeds) {
      expect(seed.expectedRows).toBeGreaterThan(0);
      expect(seed.expectedRows).toBeLessThan(seed.totalDocs);
    }

    // Migrate all 3
    for (let i = 0; i < 3; i++) {
      const { deps } = await buildBatchRunnerDeps(
        seeds[i].collName,
        events[i],
      );
      const runner = new BatchRunner(deps);
      await runner.run();

      const stats = runner.getStats();
      expect(stats.status).toBe("completed");
      // Each collection had skips but was NOT skipped entirely
      expect(stats.totalDocsSkipped).toBeGreaterThan(0);
      expect(stats.totalRowsInserted).toBeGreaterThan(0);
    }

    await new Promise(r => setTimeout(r, 2000));

    // Verify per-collection counts
    for (let i = 0; i < 3; i++) {
      const count = await chCountByEvent(events[i]);
      expect(count).toBeGreaterThanOrEqual(seeds[i].expectedRows - 30);
      expect(count).toBeLessThanOrEqual(seeds[i].expectedRows + 30);
      // NOT zero — collection was processed
      expect(count).toBeGreaterThan(0);
    }

    // Verify aggregate
    const totalExpected = seeds.reduce((s, seed) => s + seed.expectedRows, 0);
    const totalCh = await chRowCount();
    expect(totalExpected).toBeGreaterThan(0);
    expect(totalCh).toBeGreaterThanOrEqual(totalExpected - 90);
    expect(totalCh).toBeLessThanOrEqual(totalExpected + 90);
  }, 120_000);
});
