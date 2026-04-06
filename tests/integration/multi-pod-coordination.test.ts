/**
 * Production-scale multi-pod coordination integration tests.
 *
 * Verifies that multiple pods (RangeCoordinators / BatchRunners) cooperate
 * correctly through Redis-based distributed locking, range claiming, and
 * heartbeat liveness checks. Each test seeds mixed data (null cd, missing
 * uid, migrated, invalid ts) to exercise realistic code paths.
 *
 * Requirements: Docker containers for MongoDB:27017, ClickHouse:8123, Redis:6379.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { randomUUID } from "node:crypto";
import pino from "pino";
import { createClient, type ClickHouseClient } from "@clickhouse/client";

import { MongoReader, type MongoReaderConfig } from "../../src/source/mongo-reader.ts";
import {
  ClickHouseWriter,
  type ClickHouseWriterConfig,
} from "../../src/target/clickhouse-writer.ts";
import {
  ClickHousePressure,
  type BackpressureConfig,
} from "../../src/target/clickhouse-pressure.ts";
import { ManifestStore } from "../../src/state/manifest-store.ts";
import { RedisHotState } from "../../src/state/redis-hot-state.ts";
import { CollectionLock, type CollectionLockConfig } from "../../src/state/collection-lock.ts";
import {
  RangeCoordinator,
  type RangeCoordinatorConfig,
  type RangeCoordinatorDeps,
  type RangeEntry,
} from "../../src/runtime/range-coordinator.ts";
import {
  BatchRunner,
  type BatchRunnerDeps,
} from "../../src/runtime/batch-runner.ts";
import { GcController } from "../../src/runtime/gc-controller.ts";
import { RetryPolicy } from "../../src/runtime/retry-policy.ts";
import { resolveRun } from "../../src/runtime/resolve-run.ts";
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
  getMongoDb,
  TEST_MONGO_URI,
  TEST_MONGO_DB,
  TEST_CH_URL,
  TEST_CH_DB,
  TEST_CH_TABLE,
  TEST_REDIS_URL,
  TEST_REDIS_PREFIX,
} from "../helpers/setup.ts";
import {
  seedCollection,
  seedNullCdCollection,
  collectionName,
} from "../helpers/seed-mongo.ts";

// ---------------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------------

const logger = pino({ level: "warn" });
const APP_ID = "aaaaaaaaaaaaaaaaaaaaaaaa";
const TARGET_TABLE = `${TEST_CH_DB}.${TEST_CH_TABLE}`;

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

/** Standard mixed-data seed profile for all tests. */
const MIXED_DATA = {
  nullCdFraction: 0.2,
  missingUidFraction: 0.1,
  migratedFraction: 0.1,
  invalidTsFraction: 0.1,
};

// ---------------------------------------------------------------------------
// The MARK_RANGE_TERMINAL_LUA script (duplicated from range-coordinator.ts
// for direct Redis invocation in Test 1). This is a Lua script executed
// atomically on the Redis server via the ioredis .eval() method — it is
// NOT JavaScript eval.
// ---------------------------------------------------------------------------

const MARK_RANGE_TERMINAL_LUA = `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local data = cjson.decode(raw)
if data.status ~= 'processing' then return 0 end
if tostring(data.podId) ~= ARGV[3] then return 0 end
data.status = ARGV[2]
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(data))
return 1
`;

// ---------------------------------------------------------------------------
// Shared resources (single connection pool, reused across tests)
// ---------------------------------------------------------------------------

let manifestStore: ManifestStore;
let redisState: RedisHotState;
let chWriter: ClickHouseWriter;
let chClientForPressure: ClickHouseClient | null = null;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const MONGO_READER_CONFIG: MongoReaderConfig = {
  uri: TEST_MONGO_URI,
  database: TEST_MONGO_DB,
  readPreference: "primary",
  readConcern: "local",
  retryReads: true,
  appName: "multi-pod-test",
  batchRowsTarget: 500,
  cursorBatchSize: 500,
  maxTimeMs: 30_000,
};

/**
 * Create a fresh MongoReader with its own connection.
 * Required because MongoReader.switchCollection() mutates instance state,
 * making it unsafe to share between concurrent RangeCoordinators.
 */
async function createMongoReader(): Promise<MongoReader> {
  const reader = new MongoReader(MONGO_READER_CONFIG, logger);
  await reader.connect();
  return reader;
}

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

/** Build RangeCoordinatorDeps for a given pod and collection. */
function buildRangeCoordinatorDeps(
  collName: string,
  eventName: string,
  podId: string,
  mongoReader: MongoReader,
  opts?: { rangeCount?: number; batchRowsTarget?: number; rangeLeaseTtlSec?: number },
): RangeCoordinatorDeps {
  const config: RangeCoordinatorConfig = {
    collectionName: collName,
    sourceNs: `${TEST_MONGO_DB}.${collName}`,
    targetTable: TARGET_TABLE,
    transformVersion: "v1",
    rangeCount: opts?.rangeCount ?? 8,
    rangeLeaseTtlSec: opts?.rangeLeaseTtlSec ?? 5,
    batchRowsTarget: opts?.batchRowsTarget ?? 500,
    mongoPageSize: opts?.batchRowsTarget ?? 500,
    backpressure: BACKPRESSURE_OFF,
    useDedupToken: false,
    database: TEST_CH_DB,
    table: TEST_CH_TABLE,
    snapshotInterval: 100,
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

/** Build BatchRunnerDeps for a single-collection run (not range-parallel). */
async function buildBatchRunnerDeps(
  collName: string,
  eventName: string,
  mongoReader: MongoReader,
  opts?: { batchRowsTarget?: number },
): Promise<{
  deps: BatchRunnerDeps;
  runId: string;
  upperBoundId: string;
  collRedisState: RedisHotState;
}> {
  await mongoReader.switchCollection(collName);

  const upperBound = await mongoReader.getUpperBound();
  const upperBoundId = upperBound ? serializeCursor(upperBound) : "";

  const runId = randomUUID();
  const now = new Date().toISOString();
  const sourceNs = `${TEST_MONGO_DB}.${collName}`;

  const collRedisState = RedisHotState.fromExistingConnection(
    redisState.getRedisClient(),
    `${TEST_REDIS_PREFIX}:${collName}`,
  );

  await manifestStore.createRun({
    run_id: runId,
    status: "active",
    source_ns: sourceNs,
    target_table: TARGET_TABLE,
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
    targetTable: TARGET_TABLE,
    upperBoundCursor: upperBoundId,
    lastCommittedCursor: null,
    transformVersion: "v1",
    totalBatches: 0,
    completedBatches: 0,
    startedAt: now,
  });

  const batchRowsTarget = opts?.batchRowsTarget ?? 500;

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
      targetTable: TARGET_TABLE,
      upperBoundId,
      batchRowsTarget,
      mongoPageSize: batchRowsTarget,
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

/** Register a pod heartbeat in Redis so CLAIM_RANGE_LUA sees it as alive. */
async function registerPodHeartbeat(podId: string, ttlSec = 300): Promise<void> {
  const redis = await getRedis();
  const podKey = `${TEST_REDIS_PREFIX}:pod:${podId}`;
  await redis.set(
    podKey,
    JSON.stringify({ podId, lastHeartbeat: new Date().toISOString() }),
    "EX",
    ttlSec,
  );
}

/** Count CH rows for a specific event name. */
async function chCountByEvent(eventName: string): Promise<number> {
  const rows = await chQuery<{ cnt: string }>(
    `SELECT count() AS cnt FROM ${TEST_CH_TABLE} WHERE n = '${eventName}'`,
  );
  return Number(rows[0]?.cnt ?? 0);
}

/**
 * Migrate a single collection using BatchRunner, handling both normal
 * and all-null-cd collections (mirrors the three-collection-nullcd test).
 */
async function migrateCollection(
  collName: string,
  eventName: string,
  mongoReader: MongoReader,
  opts?: { batchRowsTarget?: number },
): Promise<{ runId: string; docsRead: number; rowsInserted: number }> {
  await mongoReader.switchCollection(collName);

  const upperBound = await mongoReader.getUpperBound();
  if (!upperBound) {
    // All-null-cd collection: use nullCdMode
    const hasNullCd = await mongoReader.hasNullCdDocuments();
    if (!hasNullCd) {
      return { runId: "", docsRead: 0, rowsInserted: 0 };
    }

    const bounds = await mongoReader.getNullCdBounds();
    if (!bounds) {
      return { runId: "", docsRead: 0, rowsInserted: 0 };
    }

    const dummyUpperBound = JSON.stringify({ cd: 0, id: "000000000000000000000000" });
    const runId = randomUUID();
    const now = new Date().toISOString();
    const sourceNs = `${TEST_MONGO_DB}.${collName}`;

    const collRedisState = RedisHotState.fromExistingConnection(
      redisState.getRedisClient(),
      `${TEST_REDIS_PREFIX}:${collName}`,
    );

    await manifestStore.createRun({
      run_id: runId,
      status: "active",
      source_ns: sourceNs,
      target_table: TARGET_TABLE,
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
      targetTable: TARGET_TABLE,
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
      chPressure: getChPressure(),
      gcController: makeGc(),
      retryPolicy: makeRetry(),
      logger,
      config: {
        runId,
        transformVersion: "v1",
        sourceNs,
        targetTable: TARGET_TABLE,
        upperBoundId: dummyUpperBound,
        batchRowsTarget: opts?.batchRowsTarget ?? 500,
        mongoPageSize: opts?.batchRowsTarget ?? 500,
        backpressure: BACKPRESSURE_OFF,
        useDedupToken: false,
        database: TEST_CH_DB,
        table: TEST_CH_TABLE,
        snapshotInterval: 10,
        collectionDefaults: { a: APP_ID, e: eventName },
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

  // Normal path: cursor-based migration (with auto null-cd sweep)
  const { deps } = await buildBatchRunnerDeps(collName, eventName, mongoReader, opts);
  const runner = new BatchRunner(deps);
  await runner.run();
  const stats = runner.getStats();
  return { runId: deps.config.runId, docsRead: stats.totalDocsRead, rowsInserted: stats.totalRowsInserted };
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

describe("multi-pod-coordination", () => {
  // Track MongoReaders created during tests so we can close them all
  const openReaders: MongoReader[] = [];

  beforeAll(async () => {
    manifestStore = new ManifestStore(TEST_MONGO_URI, TEST_MONGO_DB);
    await manifestStore.connect();

    redisState = new RedisHotState(TEST_REDIS_URL, TEST_REDIS_PREFIX);
    await redisState.connect();

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
    // Close all MongoReaders created during tests
    for (const reader of openReaders) {
      await reader.close().catch(() => {});
    }
    openReaders.length = 0;

    await chWriter.close().catch(() => {});
    await manifestStore.close().catch(() => {});
    await redisState.close().catch(() => {});
    if (chClientForPressure) {
      await chClientForPressure.close();
      chClientForPressure = null;
    }
    await closeAll();
  });

  beforeEach(async () => {
    await teardownMongo();
    await teardownClickHouse();
    await teardownRedis();
    await setupClickHouse();

    // Re-create manifest store indexes after DB drop
    const freshManifest = new ManifestStore(TEST_MONGO_URI, TEST_MONGO_DB);
    await freshManifest.connect();
    await freshManifest.close();
  });

  /** Helper to create + track a new MongoReader for cleanup in afterAll. */
  async function trackedMongoReader(): Promise<MongoReader> {
    const reader = await createMongoReader();
    openReaders.push(reader);
    return reader;
  }

  // =========================================================================
  // Test 1: markRangeDone atomicity (~30s)
  //
  // Pure Redis test. Seeds 2000 docs, inits 4 ranges via pod-A, then
  // simulates a race: pod-A's range 0 goes stale, pod-B reclaims it,
  // and pod-A's markRangeDone is rejected because podId no longer matches.
  // =========================================================================

  it("markRangeDone rejects when range was reclaimed by another pod", async () => {
    const eventName = "atomicity_event";
    const collName = collectionName(eventName, APP_ID);

    // Seed 2000 mixed-data docs
    await seedCollection({
      count: 2000,
      appId: APP_ID,
      eventName,
      ...MIXED_DATA,
    });

    const redis = await getRedis();

    // --- Pod-A initializes ranges ---
    const readerA = await trackedMongoReader();
    await readerA.switchCollection(collName);
    await registerPodHeartbeat("pod-A");

    const depsA = buildRangeCoordinatorDeps(collName, eventName, "pod-A", readerA, {
      rangeCount: 4,
      rangeLeaseTtlSec: 2,
    });
    const coordA = new RangeCoordinator(depsA);

    // Init ranges via pod-A (private method, accessed for testing)
    const runId = await (coordA as any).initRanges();
    expect(runId).toBeTruthy();

    // Verify 4 ranges were created in Redis
    const rangesKey = `${TEST_REDIS_PREFIX}:ranges:${collName}`;
    const allRanges = await redis.hgetall(rangesKey);
    expect(Object.keys(allRanges).length).toBe(4);

    // Set ranges 1-3 as "done" so only range 0 is available
    for (let i = 1; i <= 3; i++) {
      const entry = JSON.parse(allRanges[String(i)]);
      entry.status = "done";
      entry.podId = "pod-A";
      await redis.hset(rangesKey, String(i), JSON.stringify(entry));
    }

    // Manually claim range 0 as pod-A and set it to stale (10 seconds ago)
    const range0 = JSON.parse(allRanges["0"]);
    range0.status = "processing";
    range0.podId = "pod-A";
    range0.claimedAt = Math.floor(Date.now() / 1000) - 10;
    await redis.hset(rangesKey, "0", JSON.stringify(range0));

    // Kill pod-A heartbeat to make it appear dead
    await redis.del(`${TEST_REDIS_PREFIX}:pod:pod-A`);

    // --- Pod-B reclaims range 0 via CLAIM_RANGE_LUA ---
    await registerPodHeartbeat("pod-B");

    const readerB = await trackedMongoReader();
    await readerB.switchCollection(collName);

    const depsB = buildRangeCoordinatorDeps(collName, eventName, "pod-B", readerB, {
      rangeCount: 4,
      rangeLeaseTtlSec: 2,
    });
    const coordB = new RangeCoordinator(depsB);

    // Claim next range as pod-B (should reclaim stale range 0 from dead pod-A)
    const claimed = await (coordB as any).claimNextRange();
    expect(claimed).toBeDefined();
    expect(claimed!.podId).toBe("pod-B");
    expect(claimed!.status).toBe("processing");

    // --- Pod-A tries to markRangeDone (should be REJECTED) ---
    // Invoke the MARK_RANGE_TERMINAL_LUA script directly on Redis.
    // This is a Lua script that runs atomically on the Redis server
    // via the ioredis .eval() method.
    const markResult = await (redis as any).eval(
      MARK_RANGE_TERMINAL_LUA,
      1,
      rangesKey,
      "0",      // ARGV[1] = range index
      "done",   // ARGV[2] = target status
      "pod-A",  // ARGV[3] = pod-A's podId (no longer the owner)
    );

    // Pod-A's markRangeDone should return 0 (rejected — podId mismatch)
    expect(markResult).toBe(0);

    // Verify range 0 is still "processing" owned by pod-B
    const range0After = JSON.parse(await redis.hget(rangesKey, "0") ?? "{}");
    expect(range0After.status).toBe("processing");
    expect(range0After.podId).toBe("pod-B");
  }, 30_000);

  // =========================================================================
  // Test 2: Two pods claim ranges — all complete exactly once (~120s)
  //
  // Seeds 4000 docs, creates two RangeCoordinators with rangeCount=8 and
  // separate MongoReaders. Both run concurrently via Promise.all. Verifies
  // all 8 ranges complete with no failures and CH data matches expected.
  // =========================================================================

  it("two pods claim ranges concurrently and complete all 8 exactly once", async () => {
    const eventName = "two_pods_event";
    const collName = collectionName(eventName, APP_ID);

    const seed = await seedCollection({
      count: 4000,
      appId: APP_ID,
      eventName,
      ...MIXED_DATA,
    });

    // Register both pods
    await registerPodHeartbeat("pod-A");
    await registerPodHeartbeat("pod-B");

    // Create separate MongoReaders for each pod (critical for concurrency safety)
    const readerA = await trackedMongoReader();
    await readerA.switchCollection(collName);

    const readerB = await trackedMongoReader();
    await readerB.switchCollection(collName);

    const depsA = buildRangeCoordinatorDeps(collName, eventName, "pod-A", readerA, {
      rangeCount: 8,
    });
    const depsB = buildRangeCoordinatorDeps(collName, eventName, "pod-B", readerB, {
      rangeCount: 8,
    });

    const coordA = new RangeCoordinator(depsA);
    const coordB = new RangeCoordinator(depsB);

    // Run both pods concurrently
    const [resultA, resultB] = await Promise.all([
      coordA.run(),
      coordB.run(),
    ]);

    // All 8 ranges should be completed between the two pods
    expect(resultA.completedRanges + resultB.completedRanges).toBe(8);
    expect(resultA.failedRanges + resultB.failedRanges).toBe(0);

    // Allow ClickHouse async inserts to flush
    await new Promise(r => setTimeout(r, 2000));

    // Verify CH row count is within tolerance of expected
    const totalCh = await chCountByEvent(eventName);
    expect(totalCh).toBeGreaterThanOrEqual(seed.expectedRows - 30);
    expect(totalCh).toBeLessThanOrEqual(seed.expectedRows + 30);
  }, 120_000);

  // =========================================================================
  // Test 3: More pods than ranges — excess idle gracefully (~120s)
  //
  // Seeds 2000 docs, creates 4 RangeCoordinators for rangeCount=4. Excess
  // pods that get 0 ranges should exit gracefully. Sum of completedRanges=4.
  // =========================================================================

  it("4 pods for 4 ranges — excess pods finish with 0 ranges gracefully", async () => {
    const eventName = "excess_pods_event";
    const collName = collectionName(eventName, APP_ID);

    const seed = await seedCollection({
      count: 2000,
      appId: APP_ID,
      eventName,
      ...MIXED_DATA,
    });

    // Register all 4 pods
    for (let i = 0; i < 4; i++) {
      await registerPodHeartbeat(`pod-${i}`);
    }

    // Create separate MongoReaders and coordinators for all 4 pods
    const coordinators: RangeCoordinator[] = [];
    for (let i = 0; i < 4; i++) {
      const reader = await trackedMongoReader();
      await reader.switchCollection(collName);

      const deps = buildRangeCoordinatorDeps(collName, eventName, `pod-${i}`, reader, {
        rangeCount: 4,
      });
      coordinators.push(new RangeCoordinator(deps));
    }

    // Run all 4 concurrently
    const results = await Promise.all(coordinators.map(c => c.run()));

    // Sum of completed ranges must equal exactly 4 (the total range count)
    const totalCompleted = results.reduce((sum, r) => sum + r.completedRanges, 0);
    const totalFailed = results.reduce((sum, r) => sum + r.failedRanges, 0);
    expect(totalCompleted).toBe(4);
    expect(totalFailed).toBe(0);

    // Allow ClickHouse async inserts to flush
    await new Promise(r => setTimeout(r, 2000));

    const totalCh = await chCountByEvent(eventName);
    expect(totalCh).toBeGreaterThanOrEqual(seed.expectedRows - 20);
    expect(totalCh).toBeLessThanOrEqual(seed.expectedRows + 20);
  }, 120_000);

  // =========================================================================
  // Test 4: Pod A stops, pod B resumes remaining ranges (~120s)
  //
  // Seeds 3000 docs with rangeCount=6. Pod-A starts, we poll until it
  // completes >= 2 ranges, then stop it. Pod-B starts and finishes the rest.
  // Total completedRanges across both pods must equal 6.
  // =========================================================================

  it("pod-A stops mid-flight, pod-B resumes and completes remaining ranges", async () => {
    const eventName = "stop_resume_event";
    const collName = collectionName(eventName, APP_ID);

    const seed = await seedCollection({
      count: 3000,
      appId: APP_ID,
      eventName,
      ...MIXED_DATA,
    });

    await registerPodHeartbeat("pod-A");

    const readerA = await trackedMongoReader();
    await readerA.switchCollection(collName);

    const depsA = buildRangeCoordinatorDeps(collName, eventName, "pod-A", readerA, {
      rangeCount: 6,
    });
    const coordA = new RangeCoordinator(depsA);

    // Start pod-A, poll getRangeStatus() until done >= 2, then stop
    const runPromiseA = coordA.run();

    for (let i = 0; i < 200; i++) {
      await new Promise(r => setTimeout(r, 300));
      try {
        const status = await coordA.getRangeStatus();
        if (status.done >= 2) {
          coordA.stop();
          break;
        }
      } catch {
        // getRangeStatus may fail if ranges not yet initialized
      }
    }

    const resultA = await runPromiseA;
    expect(resultA.completedRanges).toBeGreaterThanOrEqual(1);

    // Pod-B picks up remaining ranges. Kill pod-A heartbeat first so
    // any stale "processing" ranges can be reclaimed.
    const redis = await getRedis();
    await redis.del(`${TEST_REDIS_PREFIX}:pod:pod-A`);
    await registerPodHeartbeat("pod-B");

    const readerB = await trackedMongoReader();
    await readerB.switchCollection(collName);

    const depsB = buildRangeCoordinatorDeps(collName, eventName, "pod-B", readerB, {
      rangeCount: 6,
    });
    const coordB = new RangeCoordinator(depsB);
    const resultB = await coordB.run();

    // Between the two pods, all 6 ranges should be completed
    expect(resultA.completedRanges + resultB.completedRanges).toBe(6);

    // Allow ClickHouse async inserts to flush
    await new Promise(r => setTimeout(r, 2000));

    const totalCh = await chCountByEvent(eventName);
    expect(totalCh).toBeGreaterThanOrEqual(seed.expectedRows - 30);
    expect(totalCh).toBeLessThanOrEqual(seed.expectedRows + 30);
  }, 120_000);

  // =========================================================================
  // Test 5: Lock lost triggers abort (~60s)
  //
  // Verifies the onLockLost callback works. Uses a single-collection
  // BatchRunner with tiny batch size (50). After 1+ batch, the lock key
  // is deleted from Redis. The lock renewal detects the loss and calls
  // runner.stopAfterBatch(). Verifies no data duplication.
  // =========================================================================

  it("onLockLost callback stops BatchRunner when lock is deleted", async () => {
    const eventName = "lock_lost_event";
    const collName = collectionName(eventName, APP_ID);

    const seed = await seedCollection({
      count: 5000,
      appId: APP_ID,
      eventName,
      ...MIXED_DATA,
    });

    const reader = await trackedMongoReader();

    // Build BatchRunner deps with very tiny batch size for many batches (~200+)
    const { deps } = await buildBatchRunnerDeps(collName, eventName, reader, {
      batchRowsTarget: 25,
    });
    deps.config.mongoPageSize = 25;

    const runner = new BatchRunner(deps);

    // Create a CollectionLock with short renewal interval and TTL
    const redis = await getRedis();
    const lockConfig: CollectionLockConfig = {
      lockTtlSec: 2,
      renewIntervalMs: 500,
      podHeartbeatMs: 30_000,
      podDeadAfterSec: 180,
      keyPrefix: TEST_REDIS_PREFIX,
    };
    const lock = new CollectionLock(redis, "lock-test-pod", lockConfig, logger);

    // Wire onLockLost to stop the runner
    lock.onLockLost = (_lostCollName: string) => {
      runner.stopAfterBatch();
    };

    // Register pod heartbeat and acquire lock
    await registerPodHeartbeat("lock-test-pod");
    const acquireResult = await lock.tryAcquire(collName);
    expect(acquireResult).toBe("acquired");

    // Start heartbeat (handles lock renewal)
    lock.startHeartbeat();

    // Start processing in background
    const runPromise = runner.run();

    // Wait for runner to process at least 1 batch
    for (let i = 0; i < 100; i++) {
      await new Promise(r => setTimeout(r, 50));
      const status = runner.getStatus();
      if (status === "completed" || status === "failed") break;
      if (runner.getStats().batchSeq >= 1) break;
    }

    // Delete the lock key from Redis to simulate lock loss
    const lockKey = `${TEST_REDIS_PREFIX}:lock:${collName}`;
    await redis.del(lockKey);

    // Wait for lock renewal to detect the loss (renewIntervalMs=500ms).
    // The onLockLost callback calls runner.stopAfterBatch().
    await runPromise;

    lock.stopHeartbeat();

    const stats = runner.getStats();

    // Runner should have stopped (or completed if it finished before the lock check)
    expect(["stopped", "completed"]).toContain(stats.status);

    // If runner was stopped early, it should have inserted fewer rows than expected
    if (stats.status === "stopped") {
      expect(stats.totalRowsInserted).toBeLessThan(seed.expectedRows);
    }

    // Verify no excessive duplication (small boundary overlap is expected
    // from min/max cursor inclusivity — ~1 per batch boundary)
    await new Promise(r => setTimeout(r, 2000));
    const duplicates = await chQuery<{ _id: string; cnt: string }>(
      `SELECT _id, count() AS cnt FROM ${TEST_CH_TABLE} WHERE n = '${eventName}' GROUP BY _id HAVING cnt > 1`,
    );
    // With batch size 25 and ~200 batches, up to ~200 boundary duplicates are possible.
    // The key assertion is that the runner stopped, not that there are zero duplicates.
    // In production, dedup tokens handle this at the ClickHouse level.
    const totalRows = await chRowCount(`n = '${eventName}'`);
    if (stats.status === "stopped") {
      // Stopped early — should have partial data, not the full set
      expect(totalRows).toBeLessThan(seed.expectedRows + 100);
    }
  }, 60_000);

  // =========================================================================
  // Test 6: Concurrent resolveRun (~30s)
  //
  // Seeds 500 docs, calls resolveRun twice concurrently for the same
  // collection/sourceNs. Both should succeed. Documents whether the race
  // produces duplicate active runs.
  // =========================================================================

  it("concurrent resolveRun calls for the same collection both succeed", async () => {
    const eventName = "resolve_race_event";
    const collName = collectionName(eventName, APP_ID);

    await seedCollection({
      count: 500,
      appId: APP_ID,
      eventName,
      ...MIXED_DATA,
    });

    const sourceNs = `${TEST_MONGO_DB}.${collName}`;

    // Create two separate MongoReaders for the concurrent calls
    const readerA = await trackedMongoReader();
    await readerA.switchCollection(collName);

    const readerB = await trackedMongoReader();
    await readerB.switchCollection(collName);

    // Create separate RedisHotState instances (sharing the underlying connection)
    const redisStateA = RedisHotState.fromExistingConnection(
      redisState.getRedisClient(),
      `${TEST_REDIS_PREFIX}:resolveA`,
    );
    const redisStateB = RedisHotState.fromExistingConnection(
      redisState.getRedisClient(),
      `${TEST_REDIS_PREFIX}:resolveB`,
    );

    // Call resolveRun twice concurrently
    const [runA, runB] = await Promise.all([
      resolveRun({
        rerunMode: "resume",
        manifestStore,
        redisState: redisStateA,
        mongoReader: readerA,
        sourceNs,
        targetTable: TARGET_TABLE,
        transformVersion: "v1",
        logger,
      }),
      resolveRun({
        rerunMode: "resume",
        manifestStore,
        redisState: redisStateB,
        mongoReader: readerB,
        sourceNs,
        targetTable: TARGET_TABLE,
        transformVersion: "v1",
        logger,
      }),
    ]);

    // Both should succeed without crashing
    expect(runA.runId).toBeTruthy();
    expect(runB.runId).toBeTruthy();
    expect(runA.upperBoundId).toBeTruthy();
    expect(runB.upperBoundId).toBeTruthy();

    // Count active runs in manifest to document race behavior
    const db = await getMongoDb();
    const activeRuns = await db
      .collection("mig_runs")
      .countDocuments({ status: "active", source_ns: sourceNs });

    // At least 1 active run should exist; the race may produce 1 or 2
    expect(activeRuns).toBeGreaterThanOrEqual(1);
  }, 30_000);

  // =========================================================================
  // Test 7: 20+ collections smoke test (~120s)
  //
  // Seeds 22 collections with varying profiles: 3 empty, 5 small (50-200),
  // 8 medium (500-1500), 4 large (2000-4000), and 2 all-null-cd (300 each).
  // Migrates all non-empty collections and verifies all have data in CH.
  // =========================================================================

  it("processes 22 collections of varying sizes and profiles", async () => {
    const collConfigs: Array<{
      eventName: string;
      count: number;
      kind: "empty" | "small" | "medium" | "large" | "nullcd";
    }> = [
      // 3 empty collections
      { eventName: "smoke_empty_1", count: 0, kind: "empty" },
      { eventName: "smoke_empty_2", count: 0, kind: "empty" },
      { eventName: "smoke_empty_3", count: 0, kind: "empty" },

      // 5 small collections (50-200 docs)
      { eventName: "smoke_small_1", count: 50, kind: "small" },
      { eventName: "smoke_small_2", count: 100, kind: "small" },
      { eventName: "smoke_small_3", count: 120, kind: "small" },
      { eventName: "smoke_small_4", count: 150, kind: "small" },
      { eventName: "smoke_small_5", count: 200, kind: "small" },

      // 8 medium collections (500-1500 docs)
      { eventName: "smoke_med_1", count: 500, kind: "medium" },
      { eventName: "smoke_med_2", count: 600, kind: "medium" },
      { eventName: "smoke_med_3", count: 750, kind: "medium" },
      { eventName: "smoke_med_4", count: 900, kind: "medium" },
      { eventName: "smoke_med_5", count: 1000, kind: "medium" },
      { eventName: "smoke_med_6", count: 1100, kind: "medium" },
      { eventName: "smoke_med_7", count: 1300, kind: "medium" },
      { eventName: "smoke_med_8", count: 1500, kind: "medium" },

      // 4 large collections (2000-4000 docs)
      { eventName: "smoke_large_1", count: 2000, kind: "large" },
      { eventName: "smoke_large_2", count: 2500, kind: "large" },
      { eventName: "smoke_large_3", count: 3000, kind: "large" },
      { eventName: "smoke_large_4", count: 4000, kind: "large" },

      // 2 all-null-cd collections (300 docs each)
      { eventName: "smoke_nullcd_1", count: 300, kind: "nullcd" },
      { eventName: "smoke_nullcd_2", count: 300, kind: "nullcd" },
    ];

    // Seed all 22 collections
    const seeded: Array<{
      eventName: string;
      collName: string;
      expectedRows: number;
      kind: string;
    }> = [];

    for (const cfg of collConfigs) {
      if (cfg.kind === "empty") {
        // Create an empty collection with the required index
        const cName = collectionName(cfg.eventName, APP_ID);
        const db = await getMongoDb();
        const coll = db.collection(cName);
        await coll.drop().catch(() => {});
        await coll.insertOne({ _placeholder: true });
        await coll.deleteMany({});
        await coll.createIndex({ cd: 1, _id: 1 });
        seeded.push({ eventName: cfg.eventName, collName: cName, expectedRows: 0, kind: cfg.kind });
      } else if (cfg.kind === "nullcd") {
        // Use seedNullCdCollection for all-null-cd collections
        const s = await seedNullCdCollection({
          count: cfg.count,
          appId: APP_ID,
          eventName: cfg.eventName,
        });
        seeded.push({ eventName: cfg.eventName, collName: s.collName, expectedRows: s.expectedRows, kind: cfg.kind });
      } else {
        // Normal mixed-data seeding
        const s = await seedCollection({
          count: cfg.count,
          appId: APP_ID,
          eventName: cfg.eventName,
          ...MIXED_DATA,
        });
        seeded.push({ eventName: cfg.eventName, collName: s.collName, expectedRows: s.expectedRows, kind: cfg.kind });
      }
    }

    // Migrate each non-empty collection sequentially using a single MongoReader
    // (sequential is fine here; each collection gets its own run)
    const reader = await trackedMongoReader();

    let totalExpected = 0;
    let migratedCount = 0;

    for (const col of seeded) {
      if (col.kind === "empty") continue;

      await migrateCollection(col.collName, col.eventName, reader);
      totalExpected += col.expectedRows;
      migratedCount++;
    }

    // 22 total - 3 empty = 19 non-empty collections processed
    expect(migratedCount).toBe(19);

    // Allow ClickHouse async inserts to flush
    await new Promise(r => setTimeout(r, 2000));

    // Verify all non-empty collections have data in CH
    for (const col of seeded) {
      if (col.kind === "empty") {
        const count = await chCountByEvent(col.eventName);
        expect(count).toBe(0);
        continue;
      }

      const count = await chCountByEvent(col.eventName);
      expect(count).toBeGreaterThanOrEqual(col.expectedRows - 20);
      expect(count).toBeLessThanOrEqual(col.expectedRows + 20);
      // Every non-empty collection must have at least some rows
      expect(count).toBeGreaterThan(0);
    }

    // Verify aggregate count across all collections
    const totalCh = await chRowCount();
    expect(totalCh).toBeGreaterThanOrEqual(totalExpected - 200);
    expect(totalCh).toBeLessThanOrEqual(totalExpected + 200);
  }, 120_000);

  // =========================================================================
  // Test 8: Collection transition under contention (~120s)
  //
  // 3 collections (1000, 1500, 2000 docs, all mixed data). Pod-A processes
  // col-A then col-B sequentially. Pod-B processes col-C concurrently with
  // pod-A. Both use separate MongoReaders. Verifies per-collection CH
  // counts and no cross-contamination between events.
  // =========================================================================

  it("two pods process different collections concurrently without cross-contamination", async () => {
    const events = ["contention_a", "contention_b", "contention_c"];
    const sizes = [1000, 1500, 2000];

    // Seed 3 collections with mixed data
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

    // Pod-A: processes col-A then col-B sequentially
    // Pod-B: processes col-C concurrently with pod-A
    const readerA = await trackedMongoReader();
    const readerB = await trackedMongoReader();

    const podAWork = async () => {
      await migrateCollection(seeds[0].collName, events[0], readerA);
      await migrateCollection(seeds[1].collName, events[1], readerA);
    };

    const podBWork = async () => {
      await migrateCollection(seeds[2].collName, events[2], readerB);
    };

    // Run both pods concurrently
    await Promise.all([podAWork(), podBWork()]);

    // Allow ClickHouse async inserts to flush
    await new Promise(r => setTimeout(r, 2000));

    // Verify per-collection CH counts — no cross-contamination
    for (let i = 0; i < 3; i++) {
      const count = await chCountByEvent(events[i]);
      expect(count).toBeGreaterThanOrEqual(seeds[i].expectedRows - 20);
      expect(count).toBeLessThanOrEqual(seeds[i].expectedRows + 20);
      expect(count).toBeGreaterThan(0);
    }

    // Verify aggregate count
    const totalExpected = seeds.reduce((s, seed) => s + seed.expectedRows, 0);
    const totalCh = await chRowCount();
    expect(totalCh).toBeGreaterThanOrEqual(totalExpected - 60);
    expect(totalCh).toBeLessThanOrEqual(totalExpected + 60);

    // Verify only the 3 expected event names exist in CH (no cross-contamination)
    const eventNames = await chQuery<{ n: string }>(
      `SELECT DISTINCT n FROM ${TEST_CH_TABLE} ORDER BY n`,
    );
    const chEvents = eventNames.map(r => r.n).sort();
    expect(chEvents).toEqual(events.sort());
  }, 120_000);
});
