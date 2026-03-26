/**
 * Integration test: cursor isolation in range-parallel mode.
 *
 * Verifies the fix for the critical bug where resumeFromInterruption()
 * would read the globally last-done batch across ALL ranges instead of
 * scoping its manifest query to the current range's batch_seq slot.
 *
 * In range-parallel mode:
 *   - All ranges share one runId
 *   - Each range gets its own batch_seq slot:
 *       range 0 = [0, 10000), range 1 = [10000, 20000), etc.
 *   - Manifest queries MUST be scoped with batchSeqRange: { min, max }
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import pino from "pino";

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
  TEST_MANIFEST_DB,
} from "../helpers/setup.ts";
import { seedCollection, collectionName } from "../helpers/seed-mongo.ts";

import { ManifestStore } from "../../src/state/manifest-store.ts";
import { RedisHotState } from "../../src/state/redis-hot-state.ts";
import { MongoReader, type MongoReaderConfig } from "../../src/source/mongo-reader.ts";
import { ClickHouseWriter, type ClickHouseWriterConfig } from "../../src/target/clickhouse-writer.ts";
import { ClickHousePressure, type BackpressureConfig } from "../../src/target/clickhouse-pressure.ts";
import { GcController } from "../../src/runtime/gc-controller.ts";
import { RetryPolicy } from "../../src/runtime/retry-policy.ts";
import { BatchRunner, type BatchRunnerConfig, type BatchRunnerDeps } from "../../src/runtime/batch-runner.ts";
import { serializeCursor, deserializeCursor, type Cursor } from "../../src/types/cursor.ts";
import { createClient } from "@clickhouse/client";

// ---------------------------------------------------------------------------
// Shared logger (silent for tests)
// ---------------------------------------------------------------------------
const logger = pino({ level: "silent" });

// ---------------------------------------------------------------------------
// Test-scoped constants
// ---------------------------------------------------------------------------
const APP_ID = "aaaaaaaaaaaaaaaaaaaaaaaa";
const EVENT_NAME = "cursor_isolation_event";
const COLL_NAME = collectionName(EVENT_NAME, APP_ID);
const SOURCE_NS = `${TEST_MONGO_DB}.${COLL_NAME}`;
const RUN_ID = "cursor-isolation-test-run";

// Date ranges: 6 months, one per range
const RANGE_DATES = [
  { start: new Date("2024-01-01"), end: new Date("2024-03-01") },
  { start: new Date("2024-03-01"), end: new Date("2024-05-01") },
  { start: new Date("2024-05-01"), end: new Date("2024-07-01") },
  { start: new Date("2024-07-01"), end: new Date("2024-09-01") },
  { start: new Date("2024-09-01"), end: new Date("2024-11-01") },
  { start: new Date("2024-11-01"), end: new Date("2025-01-01") },
];

// Backpressure config (disabled for tests)
const BACKPRESSURE: BackpressureConfig = {
  enabled: false,
  partsToThrowInsert: 300,
  maxPartsInTotal: 100_000,
  partitionPctHigh: 0.8,
  partitionPctLow: 0.6,
  totalPctHigh: 0.8,
  totalPctLow: 0.6,
  pollIntervalMs: 1000,
  maxPauseEpisodeMs: 60_000,
};

// ---------------------------------------------------------------------------
// Shared resources
// ---------------------------------------------------------------------------
let manifestStore: ManifestStore;
let redisState: RedisHotState;
let mongoReader: MongoReader;
let chWriter: ClickHouseWriter;

async function createChPressure(): Promise<ClickHousePressure> {
  const client = createClient({
    url: TEST_CH_URL,
    username: "default",
    password: "",
    database: TEST_CH_DB,
  });
  return new ClickHousePressure(client, BACKPRESSURE, logger);
}

function createGcController(): GcController {
  return new GcController(
    {
      enabled: false,
      rssSoftLimitBytes: 2 * 1024 * 1024 * 1024,
      rssHardLimitBytes: 3 * 1024 * 1024 * 1024,
      heapUsedRatio: 0.85,
      everyNBatches: 50,
    },
    logger,
  );
}

function createRetryPolicy(): RetryPolicy {
  return new RetryPolicy({
    maxRetries: 2,
    baseDelayMs: 100,
    maxDelayMs: 500,
  });
}

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------
describe("cursor-isolation (range-parallel)", () => {
  beforeAll(async () => {
    // Setup external stores
    await setupClickHouse();

    manifestStore = new ManifestStore(TEST_MONGO_URI, TEST_MANIFEST_DB);
    await manifestStore.connect();

    redisState = new RedisHotState(TEST_REDIS_URL, TEST_REDIS_PREFIX);
    await redisState.connect();

    const mongoReaderConfig: MongoReaderConfig = {
      uri: TEST_MONGO_URI,
      database: TEST_MONGO_DB,
      readPreference: "primary",
      readConcern: "local",
      retryReads: true,
      appName: "cursor-isolation-test",
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
    await mongoReader.close();
    await chWriter.close();
    await manifestStore.close();
    await redisState.close();
    await closeAll();
  });

  beforeEach(async () => {
    // Clean slate for each test
    await teardownMongo();
    await teardownClickHouse();
    await setupClickHouse();
    await teardownRedis();

    // Re-create manifest store indexes (teardownMongo drops the DB)
    const freshManifest = new ManifestStore(TEST_MONGO_URI, TEST_MANIFEST_DB);
    await freshManifest.connect();
    await freshManifest.close();
  });

  // -------------------------------------------------------------------------
  // Test 1: range 3 starts from its own cursor after ranges 0-2 complete
  // -------------------------------------------------------------------------
  it("range 3 starts from its own startCd after ranges 0-2 complete", async () => {
    // Seed 6000 docs spanning the full date range
    const { collName } = await seedCollection({
      count: 6000,
      appId: APP_ID,
      eventName: EVENT_NAME,
      startDate: RANGE_DATES[0].start,
      endDate: RANGE_DATES[5].end,
    });

    // Compute cursor boundaries for each range by querying MongoDB
    const db = await getMongoDb();
    const coll = db.collection(collName);

    // Get docs sorted by cd to determine range boundaries
    const allDocs = await coll
      .find({})
      .sort({ cd: 1, _id: 1 })
      .project({ cd: 1, _id: 1 })
      .toArray();

    // Split into 6 ranges
    const docsPerRange = Math.ceil(allDocs.length / 6);
    const rangeBoundaries: { startCursor: Cursor; endCursor: Cursor }[] = [];
    for (let r = 0; r < 6; r++) {
      const startIdx = r * docsPerRange;
      const endIdx = Math.min((r + 1) * docsPerRange - 1, allDocs.length - 1);
      const startDoc = allDocs[startIdx];
      const endDoc = allDocs[endIdx];
      rangeBoundaries.push({
        startCursor: {
          cd: new Date(startDoc.cd as Date).getTime(),
          id: String(startDoc._id),
        },
        endCursor: {
          cd: new Date(endDoc.cd as Date).getTime(),
          id: String(endDoc._id),
        },
      });
    }

    // Create a run in the manifest
    const runId = `${RUN_ID}-test1-${Date.now()}`;
    const upperBound = rangeBoundaries[5].endCursor;
    await manifestStore.createRun({
      run_id: runId,
      status: "active",
      source_ns: SOURCE_NS,
      target_table: TEST_CH_TABLE,
      upper_bound_cursor: serializeCursor(upperBound),
      transform_version: "v1",
      created_at: new Date().toISOString(),
    });

    // Insert fake "done" batches for ranges 0-2
    // Each range uses batch_seq slots: range 0=[0,10000), range 1=[10000,20000), range 2=[20000,30000)
    for (let rangeIdx = 0; rangeIdx < 3; rangeIdx++) {
      const offset = rangeIdx * 10000;
      const rangeStart = rangeBoundaries[rangeIdx].startCursor;
      const rangeEnd = rangeBoundaries[rangeIdx].endCursor;

      // Insert 3 done batches per range (simulating completed work)
      for (let i = 0; i < 3; i++) {
        const fraction = (i + 1) / 3;
        const batchCd = rangeStart.cd + fraction * (rangeEnd.cd - rangeStart.cd);
        const lowerCd = rangeStart.cd + (i / 3) * (rangeEnd.cd - rangeStart.cd);

        await manifestStore.insertCompletedBatch(
          {
            run_id: runId,
            batch_seq: offset + i,
            lower_exclusive_cursor: serializeCursor({
              cd: lowerCd,
              id: `fake-lower-${rangeIdx}-${i}`,
            }),
            upper_inclusive_cursor: serializeCursor({
              cd: batchCd,
              id: `fake-upper-${rangeIdx}-${i}`,
            }),
            source_docs_read: 100,
            docs_skipped: 0,
            rows_to_insert: 100,
            payload_digest: "100",
            insert_dedup_token: `dedup:${runId}:${offset + i}`,
            query_id: `query:${runId}:${offset + i}`,
            status: "done",
            retry_count: 0,
            last_error: null,
            started_at: new Date().toISOString(),
            finished_at: new Date().toISOString(),
          },
          serializeCursor({ cd: batchCd, id: `fake-upper-${rangeIdx}-${i}` }),
        );
      }
    }

    // Now set up a BatchRunner for range 3
    const range3Start = rangeBoundaries[3].startCursor;
    const range3End = rangeBoundaries[3].endCursor;

    await mongoReader.switchCollection(collName);

    // Create a per-range RedisHotState (reuses connection but different prefix)
    const range3Redis = RedisHotState.fromExistingConnection(
      redisState.getRedisClient(),
      `${TEST_REDIS_PREFIX}:range3`,
    );

    const chPressure = await createChPressure();

    const config: BatchRunnerConfig = {
      runId,
      transformVersion: "v1",
      sourceNs: SOURCE_NS,
      targetTable: TEST_CH_TABLE,
      upperBoundId: serializeCursor(range3End),
      batchRowsTarget: 500,
      mongoPageSize: 500,
      backpressure: BACKPRESSURE,
      useDedupToken: false,
      database: TEST_CH_DB,
      table: TEST_CH_TABLE,
      snapshotInterval: 100,
      collectionDefaults: { a: APP_ID, e: EVENT_NAME },
      batchSeqOffset: 30000,
      batchSeqMax: 40000,
      rangeIdx: 3,
      collectionName: collName,
      podId: "test-pod",
    };

    const deps: BatchRunnerDeps = {
      manifestStore,
      redisState: range3Redis,
      mongoReader,
      chWriter,
      chPressure,
      gcController: createGcController(),
      retryPolicy: createRetryPolicy(),
      logger,
      config,
    };

    const runner = new BatchRunner(deps);

    // Run with startCursor for range 3
    await runner.run(serializeCursor(range3Start));

    // Verify the runner completed
    expect(runner.getStatus()).toBe("completed");

    // Verify ClickHouse has rows with cd values in range 3's window
    // Range 3 timestamps should be around 2024-07-01 to 2024-09-01
    const range3StartMs = range3Start.cd;
    const range3EndMs = range3End.cd;

    // Query ClickHouse for rows inserted
    const totalRows = await chRowCount();
    expect(totalRows).toBeGreaterThan(0);

    // Verify all inserted rows have cd within range 3's time window
    const range3StartDate = new Date(range3StartMs).toISOString().replace("T", " ").replace("Z", "");
    const range3EndDate = new Date(range3EndMs + 1000).toISOString().replace("T", " ").replace("Z", "");

    const rowsInRange = await chRowCount(
      `cd >= '${range3StartDate}' AND cd <= '${range3EndDate}'`,
    );

    // All rows should be within range 3's window (the runner should not have
    // started from range 2's final cursor)
    expect(rowsInRange).toBe(totalRows);

    // Verify the runner's stats show it processed docs
    const stats = runner.getStats();
    expect(stats.totalDocsRead).toBeGreaterThan(0);
    expect(stats.totalRowsInserted).toBeGreaterThan(0);
  });

  // -------------------------------------------------------------------------
  // Test 2: bounds guard discards out-of-range cursor
  // -------------------------------------------------------------------------
  it("bounds guard discards out-of-range cursor", async () => {
    const { collName } = await seedCollection({
      count: 2000,
      appId: APP_ID,
      eventName: EVENT_NAME,
      startDate: new Date("2024-01-01"),
      endDate: new Date("2024-12-31"),
    });

    const db = await getMongoDb();
    const coll = db.collection(collName);

    const allDocs = await coll
      .find({})
      .sort({ cd: 1, _id: 1 })
      .project({ cd: 1, _id: 1 })
      .toArray();

    // Range 1 covers the second quarter of docs
    const docsPerRange = Math.ceil(allDocs.length / 4);
    const range1StartIdx = docsPerRange;
    const range1EndIdx = Math.min(2 * docsPerRange - 1, allDocs.length - 1);
    const range1Start: Cursor = {
      cd: new Date(allDocs[range1StartIdx].cd as Date).getTime(),
      id: String(allDocs[range1StartIdx]._id),
    };
    const range1End: Cursor = {
      cd: new Date(allDocs[range1EndIdx].cd as Date).getTime(),
      id: String(allDocs[range1EndIdx]._id),
    };

    const runId = `${RUN_ID}-test2-${Date.now()}`;
    await manifestStore.createRun({
      run_id: runId,
      status: "active",
      source_ns: SOURCE_NS,
      target_table: TEST_CH_TABLE,
      upper_bound_cursor: serializeCursor(range1End),
      transform_version: "v1",
      created_at: new Date().toISOString(),
    });

    // Set a Redis cursor that is WAY past range 1's endCd (e.g., year 2025)
    const outOfRangeCursor = serializeCursor({
      cd: new Date("2025-06-01").getTime(),
      id: "zzz-out-of-range",
    });

    const range1Redis = RedisHotState.fromExistingConnection(
      redisState.getRedisClient(),
      `${TEST_REDIS_PREFIX}:range1`,
    );
    await range1Redis.setLastCommittedCursor(runId, outOfRangeCursor);

    await mongoReader.switchCollection(collName);

    const chPressure = await createChPressure();

    const config: BatchRunnerConfig = {
      runId,
      transformVersion: "v1",
      sourceNs: SOURCE_NS,
      targetTable: TEST_CH_TABLE,
      upperBoundId: serializeCursor(range1End),
      batchRowsTarget: 500,
      mongoPageSize: 500,
      backpressure: BACKPRESSURE,
      useDedupToken: false,
      database: TEST_CH_DB,
      table: TEST_CH_TABLE,
      snapshotInterval: 100,
      collectionDefaults: { a: APP_ID, e: EVENT_NAME },
      batchSeqOffset: 10000,
      batchSeqMax: 20000,
      rangeIdx: 1,
      collectionName: collName,
      podId: "test-pod",
    };

    const deps: BatchRunnerDeps = {
      manifestStore,
      redisState: range1Redis,
      mongoReader,
      chWriter,
      chPressure,
      gcController: createGcController(),
      retryPolicy: createRetryPolicy(),
      logger,
      config,
    };

    const runner = new BatchRunner(deps);
    await runner.run(serializeCursor(range1Start));

    expect(runner.getStatus()).toBe("completed");

    // The runner should have processed data starting from range1Start
    // (the out-of-range cursor was discarded by the bounds guard)
    const stats = runner.getStats();
    expect(stats.totalDocsRead).toBeGreaterThan(0);
    expect(stats.totalRowsInserted).toBeGreaterThan(0);

    // Verify all rows are within range 1's time window
    const totalRows = await chRowCount();
    expect(totalRows).toBeGreaterThan(0);

    const range1StartDate = new Date(range1Start.cd)
      .toISOString().replace("T", " ").replace("Z", "");
    const range1EndDate = new Date(range1End.cd + 1000)
      .toISOString().replace("T", " ").replace("Z", "");

    const rowsInRange = await chRowCount(
      `cd >= '${range1StartDate}' AND cd <= '${range1EndDate}'`,
    );
    expect(rowsInRange).toBe(totalRows);
  });

  // -------------------------------------------------------------------------
  // Test 3: scoped getLastDoneBatch returns only this range's batches
  // -------------------------------------------------------------------------
  it("scoped getLastDoneBatch returns only this range's batches", async () => {
    const runId = `${RUN_ID}-test3-${Date.now()}`;

    // Create a fresh ManifestStore for this test to avoid stale state
    const store = new ManifestStore(TEST_MONGO_URI, TEST_MANIFEST_DB);
    await store.connect();

    try {
      await store.createRun({
        run_id: runId,
        status: "active",
        source_ns: SOURCE_NS,
        target_table: TEST_CH_TABLE,
        upper_bound_cursor: serializeCursor({ cd: Date.now(), id: "upper" }),
        transform_version: "v1",
        created_at: new Date().toISOString(),
      });

      const baseCd = new Date("2024-06-01").getTime();

      // Insert done batches for range 0: batch_seq 0..10
      for (let i = 0; i <= 10; i++) {
        const cursorCd = baseCd + i * 1000;
        await store.insertCompletedBatch(
          {
            run_id: runId,
            batch_seq: i,
            lower_exclusive_cursor: serializeCursor({ cd: cursorCd - 500, id: `r0-lower-${i}` }),
            upper_inclusive_cursor: serializeCursor({ cd: cursorCd, id: `r0-upper-${i}` }),
            source_docs_read: 50,
            docs_skipped: 0,
            rows_to_insert: 50,
            payload_digest: "50",
            insert_dedup_token: `dedup:${runId}:${i}`,
            query_id: `query:${runId}:${i}`,
            status: "done",
            retry_count: 0,
            last_error: null,
            started_at: new Date().toISOString(),
            finished_at: new Date().toISOString(),
          },
          serializeCursor({ cd: cursorCd, id: `r0-upper-${i}` }),
        );
      }

      // Insert done batches for range 1: batch_seq 10000..10010
      for (let i = 0; i <= 10; i++) {
        const seq = 10000 + i;
        const cursorCd = baseCd + 100_000 + i * 1000;
        await store.insertCompletedBatch(
          {
            run_id: runId,
            batch_seq: seq,
            lower_exclusive_cursor: serializeCursor({ cd: cursorCd - 500, id: `r1-lower-${i}` }),
            upper_inclusive_cursor: serializeCursor({ cd: cursorCd, id: `r1-upper-${i}` }),
            source_docs_read: 50,
            docs_skipped: 0,
            rows_to_insert: 50,
            payload_digest: "50",
            insert_dedup_token: `dedup:${runId}:${seq}`,
            query_id: `query:${runId}:${seq}`,
            status: "done",
            retry_count: 0,
            last_error: null,
            started_at: new Date().toISOString(),
            finished_at: new Date().toISOString(),
          },
          serializeCursor({ cd: cursorCd, id: `r1-upper-${i}` }),
        );
      }

      // ----- Assertions -----

      // Unscoped: should return the globally last batch (seq 10010)
      const globalLast = await store.getLastDoneBatch(runId);
      expect(globalLast).not.toBeNull();
      expect(globalLast!.batch_seq).toBe(10010);

      // Scoped to range 0 [0, 10000): should return batch 10
      const range0Last = await store.getLastDoneBatch(runId, { min: 0, max: 10000 });
      expect(range0Last).not.toBeNull();
      expect(range0Last!.batch_seq).toBe(10);

      // Verify range 0's cursor is from range 0's data
      const range0Cursor = deserializeCursor(range0Last!.upper_inclusive_cursor);
      expect(range0Cursor.id).toBe("r0-upper-10");

      // Scoped to range 1 [10000, 20000): should return batch 10010
      const range1Last = await store.getLastDoneBatch(runId, { min: 10000, max: 20000 });
      expect(range1Last).not.toBeNull();
      expect(range1Last!.batch_seq).toBe(10010);

      // Verify range 1's cursor is from range 1's data
      const range1Cursor = deserializeCursor(range1Last!.upper_inclusive_cursor);
      expect(range1Cursor.id).toBe("r1-upper-10");

      // Scoped to range 2 [20000, 30000): should return null (no batches)
      const range2Last = await store.getLastDoneBatch(runId, { min: 20000, max: 30000 });
      expect(range2Last).toBeNull();
    } finally {
      await store.close();
    }
  });
});
