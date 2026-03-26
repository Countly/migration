/**
 * Integration tests for MongoDB index management used by the migration.
 *
 * Verifies hasRequiredIndex(), startIndexCreation(), and that readPage
 * successfully uses the { cd: 1, _id: 1 } compound index hint.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { ObjectId } from "mongodb";
import pino from "pino";

import { MongoReader, type MongoReaderConfig } from "../../src/source/mongo-reader.ts";
import {
  getMongoDb,
  teardownMongo,
  closeAll,
  TEST_MONGO_URI,
  TEST_MONGO_DB,
} from "../helpers/setup.ts";

// ---------------------------------------------------------------------------
// Shared config
// ---------------------------------------------------------------------------

const logger = pino({ level: "silent" });

const MONGO_READER_CONFIG: MongoReaderConfig = {
  uri: TEST_MONGO_URI,
  database: TEST_MONGO_DB,
  readPreference: "primary",
  readConcern: "local",
  retryReads: true,
  appName: "integration-test-index",
  batchRowsTarget: 100,
  cursorBatchSize: 100,
  maxTimeMs: 30_000,
};

const TEST_COLL = "drill_events_index_test";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function seedSmallCollection(withIndex: boolean): Promise<void> {
  const db = await getMongoDb();
  const coll = db.collection(TEST_COLL);

  await coll.drop().catch(() => {});

  const baseTime = new Date("2024-06-01T00:00:00Z");
  const docs = [];
  for (let i = 0; i < 20; i++) {
    const cd = new Date(baseTime.getTime() + i * 60_000);
    docs.push({
      _id: new ObjectId().toHexString(),
      a: "test_app",
      e: "test_event",
      uid: `user-${i}`,
      did: `device-${i}`,
      ts: cd.getTime(),
      cd,
      c: 1,
      s: 0,
      dur: 0,
      n: "test_event",
    });
  }

  await coll.insertMany(docs);

  if (withIndex) {
    await coll.createIndex({ cd: 1, _id: 1 });
  }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

let reader: MongoReader;

beforeAll(async () => {
  await teardownMongo();
});

afterAll(async () => {
  if (reader) {
    await reader.close().catch(() => {});
  }
  await teardownMongo();
  await closeAll();
});

beforeEach(async () => {
  // Close previous reader if open
  if (reader) {
    await reader.close().catch(() => {});
  }
  reader = new MongoReader(MONGO_READER_CONFIG, logger);
  await reader.connect();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("index-management", () => {
  it("hasRequiredIndex returns false when index missing", async () => {
    await seedSmallCollection(false);

    const has = await reader.hasRequiredIndex(TEST_COLL);
    expect(has).toBe(false);
  });

  it("hasRequiredIndex returns true when index exists", async () => {
    await seedSmallCollection(true);

    const has = await reader.hasRequiredIndex(TEST_COLL);
    expect(has).toBe(true);
  });

  it("startIndexCreation creates compound index", async () => {
    await seedSmallCollection(false);

    // Verify index does not exist yet
    const before = await reader.hasRequiredIndex(TEST_COLL);
    expect(before).toBe(false);

    // Create the index
    await reader.startIndexCreation(TEST_COLL);

    // Verify it now exists
    const after = await reader.hasRequiredIndex(TEST_COLL);
    expect(after).toBe(true);
  });

  it("readPage uses the compound index hint", async () => {
    await seedSmallCollection(true);

    await reader.switchCollection(TEST_COLL);

    // Get the upper bound to use as the scan limit
    const upperBound = await reader.getUpperBound();
    expect(upperBound).not.toBeNull();

    // readPage should not throw — it uses .hint({ cd: 1, _id: 1 }) internally,
    // and if the index does not exist, MongoDB returns an error.
    const page = await reader.readPage(null, upperBound!);

    expect(page.docs.length).toBeGreaterThan(0);
    expect(page.docs.length).toBeLessThanOrEqual(20);
    expect(page.fetchMs).toBeGreaterThanOrEqual(0);
  });
});
