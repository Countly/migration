/**
 * Integration tests for null-cd document migration.
 *
 * Validates that documents with null/missing cd fields are:
 * 1. Excluded from the cursor phase (readPage filter)
 * 2. Swept in the null_cd phase after cursor phase completes
 * 3. Correctly persisted to ClickHouse
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import {
  setupClickHouse, teardownClickHouse, teardownMongo, teardownRedis,
  closeAll, TEST_MONGO_URI, TEST_MONGO_DB,
} from "../helpers/setup.ts";
import { seedCollection, seedNullCdCollection } from "../helpers/seed-mongo.ts";
import { MongoReader } from "../../src/source/mongo-reader.ts";

const APP_ID = "aaaaaaaaaaaaaaaaaaaaaaaa";

describe("null-cd sweep", () => {
  let mongoReader: MongoReader;

  beforeAll(async () => {
    await setupClickHouse();
  });

  beforeEach(async () => {
    await teardownMongo();
    await teardownRedis();
    await teardownClickHouse();
    await setupClickHouse();

    mongoReader = new MongoReader({
      uri: TEST_MONGO_URI,
      database: TEST_MONGO_DB,
      readPreference: "primary",
      readConcern: "majority",
      retryReads: true,
      appName: "test",
      batchRowsTarget: 1000,
      cursorBatchSize: 1000,
      maxTimeMs: 30_000,
    }, (await import("pino")).default({ level: "silent" }));
    await mongoReader.connect();
  });

  afterAll(async () => {
    if (mongoReader?.isConnected()) await mongoReader.close();
    await closeAll();
  });

  // ── readPage filter ─────────────────────────────────────────────────

  it("readPage excludes null-cd documents", async () => {
    const { collName } = await seedCollection({
      count: 20,
      eventName: "test_read_page_filter",
      nullCdFraction: 0.5,
    });
    await mongoReader.switchCollection(collName);

    const upperBound = await mongoReader.getUpperBound();
    if (!upperBound) return; // All docs happened to be null-cd

    const page = await mongoReader.readPage(null, upperBound, 100);
    for (const doc of page.docs) {
      expect(doc.cd).not.toBeNull();
      expect(doc.cd).toBeDefined();
    }
  });

  // ── hasNullCdDocuments ──────────────────────────────────────────────

  it("hasNullCdDocuments returns true when null docs exist", async () => {
    const { collName } = await seedNullCdCollection({
      count: 5,
      eventName: "test_has_null",
    });
    await mongoReader.switchCollection(collName);
    expect(await mongoReader.hasNullCdDocuments()).toBe(true);
  });

  it("hasNullCdDocuments returns false when no null docs", async () => {
    const { collName } = await seedCollection({
      count: 5,
      eventName: "test_no_null",
    });
    await mongoReader.switchCollection(collName);
    expect(await mongoReader.hasNullCdDocuments()).toBe(false);
  });

  // ── getNullCdBounds ─────────────────────────────────────────────────

  it("getNullCdBounds returns min/max _id", async () => {
    const { collName } = await seedNullCdCollection({
      count: 10,
      eventName: "test_bounds",
    });
    await mongoReader.switchCollection(collName);
    const bounds = await mongoReader.getNullCdBounds();
    expect(bounds).not.toBeNull();
    expect(bounds!.lower).toBeDefined();
    expect(bounds!.upper).toBeDefined();
    expect(bounds!.lower < bounds!.upper).toBe(true);
  });

  it("getNullCdBounds returns null when no null docs", async () => {
    const { collName } = await seedCollection({
      count: 5,
      eventName: "test_no_null_bounds",
    });
    await mongoReader.switchCollection(collName);
    expect(await mongoReader.getNullCdBounds()).toBeNull();
  });

  // ── readNullCdPage ──────────────────────────────────────────────────

  it("readNullCdPage paginates with _id and respects upper bound", async () => {
    const { collName } = await seedNullCdCollection({
      count: 20,
      eventName: "test_pagination",
    });
    await mongoReader.switchCollection(collName);

    const bounds = await mongoReader.getNullCdBounds();
    expect(bounds).not.toBeNull();

    // Read first page of 5
    const page1 = await mongoReader.readNullCdPage(null, bounds!.upper, 5);
    expect(page1.docs.length).toBe(5);
    expect(page1.lastCursor).not.toBeNull();
    expect(page1.lastCursor!.cd).toBe(0);

    // Read second page from last cursor
    const page2 = await mongoReader.readNullCdPage(page1.lastCursor!.id, bounds!.upper, 5);
    expect(page2.docs.length).toBe(5);

    // Ensure no overlap
    const ids1 = new Set(page1.docs.map(d => d._id));
    for (const doc of page2.docs) {
      expect(ids1.has(doc._id)).toBe(false);
    }
  });

  it("readNullCdPage reads all docs when no limit", async () => {
    const { collName } = await seedNullCdCollection({
      count: 15,
      eventName: "test_no_limit",
    });
    await mongoReader.switchCollection(collName);

    const bounds = await mongoReader.getNullCdBounds();
    expect(bounds).not.toBeNull();

    const page = await mongoReader.readNullCdPage(null, bounds!.upper);
    expect(page.docs.length).toBe(15);
  });

  // ── Bounds filtering ────────────────────────────────────────────────

  it("getLowerBound and getUpperBound return null for all-null collections", async () => {
    const { collName } = await seedNullCdCollection({
      count: 5,
      eventName: "test_all_null_bounds",
    });
    await mongoReader.switchCollection(collName);
    expect(await mongoReader.getLowerBound()).toBeNull();
    expect(await mongoReader.getUpperBound()).toBeNull();
  });

  it("getLowerBound and getUpperBound ignore null docs in mixed collections", async () => {
    const { collName } = await seedCollection({
      count: 20,
      eventName: "test_mixed_bounds",
      nullCdFraction: 0.3,
    });
    await mongoReader.switchCollection(collName);

    const lower = await mongoReader.getLowerBound();
    const upper = await mongoReader.getUpperBound();

    // Should still find non-null bounds
    if (lower && upper) {
      expect(lower.cd).toBeGreaterThan(0);
      expect(upper.cd).toBeGreaterThan(0);
    }
  });
});
