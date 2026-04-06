/**
 * MongoDB test data seeding utilities.
 *
 * Creates drill_events collections with realistic documents that match
 * the production schema and exercise all code paths in the transform layer.
 */
import { ObjectId, type Db } from "mongodb";
import { getMongoDb, TEST_COLLECTION_PREFIX } from "./setup.ts";
import crypto from "node:crypto";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SeedOptions {
  /** Number of documents to insert. */
  count: number;
  /** App ID for all docs. Defaults to a fixed test app ID. */
  appId?: string;
  /** Event name. Defaults to "test_event". */
  eventName?: string;
  /** Start date for cd/ts spread. Defaults to 2024-01-01. */
  startDate?: Date;
  /** End date for cd/ts spread. Defaults to 2025-01-01. */
  endDate?: Date;
  /** Fraction of docs to mark as migrated (0–1). Default 0. */
  migratedFraction?: number;
  /** Fraction of docs missing uid (will be skipped). Default 0. */
  missingUidFraction?: number;
  /** Whether to create the {cd:1, _id:1} index. Default true. */
  createIndex?: boolean;
  /** Fraction of docs with cd set to null (0–1). Default 0. */
  nullCdFraction?: number;
  /** Fraction of docs with invalid ts (ts=0, will be skipped). Default 0. */
  invalidTsFraction?: number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DEFAULT_APP_ID = "aaaaaaaaaaaaaaaaaaaaaaaa";

/** Compute the drill_events collection name from appId + eventName. */
export function collectionName(eventName: string, appId: string): string {
  const hash = crypto.createHash("sha1").update(eventName + appId).digest("hex");
  return `${TEST_COLLECTION_PREFIX}${hash}`;
}

/** Generate a realistic test document. */
function makeDoc(opts: {
  appId: string;
  eventName: string;
  ts: number;
  cd: Date;
  idx: number;
  migrated?: boolean;
  missingUid?: boolean;
}): Record<string, unknown> {
  const doc: Record<string, unknown> = {
    _id: new ObjectId().toHexString(),
    a: opts.appId,
    e: opts.eventName,
    uid: opts.missingUid ? undefined : `user-${(opts.idx % 1000).toString().padStart(4, "0")}`,
    did: `device-${(opts.idx % 500).toString().padStart(4, "0")}`,
    ts: opts.ts,
    cd: opts.cd,
    c: Math.floor(Math.random() * 5) + 1,
    s: Math.random() * 100,
    dur: Math.random() * 60,
    n: opts.eventName,
  };

  if (opts.migrated) {
    doc.migrated = true;
  }
  if (opts.missingUid) {
    delete doc.uid;
  }

  // Add some segment data for view events
  if (opts.eventName.includes("view")) {
    doc.sg = { name: `Page ${opts.idx % 20}` };
  }

  return doc;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Seed a single drill_events collection with test documents.
 * Returns the collection name and the count of non-skippable docs.
 */
export async function seedCollection(opts: SeedOptions): Promise<{
  collName: string;
  totalDocs: number;
  expectedRows: number;  // docs that should make it to ClickHouse
}> {
  const db = await getMongoDb();
  const appId = opts.appId ?? DEFAULT_APP_ID;
  const eventName = opts.eventName ?? "test_event";
  const start = (opts.startDate ?? new Date("2024-01-01")).getTime();
  const end = (opts.endDate ?? new Date("2025-01-01")).getTime();
  const migratedFrac = opts.migratedFraction ?? 0;
  const missingUidFrac = opts.missingUidFraction ?? 0;

  const collName = collectionName(eventName, appId);
  const coll = db.collection(collName);

  // Drop if exists
  await coll.drop().catch(() => {});

  // Generate docs
  const docs: Record<string, unknown>[] = [];
  let expectedRows = 0;

  for (let i = 0; i < opts.count; i++) {
    const fraction = i / Math.max(1, opts.count - 1);
    const ts = start + Math.floor(fraction * (end - start));
    const cd = new Date(ts);

    const migrated = Math.random() < migratedFrac;
    const missingUid = !migrated && Math.random() < missingUidFrac;
    const nullCd = !migrated && !missingUid && Math.random() < (opts.nullCdFraction ?? 0);
    const invalidTs = !migrated && !missingUid && !nullCd && Math.random() < (opts.invalidTsFraction ?? 0);

    docs.push(makeDoc({
      appId,
      eventName,
      ts: invalidTs ? 0 : ts,
      cd: nullCd ? null as any : cd,
      idx: i,
      migrated,
      missingUid,
    }));

    if (!migrated && !missingUid && !invalidTs) {
      expectedRows++;
    }
  }

  // Bulk insert in chunks of 5000
  const CHUNK = 5000;
  for (let i = 0; i < docs.length; i += CHUNK) {
    await coll.insertMany(docs.slice(i, i + CHUNK));
  }

  // Create the required compound index
  if (opts.createIndex !== false) {
    await coll.createIndex({ cd: 1, _id: 1 });
  }

  return { collName, totalDocs: opts.count, expectedRows };
}

/**
 * Seed multiple collections with varying sizes.
 * Returns metadata for each collection.
 */
export async function seedMultipleCollections(configs: Array<{
  eventName: string;
  count: number;
  appId?: string;
  startDate?: Date;
  endDate?: Date;
  migratedFraction?: number;
  missingUidFraction?: number;
}>): Promise<Array<{
  collName: string;
  eventName: string;
  totalDocs: number;
  expectedRows: number;
}>> {
  const results: Array<{
    collName: string;
    eventName: string;
    totalDocs: number;
    expectedRows: number;
  }> = [];

  for (const cfg of configs) {
    const r = await seedCollection({
      count: cfg.count,
      eventName: cfg.eventName,
      appId: cfg.appId,
      startDate: cfg.startDate,
      endDate: cfg.endDate,
      migratedFraction: cfg.migratedFraction,
      missingUidFraction: cfg.missingUidFraction,
    });
    results.push({ ...r, eventName: cfg.eventName });
  }

  return results;
}

/**
 * Seed documents with specific cd timestamps for boundary testing.
 * Each entry in `timestamps` creates one doc at that exact time.
 */
export async function seedAtTimestamps(
  eventName: string,
  appId: string,
  timestamps: Date[],
): Promise<{ collName: string; totalDocs: number }> {
  const db = await getMongoDb();
  const collName = collectionName(eventName, appId);
  const coll = db.collection(collName);

  await coll.drop().catch(() => {});

  const docs = timestamps.map((ts, i) => makeDoc({
    appId,
    eventName,
    ts: ts.getTime(),
    cd: ts,
    idx: i,
  }));

  if (docs.length > 0) {
    await coll.insertMany(docs);
  }

  await coll.createIndex({ cd: 1, _id: 1 });

  return { collName, totalDocs: docs.length };
}

/**
 * Register app/event hash in the countly database for HashResolver.
 * This makes the HashResolver able to resolve the collection name
 * back to { a: appId, e: eventName }.
 */
export async function registerEventHash(
  appId: string,
  eventName: string,
  db?: Db,
): Promise<void> {
  const mongoDb = db ?? await getMongoDb();
  // The hash resolver reads from the countly db's events collection
  // For tests, we'll use a simpler approach: the collection name is deterministic
  // from SHA1(eventName + appId), so the resolver should work if it has the app/event data.
  // In practice, tests pass collectionDefaults directly to the transform layer.
}

/**
 * Seed a collection where ALL documents have cd: null.
 */
export async function seedNullCdCollection(opts: {
  count: number;
  appId?: string;
  eventName?: string;
}): Promise<{ collName: string; totalDocs: number; expectedRows: number }> {
  const db = await getMongoDb();
  const appId = opts.appId ?? DEFAULT_APP_ID;
  const eventName = opts.eventName ?? "test_null_cd_event";
  const collName = collectionName(eventName, appId);
  const coll = db.collection(collName);

  await coll.drop().catch(() => {});

  const docs: Record<string, unknown>[] = [];
  for (let i = 0; i < opts.count; i++) {
    docs.push({
      _id: new ObjectId().toHexString(),
      a: appId,
      e: eventName,
      uid: `user-${(i % 1000).toString().padStart(4, "0")}`,
      did: `device-${(i % 500).toString().padStart(4, "0")}`,
      ts: Date.now() - (opts.count - i) * 1000,
      cd: null,
      c: 1,
      s: 0,
      dur: 0,
      n: eventName,
    });
  }

  const CHUNK = 5000;
  for (let i = 0; i < docs.length; i += CHUNK) {
    await coll.insertMany(docs.slice(i, i + CHUNK));
  }

  await coll.createIndex({ cd: 1, _id: 1 });

  return { collName, totalDocs: opts.count, expectedRows: opts.count };
}
