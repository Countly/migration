/**
 * Integration tests for schema compliance.
 *
 * Verifies that output rows produced by the transform layer match the
 * ClickHouse table schema: required fields, timestamp format, event name
 * derivation, numeric types, null handling, and end-to-end insert.
 */
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { ObjectId } from "mongodb";

import {
  transformDocument,
  type SourceDocument,
} from "../../src/transform/normalize.ts";
import {
  setupClickHouse,
  teardownClickHouse,
  closeAll,
  getClickHouseClient,
  chQuery,
  TEST_CH_DB,
  TEST_CH_TABLE,
} from "../helpers/setup.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a minimal valid source document. */
function makeValidDoc(overrides: Partial<SourceDocument> = {}): SourceDocument {
  return {
    _id: new ObjectId().toHexString(),
    a: "test_app_id",
    e: "test_event",
    uid: "user-001",
    did: "device-001",
    ts: 1711525200123,  // 2024-03-27 09:00:00.123 UTC
    cd: new Date("2024-03-27T09:00:00.123Z"),
    c: 3,
    s: 1.5,
    dur: 12.75,
    n: "test_event",
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

beforeAll(async () => {
  await teardownClickHouse();
  await setupClickHouse();
});

afterAll(async () => {
  await teardownClickHouse();
  await closeAll();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("schema-compliance", () => {
  it("all required fields present in output", () => {
    const doc = makeValidDoc();
    const { row, skipReason } = transformDocument(doc);

    expect(skipReason).toBeNull();
    expect(row).not.toBeNull();

    // All required fields from the ClickHouse schema must be present
    expect(row!._id).toBeDefined();
    expect(row!.a).toBeDefined();
    expect(row!.e).toBeDefined();
    expect(row!.n).toBeDefined();
    expect(row!.uid).toBeDefined();
    expect(row!.ts).toBeDefined();
    expect(row!.c).toBeDefined();
    expect(row!.s).toBeDefined();
    expect(row!.dur).toBeDefined();
  });

  it("timestamp formatted as DateTime64(3)", () => {
    const doc = makeValidDoc({ ts: 1711525200123 });
    const { row } = transformDocument(doc);

    expect(row).not.toBeNull();

    // formatTimestamp produces 'yyyy-MM-dd HH:mm:ss.SSS' (UTC)
    expect(row!.ts).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/);
    expect(row!.ts).toContain(".123");

    // Verify the millisecond component is preserved
    expect(row!.ts).toMatch(/\.\d{3}$/);
  });

  it("custom event normalized to [CLY]_custom", () => {
    const doc = makeValidDoc({ e: "my_custom_event" });
    const { row } = transformDocument(doc);

    expect(row).not.toBeNull();

    // Custom events (not starting with [CLY]_) get e=[CLY]_custom, n=original name
    expect(row!.e).toBe("[CLY]_custom");
    expect(row!.n).toBe("my_custom_event");
  });

  it("view event derives name from sg.name", () => {
    const doc = makeValidDoc({
      e: "[CLY]_view",
      sg: { name: "Home" },
    });
    const { row } = transformDocument(doc);

    expect(row).not.toBeNull();
    expect(row!.e).toBe("[CLY]_view");
    expect(row!.n).toBe("Home");
  });

  it("numeric fields have correct types", () => {
    const doc = makeValidDoc({ c: 5, s: 3.14, dur: 99.9 });
    const { row } = transformDocument(doc);

    expect(row).not.toBeNull();

    // c: non-negative integer
    expect(typeof row!.c).toBe("number");
    expect(Number.isInteger(row!.c)).toBe(true);
    expect(row!.c).toBeGreaterThanOrEqual(0);

    // s: float
    expect(typeof row!.s).toBe("number");

    // dur: float
    expect(typeof row!.dur).toBe("number");
  });

  it("null/missing optional fields handled", () => {
    // Document with no uid_canon, lsid, cmp
    const doc = makeValidDoc();
    delete (doc as any).uid_canon;
    delete (doc as any).lsid;
    delete (doc as any).cmp;

    const { row, skipReason } = transformDocument(doc);

    // Transform should succeed without errors
    expect(skipReason).toBeNull();
    expect(row).not.toBeNull();

    // Optional fields should be undefined or null, not cause errors
    // (the transform only copies fields that exist in the source document)
    const rowAny = row as Record<string, unknown>;
    expect(rowAny.uid_canon === undefined || rowAny.uid_canon === null).toBe(true);
    expect(rowAny.lsid === undefined || rowAny.lsid === null).toBe(true);
    expect(rowAny.cmp === undefined || rowAny.cmp === null).toBe(true);
  });

  it("end-to-end: transformed row inserts into ClickHouse without errors", async () => {
    // Transform a doc
    const docId = new ObjectId().toHexString();
    const doc = makeValidDoc({
      _id: docId,
      e: "e2e_schema_test",
      ts: 1711525200456,
    });
    const { row } = transformDocument(doc);
    expect(row).not.toBeNull();

    // Insert into ClickHouse directly
    const ch = await getClickHouseClient();
    await ch.insert({
      table: TEST_CH_TABLE,
      values: [row!],
      format: "JSONEachRow",
      clickhouse_settings: {
        date_time_input_format: "best_effort",
      },
    });

    // Wait for async insert to flush
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Query back and verify the data
    const rows = await chQuery<{
      _id: string;
      a: string;
      e: string;
      n: string;
      uid: string;
      c: string;
      s: string;
      dur: string;
    }>(`SELECT _id, a, e, n, uid, c, s, dur FROM ${TEST_CH_TABLE} WHERE _id = '${docId}'`);

    expect(rows.length).toBe(1);
    expect(rows[0]._id).toBe(docId);
    expect(rows[0].a).toBe(row!.a);
    expect(rows[0].e).toBe("[CLY]_custom");
    expect(rows[0].n).toBe("e2e_schema_test");
    expect(rows[0].uid).toBe(row!.uid);
    expect(Number(rows[0].c)).toBe(row!.c);
    expect(Number(rows[0].s)).toBeCloseTo(row!.s, 2);
    expect(Number(rows[0].dur)).toBeCloseTo(row!.dur, 2);
  });
});
