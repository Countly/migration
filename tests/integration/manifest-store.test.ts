/**
 * Integration tests for ManifestStore range-scoped query methods.
 *
 * Verifies that getLastBatch, getLastDoneBatch, getBatches, and
 * sumCompletedBatchStats correctly filter by optional BatchSeqRange,
 * and that empty ranges return null / zero as expected.
 */
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { getMongoDb, closeAll, TEST_MONGO_DB } from "../helpers/setup.ts";
import { ManifestStore } from "../../src/state/manifest-store.ts";
import type { Batch, BatchStatus } from "../../src/state/manifest-store.ts";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TEST_MONGO_URI = "mongodb://localhost:27017/?directConnection=true";
const RUN_ID = "manifest-store-range-test-run";
const SOURCE_NS = "countly_drill.drill_events_abc";
const TARGET_TABLE = "drill_events";
const TRANSFORM_VERSION = "v1-test";

// Range boundaries (matching the multi-pod range convention)
const RANGE_0 = { min: 0, max: 10_000 };      // batch_seq 0 .. 9999
const RANGE_1 = { min: 10_000, max: 20_000 };  // batch_seq 10000 .. 19999
const RANGE_2 = { min: 20_000, max: 30_000 };  // batch_seq 20000 .. 29999
const EMPTY_RANGE = { min: 90_000, max: 100_000 }; // no batches here

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeBatch(
  batchSeq: number,
  status: BatchStatus,
  docsRead: number,
  rowsInserted: number,
): Omit<Batch, "error_history" | "digest_match"> {
  return {
    run_id: RUN_ID,
    batch_seq: batchSeq,
    lower_exclusive_cursor: `{"cd":${batchSeq * 100},"id":"lower_${batchSeq}"}`,
    upper_inclusive_cursor: `{"cd":${(batchSeq + 1) * 100},"id":"upper_${batchSeq}"}`,
    source_docs_read: docsRead,
    docs_skipped: 0,
    rows_to_insert: rowsInserted,
    payload_digest: `digest_${batchSeq}`,
    insert_dedup_token: `dedup_${batchSeq}`,
    query_id: `qid_${batchSeq}`,
    status,
    retry_count: 0,
    last_error: null,
    started_at: new Date().toISOString(),
    finished_at: status === "done" ? new Date().toISOString() : null,
  };
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

let store: ManifestStore;

beforeAll(async () => {
  store = new ManifestStore(TEST_MONGO_URI, TEST_MONGO_DB);
  await store.connect();

  // Clean up any previous test data for this run
  await store.deleteRunData(RUN_ID);

  // Create the run record
  await store.createRun({
    run_id: RUN_ID,
    status: "active",
    source_ns: SOURCE_NS,
    target_table: TARGET_TABLE,
    upper_bound_cursor: '{"cd":9999999999999,"id":"zzz"}',
    transform_version: TRANSFORM_VERSION,
    created_at: new Date().toISOString(),
  });

  // ── Seed batches across 3 ranges ────────────────────────────────────
  // Range 0: batch_seq 0..5  — all done, 100 docs / 90 rows each
  for (let i = 0; i <= 5; i++) {
    await store.insertBatch(makeBatch(i, "done", 100, 90));
  }

  // Range 1: batch_seq 10000..10005 — 4 done + 1 failed + 1 inflight
  for (let i = 0; i <= 3; i++) {
    await store.insertBatch(makeBatch(10_000 + i, "done", 200, 180));
  }
  await store.insertBatch(makeBatch(10_004, "failed", 200, 0));
  await store.insertBatch(makeBatch(10_005, "inflight", 200, 0));

  // Range 2: batch_seq 20000..20005 — all done, 50 docs / 45 rows each
  for (let i = 0; i <= 5; i++) {
    await store.insertBatch(makeBatch(20_000 + i, "done", 50, 45));
  }
});

afterAll(async () => {
  if (store) {
    await store.deleteRunData(RUN_ID);
    await store.close();
  }
  await closeAll();
});

// ---------------------------------------------------------------------------
// getLastBatch
// ---------------------------------------------------------------------------

describe("getLastBatch", () => {
  it("returns the global last batch (highest batch_seq) when no range is provided", async () => {
    const last = await store.getLastBatch(RUN_ID);
    expect(last).not.toBeNull();
    // batch_seq 20005 is the highest across all ranges
    expect(last!.batch_seq).toBe(20_005);
  });

  it("returns only range-0 last batch when scoped to {min:0, max:10000}", async () => {
    const last = await store.getLastBatch(RUN_ID, RANGE_0);
    expect(last).not.toBeNull();
    expect(last!.batch_seq).toBe(5);
    expect(last!.run_id).toBe(RUN_ID);
  });

  it("returns only range-1 last batch when scoped to {min:10000, max:20000}", async () => {
    const last = await store.getLastBatch(RUN_ID, RANGE_1);
    expect(last).not.toBeNull();
    // 10005 is the highest in range 1 (inflight)
    expect(last!.batch_seq).toBe(10_005);
  });

  it("returns null for an empty range", async () => {
    const last = await store.getLastBatch(RUN_ID, EMPTY_RANGE);
    expect(last).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// getLastDoneBatch
// ---------------------------------------------------------------------------

describe("getLastDoneBatch", () => {
  it("returns the global last done batch when no range is provided", async () => {
    const last = await store.getLastDoneBatch(RUN_ID);
    expect(last).not.toBeNull();
    expect(last!.status).toBe("done");
    // batch_seq 20005 is the highest done batch across all ranges
    expect(last!.batch_seq).toBe(20_005);
  });

  it("returns only range-1 last done batch when scoped to {min:10000, max:20000}", async () => {
    const last = await store.getLastDoneBatch(RUN_ID, RANGE_1);
    expect(last).not.toBeNull();
    expect(last!.status).toBe("done");
    // In range 1, done batches are 10000..10003; 10004 is failed, 10005 is inflight
    expect(last!.batch_seq).toBe(10_003);
  });

  it("returns range-0 last done batch correctly", async () => {
    const last = await store.getLastDoneBatch(RUN_ID, RANGE_0);
    expect(last).not.toBeNull();
    expect(last!.batch_seq).toBe(5);
    expect(last!.status).toBe("done");
  });

  it("returns null for an empty range", async () => {
    const last = await store.getLastDoneBatch(RUN_ID, EMPTY_RANGE);
    expect(last).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// getBatches
// ---------------------------------------------------------------------------

describe("getBatches", () => {
  it("returns only done batches in range 0 when filtered by status and batchSeqRange", async () => {
    const batches = await store.getBatches(RUN_ID, {
      status: "done",
      batchSeqRange: RANGE_0,
    });
    expect(batches).toHaveLength(6); // batch_seq 0..5, all done
    for (const b of batches) {
      expect(b.status).toBe("done");
      expect(b.batch_seq).toBeGreaterThanOrEqual(RANGE_0.min);
      expect(b.batch_seq).toBeLessThan(RANGE_0.max);
    }
    // Verify sorted ascending by batch_seq
    for (let i = 1; i < batches.length; i++) {
      expect(batches[i].batch_seq).toBeGreaterThan(batches[i - 1].batch_seq);
    }
  });

  it("returns only done batches in range 1 (excluding failed and inflight)", async () => {
    const batches = await store.getBatches(RUN_ID, {
      status: "done",
      batchSeqRange: RANGE_1,
    });
    expect(batches).toHaveLength(4); // 10000..10003 are done
    for (const b of batches) {
      expect(b.status).toBe("done");
      expect(b.batch_seq).toBeGreaterThanOrEqual(RANGE_1.min);
      expect(b.batch_seq).toBeLessThan(RANGE_1.max);
    }
  });

  it("returns all batches (any status) in range 1 when no status filter", async () => {
    const batches = await store.getBatches(RUN_ID, {
      batchSeqRange: RANGE_1,
    });
    // 10000..10005 = 6 batches total (4 done + 1 failed + 1 inflight)
    expect(batches).toHaveLength(6);
  });

  it("returns an empty array for an empty range", async () => {
    const batches = await store.getBatches(RUN_ID, {
      status: "done",
      batchSeqRange: EMPTY_RANGE,
    });
    expect(batches).toHaveLength(0);
  });

  it("respects the limit parameter", async () => {
    const batches = await store.getBatches(RUN_ID, {
      status: "done",
      batchSeqRange: RANGE_0,
      limit: 3,
    });
    expect(batches).toHaveLength(3);
    // Should return the first 3 (sorted ascending)
    expect(batches[0].batch_seq).toBe(0);
    expect(batches[1].batch_seq).toBe(1);
    expect(batches[2].batch_seq).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// sumCompletedBatchStats
// ---------------------------------------------------------------------------

describe("sumCompletedBatchStats", () => {
  it("sums all done batches across all ranges when no range is provided", async () => {
    const stats = await store.sumCompletedBatchStats(RUN_ID);
    // Range 0: 6 done * 100 docs = 600 docs, 6 * 90 = 540 rows
    // Range 1: 4 done * 200 docs = 800 docs, 4 * 180 = 720 rows
    // Range 2: 6 done *  50 docs = 300 docs, 6 * 45  = 270 rows
    // Total: 1700 docs, 1530 rows
    expect(stats.docsRead).toBe(1700);
    expect(stats.rowsInserted).toBe(1530);
  });

  it("sums only range-0 done batches when scoped to {min:0, max:10000}", async () => {
    const stats = await store.sumCompletedBatchStats(RUN_ID, RANGE_0);
    // 6 done * 100 = 600 docs, 6 * 90 = 540 rows
    expect(stats.docsRead).toBe(600);
    expect(stats.rowsInserted).toBe(540);
  });

  it("sums only range-1 done batches (excluding failed/inflight)", async () => {
    const stats = await store.sumCompletedBatchStats(RUN_ID, RANGE_1);
    // 4 done * 200 = 800 docs, 4 * 180 = 720 rows
    expect(stats.docsRead).toBe(800);
    expect(stats.rowsInserted).toBe(720);
  });

  it("sums only range-2 done batches when scoped", async () => {
    const stats = await store.sumCompletedBatchStats(RUN_ID, RANGE_2);
    // 6 done * 50 = 300 docs, 6 * 45 = 270 rows
    expect(stats.docsRead).toBe(300);
    expect(stats.rowsInserted).toBe(270);
  });

  it("returns zero for an empty range", async () => {
    const stats = await store.sumCompletedBatchStats(RUN_ID, EMPTY_RANGE);
    expect(stats.docsRead).toBe(0);
    expect(stats.rowsInserted).toBe(0);
  });

  it("returns zero for a non-existent run", async () => {
    const stats = await store.sumCompletedBatchStats("nonexistent-run-id");
    expect(stats.docsRead).toBe(0);
    expect(stats.rowsInserted).toBe(0);
  });
});
