/**
 * Integration tests for cdToEpoch() and DateTime64(3) handling.
 *
 * Validates that the cursor utility correctly normalises MongoDB cd field
 * values into epoch milliseconds, and that millisecond precision is preserved
 * end-to-end through the ClickHouse DateTime64(3) column.
 */
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { cdToEpoch } from "../../src/types/cursor.ts";
import { formatTimestamp } from "../../src/transform/validators.ts";
import {
  getClickHouseClient,
  setupClickHouse,
  teardownClickHouse,
  closeAll,
  TEST_CH_DB,
  TEST_CH_TABLE,
} from "../helpers/setup.ts";

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

beforeAll(async () => {
  await setupClickHouse();
});

afterAll(async () => {
  await teardownClickHouse();
  await closeAll();
});

// ---------------------------------------------------------------------------
// cdToEpoch unit-level tests
// ---------------------------------------------------------------------------

describe("cdToEpoch", () => {
  it("converts Date objects to epoch milliseconds", () => {
    const d = new Date("2024-03-27T10:00:00.123Z");
    expect(cdToEpoch(d)).toBe(d.getTime());
  });

  it("returns the same value for epoch milliseconds (number >= 9.5e11)", () => {
    const ms = 1711525200123; // 2024-03-27T09:00:00.123Z
    expect(cdToEpoch(ms)).toBe(ms);
  });

  it("returns the same value for a large epoch millis without fractional part", () => {
    const ms = 1700000000000;
    expect(cdToEpoch(ms)).toBe(1700000000000);
  });

  it("multiplies epoch seconds (number >= 9.5e8 and < 9.5e11) by 1000", () => {
    const sec = 1711525200; // 2024-03-27T09:00:00Z
    expect(cdToEpoch(sec)).toBe(1711525200000);
  });

  it("multiplies a borderline epoch-seconds value (just above 9.5e8) by 1000", () => {
    const sec = 950000001;
    expect(cdToEpoch(sec)).toBe(950000001000);
  });

  it("parses string numbers and applies the same numeric rules", () => {
    // String that looks like epoch milliseconds
    expect(cdToEpoch("1711525200123")).toBe(1711525200123);

    // String that looks like epoch seconds
    expect(cdToEpoch("1711525200")).toBe(1711525200000);
  });

  it("returns 0 for null", () => {
    expect(cdToEpoch(null)).toBe(0);
  });

  it("returns 0 for undefined", () => {
    expect(cdToEpoch(undefined)).toBe(0);
  });

  it("returns 0 for NaN", () => {
    expect(cdToEpoch(NaN)).toBe(0);
  });

  it("returns 0 for Infinity", () => {
    expect(cdToEpoch(Infinity)).toBe(0);
  });

  it("returns 0 for -Infinity", () => {
    expect(cdToEpoch(-Infinity)).toBe(0);
  });

  it("returns 0 for non-numeric strings", () => {
    expect(cdToEpoch("not-a-number")).toBe(0);
    expect(cdToEpoch("")).toBe(0);
  });

  it("returns 0 for numbers below the epoch-seconds threshold (< 9.5e8)", () => {
    expect(cdToEpoch(100)).toBe(0);
    expect(cdToEpoch(0)).toBe(0);
    expect(cdToEpoch(-1)).toBe(0);
  });

  it("returns 0 for an invalid Date object", () => {
    expect(cdToEpoch(new Date("invalid"))).toBe(0);
  });

  it("floors fractional millisecond values", () => {
    expect(cdToEpoch(1711525200123.999)).toBe(1711525200123);
  });
});

// ---------------------------------------------------------------------------
// DateTime64(3) precision end-to-end
// ---------------------------------------------------------------------------

describe("DateTime64(3) precision", () => {
  const PRECISION_ID = "__dt64_precision_test__";
  const TS_WITH_MS = 1711525200123; // has .123 milliseconds

  beforeAll(async () => {
    const ch = await getClickHouseClient();

    // Clean up any previous test row
    await ch.command({
      query: `ALTER TABLE ${TEST_CH_DB}.${TEST_CH_TABLE} DELETE WHERE _id = '${PRECISION_ID}'`,
    });

    // Wait for the mutation to apply (lightweight table, should be fast)
    await new Promise((r) => setTimeout(r, 1000));

    // Insert a row with a known millisecond-precise timestamp
    const tsFormatted = formatTimestamp(TS_WITH_MS); // '2024-03-27 09:00:00.123'
    await ch.insert({
      table: `${TEST_CH_DB}.${TEST_CH_TABLE}`,
      values: [
        {
          _id: PRECISION_ID,
          a: "test_app",
          e: "[CLY]_custom",
          n: "precision_test",
          uid: "uid_precision",
          did: "did_precision",
          ts: tsFormatted,
          c: 1,
          s: 0,
          dur: 0,
        },
      ],
      format: "JSONEachRow",
    });
  });

  it("preserves .123 millisecond precision in ClickHouse ts column", async () => {
    const ch = await getClickHouseClient();
    const result = await ch.query({
      query: `
        SELECT
          toUnixTimestamp64Milli(ts) AS ts_ms,
          formatDateTime(ts, '%Y-%m-%d %H:%i:%S', 'UTC') AS ts_sec,
          toString(ts) AS ts_full
        FROM ${TEST_CH_DB}.${TEST_CH_TABLE}
        WHERE _id = '${PRECISION_ID}'
      `,
      format: "JSONEachRow",
    });

    const rows = await result.json<
      { ts_ms: string; ts_sec: string; ts_full: string }[]
    >();
    expect(rows).toHaveLength(1);

    const row = rows[0];
    // The epoch millis should match exactly
    expect(Number(row.ts_ms)).toBe(TS_WITH_MS);

    // The formatted value should include .123
    expect(row.ts_full).toContain(".123");
  });

  it("formatTimestamp produces the expected DateTime64(3) string", () => {
    const formatted = formatTimestamp(TS_WITH_MS);
    expect(formatted).toBe("2024-03-27 09:00:00.123");
  });

  it("formatTimestamp preserves .000 for exact-second timestamps", () => {
    const exactSecond = 1711525200000;
    expect(formatTimestamp(exactSecond)).toBe("2024-03-27 09:00:00.000");
  });
});
