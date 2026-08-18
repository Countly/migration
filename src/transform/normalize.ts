/**
 * TypeScript port of ClyEventNormalize.java.
 *
 * Transforms raw MongoDB event documents into ClickHouse-ready rows,
 * applying field validation, event-name derivation, and timestamp
 * normalization.
 *
 * This module implements the drill-event normalization spec. The spec was
 * defined together with a matching rewrite of countly-platform's
 * api/utils/eventTransformer.ts (branch claude/jovial-shannon-b3dd29) and the
 * goldens in tests/differential/ were generated from that code — but this
 * tool does NOT depend on that branch merging. It writes ClickHouse directly;
 * platform code never runs on the migration path. The differential harness
 * pins THIS repo's behavior against the frozen goldens.
 *
 * What consistency actually requires (and why it holds against platform
 * main unmerged): row semantics that span history + live queries — custom
 * events as e='[CLY]_custom' with the name in n (confirmed live behavior),
 * uid_canon left to the identity machinery (both sides), cd = historical
 * time for migrated rows vs receive-time for live rows. Everything else in
 * this spec (NaN/Decimal128/Long stringification, ts heuristics, clamps,
 * skip rules) concerns BSON-only shapes that live SDK ingestion can never
 * receive through JSON — divergence there is unobservable.
 *
 * If the platform PR merges with behavior changes, regenerate goldens there
 * and re-sync tests/differential/.
 */

import { SkipReason, SkipCounter } from './skip-reasons.ts';
import {
  isBlank,
  asString,
  toEpochMillis,
  toDouble,
  clampUInt32,
  clampDateTime64,
  isPlainObject,
  sanitizeJsonValue,
  formatTimestamp,
  firstNonBlank,
} from './validators.ts';
import type { CollectionDefaults } from './hash-resolver.ts';
import type { CoercionCounter } from './coercions.ts';

// ────────────────────────────────────────────────────────────────────────────
// Constants
// ────────────────────────────────────────────────────────────────────────────

const CLY_PREFIX = '[CLY]_';

// ────────────────────────────────────────────────────────────────────────────
// Public types
// ────────────────────────────────────────────────────────────────────────────

/**
 * Shape of a raw MongoDB document after projection.
 * All fields are optional because any field may be absent in a given document.
 */
export interface SourceDocument {
  _id?: string;
  a?: string;
  e?: string;
  n?: string;
  uid?: string;
  uid_canon?: string;
  did?: string;
  lsid?: string;
  ts?: unknown;
  up?: unknown;
  custom?: unknown;
  cmp?: unknown;
  sg?: unknown;
  c?: unknown;
  s?: unknown;
  dur?: unknown;
  lu?: unknown;
  cd?: unknown;
  migrated?: boolean;
}

/** Shape of a fully transformed row ready for ClickHouse insertion. */
export interface OutputRow {
  _id: string;
  a: string;
  e: string;
  n: string;
  uid: string;
  uid_canon?: string;
  did: string;
  lsid?: string;
  ts: string;
  up?: Record<string, unknown>;
  custom?: Record<string, unknown>;
  cmp?: Record<string, unknown>;
  sg?: Record<string, unknown>;
  c: number;
  s: number;
  dur: number;
  lu?: string;
  cd: string;
}

export interface TransformResult {
  /** The transformed row, or null if the document was skipped. */
  row: OutputRow | null;
  /** The reason the document was skipped, or null if it was transformed. */
  skipReason: SkipReason | null;
}

// ────────────────────────────────────────────────────────────────────────────
// Single-document transform
// ────────────────────────────────────────────────────────────────────────────

/**
 * Transforms a single raw MongoDB document into a ClickHouse-ready row.
 *
 * Returns `{ row, skipReason }` where exactly one of the two is non-null.
 */
export function transformDocument(
  doc: SourceDocument,
  defaults?: CollectionDefaults,
  coercions?: CoercionCounter,
): TransformResult {
  try {
    return doTransform(doc, defaults, coercions);
  } catch {
    return { row: null, skipReason: SkipReason.TRANSFORM_ERROR };
  }
}

// ────────────────────────────────────────────────────────────────────────────
// Batch transform
// ────────────────────────────────────────────────────────────────────────────

/**
 * Transforms a batch of documents, counting skip reasons and collecting
 * a limited set of skip samples.
 *
 * @param docs         Array of raw MongoDB documents.
 * @param skipCounter  Counter to increment for each skipped document.
 * @returns            Transformed rows and up to 10 skip samples.
 */
export function transformBatch(
  docs: SourceDocument[],
  skipCounter: SkipCounter,
  defaults?: CollectionDefaults,
  coercions?: CoercionCounter,
): { rows: OutputRow[]; skippedSamples: Array<{ _id: string; reason: SkipReason }> } {
  const rows: OutputRow[] = [];
  const skippedSamples: Array<{ _id: string; reason: SkipReason }> = [];
  const MAX_SKIP_SAMPLES = 10;

  for (const doc of docs) {
    const { row, skipReason } = transformDocument(doc, defaults, coercions);

    if (row !== null) {
      rows.push(row);
    } else if (skipReason !== null) {
      skipCounter.increment(skipReason);
      if (skippedSamples.length < MAX_SKIP_SAMPLES) {
        const id = asString(doc['_id']) ?? '<unknown>';
        skippedSamples.push({ _id: id, reason: skipReason });
      }
    }
  }

  return { rows, skippedSamples };
}

// ────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ────────────────────────────────────────────────────────────────────────────

function doTransform(
  doc: SourceDocument,
  defaults?: CollectionDefaults,
  coercions?: CoercionCounter,
): TransformResult {
  // ── Skip if already migrated ──────────────────────────────────────────
  if (doc.migrated === true) {
    return { row: null, skipReason: SkipReason.ALREADY_MARKED_MIGRATED };
  }

  // ── Required field checks ─────────────────────────────────────────────
  // Fall back to collection-level defaults derived from the collection hash
  // when the document itself is missing `a` or `e`.
  const a = asString(doc.a) ?? defaults?.a ?? null;
  if (a === null) {
    return { row: null, skipReason: SkipReason.MISSING_A };
  }

  const e = asString(doc.e) ?? defaults?.e ?? null;
  if (e === null) {
    return { row: null, skipReason: SkipReason.MISSING_E };
  }

  const uid = asString(doc.uid);
  if (uid === null) {
    return { row: null, skipReason: SkipReason.MISSING_UID };
  }

  const _id = asString(doc._id);
  if (_id === null) {
    return { row: null, skipReason: SkipReason.MISSING_ID };
  }

  // ── Timestamp validation ──────────────────────────────────────────────
  const tsMillis = toEpochMillis(doc.ts);
  if (tsMillis === null || tsMillis <= 0) {
    return { row: null, skipReason: SkipReason.INVALID_TS };
  }

  // ── Event name derivation ─────────────────────────────────────────────
  // An existing non-blank doc.n always wins so migrated rows match the rows
  // live ingestion produced for the same document (dedup identity). Legacy
  // documents have no `n`, so for them the sg-derived name applies as before.
  let eventName = e;
  let n: string | null = asString(doc.n);

  if (e.startsWith(CLY_PREFIX)) {
    if (n === null) {
      const sg = (isPlainObject(doc.sg) ? doc.sg : {}) as Record<string, unknown>;

      switch (e) {
        case '[CLY]_view':
          n = asString(sg['name']);
          break;
        case '[CLY]_action':
          n = firstNonBlank(asString(sg['name']), asString(sg['view']));
          break;
        case '[CLY]_nps':
        case '[CLY]_survey':
        case '[CLY]_star_rating':
          n = asString(sg['widget_id']);
          break;
        case '[CLY]_crash':
          n = asString(sg['group']);
          break;
        default:
          n = null;
          break;
      }
    }
  } else {
    // Custom event: n = original event name, e becomes [CLY]_custom
    if (n === null) {
      n = eventName;
    }
    eventName = '[CLY]_custom';
  }

  // Final fallback: if n is still blank, use e
  if (isBlank(n)) {
    n = eventName;
  }

  // ── Build output row ──────────────────────────────────────────────────
  const row: OutputRow = {
    _id,
    a,
    e: eventName,
    n: n as string,
    uid,
    did: asString(doc.did) ?? '',
    ts: '',
    c: clampUInt32(doc.c),  // counted below when clamping changed the value
    s: toDouble(doc.s, 0.0),
    dur: toDouble(doc.dur, 0.0),
    cd: '',
  };

  {
    const cRaw = Math.floor(toDouble(doc.c, 0));
    if (row.c !== cRaw) coercions?.record('clamp_uint32', 'c', cRaw, row.c);
  }

  const uidCanon = asString(doc.uid_canon);
  if (uidCanon !== null) {
    row.uid_canon = uidCanon;
  }
  const lsid = asString(doc.lsid);
  if (lsid !== null) {
    row.lsid = lsid;
  }

  // ── Timestamp normalisation ───────────────────────────────────────────
  // Countly-owned timestamps clamp to the DateTime64(3) column range.
  if (clampDateTime64(tsMillis) !== tsMillis) coercions?.record('clamp_datetime', 'ts', tsMillis, clampDateTime64(tsMillis));
  row.ts = formatTimestamp(clampDateTime64(tsMillis));

  const luMillis = toEpochMillis(doc.lu);
  if (luMillis !== null && luMillis > 0) {
    row.lu = formatTimestamp(clampDateTime64(luMillis));
  }

  // `cd` must always be emitted. The ClickHouse column is declared
  // `cd DateTime64(3) DEFAULT now64(3)`, so omitting it from the JSONEachRow
  // payload makes ClickHouse stamp the row with the migration's wall-clock
  // time instead of the event's real creation time. Downstream consumers
  // filter on `cd` (MAU/datapoint terms, the dedup job), so a defaulted value
  // silently pulls the entire migrated history into whichever term the
  // migration happened to run in.
  //
  // Documents predating the introduction of `cd` have no value to recover --
  // those are the ones handled by the null-cd sweep. Falling back to `ts`
  // keeps them in a plausible term and, unlike now64(3), is deterministic, so
  // re-running or resuming a migration is idempotent.
  const cdMillis = toEpochMillis(doc.cd);
  row.cd = formatTimestamp(clampDateTime64(cdMillis !== null && cdMillis > 0 ? cdMillis : tsMillis));

  // ── JSON columns ──────────────────────────────────────────────────────
  // Only plain objects are insertable into the JSON columns; customer-owned
  // values that don't fit JSON numeric representation are stringified
  // losslessly (NaN/±Infinity, BSON Decimal128/Long, bigint).
  if (isPlainObject(doc.up)) {
    row.up = sanitizeJsonValue(doc.up, (k, o, c) => coercions?.record(k, 'up', o, c)) as Record<string, unknown>;
  }
  if (isPlainObject(doc.custom)) {
    row.custom = sanitizeJsonValue(doc.custom, (k, o, c) => coercions?.record(k, 'custom', o, c)) as Record<string, unknown>;
  }
  if (isPlainObject(doc.cmp)) {
    row.cmp = sanitizeJsonValue(doc.cmp, (k, o, c) => coercions?.record(k, 'cmp', o, c)) as Record<string, unknown>;
  }
  if (isPlainObject(doc.sg)) {
    row.sg = sanitizeJsonValue(doc.sg, (k, o, c) => coercions?.record(k, 'sg', o, c)) as Record<string, unknown>;
  }

  return { row, skipReason: null };
}
