/**
 * TypeScript port of ClyEventNormalize.java.
 *
 * Transforms raw MongoDB event documents into ClickHouse-ready rows,
 * applying field validation, event-name derivation, and timestamp
 * normalization.
 */

import { SkipReason, SkipCounter } from './skip-reasons.ts';
import {
  isBlank,
  asString,
  toEpochMillis,
  toDouble,
  formatTimestamp,
  firstNonBlank,
} from './validators.ts';
import type { CollectionDefaults } from './hash-resolver.ts';

// ────────────────────────────────────────────────────────────────────────────
// Constants
// ────────────────────────────────────────────────────────────────────────────

/**
 * The complete set of fields that may appear in a transformed output row.
 * Anything not in this set is stripped before the row is emitted.
 */
const KNOWN_FIELDS = new Set<string>([
  'a', 'e', 'n', 'uid', 'uid_canon', 'did', 'lsid',
  '_id', 'ts', 'up', 'custom', 'cmp', 'sg', 'c', 's', 'dur', 'lu', 'cd',
]);

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
  cmp?: string;
  sg?: Record<string, unknown>;
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
  up?: unknown;
  custom?: unknown;
  cmp?: string;
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
export function transformDocument(doc: SourceDocument, defaults?: CollectionDefaults): TransformResult {
  try {
    return doTransform(doc, defaults);
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
): { rows: OutputRow[]; skippedSamples: Array<{ _id: string; reason: SkipReason }> } {
  const rows: OutputRow[] = [];
  const skippedSamples: Array<{ _id: string; reason: SkipReason }> = [];
  const MAX_SKIP_SAMPLES = 10;

  for (const doc of docs) {
    const { row, skipReason } = transformDocument(doc, defaults);

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

function doTransform(doc: SourceDocument, defaults?: CollectionDefaults): TransformResult {
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

  // ── Build output row ──────────────────────────────────────────────────
  // We build via a mutable bag and cast at the end since all required
  // OutputRow fields are guaranteed to be set by the code below.
  const row: Record<string, unknown> = {};

  // Copy all known fields from the source document
  for (const key of KNOWN_FIELDS) {
    if (key in doc) {
      row[key] = (doc as Record<string, unknown>)[key];
    }
  }

  // Overwrite validated / required fields
  row['a'] = a;
  row['e'] = e;
  row['uid'] = uid;
  row['_id'] = _id;

  // ── Defaults ──────────────────────────────────────────────────────────
  const did = asString(doc.did);
  row['did'] = did ?? '';

  row['s'] = toDouble(doc.s, 0.0);
  row['dur'] = toDouble(doc.dur, 0.0);
  row['c'] = Math.max(0, Math.floor(toDouble(doc.c, 0)));

  // ── Event name derivation ─────────────────────────────────────────────
  let eventName = e;
  let n: string | null = null;

  if (e.startsWith(CLY_PREFIX)) {
    const sg = (doc.sg ?? {}) as Record<string, unknown>;

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
        // Keep existing n from the document, if any
        n = asString(doc.n) ?? null;
        break;
    }
  } else {
    // Custom event: n = original event name, e becomes [CLY]_custom
    n = eventName;
    eventName = '[CLY]_custom';
    row['e'] = eventName;
  }

  // Final fallback: if n is still blank, use e
  if (isBlank(n)) {
    n = eventName;
  }
  row['n'] = n;

  // ── Timestamp normalisation ───────────────────────────────────────────
  row['ts'] = formatTimestamp(tsMillis);

  const luMillis = toEpochMillis(doc.lu);
  if (luMillis !== null) {
    row['lu'] = formatTimestamp(luMillis);
  } else {
    delete row['lu'];
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
  row['cd'] = formatTimestamp(cdMillis !== null && cdMillis > 0 ? cdMillis : tsMillis);

  return { row: row as unknown as OutputRow, skipReason: null };
}
