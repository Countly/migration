/**
 * Compound cursor for ordered pagination over (cd, _id).
 *
 * MongoDB's custom string _id is not monotonic with insert time, so using
 * _id alone as a cursor can miss documents inserted during a live migration.
 * This compound cursor uses cd (immutable creation timestamp) as the primary
 * ordering key and _id as a deterministic tiebreaker.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Compound cursor representing a position in the (cd, _id) ordered space.
 *
 * `cd` is stored as epoch milliseconds (matching MongoDB's internal Date
 * representation) for unambiguous numeric comparison.
 */
export interface Cursor {
  cd: number; // epoch milliseconds
  id: string; // document _id
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

/**
 * Compare two cursors.  Returns -1 if a < b, 0 if equal, 1 if a > b.
 *
 * Primary sort: `cd` (numeric).
 * Tiebreaker:   `id` (lexicographic).
 */
export function compareCursors(a: Cursor, b: Cursor): number {
  if (a.cd < b.cd) return -1;
  if (a.cd > b.cd) return 1;
  if (a.id < b.id) return -1;
  if (a.id > b.id) return 1;
  return 0;
}

// ---------------------------------------------------------------------------
// Serialization (for MongoDB TEXT columns and Redis JSON)
// ---------------------------------------------------------------------------

/** Serialize a cursor to a deterministic JSON string. */
export function serializeCursor(c: Cursor): string {
  return JSON.stringify({ cd: c.cd, id: c.id });
}

/** Deserialize a cursor from a JSON string produced by serializeCursor. */
export function deserializeCursor(s: string): Cursor {
  let obj: unknown;
  try {
    obj = JSON.parse(s);
  } catch {
    throw new Error(`Invalid cursor JSON: ${s}`);
  }
  if (
    typeof obj !== 'object' || obj === null ||
    typeof (obj as Record<string, unknown>).cd !== 'number' ||
    typeof (obj as Record<string, unknown>).id !== 'string'
  ) {
    throw new Error(`Malformed cursor: missing cd (number) or id (string) in ${s}`);
  }
  return { cd: (obj as Cursor).cd, id: (obj as Cursor).id };
}

// ---------------------------------------------------------------------------
// MongoDB value conversion
// ---------------------------------------------------------------------------

/**
 * Convert a raw MongoDB `cd` field value to epoch milliseconds.
 *
 * Handles Date objects, numeric seconds/milliseconds, and numeric strings.
 * Returns 0 for null, undefined, or unparseable values.
 */
export function cdToEpoch(cdValue: unknown): number {
  if (cdValue === null || cdValue === undefined) return 0;

  if (cdValue instanceof Date) {
    const ms = cdValue.getTime();
    return isNaN(ms) ? 0 : ms;
  }

  if (typeof cdValue === 'number') {
    if (!isFinite(cdValue)) return 0;
    if (cdValue >= 9.5e11) return Math.floor(cdValue);
    if (cdValue >= 9.5e8) return Math.floor(cdValue * 1000);
    return 0;
  }

  if (typeof cdValue === 'string') {
    const parsed = Number(cdValue);
    return isNaN(parsed) ? 0 : cdToEpoch(parsed);
  }

  return 0;
}
