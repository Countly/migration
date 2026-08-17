/**
 * Field validation and conversion utilities for the migration transform layer.
 *
 * These helpers implement the shared drill-event normalization spec that
 * countly-platform's live ingestion transformer
 * (api/utils/eventTransformer.ts) also implements. Both sides must produce
 * IDENTICAL countly_drill.drill_events rows for the same input document; the
 * differential harness in tests/differential/ enforces this in CI.
 */

const UINT32_MAX = 4294967295;

/** DateTime64(3) representable range: 1900-01-01 00:00:00.000 .. 2299-12-31 23:59:59.999 UTC. */
export const DATETIME64_MIN_MS = Date.UTC(1900, 0, 1, 0, 0, 0, 0);
export const DATETIME64_MAX_MS = Date.UTC(2299, 11, 31, 23, 59, 59, 999);

/**
 * Returns true if the value is null, undefined, or an empty/whitespace-only string.
 */
export function isBlank(s: unknown): boolean {
  if (s === null || s === undefined) {
    return true;
  }
  if (typeof s === 'string') {
    return s.trim().length === 0;
  }
  return false;
}

/**
 * Converts the value to a string, returning null if blank.
 */
export function asString(o: unknown): string | null {
  if (o === null || o === undefined) {
    return null;
  }
  const str = String(o);
  return str.trim().length === 0 ? null : str;
}

/**
 * Converts a timestamp value to epoch milliseconds.
 *
 * Handles:
 *  - Number: if >= 9.5e11 treat as millis; if >= 9.5e8 treat as seconds (* 1000); else null
 *  - String: parse as number, then apply number rules
 *  - Object with `$date` key (Mongo Extended JSON): recurse on the value
 *  - Otherwise null
 */
export function toEpochMillis(ts: unknown): number | null {
  if (ts === null || ts === undefined) {
    return null;
  }

  if (ts instanceof Date) {
    const ms = ts.getTime();
    return isNaN(ms) ? null : ms;
  }

  if (typeof ts === 'number') {
    if (!isFinite(ts)) {
      return null;
    }
    if (ts >= 9.5e11) {
      return Math.floor(ts);
    }
    if (ts >= 9.5e8) {
      return Math.floor(ts * 1000);
    }
    return null;
  }

  if (typeof ts === 'string') {
    const parsed = Number(ts);
    if (isNaN(parsed)) {
      return null;
    }
    return toEpochMillis(parsed);
  }

  if (typeof ts === 'object' && ts !== null && '$date' in (ts as Record<string, unknown>)) {
    return toEpochMillis((ts as Record<string, unknown>)['$date']);
  }

  return null;
}

/**
 * Clamps epoch milliseconds to the DateTime64(3) column range
 * (Countly-owned timestamps clamp to column ranges by policy).
 */
export function clampDateTime64(epochMs: number): number {
  return Math.min(Math.max(epochMs, DATETIME64_MIN_MS), DATETIME64_MAX_MS);
}

/**
 * Parses a value as a double (floating-point number), returning the default if
 * the value is null, undefined, or not parseable.
 */
export function toDouble(val: unknown, defaultVal: number): number {
  if (val === null || val === undefined) {
    return defaultVal;
  }
  if (typeof val === 'number') {
    return isFinite(val) ? val : defaultVal;
  }
  const parsed = Number(val);
  return (isNaN(parsed) || !isFinite(parsed)) ? defaultVal : parsed;
}

/**
 * Clamps a count value to the UInt32 column range: [0, 4294967295], integer.
 */
export function clampUInt32(val: unknown): number {
  const num = Math.floor(toDouble(val, 0));
  return Math.min(Math.max(num, 0), UINT32_MAX);
}

/**
 * Returns true for plain (non-array, non-Date) objects usable as JSON column payloads.
 */
export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date);
}

/**
 * Deep-sanitizes customer-owned JSON column values (up/custom/cmp/sg).
 * Values JSON cannot carry numerically are stringified losslessly instead of
 * degrading to null: NaN/±Infinity become "NaN"/"Infinity"/"-Infinity",
 * BSON Decimal128/Long become their decimal string, bigint becomes a string.
 * `undefined` values are dropped from objects and become null inside arrays,
 * matching JSON serialization.
 */
export type OnCoerce = (kind: string, original: unknown, coerced: unknown) => void;

export function sanitizeJsonValue(value: unknown, onCoerce?: OnCoerce): unknown {
  if (typeof value === 'number') {
    if (isNaN(value)) {
      onCoerce?.('stringify_nonfinite', value, 'NaN');
      return 'NaN';
    }
    if (value === Infinity) {
      onCoerce?.('stringify_nonfinite', value, 'Infinity');
      return 'Infinity';
    }
    if (value === -Infinity) {
      onCoerce?.('stringify_nonfinite', value, '-Infinity');
      return '-Infinity';
    }
    return value;
  }
  if (typeof value === 'bigint') {
    onCoerce?.('stringify_bigint', value, value.toString());
    return value.toString();
  }
  if (value === null || typeof value !== 'object') {
    return value;
  }
  if (value instanceof Date) {
    return value;
  }
  const bsonType = (value as Record<string, unknown>)['_bsontype'];
  if (bsonType === 'Decimal128' || bsonType === 'Long') {
    const coerced = String(value);
    onCoerce?.('stringify_bson_' + String(bsonType).toLowerCase(), value, coerced);
    return coerced;
  }
  if (Array.isArray(value)) {
    return value.map((item) => {
      const sanitized = sanitizeJsonValue(item, onCoerce);
      return sanitized === undefined ? null : sanitized;
    });
  }
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(value as Record<string, unknown>)) {
    const sanitized = sanitizeJsonValue((value as Record<string, unknown>)[key], onCoerce);
    if (sanitized !== undefined) {
      out[key] = sanitized;
    }
  }
  return out;
}

/**
 * Formats an epoch-millisecond timestamp as 'yyyy-MM-dd HH:mm:ss.SSS' in UTC.
 */
export function formatTimestamp(epochMs: number): string {
  const d = new Date(epochMs);
  const yyyy = d.getUTCFullYear().toString().padStart(4, '0');
  const MM = (d.getUTCMonth() + 1).toString().padStart(2, '0');
  const dd = d.getUTCDate().toString().padStart(2, '0');
  const HH = d.getUTCHours().toString().padStart(2, '0');
  const mm = d.getUTCMinutes().toString().padStart(2, '0');
  const ss = d.getUTCSeconds().toString().padStart(2, '0');
  const SSS = d.getUTCMilliseconds().toString().padStart(3, '0');
  return `${yyyy}-${MM}-${dd} ${HH}:${mm}:${ss}.${SSS}`;
}

/**
 * Returns the first non-blank value from the arguments, or null if all are blank.
 */
export function firstNonBlank(...values: (string | null | undefined)[]): string | null {
  for (const v of values) {
    if (!isBlank(v)) {
      return v as string;
    }
  }
  return null;
}
