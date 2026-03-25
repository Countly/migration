/**
 * Field validation and conversion utilities for the migration transform layer.
 */

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
