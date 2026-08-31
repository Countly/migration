/**
 * Classifies errors from ClickHouse inserts (and the surrounding pipeline)
 * into `permanent` (the data will never insert — retrying is pointless) and
 * `transient` (network/load — retrying works).
 *
 * Unknown errors default to `transient`: misclassifying either way is
 * recoverable (the chunk fails and is redone / replayed), but defaulting
 * unknowns to retry means a new transient error class doesn't silently
 * divert good data into the failed pile.
 */

export type ErrorClass = 'permanent' | 'transient';

/**
 * ClickHouse exception codes that indicate the payload itself is unacceptable.
 * Source: ClickHouse ErrorCodes; verified empirically (e.g. 41 on bad DateTime).
 */
const PERMANENT_CH_CODES = new Set([
  '6',   // CANNOT_PARSE_TEXT
  '26',  // CANNOT_PARSE_ESCAPE_SEQUENCE
  '27',  // CANNOT_PARSE_INPUT_ASSERTION_FAILED
  '38',  // CANNOT_PARSE_DATE
  '41',  // CANNOT_PARSE_DATETIME
  '53',  // TYPE_MISMATCH
  '69',  // ARGUMENT_OUT_OF_BOUND
  '72',  // CANNOT_PARSE_NUMBER
  '117', // INCORRECT_DATA
  '130', // CANNOT_READ_ARRAY_FROM_TEXT
  '467', // CANNOT_PARSE_BOOL
  '490', // CANNOT_PARSE_IPV4
  '491', // CANNOT_PARSE_IPV6
]);

/**
 * ClickHouse capacity/limit codes — transient: they clear when the operator
 * frees disk / load subsides. The retry→attempts→circuit-breaker chain turns
 * a persistent disk-full into a paused engine awaiting the operator.
 *  243 NOT_ENOUGH_SPACE, 202 TOO_MANY_SIMULTANEOUS_QUERIES,
 *  209 SOCKET_TIMEOUT, 210 NETWORK_ERROR (already covered by default-transient,
 *  listed for documentation).
 */

/** Node/undici-level network error codes — always transient. */
const TRANSIENT_SYSTEM_CODES = new Set([
  'ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT', 'EAI_AGAIN',
  'ENOTFOUND', 'EHOSTUNREACH', 'ENETUNREACH', 'UND_ERR_SOCKET',
]);

/** Client-side serialization failures that no retry can fix. */
const PERMANENT_MESSAGE_PATTERNS = [
  /do not know how to serialize/i, // JSON.stringify on BigInt
  /circular structure/i,
];

/**
 * Classify an error thrown by a ClickHouse insert (or the code around it).
 */
export function classifyError(err: unknown): ErrorClass {
  const e = err as { code?: unknown; message?: unknown } | null;
  const code = e && e.code !== undefined ? String(e.code) : '';
  const message = e && typeof e.message === 'string' ? e.message : '';

  if (PERMANENT_CH_CODES.has(code)) {
    return 'permanent';
  }
  if (TRANSIENT_SYSTEM_CODES.has(code)) {
    return 'transient';
  }
  for (const pattern of PERMANENT_MESSAGE_PATTERNS) {
    if (pattern.test(message)) {
      return 'permanent';
    }
  }
  // Unknown → transient (retry). See module doc for rationale.
  return 'transient';
}
