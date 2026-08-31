/**
 * Canonical ClickHouse row model for the ingestion-matching transformer
 * differential harness.
 *
 * Both pipelines write the same table but over different wire formats:
 *   - countly-platform: transformToKafkaEventFormat() -> JSON.stringify ->
 *     Kafka -> ClickHouse Kafka Connect sink (numeric epoch-ms timestamps)
 *   - Countly/migration: doTransform() -> @clickhouse/client JSONEachRow
 *     ('yyyy-MM-dd HH:mm:ss.SSS' timestamp strings)
 *
 * This module maps either wire payload to the effective countly_drill.drill_events
 * row so the two can be deep-equal compared. Values that a given wire format
 * could not insert cleanly (type mismatch for the target column, out-of-range
 * numerics, null into a non-nullable column) are represented as explicit
 * "__UNINSERTABLE__(...)" sentinel strings instead of being smoothed over —
 * an aligned implementation must never produce them, so any sentinel in a
 * golden or a diff is itself a reportable divergence.
 *
 * Assumption (deployment invariant, asserted by the migration's
 * datetime-handling integration test): the ClickHouse server timezone is UTC,
 * so the migration's naive timestamp strings and the connector's epoch-ms
 * numbers denote the same instant.
 *
 * This file is part of the cross-repo differential contract and must stay
 * byte-identical between:
 *   countly-platform: test/unit/fixtures/drill-transform-differential/canonicalize.mjs
 *   Countly/migration: tests/differential/canonicalize.mjs
 */

/** Columns of countly_drill.drill_events (plugins/clickhouse/api/sql/01-drill_events.sql). */
export const SCHEMA_COLUMNS = [
    'a', 'e', 'n', 'uid', 'uid_canon', 'did', 'lsid', '_id',
    'ts', 'up', 'custom', 'cmp', 'sg', 'c', 's', 'dur', 'lu', 'cd',
];

const UINT32_MAX = 4294967295;
/** DateTime64(3) representable range: 1900-01-01 00:00:00.000 .. 2299-12-31 23:59:59.999 UTC. */
export const DATETIME64_MIN_MS = Date.UTC(1900, 0, 1, 0, 0, 0, 0);
export const DATETIME64_MAX_MS = Date.UTC(2299, 11, 31, 23, 59, 59, 999);

const TS_STRING_RE = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/;

/**
 * Format an epoch-ms timestamp as 'yyyy-MM-dd HH:mm:ss.SSS' in UTC
 * (identical to the migration's validators.formatTimestamp).
 * @param {number} epochMs - epoch milliseconds
 * @returns {string} formatted timestamp
 */
export function formatTimestamp(epochMs) {
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
 * Deep-sort object keys so JSON-column comparisons are order-insensitive.
 * @param {*} value - any JSON value
 * @returns {*} value with all object keys sorted
 */
export function deepSortKeys(value) {
    if (Array.isArray(value)) {
        return value.map(deepSortKeys);
    }
    if (value !== null && typeof value === 'object') {
        const out = {};
        for (const key of Object.keys(value).sort()) {
            out[key] = deepSortKeys(value[key]);
        }
        return out;
    }
    return value;
}

/**
 * Build an uninsertable-value sentinel.
 * @param {string} column - column name
 * @param {string} why - short reason
 * @param {*} value - offending value (post JSON round-trip)
 * @returns {string} sentinel string
 */
function uninsertable(column, why, value) {
    return `__UNINSERTABLE__(${column}: ${why}: ${JSON.stringify(value)})`;
}

/**
 * Canonicalize one String-typed column value.
 * @param {object} payload - JSON-round-tripped wire payload
 * @param {string} column - column name
 * @param {{nullable?: boolean}} [opts] - column options
 * @returns {*} canonical value
 */
function stringColumn(payload, column, opts = {}) {
    const value = payload[column];
    if (value === undefined || value === null) {
        return opts.nullable ? null : (column in payload ? uninsertable(column, 'null into non-nullable String', value) : '');
    }
    if (typeof value !== 'string') {
        return uninsertable(column, 'non-string into String column', value);
    }
    return value;
}

/**
 * Canonicalize a DateTime64(3) column from a wire value that may be an
 * epoch-ms number (Kafka path) or a pre-formatted string (JSONEachRow path).
 * @param {*} value - wire value
 * @param {string} column - column name
 * @returns {*} canonical 'yyyy-MM-dd HH:mm:ss.SSS' string or sentinel
 */
function dateTimeColumn(value, column) {
    if (typeof value === 'number') {
        if (!Number.isFinite(value) || !Number.isInteger(value)) {
            return uninsertable(column, 'non-integer epoch value', value);
        }
        if (value < DATETIME64_MIN_MS || value > DATETIME64_MAX_MS) {
            return uninsertable(column, 'epoch-ms outside DateTime64(3) range', value);
        }
        return formatTimestamp(value);
    }
    if (typeof value === 'string') {
        if (!TS_STRING_RE.test(value)) {
            return uninsertable(column, 'unparseable DateTime64 string', value);
        }
        const ms = Date.parse(value.replace(' ', 'T') + 'Z');
        if (Number.isNaN(ms) || ms < DATETIME64_MIN_MS || ms > DATETIME64_MAX_MS) {
            return uninsertable(column, 'DateTime64 string outside range', value);
        }
        return formatTimestamp(ms);
    }
    return uninsertable(column, 'unsupported DateTime64 wire type', value);
}

/**
 * Canonicalize a JSON-typed column (up/custom/cmp/sg).
 * @param {object} payload - JSON-round-tripped wire payload
 * @param {string} column - column name
 * @param {{nullable?: boolean}} [opts] - column options
 * @returns {*} canonical value
 */
function jsonColumn(payload, column, opts = {}) {
    const value = payload[column];
    if (value === undefined || value === null) {
        // Missing/null -> Nullable(JSON) NULL, non-nullable JSON default {}.
        return opts.nullable ? null : {};
    }
    if (typeof value !== 'object' || Array.isArray(value)) {
        return uninsertable(column, 'non-object into JSON column', value);
    }
    return deepSortKeys(value);
}

/**
 * Canonicalize the c (UInt32) column.
 * @param {object} payload - JSON-round-tripped wire payload
 * @returns {*} canonical value
 */
function uint32Column(payload) {
    const value = payload.c;
    if (value === undefined) {
        return 0; // column type default
    }
    if (typeof value !== 'number' || !Number.isInteger(value) || value < 0 || value > UINT32_MAX) {
        return uninsertable('c', 'not a UInt32', value);
    }
    return value;
}

/**
 * Canonicalize a Float64 column (s/dur).
 * @param {object} payload - JSON-round-tripped wire payload
 * @param {string} column - column name
 * @returns {*} canonical value
 */
function float64Column(payload, column) {
    const value = payload[column];
    if (value === undefined) {
        return 0;
    }
    if (typeof value !== 'number') {
        return uninsertable(column, 'not a Float64', value);
    }
    return value;
}

/**
 * Canonicalize a transformed wire payload (either pipeline) into the effective
 * drill_events row. Pass the transform's direct output; the JSON round-trip
 * both wire formats perform (JSON.stringify for Kafka, JSONEachRow
 * serialization for the migration) is applied here, so toJSON() conversions,
 * NaN->null, and undefined-dropping match the real wire behavior.
 *
 * @param {object|null} output - transform output (null/undefined = skipped)
 * @returns {{skip: true}|{row: object}} canonical result
 */
export function canonicalizeOutput(output) {
    if (output === null || output === undefined) {
        return { skip: true };
    }
    const payload = JSON.parse(JSON.stringify(output));
    const row = {};
    row.a = stringColumn(payload, 'a');
    row.e = stringColumn(payload, 'e');
    row.n = stringColumn(payload, 'n');
    row.uid = stringColumn(payload, 'uid');
    row.uid_canon = stringColumn(payload, 'uid_canon', { nullable: true });
    row.did = stringColumn(payload, 'did');
    row.lsid = stringColumn(payload, 'lsid', { nullable: true });
    row._id = stringColumn(payload, '_id');

    if (payload.ts === undefined || payload.ts === null) {
        row.ts = uninsertable('ts', 'missing required timestamp', payload.ts);
    }
    else {
        row.ts = dateTimeColumn(payload.ts, 'ts');
    }
    row.lu = (payload.lu === undefined || payload.lu === null) ? null : dateTimeColumn(payload.lu, 'lu');
    // cd has DEFAULT now64(3): omitting it yields a nondeterministic
    // ingestion-time stamp, which can never match a deterministic transform.
    row.cd = (payload.cd === undefined) ? '__NONDETERMINISTIC__(cd: now64(3) column default)' : dateTimeColumn(payload.cd, 'cd');

    row.up = jsonColumn(payload, 'up');
    row.custom = jsonColumn(payload, 'custom', { nullable: true });
    row.cmp = jsonColumn(payload, 'cmp', { nullable: true });
    row.sg = jsonColumn(payload, 'sg');

    row.c = uint32Column(payload);
    row.s = float64Column(payload, 's');
    row.dur = float64Column(payload, 'dur');
    return { row };
}
