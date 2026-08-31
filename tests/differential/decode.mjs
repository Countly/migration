/**
 * Corpus decoder for the ingestion-matching transformer differential harness.
 *
 * The fixture corpus (corpus.json) is plain JSON, but the input documents it
 * describes contain values plain JSON cannot express (NaN, Infinity, Date
 * instances, BSON Decimal128, driver-promoted Int64 doubles, huge strings).
 * Those are encoded as `{"$$t": ...}` wrapper objects and decoded here into
 * the exact JavaScript values the MongoDB Node driver (default options,
 * promoteLongs=true) would hand to either transform.
 *
 * This file is part of the cross-repo differential contract and must stay
 * byte-identical between:
 *   countly-platform: test/unit/fixtures/drill-transform-differential/decode.mjs
 *   Countly/migration: tests/differential/decode.mjs
 */

import { Decimal128, Long } from 'mongodb';

/**
 * Recursively decode a corpus-encoded value into the runtime value the
 * transforms receive.
 * @param {*} value - encoded corpus value
 * @returns {*} decoded runtime value
 */
export function decodeValue(value) {
    if (Array.isArray(value)) {
        return value.map(decodeValue);
    }
    if (value === null || typeof value !== 'object') {
        return value;
    }
    if (typeof value.$$t === 'string') {
        switch (value.$$t) {
        case 'nan':
            return NaN;
        case 'inf':
            return Infinity;
        case '-inf':
            return -Infinity;
        case 'undef':
            return undefined;
        case 'date':
            // A real Date instance, as the driver returns for BSON dates.
            return new Date(value.ms);
        case 'decimal128':
            // Decimal128 is NOT promoted by the driver; transforms see the BSON object.
            return Decimal128.fromString(value.v);
        case 'driverLong':
            // BSON Int64 read with default driver options (promoteLongs=true):
            // the transform sees a (possibly precision-lossy) JS double.
            return Long.fromString(value.v).toNumber();
        case 'bigstring':
            return value.c.repeat(value.n);
        default:
            throw new Error(`Unknown corpus wrapper type: ${value.$$t}`);
        }
    }
    const out = {};
    for (const key of Object.keys(value)) {
        out[key] = decodeValue(value[key]);
    }
    return out;
}

/**
 * Decode one corpus entry's `doc` into a runtime input document.
 * @param {{id: string, doc: object}} entry - corpus entry
 * @returns {object} decoded document
 */
export function decodeDoc(entry) {
    return decodeValue(entry.doc);
}
