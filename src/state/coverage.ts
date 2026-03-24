import { type Cursor, compareCursors, deserializeCursor } from '../types/cursor.ts';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface CoverageInterval {
    lowerExclusive: Cursor;
    upperInclusive: Cursor;
}

/**
 * Minimal batch shape required for coverage computation.
 * Compatible with the full Batch type from manifest-store.
 */
export interface CompletedBatch {
    lower_exclusive_cursor: string;  // serialized Cursor JSON
    upper_inclusive_cursor: string;   // serialized Cursor JSON
    status: string;
}

// ---------------------------------------------------------------------------
// Coverage functions
// ---------------------------------------------------------------------------

/**
 * Build sorted coverage intervals from completed (status === "done") batches.
 *
 * The intervals are sorted by their lower-exclusive cursor using compound
 * cursor comparison (cd first, then _id as tiebreaker).
 */
export function buildCoverageFromBatches(
    batches: CompletedBatch[],
): CoverageInterval[] {
    const done = batches.filter((b) => b.status === "done");

    const ZERO_CURSOR: Cursor = { cd: 0, id: '' };

    const intervals: CoverageInterval[] = done.map((b) => ({
        lowerExclusive: b.lower_exclusive_cursor
            ? deserializeCursor(b.lower_exclusive_cursor)
            : ZERO_CURSOR,
        upperInclusive: deserializeCursor(b.upper_inclusive_cursor),
    }));

    intervals.sort((a, b) =>
        compareCursors(a.lowerExclusive, b.lowerExclusive),
    );

    return intervals;
}

/**
 * Merge adjacent or overlapping intervals into the smallest set of
 * contiguous intervals.
 *
 * Two intervals are considered adjacent when the upper bound of one equals
 * the lower bound of the next (i.e., they share a boundary cursor).
 *
 * Intervals MUST be sorted by `lowerExclusive` before calling this
 * function (use `buildCoverageFromBatches` which already sorts).
 */
export function compactIntervals(
    intervals: CoverageInterval[],
): CoverageInterval[] {
    if (intervals.length === 0) return [];

    // Assumes input is already sorted by lowerExclusive (buildCoverageFromBatches sorts).
    const merged: CoverageInterval[] = [{ ...intervals[0] }];

    for (let i = 1; i < intervals.length; i++) {
        const current = intervals[i];
        const last = merged[merged.length - 1];

        // Adjacent: last.upper === current.lower  (boundary cursors match)
        // Overlapping: last.upper >= current.lower
        if (compareCursors(last.upperInclusive, current.lowerExclusive) >= 0) {
            // Extend upper bound if the current interval reaches further
            if (compareCursors(current.upperInclusive, last.upperInclusive) > 0) {
                last.upperInclusive = current.upperInclusive;
            }
        } else {
            merged.push({ ...current });
        }
    }

    return merged;
}

