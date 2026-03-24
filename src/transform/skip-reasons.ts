export const SkipReason = {
  ALREADY_MARKED_MIGRATED: 'already_marked_migrated',
  MISSING_A: 'missing_a',
  MISSING_E: 'missing_e',
  MISSING_UID: 'missing_uid',
  MISSING_ID: 'missing__id',
  INVALID_TS: 'invalid_ts',
  TRANSFORM_ERROR: 'transform_error',
} as const;

export type SkipReason = (typeof SkipReason)[keyof typeof SkipReason];

const ALL_REASONS = Object.values(SkipReason) as SkipReason[];

export class SkipCounter {
  private counts: Record<SkipReason, number>;

  constructor() {
    this.counts = Object.fromEntries(ALL_REASONS.map(r => [r, 0])) as Record<SkipReason, number>;
  }

  increment(reason: SkipReason): void {
    this.counts[reason]++;
  }

  getCounts(): Record<SkipReason, number> {
    return { ...this.counts };
  }

  getTotal(): number {
    return Object.values(this.counts).reduce((sum, count) => sum + count, 0);
  }

  reset(): void {
    for (const key of ALL_REASONS) {
      this.counts[key] = 0;
    }
  }
}
