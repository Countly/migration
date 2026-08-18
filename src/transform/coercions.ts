/**
 * CoercionCounter — per-(rule, key) accounting of every value the transform
 * had to alter, with samples. Feeds the dry-run / final report so "what did
 * we change?" is always answerable.
 *
 * The coercion RULES themselves live in validators.ts (clampUInt32,
 * clampDateTime64, sanitizeJsonValue) — they are part of the shared
 * normalization spec enforced by tests/differential/ and must match
 * countly-platform's live ingestion exactly. This module only counts.
 */

export interface CoercionSample {
  key: string;
  original: string;
  coerced: string;
}

export class CoercionCounter {
  private counts = new Map<string, number>();
  private samples = new Map<string, CoercionSample>();
  private static readonly MAX_SAMPLED_KEYS = 200;
  // Distinct (rule, field) keys are bounded so pathological data (millions of
  // distinct field names) cannot grow this map without limit; the overflow
  // bucket keeps the TOTAL exact either way.
  private static readonly MAX_DISTINCT_KEYS = 10_000;
  private static readonly OVERFLOW_KEY = 'other:distinct-key-cap-reached';

  record(rule: string, key: string, original: unknown, coerced: unknown): void {
    let k = `${rule}:${key}`;
    if (!this.counts.has(k) && this.counts.size >= CoercionCounter.MAX_DISTINCT_KEYS) {
      k = CoercionCounter.OVERFLOW_KEY;
    }
    this.counts.set(k, (this.counts.get(k) ?? 0) + 1);
    if (!this.samples.has(k) && this.samples.size < CoercionCounter.MAX_SAMPLED_KEYS) {
      this.samples.set(k, { key, original: String(original).slice(0, 100), coerced: String(coerced).slice(0, 100) });
    }
  }

  getTotal(): number {
    let t = 0;
    for (const n of this.counts.values()) t += n;
    return t;
  }

  getReport(): Array<{ rule_key: string; count: number; sample: CoercionSample | null }> {
    return [...this.counts.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([ruleKey, count]) => ({ rule_key: ruleKey, count, sample: this.samples.get(ruleKey) ?? null }));
  }
}
