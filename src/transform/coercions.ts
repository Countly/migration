/**
 * Coercion policy + counter (two-tier rule, agreed in the migration plan):
 *
 *  - Countly-owned numeric fields (c): semantics are ours — clamp to the
 *    target column range. Overflow is corruption, not information.
 *  - Customer-owned bags (sg / custom / cmp): never guess — values that
 *    cannot survive the numeric path (non-finite, beyond safe integer
 *    precision, BigInt) are stringified LOSSLESSLY. ClickHouse JSON columns
 *    are per-value typed, so a mixed-type key behaves the same as the
 *    customer's live traffic would.
 *
 * Every coercion is counted per (rule, key) with samples — that feed becomes
 * the dry-run / final report, so "what did we change?" is always answerable.
 */

export const UINT32_MAX = 4_294_967_295;

export interface CoercionSample {
  key: string;
  original: string;
  coerced: string;
}

export class CoercionCounter {
  private counts = new Map<string, number>();
  private samples = new Map<string, CoercionSample>();
  private static readonly MAX_SAMPLED_KEYS = 200;

  record(rule: string, key: string, original: unknown, coerced: unknown): void {
    const k = `${rule}:${key}`;
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

/** True when a numeric value cannot survive the JSON→ClickHouse numeric path. */
function needsStringify(v: unknown): boolean {
  if (typeof v === 'bigint') return true;
  if (typeof v === 'number') {
    return !Number.isFinite(v) || Math.abs(v) > Number.MAX_SAFE_INTEGER;
  }
  return false;
}

/**
 * Apply the customer-owned-bag rule to a segmentation-like object.
 * Returns the SAME reference when nothing needed coercion (zero-copy hot
 * path); a shallow copy with fixed values otherwise. Never mutates input.
 */
export function coerceBag(
  bag: unknown,
  bagName: string,
  counter?: CoercionCounter,
): unknown {
  if (bag === null || bag === undefined || typeof bag !== 'object' || Array.isArray(bag)) {
    return bag;
  }
  const obj = bag as Record<string, unknown>;
  let copy: Record<string, unknown> | null = null;
  for (const [key, value] of Object.entries(obj)) {
    if (needsStringify(value)) {
      if (!copy) copy = { ...obj };
      const coerced = String(value);
      copy[key] = coerced;
      counter?.record('stringify_unsafe_number', `${bagName}.${key}`, value, coerced);
    }
  }
  return copy ?? bag;
}
