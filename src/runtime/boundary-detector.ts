/**
 * Tee-boundary auto-detection + tee-sync parity monitor.
 *
 * Setting: nginx tees the same SDK requests to both stacks (either
 * direction — old primary mirroring to new, or new primary mirroring to
 * old). ClickHouse is always the store that starts COLD, so its first-ever
 * cd marks when the tee went live, whichever side is primary. The same
 * event exists in both stores under DIFFERENT identities (new _id, new
 * cd), so per-event matching is impossible by design (and device ts is
 * unreliable when requests omit it — server-stamped independently on each
 * side). Detection therefore uses RATE SHAPES, never identities:
 *
 *  - anchor: min(cd) in ClickHouse (only valid BEFORE any bulk migration —
 *    migrated historical rows would poison it; detection refuses then)
 *  - per-minute count curves from both stores around the anchor
 *  - if the tee was flipped inside the recommended ingestion pause, both
 *    curves show a zero-traffic gap minute → the suggested bound comes
 *    from inside the gap and the seam is provably exact
 *  - without a gap, the anchor is suggested and the ambiguity is
 *    QUANTIFIED: how many old-side docs sit in the fuzzy minutes around it
 *    (the operator decides the dup/loss trade with a number in hand)
 *
 * The sync section runs regardless of migration state: hourly count parity
 * between the stores from the boundary onward. nginx mirror is
 * fire-and-forget — a secondary outage silently drops mirrored requests,
 * and only count parity reveals the hole (each flagged hour is a bounded
 * backfill window).
 */
import type { Db } from 'mongodb';
import type { Logger } from 'pino';

import type { Config } from '../config/schema.ts';
import type { StagingManager } from '../target/staging-manager.ts';
import type { LedgerStore } from '../state/ledger-store.ts';
import { discoverCollections } from '../source/discover-collections.ts';

export interface BoundaryProgress {
  status: 'not_run' | 'running' | 'completed' | 'failed';
  phase: string;
  collectionsScanned: number;
  totalCollections: number;
  startedAt: number | null;
  finishedAt: number | null;
  error: string | null;
  report: BoundaryReport | null;
}

export interface BoundaryReport {
  detection: {
    status: 'ok' | 'refused' | 'no_data';
    reason?: string;
    anchorMs?: number;
    suggestedBoundMs?: number;
    method?: 'gap' | 'anchor';
    gap?: { fromMs: number; toMs: number } | null;
    /** old-side docs within ±2 min of the suggestion — the dup/loss stake when no gap exists */
    ambiguousMongoDocs?: number;
    minutes?: Array<{ minuteMs: number; mongo: number; ch: number }>;
  };
  sync: {
    status: 'ok' | 'no_boundary';
    fromMs?: number;
    hours?: Array<{ hourMs: number; mongo: number; ch: number; driftPct: number; flagged: boolean }>;
    flaggedHours?: number;
  };
}

export function newBoundaryProgress(): BoundaryProgress {
  return {
    status: 'not_run', phase: '', collectionsScanned: 0, totalCollections: 0,
    startedAt: null, finishedAt: null, error: null, report: null,
  };
}

interface Deps {
  config: Config;
  logger: Logger;
  db: Db;
  staging: StagingManager;
  ledger: LedgerStore;
  progress: BoundaryProgress;
  bandMinutes?: number;
  syncHoursCap?: number;
}

/** Sum per-bucket doc counts across every drill collection (cd-indexed range scans). */
async function mongoCountsPerBucket(
  deps: Deps, collections: string[], fromMs: number, toMs: number, bucketSec: number, phase: string,
): Promise<Map<number, number>> {
  const out = new Map<number, number>();
  deps.progress.phase = phase;
  deps.progress.collectionsScanned = 0;
  for (const name of collections) {
    const rows = await deps.db.collection(name).aggregate<{ _id: number; n: number }>([
      { $match: { cd: { $gte: new Date(fromMs), $lt: new Date(toMs) } } },
      { $group: {
        _id: { $toLong: { $dateTrunc: { date: '$cd', unit: 'second', binSize: bucketSec } } },
        n: { $sum: 1 },
      } },
    ]).toArray();
    for (const r of rows) out.set(r._id, (out.get(r._id) ?? 0) + r.n);
    deps.progress.collectionsScanned++;
  }
  return out;
}

export async function detectBoundary(deps: Deps): Promise<BoundaryReport> {
  const { config, staging, ledger, progress } = deps;
  const bandMs = (deps.bandMinutes ?? 30) * 60_000;
  const report: BoundaryReport = {
    detection: { status: 'no_data' },
    sync: { status: 'no_boundary' },
  };

  const collections = await discoverCollections(deps.db, config.source.collectionPrefix, deps.logger);
  progress.totalCollections = collections.length;

  const anchorMs = await staging.minLiveCd();

  // ── Detection (pre-migration only: migrated rows poison the anchor) ────
  const mapped = await ledger.countForRun(config.ledger.runId);
  if (anchorMs === null) {
    report.detection = { status: 'no_data', reason: 'ClickHouse is empty — the tee does not appear to be active yet' };
  } else if (mapped > 0) {
    report.detection = {
      status: 'refused',
      reason: `run "${config.ledger.runId}" already has ${mapped} chunks — the ClickHouse anchor is only trustworthy BEFORE migration; detect with a fresh run id or rely on the recorded bound`,
      anchorMs,
    };
  } else if (anchorMs < Date.now() - 90 * 86_400_000) {
    report.detection = {
      status: 'refused',
      reason: `earliest ClickHouse cd is ${new Date(anchorMs).toISOString()} — the table already contains historical rows; the tee anchor cannot be derived from it`,
      anchorMs,
    };
  } else {
    const lo = anchorMs - bandMs;
    const hi = anchorMs + bandMs;
    const chMin = await staging.liveCountsPerBucket(lo, hi, 60);
    const mgMin = await mongoCountsPerBucket(deps, collections, lo, hi, 60, 'scanning the boundary band (per-minute)');

    const minutes: Array<{ minuteMs: number; mongo: number; ch: number }> = [];
    for (let m = Math.floor(lo / 60_000) * 60_000; m < hi; m += 60_000) {
      minutes.push({ minuteMs: m, mongo: mgMin.get(m) ?? 0, ch: chMin.get(m) ?? 0 });
    }

    // Gap: a zero-traffic minute on BOTH sides, with old-side traffic before
    // it and tee traffic after it — the ingestion-pause signature.
    let gap: { fromMs: number; toMs: number } | null = null;
    for (let i = 1; i < minutes.length - 1; i++) {
      const m = minutes[i];
      if (m.mongo !== 0 || m.ch !== 0) continue;
      const before = minutes.slice(0, i).some((x) => x.mongo > 0);
      const after = minutes.slice(i + 1).some((x) => x.ch > 0);
      if (!before || !after) continue;
      let j = i;
      while (j + 1 < minutes.length && minutes[j + 1].mongo === 0 && minutes[j + 1].ch === 0) j++;
      gap = { fromMs: minutes[i].minuteMs, toMs: minutes[j].minuteMs + 60_000 };
      break;
    }

    if (gap) {
      report.detection = {
        status: 'ok', method: 'gap', anchorMs, gap,
        suggestedBoundMs: gap.fromMs + Math.floor((gap.toMs - gap.fromMs) / 2),
        ambiguousMongoDocs: 0,
        minutes,
      };
    } else {
      const ambiguous = minutes
        .filter((m) => Math.abs(m.minuteMs - anchorMs) <= 120_000)
        .reduce((s, m) => s + m.mongo, 0);
      report.detection = {
        status: 'ok', method: 'anchor', anchorMs, gap: null,
        suggestedBoundMs: anchorMs,
        ambiguousMongoDocs: ambiguous,
        minutes,
      };
    }
  }

  // ── Sync parity (valid before AND after migration) ─────────────────────
  const syncFrom = config.ledger.cdUpperBoundMs
    ?? report.detection.suggestedBoundMs
    ?? anchorMs;
  if (syncFrom !== null && syncFrom !== undefined) {
    const capMs = (deps.syncHoursCap ?? 72) * 3_600_000;
    const from = Math.max(syncFrom, Date.now() - capMs);
    const to = Date.now();
    if (to - from > 5 * 60_000) {
      const chHr = await staging.liveCountsPerBucket(from, to, 3600);
      const mgHr = await mongoCountsPerBucket(deps, collections, from, to, 3600, 'scanning tee-sync parity (per-hour)');
      const hours: BoundaryReport['sync']['hours'] = [];
      let flagged = 0;
      for (let h = Math.floor(from / 3_600_000) * 3_600_000; h < to; h += 3_600_000) {
        const mongo = mgHr.get(h) ?? 0;
        const chN = chHr.get(h) ?? 0;
        const denom = Math.max(mongo, chN);
        const driftPct = denom === 0 ? 0 : Math.round(Math.abs(mongo - chN) / denom * 1000) / 10;
        // the CURRENT hour is still filling — never flag it
        const current = h + 3_600_000 > to;
        const isFlagged = !current && denom >= 100 && driftPct > 3;
        if (isFlagged) flagged++;
        hours.push({ hourMs: h, mongo, ch: chN, driftPct, flagged: isFlagged });
      }
      report.sync = { status: 'ok', fromMs: from, hours, flaggedHours: flagged };
    }
  }

  return report;
}
