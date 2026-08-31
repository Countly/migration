/**
 * Ledger rebuild: regenerate `mig_ranges` from the data itself when the
 * progress state in MongoDB is lost or corrupted.
 *
 * How it works — the source is frozen (post-cutover), so the chunk grid can
 * be regenerated deterministically from Mongo's cd span, and each window's
 * migration status is decided by comparing exact counts:
 *
 *   Mongo docs in [lower_cd, upper_cd)   vs   live ClickHouse rows in the
 *                                             same window, scoped to this
 *                                             collection's (a, e)
 *
 *   equal            → done      (window fully migrated)
 *   ClickHouse = 0   → pending   (window not migrated yet)
 *   anything else    → failed    (partial — Retry failed chunks purges the
 *                                 window and redoes it cleanly)
 *
 * Live ingestion writing NEW data into the same table does not interfere:
 * new rows carry cd >= cutover time, while every regenerated window ends at
 * the frozen source's max cd — the windows cannot contain post-cutover rows.
 *
 * The one exception is the tool's own null-cd sweep: those rows carry
 * ts-derived cd values that DO land inside regular windows. Their Mongo ids
 * are known ({cd: null} in the frozen source), so the rebuild fetches their
 * live cd values and subtracts them per window before comparing.
 */

import type { Logger } from 'pino';
import { MongoClient } from 'mongodb';
import type { Config } from '../config/schema.ts';
import type { HashResolver } from '../transform/hash-resolver.ts';
import { LedgerStore, type ChunkDoc } from '../state/ledger-store.ts';
import type { DlqStore } from '../state/dlq-store.ts';
import { StagingManager } from '../target/staging-manager.ts';
import { discoverCollections } from '../source/discover-collections.ts';
import { computeChunkBounds } from './chunk-orchestrator.ts';
import { chScopeOf } from '../transform/hash-resolver.ts';
import { toEpochMillis, clampDateTime64 } from '../transform/validators.ts';

export interface RebuildCollectionSummary {
  collection: string;
  scoped: boolean;
  chunks: number;
  done: number;
  pending: number;
  failed: number;
  mongoDocs: number;
  liveRows: number;
  nullCdDocs: number;
  nullCdSwept: number;
}

export interface RebuildProgress {
  status: 'not_run' | 'running' | 'completed' | 'failed';
  phase: string;
  collectionsDone: number;
  collectionsTotal: number;
  summary: RebuildCollectionSummary[];
  /** checkOnly audits: windows where source count != live count */
  mismatchedWindows: Array<{ collection: string; lowerCd: string; upperCd: string; source: number; live: number }>;
  /** live > source: docs deleted from Mongo after migration (retention TTL, GDPR) — drift, not a defect. */
  deletionDriftWindows: Array<{ collection: string; lowerCd: string; upperCd: string; source: number; live: number }>;
  /** counts MATCH but the cd-sum fingerprint differs: same number of docs, WRONG docs (identity swap). */
  checksumMismatchWindows: Array<{ collection: string; lowerCd: string; upperCd: string; count: number; sumDeltaMs: number }>;
  error: string | null;
  startedAt: number | null;
  finishedAt: number | null;
}

export function newRebuildProgress(): RebuildProgress {
  return {
    status: 'not_run', phase: '', collectionsDone: 0, collectionsTotal: 0,
    summary: [], mismatchedWindows: [], deletionDriftWindows: [], checksumMismatchWindows: [], error: null, startedAt: null, finishedAt: null,
  };
}

/** Cap on null-cd outlier ids held in memory per collection. */
const MAX_NULLCD_IDS = 1_000_000;

export async function rebuildLedger(opts: {
  config: Config;
  logger: Logger;
  ledger: LedgerStore;
  dlq: DlqStore;
  hashResolver: HashResolver;
  progress: RebuildProgress;
  /**
   * Audit mode: recount every window (source Mongo vs scoped live ClickHouse)
   * and REPORT mismatches without touching the ledger. This is the defense
   * against the one silent-loss class count-based chunk verification cannot
   * see: a reader under-read whose tally is self-consistent — the SOURCE is
   * the truth, not the tally.
   */
  checkOnly?: boolean;
}): Promise<void> {
  const { config, ledger, dlq, hashResolver, progress, checkOnly = false } = opts;
  const logger = opts.logger.child({ component: 'LedgerRebuild' });
  const runId = config.ledger.runId;

  // Own connections — never disturbs the main orchestrator's bindings.
  const mongo = new MongoClient(config.source.uri);
  const staging = new StagingManager(
    {
      url: config.target.url, database: config.target.db, table: config.target.table,
      username: config.target.username, password: config.target.password,
      queryTimeoutMs: config.target.queryTimeoutMs,
    },
    logger,
  );

  try {
    await mongo.connect();
    await staging.connect();
    const db = mongo.db(config.source.db);

    progress.phase = 'discovering collections';
    let collections = await discoverCollections(db, config.source.collectionPrefix, logger);
    const skipEventNames = new Set(['[CLY]_apm_device', '[CLY]_apm_network']);
    collections = collections.filter((name) => {
      const defaults = hashResolver.resolveCollectionName(name, config.source.collectionPrefix);
      return !(defaults && skipEventNames.has(defaults.e));
    });
    progress.collectionsTotal = collections.length;

    const now = new Date();
    const allDocs: ChunkDoc[] = [];

    for (const collection of collections) {
      progress.phase = `analyzing ${collection}`;
      const coll = db.collection(collection);
      const defaults = hashResolver.resolveCollectionName(collection, config.source.collectionPrefix);
      const scope = defaults ? chScopeOf(defaults) : null;

      // cd span of the frozen source (same probes the engine uses)
      const [lowDoc] = await coll.find({ cd: { $type: 'date' } }).sort({ cd: 1 }).limit(1)
        .project({ cd: 1 }).toArray();
      const [highDoc] = await coll.find({ cd: { $type: 'date' } }).sort({ cd: -1 }).limit(1)
        .project({ cd: 1 }).toArray();

      const summary: RebuildCollectionSummary = {
        collection, scoped: !!scope, chunks: 0, done: 0, pending: 0, failed: 0,
        mongoDocs: 0, liveRows: 0, nullCdDocs: 0, nullCdSwept: 0,
      };

      // Null-cd outliers: fetch ids + live cd values so sweep rows can be
      // subtracted from the regular windows their ts-derived cd landed in.
      const nullCdIds: string[] = [];
      let derivedLo = Infinity, derivedHi = -Infinity;
      const idCursor = coll.find({ cd: null }, { projection: { _id: 1, ts: 1 } }).batchSize(10_000);
      for await (const doc of idCursor) {
        nullCdIds.push(String(doc._id));
        const tsMs = toEpochMillis(doc.ts);
        if (tsMs !== null && tsMs > 0) {
          const d = clampDateTime64(tsMs); // the sweep's derived cd
          if (d < derivedLo) derivedLo = d;
          if (d > derivedHi) derivedHi = d;
        }
        if (nullCdIds.length >= MAX_NULLCD_IDS) {
          throw new Error(`${collection}: more than ${MAX_NULLCD_IDS.toLocaleString('en-US')} null-cd documents — not outliers; rebuild does not support this shape`);
        }
      }
      summary.nullCdDocs = nullCdIds.length;
      const liveNullCd = nullCdIds.length > 0
        ? await staging.fetchLiveCdByIds(nullCdIds, derivedLo <= derivedHi ? { loMs: derivedLo, hiMs: derivedHi } : undefined)
        : new Map<string, number>();
      summary.nullCdSwept = liveNullCd.size;
      const sweptCds = [...liveNullCd.values()].sort((a, b) => a - b);

      let bounds: Array<{ lowerCd: number; upperCd: number }> = [];
      if (lowDoc && highDoc) {
        const lowerCd = (lowDoc.cd as Date).getTime();
        const upperCd = (highDoc.cd as Date).getTime();
        const estimated = await coll.estimatedDocumentCount();
        bounds = computeChunkBounds(lowerCd, upperCd, estimated, config.ledger.chunkDocsTarget, config.ledger.maxChunkDays);
      }

      let idx = 0;
      for (const b of bounds) {
        progress.phase = `counting ${collection} chunk ${idx + 1}/${bounds.length}`;
        // count + cd-sum in one index-covered pass: the sum is an order-free
        // fingerprint of WHICH docs the window holds, not just how many
        const [mongoAgg] = await coll.aggregate<{ n: number; sumCd: number }>([
          { $match: { cd: { $gte: new Date(b.lowerCd), $lt: new Date(b.upperCd) } } },
          { $group: { _id: null, n: { $sum: 1 }, sumCd: { $sum: { $mod: [{ $toLong: '$cd' }, 4294967296] } } } },
        ]).toArray();
        const mongoCount = mongoAgg?.n ?? 0;
        const mongoSumCd = mongoAgg?.sumCd ?? 0;
        const liveAgg = await staging.countAndSumLiveCdRange(b.lowerCd, b.upperCd, scope);
        const liveRaw = liveAgg.n;
        // Subtract this collection's sweep rows whose derived cd fell in-window
        let lo = 0, hi = sweptCds.length;
        while (lo < hi) { const m = (lo + hi) >> 1; if (sweptCds[m] < b.lowerCd) lo = m + 1; else hi = m; }
        let sweptIn = 0;
        let sweptSum = 0;
        for (let i = lo; i < sweptCds.length && sweptCds[i] < b.upperCd; i++) { sweptIn++; sweptSum += sweptCds[i] % 4294967296; } // same mod as both fingerprints
        const live = liveRaw - sweptIn;
        const liveSumCd = liveAgg.sumCd - sweptSum;

        // Docs in this window that are KNOWN unmigrated (pending/waived DLQ)
        // legitimately explain source > live — without this, a window whose
        // only shortfall is its own DLQ'd docs gets flagged/redone forever.
        const unresolved = await dlq.countUnresolvedInWindow(runId, collection, b.lowerCd, b.upperCd);

        // Unscopable collection (base drill_events with embedded a/e) among
        // OTHERS: an unscoped window count includes sibling rows, so no
        // classification is possible — mark pending. Redo is idempotent:
        // promotion pair-checks staged rows before every attach, so already
        // -migrated partitions are skipped, never duplicated.
        const unscopableInMulti = !scope && collections.length > 1;
        const status: ChunkDoc['status'] = unscopableInMulti
          ? 'pending'
          : live + unresolved === mongoCount ? 'done' : live === 0 ? 'pending' : 'failed';
        // pending (live=0) is 'not migrated yet', not a disagreement
        if (checkOnly && !unscopableInMulti && live + unresolved !== mongoCount && live !== 0) {
          // live > source = the SOURCE shrank after migration (retention
          // TTL, GDPR purges) — report as drift, not as a defect; only
          // live < source means data is missing from the target.
          const bucket = live + unresolved > mongoCount ? progress.deletionDriftWindows : progress.mismatchedWindows;
          if (bucket.length < 200) {
            bucket.push({
              collection, lowerCd: new Date(b.lowerCd).toISOString(), upperCd: new Date(b.upperCd).toISOString(),
              source: mongoCount, live,
            });
          }
        }
        // Checksum: only meaningful on windows that are count-exact with no
        // DLQ residue — equal counts hiding DIFFERENT docs is the one error
        // class pure counting cannot see. Number-safety: cd sums stay well
        // under 2^53 for any window a chunk target allows.
        if (checkOnly && !unscopableInMulti && unresolved === 0 && live === mongoCount && live > 0
            && liveSumCd !== mongoSumCd && progress.checksumMismatchWindows.length < 200) {
          progress.checksumMismatchWindows.push({
            collection, lowerCd: new Date(b.lowerCd).toISOString(), upperCd: new Date(b.upperCd).toISOString(),
            count: mongoCount, sumDeltaMs: liveSumCd - mongoSumCd,
          });
        }
        summary.mongoDocs += mongoCount;
        summary.liveRows += live;
        summary[status === 'done' ? 'done' : status === 'pending' ? 'pending' : 'failed']++;

        allDocs.push({
          _id: `${runId}:${collection}:${idx}`,
          run_id: runId, collection,
          scope_a: scope?.a ?? null, scope_e: scope?.e ?? null, scope_n: scope?.n ?? null,
          idx, lower_cd: b.lowerCd, upper_cd: b.upperCd,
          status, pod_id: null, lease_until: null, staging_table: null,
          docs_read: status === 'done' ? mongoCount : 0,
          docs_skipped: 0,
          rows_expected: status === 'done' ? mongoCount : 0,
          partitions: [], attached: [],
          attach_method: null, attempts: 0,
          last_error: status === 'failed' ? `rebuilt from data: live=${live} mongo=${mongoCount} — retry purges and redoes this window` : null,
          transform_version: config.transform.version,
          updated_at: now,
        });
        idx++;
      }

      // Sentinel sweep chunk for the null-cd outliers
      if (nullCdIds.length > 0) {
        const swept = liveNullCd.size;
        const status: ChunkDoc['status'] =
          swept === nullCdIds.length ? 'done' : swept === 0 ? 'pending' : 'failed';
        summary[status === 'done' ? 'done' : status === 'pending' ? 'pending' : 'failed']++;
        allDocs.push({
          _id: `${runId}:${collection}:${idx}`,
          run_id: runId, collection,
          scope_a: scope?.a ?? null, scope_e: scope?.e ?? null, scope_n: scope?.n ?? null,
          idx, lower_cd: -1, upper_cd: 0,
          status, pod_id: null, lease_until: null, staging_table: null,
          docs_read: status === 'done' ? nullCdIds.length : 0,
          docs_skipped: 0,
          rows_expected: status === 'done' ? nullCdIds.length : 0,
          partitions: [], attached: [],
          attach_method: null, attempts: 0,
          last_error: status === 'failed' ? `rebuilt from data: swept=${swept} of ${nullCdIds.length} null-cd docs` : null,
          transform_version: config.transform.version,
          updated_at: now,
        });
        idx++;
      }

      summary.chunks = idx;
      progress.summary.push(summary);
      progress.collectionsDone++;
      logger.info(summary, 'Collection analyzed');
    }

    if (checkOnly) {
      progress.phase = 'done';
      logger.info(
        { windows: allDocs.length, mismatches: progress.mismatchedWindows.length, deletionDrift: progress.deletionDriftWindows.length, checksumMismatches: progress.checksumMismatchWindows.length },
        'Source audit complete — ledger untouched',
      );
    } else {
      progress.phase = 'writing ledger';
      await ledger.replaceAllForRun(runId, allDocs);
      progress.phase = 'done';
      logger.info(
        { chunks: allDocs.length, collections: collections.length },
        'Ledger rebuilt from data — restart or resume the engine to continue the run',
      );
    }
  } finally {
    await mongo.close().catch(() => {});
    await staging.close().catch(() => {});
  }
}
