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
import { StagingManager } from '../target/staging-manager.ts';
import { discoverCollections } from '../source/discover-collections.ts';
import { computeChunkBounds } from './chunk-orchestrator.ts';
import { chScopeOf } from '../transform/hash-resolver.ts';

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
  error: string | null;
  startedAt: number | null;
  finishedAt: number | null;
}

export function newRebuildProgress(): RebuildProgress {
  return {
    status: 'not_run', phase: '', collectionsDone: 0, collectionsTotal: 0,
    summary: [], error: null, startedAt: null, finishedAt: null,
  };
}

/** Cap on null-cd outlier ids held in memory per collection. */
const MAX_NULLCD_IDS = 1_000_000;

export async function rebuildLedger(opts: {
  config: Config;
  logger: Logger;
  ledger: LedgerStore;
  hashResolver: HashResolver;
  progress: RebuildProgress;
}): Promise<void> {
  const { config, ledger, hashResolver, progress } = opts;
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
      const idCursor = coll.find({ cd: null }, { projection: { _id: 1 } }).batchSize(10_000);
      for await (const doc of idCursor) {
        nullCdIds.push(String(doc._id));
        if (nullCdIds.length >= MAX_NULLCD_IDS) {
          throw new Error(`${collection}: more than ${MAX_NULLCD_IDS.toLocaleString('en-US')} null-cd documents — not outliers; rebuild does not support this shape`);
        }
      }
      summary.nullCdDocs = nullCdIds.length;
      const liveNullCd = nullCdIds.length > 0 ? await staging.fetchLiveCdByIds(nullCdIds) : new Map<string, number>();
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
        const mongoCount = await coll.countDocuments({
          cd: { $gte: new Date(b.lowerCd), $lt: new Date(b.upperCd) },
        });
        const liveRaw = await staging.countLiveInCdRange(b.lowerCd, b.upperCd, scope);
        // Subtract this collection's sweep rows whose derived cd fell in-window
        let lo = 0, hi = sweptCds.length;
        while (lo < hi) { const m = (lo + hi) >> 1; if (sweptCds[m] < b.lowerCd) lo = m + 1; else hi = m; }
        let sweptIn = 0;
        for (let i = lo; i < sweptCds.length && sweptCds[i] < b.upperCd; i++) sweptIn++;
        const live = liveRaw - sweptIn;

        const status: ChunkDoc['status'] =
          live === mongoCount ? 'done' : live === 0 ? 'pending' : 'failed';
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

    progress.phase = 'writing ledger';
    await ledger.replaceAllForRun(runId, allDocs);
    progress.phase = 'done';
    logger.info(
      { chunks: allDocs.length, collections: collections.length },
      'Ledger rebuilt from data — restart or resume the engine to continue the run',
    );
  } finally {
    await mongo.close().catch(() => {});
    await staging.close().catch(() => {});
  }
}
