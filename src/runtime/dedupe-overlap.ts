/**
 * Tee-overlap dedupe: remove the duplicates a missing cd bound created.
 *
 * On a mirrored cutover (new cluster primary, nginx mirroring to the old
 * stack — or the reverse) a migration run WITHOUT LEDGER_CD_UPPER_BOUND
 * copies the mirror's re-ingested docs on top of the rows the new cluster
 * already ingested natively: every event in the overlap window exists twice
 * in ClickHouse, under two different _ids.
 *
 * The two copies are cleanly separable: the migrated copy carries an _id
 * that exists in the OLD cluster's Mongo; the native row's _id was minted by
 * the new cluster and does not. And because the mirror only ever re-ingests
 * requests the new cluster served first, every migrated row in the overlap
 * window duplicates a native row — deleting all id-matched rows in the
 * window removes exactly the duplicates, never data.
 *
 * Safety: dry-run by default (counts only); execute is refused until a dry
 * run over the SAME window has completed in this process, and the old
 * cluster's Mongo must still be reachable (it is the separator — this is
 * why cleanup must happen BEFORE the old stack is decommissioned).
 */

import type { Logger } from 'pino';
import { MongoClient } from 'mongodb';
import type { Config } from '../config/schema.ts';
import { StagingManager } from '../target/staging-manager.ts';
import { discoverCollections } from '../source/discover-collections.ts';

export interface DedupeOverlapState {
  status: 'not_run' | 'running' | 'completed' | 'failed';
  phase: string;
  execute: boolean;
  fromMs: number | null;
  toMs: number | null;
  collections: Array<{ collection: string; mongoDocsInWindow: number; chMatched: number; deleted: number }>;
  totals: { mongoDocsInWindow: number; chMatched: number; deleted: number };
  /** Window of the last COMPLETED dry run — the license to execute. */
  lastDryRun: { fromMs: number; toMs: number; chMatched: number; at: number } | null;
  error: string | null;
  startedAt: number | null;
  finishedAt: number | null;
}

export function newDedupeOverlapState(): DedupeOverlapState {
  return {
    status: 'not_run', phase: '', execute: false, fromMs: null, toMs: null,
    collections: [], totals: { mongoDocsInWindow: 0, chMatched: 0, deleted: 0 },
    lastDryRun: null, error: null, startedAt: null, finishedAt: null,
  };
}

const ID_BATCH = 200_000;

export async function runDedupeOverlap(
  deps: { config: Config; logger: Logger },
  state: DedupeOverlapState,
  opts: { fromMs: number; toMs: number; execute: boolean },
): Promise<void> {
  const { config } = deps;
  const logger = deps.logger.child({ component: 'DedupeOverlap' });
  const lastDry = state.lastDryRun;

  Object.assign(state, newDedupeOverlapState(), {
    status: 'running', startedAt: Date.now(), phase: 'starting',
    execute: opts.execute, fromMs: opts.fromMs, toMs: opts.toMs, lastDryRun: lastDry,
  });

  // Own connections, like the rebuild — never disturbs the orchestrator's.
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

    state.phase = 'discovering collections';
    const collections = await discoverCollections(db, config.source.collectionPrefix, logger);
    const from = new Date(opts.fromMs);
    const to = new Date(opts.toMs);

    for (const collection of collections) {
      state.phase = `scanning ${collection}`;
      const coll = db.collection(collection);
      const row = { collection, mongoDocsInWindow: 0, chMatched: 0, deleted: 0 };

      // Old-Mongo ids in the window = the mirror's re-ingested docs — the
      // exact set whose migrated copies are duplicates.
      let batch: string[] = [];
      const flush = async (): Promise<void> => {
        if (batch.length === 0) return;
        const matched = await staging.countMatchingIdsInWindow(batch, opts.fromMs, opts.toMs);
        row.chMatched += matched;
        if (opts.execute && matched > 0) {
          await staging.deleteMatchingIdsInWindow(batch, opts.fromMs, opts.toMs);
          row.deleted += matched;
        }
        batch = [];
      };
      const cursor = coll.find({ cd: { $gte: from, $lt: to } }, { projection: { _id: 1 } }).batchSize(10_000);
      for await (const doc of cursor) {
        row.mongoDocsInWindow++;
        batch.push(String(doc._id));
        if (batch.length >= ID_BATCH) await flush();
      }
      await flush();

      if (row.mongoDocsInWindow > 0 || row.chMatched > 0) state.collections.push(row);
      state.totals.mongoDocsInWindow += row.mongoDocsInWindow;
      state.totals.chMatched += row.chMatched;
      state.totals.deleted += row.deleted;
    }

    state.status = 'completed';
    state.phase = 'done';
    state.finishedAt = Date.now();
    if (!opts.execute) {
      state.lastDryRun = { fromMs: opts.fromMs, toMs: opts.toMs, chMatched: state.totals.chMatched, at: Date.now() };
    }
    logger.info(
      { execute: opts.execute, ...state.totals, collections: state.collections.length },
      opts.execute ? 'Tee-overlap duplicates deleted' : 'Tee-overlap dedupe dry run complete — nothing deleted',
    );
  } catch (err) {
    state.status = 'failed';
    state.error = (err as Error).message;
    state.phase = 'failed';
    state.finishedAt = Date.now();
    logger.error({ err }, 'Tee-overlap dedupe failed');
  } finally {
    await mongo.close().catch(() => {});
    await staging.close().catch(() => {});
  }
}
