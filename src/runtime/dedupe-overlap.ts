/**
 * Tee-overlap dedupe: remove the duplicates a missing cd bound created.
 *
 * On a mirrored cutover (new cluster primary, nginx mirroring to the old
 * stack — or the reverse) a migration run WITHOUT LEDGER_CD_UPPER_BOUND
 * copies the mirror's re-ingested docs on top of the rows the new cluster
 * already ingested natively: every event in the overlap window exists twice
 * in ClickHouse, under two different _ids.
 *
 * The migrated copy is identifiable — its _id exists in the OLD cluster's
 * Mongo; a native row's _id was minted by the new cluster and does not.
 * But an id match alone is NOT proof of duplication: when the tee (or the
 * new cluster's ingestion) dropped a request, the migrated row is the ONLY
 * copy of that event, and deleting it would lose data. The identities
 * differ per side, so no per-event pairing exists — instead every hour
 * bucket must carry COUNT evidence of native counterparts:
 *
 *   native(bucket) = live rows in bucket − id-matched rows in bucket
 *   safe          ⇔ native ≥ matched − slack
 *
 * In a healthy tee every matched row duplicates a native one, so native is
 * at least matched (plus mirror losses only ever shrink matched). A bucket
 * where native falls short holds migrated rows WITHOUT counterparts —
 * those are skipped, reported, and never deleted. Strictness is default:
 * ZERO slack, so even one uncovered matched row marks the bucket unsafe;
 * ingest-timing straddle at bucket edges can flag a few healthy buckets,
 * and the operator may consciously allow it with slackPct (≤5%). Known
 * limit of count evidence: a loss exactly offset by mirror-dropped natives
 * in the SAME hour is invisible — which is why unsafe hours must be taken
 * seriously, not overridden casually.
 *
 * Safety: dry-run by default (counts only); execute is refused until a dry
 * run over the SAME window has completed in this process, and the old
 * cluster's Mongo must still be reachable (it is the separator — cleanup
 * must happen BEFORE the old stack is decommissioned).
 */

import type { Logger } from 'pino';
import { MongoClient } from 'mongodb';
import type { Config } from '../config/schema.ts';
import type { HashResolver } from '../transform/hash-resolver.ts';
import type { LedgerStore } from '../state/ledger-store.ts';
import { chScopeOf } from '../transform/hash-resolver.ts';
import { StagingManager } from '../target/staging-manager.ts';
import { discoverCollections } from '../source/discover-collections.ts';

export interface DedupeUnsafeBucket {
  fromMs: number;
  toMs: number;
  matched: number;
  native: number;
  /** Why the bucket was skipped: missing native counterpart evidence, or a collection whose live counts cannot be scoped. */
  reason: 'no-native-evidence' | 'no-scope';
}

export interface DedupeCollectionRow {
  collection: string;
  /** Scoped (a,e,n) live counts — exact safety evidence. Unscoped rows use table-wide counts (weaker). */
  scoped: boolean;
  mongoDocsInWindow: number;
  chMatched: number;
  deleted: number;
  /** Buckets whose migrated rows lack count-evidence of native counterparts — never deleted. */
  unsafe: DedupeUnsafeBucket[];
}

export interface DedupeOverlapState {
  status: 'not_run' | 'running' | 'completed' | 'failed';
  phase: string;
  execute: boolean;
  fromMs: number | null;
  toMs: number | null;
  collections: DedupeCollectionRow[];
  totals: { mongoDocsInWindow: number; chMatched: number; deleted: number; unsafeMatched: number };
  /** Window + slack + run fingerprint of the last COMPLETED dry run — the license to execute (same window, same slack, unchanged run state). */
  lastDryRun: { fromMs: number; toMs: number; slackPct: number; chMatched: number; fingerprint: string | null; at: number } | null;
  /** Set when the run's chunk state changed while dedupe scanned — counts are stale; re-run the dry run. */
  runStateChanged: boolean;
  error: string | null;
  startedAt: number | null;
  finishedAt: number | null;
}

export function newDedupeOverlapState(): DedupeOverlapState {
  return {
    status: 'not_run', phase: '', execute: false, fromMs: null, toMs: null,
    collections: [], totals: { mongoDocsInWindow: 0, chMatched: 0, deleted: 0, unsafeMatched: 0 },
    lastDryRun: null, runStateChanged: false, error: null, startedAt: null, finishedAt: null,
  };
}

export const effectiveSlackPct = (v: number | undefined): number => Math.min(5, Math.max(0, v ?? 0));

// same shape cap as the rebuild: null-cd docs are outliers by construction
const MAX_NULLCD_IDS = 1_000_000;
const BUCKET_MS = 3_600_000;
/** Hard ceiling on one bucket's ids held in memory — pick a smaller window if hit. */
const MAX_BUCKET_IDS = 3_000_000;

export async function runDedupeOverlap(
  deps: { config: Config; logger: Logger; hashResolver: HashResolver; ledger?: LedgerStore },
  state: DedupeOverlapState,
  opts: { fromMs: number; toMs: number; execute: boolean; slackPct?: number; expectedFingerprint?: string | null },
): Promise<void> {
  const { config, hashResolver } = deps;
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
    // fail closed: without the initial fingerprint the staleness guard is
    // blind, and execute could be licensed against unreviewed counts.
    // Execute ANCHORS to the licensed fingerprint from the reviewed dry run
    // — the fresh read must EQUAL it, never replace it: adopting a moved
    // baseline would bless changes the operator never reviewed.
    let fpBefore: string | null = null;
    if (deps.ledger) {
      fpBefore = await deps.ledger.runFingerprint(config.ledger.runId);
      if (opts.execute && opts.expectedFingerprint != null && fpBefore !== opts.expectedFingerprint) {
        throw new Error('run chunk state changed since the reviewed dry run — execute refused before scanning; re-run the dry run with all pods idle');
      }
      if (opts.execute && opts.expectedFingerprint != null) fpBefore = opts.expectedFingerprint;
    }

    state.phase = 'discovering collections';
    const collections = await discoverCollections(db, config.source.collectionPrefix, logger);
    const from = new Date(opts.fromMs);
    const to = new Date(opts.toMs);

    for (const collection of collections) {
      state.phase = `scanning ${collection}`;
      const coll = db.collection(collection);
      const defaults = hashResolver.resolveCollectionName(collection, config.source.collectionPrefix);
      const scope = defaults ? chScopeOf(defaults) : null;
      const row: DedupeCollectionRow = { collection, scoped: !!scope, mongoDocsInWindow: 0, chMatched: 0, deleted: 0, unsafe: [] };

      // Migrated null-cd docs land in ClickHouse at a ts-DERIVED cd — inside
      // the overlap window they are counted by the live totals below, but the
      // cd-ordered cursor never selects their Mongo docs, so uncorrected they
      // would read as NATIVE counterparts and could vouch for deleting rows
      // that are the only copy of their event. Count them per hour bucket and
      // subtract them from the evidence. (Only scoped collections matter:
      // unscoped ones never reach the evidence test.)
      const sweepByBucket = new Map<number, number>();
      if (scope) {
        const nullCdIds: string[] = [];
        const nullCursor = coll.find({ cd: null }, { projection: { _id: 1 } }).batchSize(10_000);
        for await (const doc of nullCursor) {
          nullCdIds.push(String(doc._id));
          if (nullCdIds.length > MAX_NULLCD_IDS) {
            throw new Error(`${collection}: more than ${MAX_NULLCD_IDS.toLocaleString('en-US')} null-cd documents — their sweep rows cannot be separated from native evidence; this collection needs manual review`);
          }
        }
        if (nullCdIds.length > 0) {
          // ROW counts, not distinct ids: duplicate sweep copies (ambiguous
          // insert retries) are in liveTotal too and must all be subtracted
          const counted = await staging.countRowsByHourBucket(nullCdIds, opts.fromMs, opts.toMs, scope);
          for (const [b, n] of counted) sweepByBucket.set(b, (sweepByBucket.get(b) ?? 0) + n);
        }
      }

      const processBucket = async (pairs: Array<{ id: string; cdMs: number }>, loMs: number, hiMs: number): Promise<void> => {
        if (pairs.length === 0) return;
        // PAIR-exact matching, never id-in-window: a native retry that
        // reused a migrated doc's _id at a slightly different cd in the
        // same hour is native traffic — counting or deleting it as the
        // migrated copy would lose the only surviving copy of its event.
        // (Scope still guards the coincidence of a sibling collection's row
        // sharing the exact same pair.)
        const matched = await staging.countMatchingPairs(pairs, scope);
        row.chMatched += matched;
        state.totals.chMatched += matched;
        if (matched === 0) return;
        // No (a,e,n) scope → live counts are TABLE-WIDE and sibling
        // collections' native traffic would vouch for this one's outage
        // buckets. No usable evidence — never delete, always report.
        if (!scope) {
          row.unsafe.push({ fromMs: loMs, toMs: hiMs, matched, native: -1, reason: 'no-scope' });
          state.totals.unsafeMatched += matched;
          return;
        }
        // Count evidence of native counterparts: what remains in this bucket
        // after the matched rows is the native side. Falling short means some
        // migrated rows are the ONLY copy of their event — never delete those.
        const liveTotal = await staging.countLiveInCdRange(loMs, hiMs, scope);
        // sweep rows are MIGRATED, not native — they never count as evidence
        const sweep = sweepByBucket.get(Math.floor(loMs / BUCKET_MS) * BUCKET_MS) ?? 0;
        const native = liveTotal - matched - sweep;
        // strict by default: every matched row needs a native counterpart in
        // its bucket. slackPct (operator-chosen, ≤5%) only absorbs
        // ingest-timing straddle at bucket edges; zero natives is the outage
        // signature outright and no slack ever waves it through
        const slack = Math.ceil(matched * (effectiveSlackPct(opts.slackPct) / 100));
        if (native < matched - slack || native <= 0) {
          row.unsafe.push({ fromMs: loMs, toMs: hiMs, matched, native, reason: 'no-native-evidence' });
          state.totals.unsafeMatched += matched;
          return;
        }
        if (opts.execute) {
          // durable fence before EVERY actual DELETE command: the stride is
          // the staging layer's own page size (same constant, so they cannot
          // drift), meaning each fenced call issues exactly one command.
          // Within one command the exposure is milliseconds; an attach takes
          // a chunk's full read-transform-insert-verify cycle.
          for (let i = 0; i < pairs.length; i += StagingManager.ID_PARAM_PAGE) {
            if (deps.ledger && fpBefore !== null) {
              const fpNow = await deps.ledger.runFingerprint(config.ledger.runId);
              if (fpNow !== fpBefore) {
                throw new Error('run chunk state changed during execute — aborted before the next delete page; re-run the dry run with all pods idle');
              }
            }
            await staging.deleteLiveByPairs(pairs.slice(i, i + StagingManager.ID_PARAM_PAGE), scope);
          }
          row.deleted += matched;
          state.totals.deleted += matched;
        }
      };

      // Old-Mongo (_id, cd) pairs in the window (cd order → contiguous hour buckets)
      let bucketStart = -1;
      let pairs: Array<{ id: string; cdMs: number }> = [];
      const cursor = coll.find({ cd: { $gte: from, $lt: to } }, { projection: { _id: 1, cd: 1 } })
        .sort({ cd: 1 }).batchSize(10_000);
      for await (const doc of cursor) {
        row.mongoDocsInWindow++;
        state.totals.mongoDocsInWindow++;
        const cdMs = (doc.cd as Date).getTime();
        const bucket = Math.floor(cdMs / BUCKET_MS) * BUCKET_MS;
        if (bucket !== bucketStart) {
          await processBucket(pairs, Math.max(bucketStart, opts.fromMs), Math.min(bucketStart + BUCKET_MS, opts.toMs));
          bucketStart = bucket;
          pairs = [];
        }
        pairs.push({ id: String(doc._id), cdMs });
        if (pairs.length > MAX_BUCKET_IDS) {
          throw new Error(`${collection}: more than ${MAX_BUCKET_IDS.toLocaleString('en-US')} docs in one hour bucket — run the dedupe over a smaller {fromMs, toMs} window`);
        }
      }
      await processBucket(pairs, Math.max(bucketStart, opts.fromMs), Math.min(bucketStart + BUCKET_MS, opts.toMs));

      if (row.mongoDocsInWindow > 0 || row.chMatched > 0) state.collections.push(row);
    }

    // Dedupe is a POST-COMPLETION tool: the claims fence at start is a
    // snapshot, so if anyone re-opened work mid-scan (retry-failed, top-up
    // mapping) the counts above are stale — detect and say so rather than
    // hold a cluster-wide claim barrier for an operator-induced edge case.
    let fpAfter: string | null = null;
    if (deps.ledger && fpBefore !== null) {
      // durable marker, not a claims poll: work that starts AND finishes
      // during the scan still moves the fingerprint
      fpAfter = await deps.ledger.runFingerprint(config.ledger.runId).catch(() => null);
      if (fpAfter !== fpBefore) {
        state.runStateChanged = true;
        logger.warn({ fpBefore, fpAfter }, 'Run chunk state changed during dedupe — counts are stale; re-run the dry run once the pods are idle');
      }
    }
    state.status = 'completed';
    state.phase = 'done';
    state.finishedAt = Date.now();
    if (!opts.execute) {
      state.lastDryRun = { fromMs: opts.fromMs, toMs: opts.toMs, slackPct: effectiveSlackPct(opts.slackPct), chMatched: state.totals.chMatched, fingerprint: fpAfter, at: Date.now() };
    }
    logger.info(
      { execute: opts.execute, ...state.totals, collections: state.collections.length },
      opts.execute
        ? (state.totals.unsafeMatched > 0
          ? 'Tee-overlap duplicates deleted — SOME BUCKETS SKIPPED: migrated rows there lack native counterparts (see unsafe buckets)'
          : 'Tee-overlap duplicates deleted')
        : 'Tee-overlap dedupe dry run complete — nothing deleted',
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
