/**
 * Mirror mode: live replication of NEW drill_events writes from the old
 * (still-authoritative) Mongo cluster into ClickHouse, for the mirror-first
 * playbook — customer keeps the old architecture, validates the new one
 * side-by-side, and the bulk migration backfills history up to the
 * checkpoint this mirror records when it first starts.
 *
 * Design invariants:
 *  - ONE database-level change stream, namespace-filtered to the drill
 *    prefix (never thousands of per-collection streams).
 *  - The transform is the SAME transform v2 the bulk migration uses — the
 *    two halves of the timeline are parity-identical by construction, and
 *    cd is preserved, so (_id, cd) provenance works across the seam.
 *  - Checkpoint T0 is captured AFTER the stream is open: anything that
 *    slips into the overlap is delivered by the stream AND migrated
 *    (cd < T0), and converges through the pair-checked insert below —
 *    the ordering makes loss impossible, only convergent overlap.
 *  - Crash-safe: resume token persisted after every flushed batch; on
 *    redelivery the pair-check skips rows already live with the same
 *    (_id, cd).
 *  - A ClickHouse outage makes the mirror LAG, never lose: the batch is
 *    retried with backoff until it lands (the stream buffers behind it).
 */
import { MongoClient, type ChangeStream, type Document } from 'mongodb';
import type { Logger } from 'pino';

import type { Config } from '../config/schema.ts';
import type { StagingManager } from '../target/staging-manager.ts';
import type { DlqStore } from '../state/dlq-store.ts';
import type { MirrorStore } from '../state/mirror-store.ts';
import type { HashResolver } from '../transform/hash-resolver.ts';
import { transformDocument, type SourceDocument, type OutputRow } from '../transform/normalize.ts';
import { CoercionCounter } from '../transform/coercions.ts';
import { SkipReason } from '../transform/skip-reasons.ts';
import { classifyError } from './error-classifier.ts';

interface MirrorDeps {
  config: Config;
  logger: Logger;
  staging: StagingManager;
  dlq: DlqStore;
  mirrorStore: MirrorStore;
  hashResolver: HashResolver;
}

interface PendingEvent {
  collection: string;
  doc: SourceDocument;
  token: Document;
  clusterTimeMs: number | null;
}

const cdMsOf = (r: OutputRow): number => Date.parse(r.cd.replace(' ', 'T') + 'Z');

export class MirrorEngine {
  private readonly d: MirrorDeps;
  private readonly logger: Logger;
  private readonly coercions = new CoercionCounter();
  private client: MongoClient | null = null;
  private stream: ChangeStream | null = null;
  private running = false;
  private status: 'idle' | 'running' | 'stopped' | 'failed' = 'idle';
  private fatalError: string | null = null;
  private checkpointMs = 0;
  private docsMirrored = 0;
  private docsSkipped = 0;
  private docsDlq = 0;
  private nonInsertOps = 0;
  private lastEventMs: number | null = null;
  private atHead = false;
  private lastTouchMs = 0;
  private readonly unknownCollections = new Set<string>();

  constructor(deps: MirrorDeps) {
    this.d = deps;
    this.logger = deps.logger.child({ component: 'MirrorEngine' });
  }

  getStats(): Record<string, unknown> {
    return {
      status: this.status,
      fatalError: this.fatalError,
      checkpointMs: this.checkpointMs,
      docsMirrored: this.docsMirrored,
      docsSkipped: this.docsSkipped,
      docsDlq: this.docsDlq,
      nonInsertOps: this.nonInsertOps,
      atHead: this.atHead,
      lagMs: this.atHead ? 0 : this.lastEventMs !== null ? Math.max(0, Date.now() - this.lastEventMs) : null,
    };
  }

  stop(): void {
    this.running = false;
  }

  async run(): Promise<void> {
    const { config, mirrorStore } = this.d;
    const runId = config.ledger.runId;
    this.status = 'running';
    this.running = true;

    this.client = new MongoClient(config.source.uri);
    await this.client.connect();
    const db = this.client.db(config.source.db);
    const prefix = config.source.collectionPrefix;

    const existing = await mirrorStore.load(runId);
    const pipeline = [
      { $match: { 'ns.coll': { $regex: `^${prefix}` } } },
    ];
    this.stream = db.watch(pipeline, {
      ...(existing?.resume_token ? { resumeAfter: existing.resume_token } : {}),
      maxAwaitTimeMS: config.mirror.batchMs,
      batchSize: 1_000,
    });

    // Checkpoint AFTER the stream is open: docs landing between stream-open
    // and this timestamp are BOTH streamed and covered by the bulk bound —
    // convergent overlap, never a gap.
    let t0 = Date.now();
    try {
      const ss = await this.client.db('admin').command({ serverStatus: 1 });
      if (ss.localTime instanceof Date) t0 = ss.localTime.getTime();
    } catch { /* restricted deployments: wall clock is close enough with the overlap ordering */ }
    const state = existing ?? await mirrorStore.init(runId, t0);
    this.checkpointMs = state.checkpoint_ms;
    this.docsMirrored = state.docs_mirrored;
    this.docsSkipped = state.docs_skipped;
    this.docsDlq = state.docs_dlq;
    this.nonInsertOps = state.non_insert_ops;
    this.lastEventMs = state.last_event_ms;
    this.logger.info(
      { runId, checkpoint: new Date(this.checkpointMs).toISOString(), resumed: !!existing?.resume_token },
      existing ? 'Mirror resumed from saved token' : 'Mirror started — record this checkpoint as LEDGER_CD_UPPER_BOUND for the bulk migration',
    );

    let batch: PendingEvent[] = [];
    let nonInsertPending = 0;
    let lastToken: Document | null = null;
    try {
      while (this.running) {
        let ev: Document | null = null;
        try {
          ev = await this.stream.tryNext();
        } catch (err) {
          const msg = (err as Error).message ?? String(err);
          if (/resume token|ChangeStreamHistoryLost|oplog/i.test(msg)) {
            this.fatalError = `Change stream cannot resume (oplog rolled past the saved token): ${msg}. ` +
              'The mirror has a GAP — do not switch traffic. Re-run the bulk migration for the gap window ' +
              '(map with LEDGER_CD_UPPER_BOUND=now after restarting the mirror fresh), then verify.';
            this.status = 'failed';
            this.logger.error({ err: msg }, this.fatalError);
            return;
          }
          this.logger.warn({ err: msg }, 'Change stream read error — retrying');
          await new Promise((r) => setTimeout(r, 2_000));
          continue;
        }

        if (ev) {
          this.atHead = false;
          lastToken = ev._id as Document;
          const ct = ev.clusterTime && typeof (ev.clusterTime as { getHighBits?: () => number }).getHighBits === 'function'
            ? (ev.clusterTime as { getHighBits: () => number }).getHighBits() * 1000
            : null;
          if (ev.operationType === 'insert' && ev.fullDocument) {
            batch.push({
              collection: (ev.ns as { coll: string }).coll,
              doc: ev.fullDocument as SourceDocument,
              token: ev._id as Document,
              clusterTimeMs: ct,
            });
          } else if (ev.operationType === 'invalidate') {
            this.fatalError = 'Change stream invalidated (database dropped/renamed) — mirror stopped';
            this.status = 'failed';
            this.logger.error(this.fatalError);
            return;
          } else {
            // updates/deletes/drops on drill data are NOT mirrored (v1 is
            // insert-only); surfaced so the cutover runbook's reconciliation
            // step knows whether it has work to do.
            nonInsertPending++;
            if (ct) this.lastEventMs = ct;
          }
          if (batch.length < this.d.config.mirror.batchDocs) continue;
        }
        if (batch.length === 0 && nonInsertPending === 0) {
          // tryNext returned empty: we are AT THE STREAM HEAD — heartbeat so
          // other pods' dashboards can tell "caught up" from "mirror died"
          this.atHead = true;
          if (Date.now() - this.lastTouchMs > 15_000) {
            this.lastTouchMs = Date.now();
            await mirrorStore.touch(runId, true).catch(() => {});
          }
          continue;
        }

        await this.flush(batch, nonInsertPending, lastToken!);
        batch = [];
        nonInsertPending = 0;
      }
      this.status = 'stopped';
      this.logger.info('Mirror stopped');
    } finally {
      await this.stream.close().catch(() => {});
      await this.client.close().catch(() => {});
    }
  }

  /** Transform + pair-check + insert one batch; retried until it lands. */
  private async flush(batch: PendingEvent[], nonInsert: number, token: Document): Promise<void> {
    const { config, staging, dlq, mirrorStore } = this.d;
    const prefix = config.source.collectionPrefix;

    const rows: OutputRow[] = [];
    let skipped = 0;
    let dlqd = 0;
    let lastEventMs: number | null = null;
    for (const ev of batch) {
      if (ev.clusterTimeMs) lastEventMs = ev.clusterTimeMs;
      let defaults = this.d.hashResolver.resolveCollectionName(ev.collection, prefix) ?? undefined;
      if (!defaults && ev.collection !== prefix && !this.unknownCollections.has(ev.collection)) {
        // A collection created after startup (new app/event): refresh the
        // resolver once per unknown name, then give up quietly on repeats.
        this.unknownCollections.add(ev.collection);
        await this.d.hashResolver.build().catch(() => {});
        defaults = this.d.hashResolver.resolveCollectionName(ev.collection, prefix) ?? undefined;
      }
      const { row, skipReason } = transformDocument(ev.doc, defaults, this.coercions);
      if (row) rows.push(row);
      else if (skipReason === SkipReason.TRANSFORM_ERROR) {
        dlqd++;
        await dlq.add([{
          run_id: config.ledger.runId, collection: ev.collection, chunk_id: 'mirror',
          source_id: String((ev.doc as { _id?: unknown })._id ?? ''),
          reason: 'transform_error', error: 'mirror: transform failed',
          raw_doc: ev.doc as Record<string, unknown>, transform_version: config.transform.version,
        }]).catch((e) => this.logger.warn({ err: (e as Error).message }, 'Mirror DLQ write failed'));
      } else {
        skipped++;
      }
    }

    // Retry-until-landed: a target outage makes the mirror lag, never lose.
    for (let attempt = 0; ; attempt++) {
      try {
        let toInsert = rows;
        if (rows.length > 0) {
          // Redelivery (crash between insert and token save) and the
          // checkpoint overlap both converge here: identical (_id, cd)
          // pairs already live are skipped.
          const cdVals = rows.map(cdMsOf);
          const liveCd = await staging.fetchLiveCdByIds(
            rows.map((r) => r._id),
            { loMs: Math.min(...cdVals), hiMs: Math.max(...cdVals) },
          );
          toInsert = rows.filter((r) => liveCd.get(r._id) !== cdMsOf(r));
          if (toInsert.length > 0) {
            const tokenKey = JSON.stringify(token).slice(-48);
            await staging.insertIntoLive(toInsert, `mirror:${tokenKey}`);
          }
        }
        this.docsMirrored += toInsert.length;
        this.docsSkipped += skipped;
        this.docsDlq += dlqd;
        this.nonInsertOps += nonInsert;
        if (lastEventMs) this.lastEventMs = lastEventMs;
        await mirrorStore.progress(config.ledger.runId, token,
          { mirrored: toInsert.length, skipped, dlq: dlqd, nonInsert }, lastEventMs);
        return;
      } catch (err) {
        if (!this.running) throw err;
        const cls = classifyError(err);
        const delay = Math.min(30_000, 1_000 * 2 ** Math.min(attempt, 5));
        this.logger.warn(
          { err: (err as Error).message, class: cls, attempt, retryInMs: delay },
          'Mirror flush failed — retrying (mirror lags, never drops)',
        );
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }
}
