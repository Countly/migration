/**
 * ChunkOrchestrator — the `ledger` engine.
 *
 * Work model: each collection is split into cd-bounded chunks. Per chunk:
 * claim (atomic, leased, newest-first) → stream-copy into a per-chunk staging
 * table (one long-lived cursor; concurrent synchronous inserts) → verify
 * (read tally vs exact ClickHouse count) → promote (verify-then-ATTACH per
 * partition, INSERT SELECT fallback) → drop staging → done.
 *
 * Failure model:
 *  - permanent insert errors are BISECTED down to the offending documents,
 *    which land in the DLQ with their full raw source doc (replayable);
 *  - a circuit breaker pauses the engine when failures look systematic;
 *  - ClickHouse parts pressure is respected via a TTL-cached sampler;
 *  - crash recovery never trusts the ledger: in_progress → drop + redo,
 *    written → recount, attaching → verify-then-attach per partition;
 *  - an invariant monitor spot-checks done chunks against the live table.
 *
 * No Redis anywhere; MongoDB (ledger + DLQ) + ClickHouse are the only
 * dependencies. Dry-run mode targets a Null-engine clone with ≤5% sampling.
 */

import type { Logger } from 'pino';
import type { Config } from '../config/schema.ts';
import type { MongoReader } from '../source/mongo-reader.ts';
import type { HashResolver, CollectionDefaults } from '../transform/hash-resolver.ts';
import { chScopeOf, type ChScope } from '../transform/hash-resolver.ts';
import { toEpochMillis, clampDateTime64 } from '../transform/validators.ts';
import type { RetryPolicy } from './retry-policy.ts';
import type { ClickHousePressure, PressureState } from '../target/clickhouse-pressure.ts';
import { LedgerStore, type ChunkDoc } from '../state/ledger-store.ts';
import { DlqStore } from '../state/dlq-store.ts';
import { StagingManager } from '../target/staging-manager.ts';
import { transformDocument, type OutputRow, type SourceDocument } from '../transform/normalize.ts';
import { SkipCounter, SkipReason } from '../transform/skip-reasons.ts';
import { CoercionCounter } from '../transform/coercions.ts';
import { classifyError } from './error-classifier.ts';
import { discoverCollections } from '../source/discover-collections.ts';
import type { Cursor } from '../types/cursor.ts';
import { createHash } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';

export interface ChunkOrchestratorDeps {
  config: Config;
  logger: Logger;
  mongoReader: MongoReader;
  ledger: LedgerStore;
  dlq: DlqStore;
  staging: StagingManager;
  retryPolicy: RetryPolicy;
  hashResolver: HashResolver;
  chPressure?: ClickHousePressure;
}

export interface LedgerEngineStats {
  engine: 'ledger';
  runId: string;
  podId: string;
  status: string;
  fatalError: string | null;
  dryRun: boolean;
  currentCollection: string | null;
  currentChunk: string | null;
  totalDocsRead: number;
  totalDocsSkipped: number;
  totalRowsInserted: number;
  totalDocsDlq: number;
  totalCoercions: number;
  chunksDone: number;
  chunksFailed: number;
  docsPerSecond: number;
  stageMs: { read: number; transform: number; insert: number; verify: number; attach: number; pressureWait: number };
  dedupWorks: boolean | null;
  chunkStatusCounts: Record<string, number>;
}

/**
 * Chunk grid from cd span + doc estimate. Two independent sizing signals:
 * doc estimate AND time span — the span floor protects against a corrupted
 * estimate (metadata fastcount resets after unclean mongod shutdowns)
 * producing a whole-collection chunk. Pure; shared with ledger rebuild.
 */
export function computeChunkBounds(
  lowerCd: number,
  upperCd: number,
  estimated: number,
  chunkDocsTarget: number,
  maxChunkDays: number,
): Array<{ lowerCd: number; upperCd: number }> {
  const spanMs = upperCd + 1 - lowerCd;
  const byDocs = Math.ceil(estimated / chunkDocsTarget);
  const bySpan = Math.ceil(spanMs / (maxChunkDays * 86_400_000));
  const chunkCount = Math.max(1, Math.min(50_000, Math.max(byDocs, bySpan)));
  const bounds: Array<{ lowerCd: number; upperCd: number }> = [];
  for (let i = 0; i < chunkCount; i++) {
    const lo = lowerCd + Math.floor((spanMs * i) / chunkCount);
    const hi = i === chunkCount - 1 ? upperCd + 1 : lowerCd + Math.floor((spanMs * (i + 1)) / chunkCount);
    if (hi > lo) bounds.push({ lowerCd: lo, upperCd: hi });
  }
  return bounds;
}

const MAX_CHUNK_ATTEMPTS = 3;
const BISECT_LOG_THRESHOLD = 1;

function shortHash(s: string): string {
  return createHash('sha1').update(s).digest('hex').slice(0, 8);
}

export class ChunkOrchestrator {
  private readonly d: ChunkOrchestratorDeps;
  private readonly logger: Logger;
  private readonly runId: string;
  private readonly podId: string;
  private readonly dryRun: boolean;

  private status = 'idle';
  private fatalError: string | null = null;
  private multiCollection = false;
  private finishedAt = 0;
  /** Live progress of a running verify (billion-scale runs take minutes). */
  readonly verifyProgress = { running: false, checked: 0, total: 0, phase: '' };
  private stopping = false;
  private paused = false;
  private currentCollection: string | null = null;
  private currentChunk: string | null = null;
  private startedAt = 0;
  private consecutiveFailed = 0;
  private lastReclaimAt = 0;
  private monitorTimer: ReturnType<typeof setInterval> | null = null;

  private readonly coercions = new CoercionCounter();
  private readonly skips = new SkipCounter();
  private totalDocsRead = 0;
  private totalDocsSkipped = 0;
  private totalRowsInserted = 0;
  private totalDocsDlq = 0;
  private chunksDone = 0;
  private chunksFailed = 0;
  private stageMs = { read: 0, transform: 0, insert: 0, verify: 0, attach: 0, pressureWait: 0 };
  private lastStatusCounts: Record<string, number> = {};

  private lastPressure: { state: PressureState; at: number } | null = null;

  constructor(deps: ChunkOrchestratorDeps) {
    this.d = deps;
    this.logger = deps.logger.child({ component: 'ChunkOrchestrator' });
    this.dryRun = deps.config.ledger.dryRun;
    this.runId = this.dryRun ? `${deps.config.ledger.runId}-dry` : deps.config.ledger.runId;
    this.podId = deps.config.worker.podId;
  }

  // -------------------------------------------------------------------------
  // Controls
  // -------------------------------------------------------------------------

  stopAfterChunk(): void { this.stopping = true; }
  pause(): void { this.paused = true; if (this.status === 'running') this.status = 'paused'; }
  resume(): void { this.paused = false; if (this.status === 'paused') this.status = 'running'; }
  getStatus(): string { return this.status; }

  /** ClickHouse row-identity scope of a chunk's collection; null when unresolvable. */
  private scopeOf(chunk: ChunkDoc): ChScope | null {
    if (!chunk.scope_a || !chunk.scope_e) return null;
    return { a: chunk.scope_a, e: chunk.scope_e, ...(chunk.scope_n ? { n: chunk.scope_n } : {}) };
  }

  /** A startup/config failure should not kill the console — surface it instead. */
  markFatal(message: string): void {
    this.status = 'failed';
    this.fatalError = message;
    this.finishedAt = Date.now();
  }

  // -------------------------------------------------------------------------
  // Main
  // -------------------------------------------------------------------------

  async run(): Promise<void> {
    this.status = 'running';
    this.startedAt = Date.now();
    this.finishedAt = 0;
    const { config } = this.d;

    if (this.dryRun) {
      await this.d.staging.createDryRunTable();
      this.logger.warn(
        { samplePct: config.ledger.dryRunSamplePct },
        'DRY RUN: sampled rehearsal against a Null-engine clone — nothing is stored, nothing is promoted',
      );
    } else {
      await this.d.staging.runDedupCanary();
      this.startInvariantMonitor();
    }

    const db = this.d.mongoReader.getDatabase();
    let collections = await discoverCollections(db, config.source.collectionPrefix, this.logger);

    const skipEventNames = new Set(['[CLY]_apm_device', '[CLY]_apm_network']);
    collections = collections.filter((name) => {
      const defaults = this.d.hashResolver.resolveCollectionName(name, config.source.collectionPrefix);
      return !(defaults && skipEventNames.has(defaults.e));
    });

    this.multiCollection = collections.length > 1;
    this.logger.info({ collections: collections.length, runId: this.runId, dryRun: this.dryRun }, 'Ledger engine starting');

    for (const collection of collections) {
      if (this.stopping) break;
      await this.processCollection(collection);
    }

    if (this.monitorTimer) clearInterval(this.monitorTimer);
    this.status = this.stopping ? 'stopped' : 'completed';
    this.finishedAt = Date.now();
    this.logger.info(
      {
        status: this.status,
        chunksDone: this.chunksDone,
        chunksFailed: this.chunksFailed,
        totalDocsRead: this.totalDocsRead,
        totalRowsInserted: this.totalRowsInserted,
        totalDocsDlq: this.totalDocsDlq,
        totalCoercions: this.coercions.getTotal(),
        elapsedSec: Math.round((Date.now() - this.startedAt) / 1000),
      },
      'Ledger engine finished',
    );
  }

  // -------------------------------------------------------------------------
  // Per-collection flow
  // -------------------------------------------------------------------------

  private async processCollection(collection: string): Promise<void> {
    const { config, mongoReader, ledger } = this.d;
    this.currentCollection = collection;
    const log = this.logger.child({ collection });

    await mongoReader.switchCollection(collection);

    if (!(await mongoReader.hasRequiredIndex(collection))) {
      log.info('Building {cd:1,_id:1} index');
      await mongoReader.startIndexCreation(collection);
    }

    const lower = await mongoReader.getLowerBound();
    const upper = await mongoReader.getUpperBound();
    if (!lower || !upper) {
      log.info('Collection empty (no cd-bearing docs), skipping');
      return;
    }

    const estimated = await mongoReader.getEstimatedCount();
    let bounds = computeChunkBounds(lower.cd, upper.cd, estimated, config.ledger.chunkDocsTarget, config.ledger.maxChunkDays);

    // Dry run: keep every k-th chunk so old and new data shapes are both covered.
    if (this.dryRun) {
      const k = Math.max(1, Math.ceil(100 / config.ledger.dryRunSamplePct));
      bounds = bounds.filter((_, i) => i % k === 0);
    }

    // Null-cd sweep chunk: documents with no `cd` value are invisible to the
    // cd-bounded chunks — they get one dedicated chunk, paged by `_id`.
    // Sentinel bounds {-1, 0} mark it.
    if (!this.dryRun && (await mongoReader.hasNullCdDocuments())) {
      bounds.push({ lowerCd: -1, upperCd: 0 });
      log.info('Collection has null-cd documents — added null-cd sweep chunk');
    }

    const defaults = this.d.hashResolver.resolveCollectionName(collection, config.source.collectionPrefix) ?? undefined;
    const created = await ledger.initChunks(this.runId, collection, bounds, config.transform.version, defaults ? chScopeOf(defaults) : null);
    log.info({ estimated, chunks: bounds.length, created, scoped: !!defaults, dryRun: this.dryRun }, 'Chunk list ready');

    await this.recoverChunks(collection, log);

    for (;;) {
      if (this.stopping) return;
      while (this.paused && !this.stopping) await sleep(1_000);
      await this.reclaimExpiredLeases(collection, log);

      // The null-cd sweep must run strictly after every regular chunk is
      // terminal — its rows land inside regular chunks' cd windows and would
      // poison their verify-then-attach checks.
      const regularsRemaining = await ledger.countRegularNonTerminal(this.runId, collection);
      const chunk = await ledger.claimNext(this.runId, collection, this.podId, config.ledger.leaseSec, regularsRemaining > 0);
      if (chunk && chunk.attempts > MAX_CHUNK_ATTEMPTS && this.isSplittable(chunk)) {
        // Poison-pill quarantine: this chunk keeps killing the process (a
        // clean data error would have failed it long before exhausting
        // crash-retries). Don't retry the same span again — bisect it, so
        // repeated splitting converges on a tiny window around the poison
        // document instead of quarantining millions of docs.
        const parts = await ledger.splitChunk(chunk, 4);
        log.warn(
          { chunk: chunk._id, attempts: chunk.attempts, parts },
          'Chunk exhausted crash-retries — split into sub-chunks (poison-pill hunt)',
        );
        continue;
      }
      if (!chunk) {
        // Nothing pending — but "complete" means NO non-terminal chunks.
        // Chunks may still be leased by another pod (or orphaned by a dead
        // one); wait for their leases instead of declaring a hole "done".
        const nonTerminal = await ledger.findRecoverable(this.runId, collection, true);
        if (nonTerminal.length === 0) break;
        if (!config.worker.enabled) {
          // Single-pod: no other pod can own these — recover immediately.
          for (const orphan of nonTerminal) await this.recoverOne(orphan, log);
          continue;
        }
        log.info(
          { waitingOn: nonTerminal.length },
          'No pending chunks; waiting on leased in-flight chunks (reclaim on lease expiry)',
        );
        await sleep(Math.min((config.ledger.leaseSec * 1000) / 2, 15_000));
        continue;
      }
      if (chunk.attempts > MAX_CHUNK_ATTEMPTS) {
        // Too small to split further (or the null-cd sentinel): quarantine.
        // The window is now tiny — the offending doc(s) are inspectable
        // directly in the source between lower_cd and upper_cd.
        await ledger.transition(chunk._id, 'in_progress', 'failed', {
          last_error: `exceeded ${MAX_CHUNK_ATTEMPTS} attempts (crash quarantine — inspect source docs in this cd window)`,
        });
        this.noteChunkFailure(log);
        continue;
      }
      await this.processChunk(chunk, defaults, log);
      this.lastStatusCounts = await ledger.statusCounts(this.runId, collection);
    }

    await this.sweepOrphanStaging(collection);
    this.lastStatusCounts = await ledger.statusCounts(this.runId, collection);
    log.info({ statusCounts: this.lastStatusCounts }, 'Collection complete');
    this.currentCollection = null;
  }

  // -------------------------------------------------------------------------
  // Recovery & multi-pod lease reclaim
  // -------------------------------------------------------------------------

  private async recoverChunks(collection: string, log: Logger): Promise<void> {
    const includeAll = !this.d.config.worker.enabled;
    const recoverable = await this.d.ledger.findRecoverable(this.runId, collection, includeAll);
    for (const chunk of recoverable) {
      await this.recoverOne(chunk, log);
    }
  }

  /** Periodic tick (multi-pod): reclaim chunks whose owner's lease expired. */
  private async reclaimExpiredLeases(collection: string, log: Logger): Promise<void> {
    if (!this.d.config.worker.enabled) return;
    const intervalMs = Math.min((this.d.config.ledger.leaseSec * 1000) / 2, 30_000);
    if (Date.now() - this.lastReclaimAt < intervalMs) return;
    this.lastReclaimAt = Date.now();
    const expired = await this.d.ledger.findRecoverable(this.runId, collection, false);
    for (const chunk of expired) {
      log.warn({ chunk: chunk._id, pod: chunk.pod_id }, 'Reclaiming chunk from expired lease');
      await this.recoverOne(chunk, log);
    }
  }

  private async recoverOne(chunk: ChunkDoc, log: Logger): Promise<void> {
    const { ledger, staging } = this.d;
    const stagingTable = chunk.staging_table ?? this.stagingName(chunk.collection, chunk.idx);
    log.info({ chunk: chunk._id, status: chunk.status }, 'Recovering chunk');

    if (chunk.status === 'in_progress') {
      // Mid-copy crash: never reconstruct — drop and redo.
      await staging.dropStaging(stagingTable);
      await ledger.transition(chunk._id, 'in_progress', 'pending', { staging_table: null, pod_id: null });
      return;
    }
    if (chunk.status === 'written') {
      const count = await staging.countRows(stagingTable).catch(() => -1);
      if (count === chunk.rows_expected && count >= 0) {
        await this.promoteChunk({ ...chunk, staging_table: stagingTable }, log);
      } else {
        await staging.dropStaging(stagingTable);
        await ledger.transition(chunk._id, 'written', 'pending', { staging_table: null, pod_id: null });
      }
      return;
    }
    if (chunk.status === 'attaching') {
      // The one state where blind retry is unsafe (double-attach duplicates).
      await this.finishAttaching({ ...chunk, staging_table: stagingTable }, log);
    }
  }

  // -------------------------------------------------------------------------
  // Backpressure (TTL-cached — never 3 system queries per batch)
  // -------------------------------------------------------------------------

  private async respectBackpressure(): Promise<void> {
    const { chPressure, config } = this.d;
    if (!chPressure || !config.backpressure.enabled || this.dryRun) return;

    const now = Date.now();
    if (this.lastPressure && now - this.lastPressure.at < config.backpressure.pollIntervalMs) {
      if (!this.lastPressure.state.shouldPause) return;
    }

    const t0 = performance.now();
    let state = await chPressure.sample(config.target.db, config.target.table);
    this.lastPressure = { state, at: Date.now() };

    if (state.shouldPause) {
      this.logger.warn({ reason: state.pauseReason }, 'ClickHouse backpressure — pausing inserts');
      const deadline = Date.now() + config.backpressure.maxPauseEpisodeMs;
      while (Date.now() < deadline && !this.stopping) {
        await sleep(config.backpressure.pollIntervalMs);
        state = await chPressure.sample(config.target.db, config.target.table);
        this.lastPressure = { state, at: Date.now() };
        if (state.canResume) break;
      }
    }
    this.stageMs.pressureWait += performance.now() - t0;
  }

  // -------------------------------------------------------------------------
  // Chunk processing
  // -------------------------------------------------------------------------

  private stagingName(collection: string, idx: number): string {
    return `${this.d.config.target.table}__stg_${shortHash(`${this.runId}:${collection}`)}_${idx}`;
  }

  private async processChunk(
    chunk: ChunkDoc,
    defaults: CollectionDefaults | undefined,
    log: Logger,
  ): Promise<void> {
    const { config, mongoReader, ledger, staging } = this.d;
    this.currentChunk = chunk._id;
    const stagingTable = this.dryRun ? staging.dryRunTable : this.stagingName(chunk.collection, chunk.idx);
    const clog = log.child({ chunk: chunk.idx, staging: stagingTable });

    const heartbeat = setInterval(() => {
      ledger.heartbeat(chunk._id, this.podId, config.ledger.leaseSec).catch(() => {});
    }, Math.max(10_000, (config.ledger.leaseSec * 1000) / 3));

    try {
      if (!this.dryRun) {
        await staging.createStaging(stagingTable);
      }
      await ledger.transition(chunk._id, 'in_progress', 'in_progress', { staging_table: stagingTable });

      const result = await this.copyChunk(chunk, stagingTable, defaults, clog);

      this.totalDocsRead += result.docsRead;
      this.totalDocsSkipped += result.docsSkipped;
      this.totalDocsDlq += result.docsDlq;

      const rowsExpected = result.docsRead - result.docsSkipped - result.docsDlq;
      await ledger.transition(chunk._id, 'in_progress', 'written', {
        docs_read: result.docsRead,
        docs_skipped: result.docsSkipped,
        rows_expected: rowsExpected,
      });

      // Circuit breaker: a high in-chunk failure rate is a systematic bug,
      // not dirty data — halt before the DLQ balloons into a dataset copy.
      const failRate = result.docsRead > 1_000
        ? (result.docsDlq + result.transformErrors) / result.docsRead : 0;
      if (failRate > config.ledger.breakerPct / 100) {
        clog.error(
          { failRate: (failRate * 100).toFixed(1) + '%', dlq: result.docsDlq, transformErrors: result.transformErrors },
          'Circuit breaker tripped — pausing engine (systematic failure suspected)',
        );
        if (!this.dryRun) await staging.dropStaging(stagingTable).catch(() => {});
        await ledger.transition(chunk._id, 'written', 'failed', {
          staging_table: null,
          last_error: `circuit breaker: ${(failRate * 100).toFixed(1)}% of docs failed`,
        });
        this.noteChunkFailure(clog);
        this.pause();
        return;
      }

      if (this.dryRun) {
        // Null-engine target: nothing stored, nothing to verify or promote.
        await ledger.transition(chunk._id, 'written', 'done', { attach_method: null });
        this.chunksDone++;
        this.consecutiveFailed = 0;
        clog.info({ docsRead: result.docsRead, dlq: result.docsDlq }, 'Dry-run chunk done');
        return;
      }

      // Verify: read tally vs exact ClickHouse count.
      const vStart = performance.now();
      const landed = await staging.countRows(stagingTable);
      this.stageMs.verify += performance.now() - vStart;

      if (landed !== rowsExpected) {
        clog.warn({ landed, rowsExpected }, 'Verification mismatch — dropping chunk for redo');
        await staging.dropStaging(stagingTable);
        await ledger.transition(chunk._id, 'written', 'pending', {
          staging_table: null,
          pod_id: null,
          last_error: `verify mismatch: expected ${rowsExpected}, landed ${landed}`,
        });
        return;
      }

      await this.promoteChunk(
        { ...chunk, staging_table: stagingTable, rows_expected: rowsExpected, docs_read: result.docsRead, docs_skipped: result.docsSkipped },
        clog,
      );
      this.consecutiveFailed = 0;

      clog.info(
        { docsRead: result.docsRead, docsSkipped: result.docsSkipped, dlq: result.docsDlq, rowsExpected },
        'Chunk done',
      );
    } catch (err) {
      const error = err as Error;
      const isPermanent = classifyError(err) === 'permanent';
      clog.error({ error: error.message, isPermanent }, 'Chunk failed');
      if (!this.dryRun) await staging.dropStaging(stagingTable).catch(() => {});
      const target = isPermanent || chunk.attempts >= MAX_CHUNK_ATTEMPTS ? 'failed' : 'pending';
      await ledger.transition(chunk._id, ['in_progress', 'written'], target, {
        staging_table: null,
        pod_id: null,
        last_error: error.message.slice(0, 500),
      });
      if (target === 'failed') this.noteChunkFailure(clog);
    } finally {
      clearInterval(heartbeat);
      this.currentChunk = null;
    }
  }

  /**
   * Stream-copy one chunk into its staging table.
   * One long-lived cursor (reopened from the last committed position on
   * cursor death), per-doc transform that keeps the raw doc paired with its
   * row (for DLQ), and a bounded window of concurrent inserts with
   * bisection on permanent errors.
   */
  private isNullCdChunk(chunk: ChunkDoc): boolean {
    return chunk.lower_cd === -1 && chunk.upper_cd === 0;
  }

  /** Splittable for the poison-pill hunt: cd-bounded and wider than 1 minute. */
  private isSplittable(chunk: ChunkDoc): boolean {
    return !this.isNullCdChunk(chunk) && chunk.upper_cd - chunk.lower_cd > 60_000;
  }

  private async copyChunk(
    chunk: ChunkDoc,
    stagingTable: string,
    defaults: CollectionDefaults | undefined,
    clog: Logger,
  ): Promise<{ docsRead: number; docsSkipped: number; docsDlq: number; transformErrors: number }> {
    const { config, mongoReader } = this.d;

    if (this.isNullCdChunk(chunk)) {
      return this.copyNullCdChunk(chunk, stagingTable, defaults, clog);
    }
    const upperBound: Cursor = { cd: chunk.upper_cd, id: '' };
    let resumeFrom: Cursor | null = { cd: chunk.lower_cd, id: '' };
    let skipFirstId: string | null = null;

    const state = { docsRead: 0, docsSkipped: 0, docsDlq: 0, transformErrors: 0, batchSeq: 0, firstError: null as Error | null };
    const inflight: Promise<void>[] = [];

    for (let attempt = 0; attempt < 5 && !state.firstError; attempt++) {
      try {
        const stream = mongoReader.readStream(resumeFrom, upperBound, config.source.mongoPageSize);
        for await (const page of stream) {
          this.stageMs.read += page.fetchMs;

          let docs = page.docs;
          // min() is inclusive: on (re)open, drop the already-processed boundary doc.
          if (skipFirstId !== null && String(docs[0]?._id) === skipFirstId) docs = docs.slice(1);
          skipFirstId = null;

          if (docs.length === 0) { resumeFrom = page.lastCursor; continue; }
          await this.processPage(docs, chunk, stagingTable, defaults, state, inflight, clog);

          resumeFrom = page.lastCursor;
          if (state.firstError) break;
        }
        break; // stream exhausted cleanly
      } catch (err) {
        // Cursor died — reopen from the last committed position.
        clog.warn({ error: (err as Error).message, attempt }, 'Read stream failed — reopening from last cursor');
        skipFirstId = resumeFrom && resumeFrom.id !== '' ? resumeFrom.id : null;
        if (attempt === 4) throw err;
        await sleep(1_000 * (attempt + 1));
      }
    }

    const iStart = performance.now();
    await Promise.all(inflight);
    this.stageMs.insert += performance.now() - iStart;

    if (state.firstError) throw state.firstError;

    // DLQ'd transform errors are already counted in docsSkipped; insert-DLQ'd
    // docs are not skipped (they were readable and transformable).
    return { docsRead: state.docsRead, docsSkipped: state.docsSkipped, docsDlq: state.docsDlq, transformErrors: state.transformErrors };
  }

  /** Null-cd sweep: page by `_id` over docs with no cd value. */
  private async copyNullCdChunk(
    chunk: ChunkDoc,
    stagingTable: string,
    defaults: CollectionDefaults | undefined,
    clog: Logger,
  ): Promise<{ docsRead: number; docsSkipped: number; docsDlq: number; transformErrors: number }> {
    const { config, mongoReader } = this.d;
    const bounds = await mongoReader.getNullCdBounds();
    const state = { docsRead: 0, docsSkipped: 0, docsDlq: 0, transformErrors: 0, batchSeq: 0, firstError: null as Error | null };
    if (!bounds) return state;

    const inflight: Promise<void>[] = [];
    let lastId: string | null = null;
    for (;;) {
      const page = await mongoReader.readNullCdPage(lastId, bounds.upper, config.source.mongoPageSize);
      this.stageMs.read += page.fetchMs;
      if (page.docs.length === 0) break;
      await this.processPage(page.docs, chunk, stagingTable, defaults, state, inflight, clog);
      lastId = page.lastCursor!.id;
      if (state.firstError || page.docs.length < config.source.mongoPageSize) break;
    }

    const iStart = performance.now();
    await Promise.all(inflight);
    this.stageMs.insert += performance.now() - iStart;
    if (state.firstError) throw state.firstError;
    return state;
  }

  /** Shared per-page pipeline: transform (with raw-doc pairing), DLQ capture,
   *  backpressure, and windowed insert-or-bisect. */
  private async processPage(
    docs: SourceDocument[],
    chunk: ChunkDoc,
    stagingTable: string,
    defaults: CollectionDefaults | undefined,
    state: { docsRead: number; docsSkipped: number; docsDlq: number; transformErrors: number; batchSeq: number; firstError: Error | null },
    inflight: Promise<void>[],
    clog: Logger,
  ): Promise<void> {
    const { config } = this.d;

    // Chaos hook for poison-pill drills (bench/poison-drill.ts): hard-kills
    // the process when a specific doc is touched. Inert unless the test-only
    // env var is set — simulates a doc that OOMs/crashes the transform.
    if (process.env.LEDGER_TEST_CRASH_ID
        && docs.some((d) => String(d._id) === process.env.LEDGER_TEST_CRASH_ID)) {
      this.logger.fatal({ poison: process.env.LEDGER_TEST_CRASH_ID }, 'CHAOS: simulated poison-pill crash');
      process.exit(137);
    }

    state.docsRead += docs.length;

    const tfStart = performance.now();
    const rows: OutputRow[] = [];
    const srcs: SourceDocument[] = [];
    const dlqBatch: Parameters<DlqStore['add']>[0] = [];
    for (const doc of docs) {
      const { row, skipReason } = transformDocument(doc, defaults, this.coercions);
      if (row !== null) {
        rows.push(row);
        srcs.push(doc);
      } else if (skipReason !== null) {
        this.skips.increment(skipReason);
        state.docsSkipped++;
        // Every unmigratable doc (except already-migrated) is captured with
        // its raw source doc — accounted for and replayable, never dropped.
        if (skipReason !== SkipReason.ALREADY_MARKED_MIGRATED) {
          state.transformErrors++;
          if (config.ledger.captureTransformErrors) {
            dlqBatch.push({
              run_id: this.runId,
              collection: chunk.collection,
              chunk_id: chunk._id,
              source_id: String(doc._id ?? `unknown_${state.docsRead}`),
              raw_doc: doc as Record<string, unknown>,
              reason: skipReason === SkipReason.TRANSFORM_ERROR ? 'transform_error' : 'skipped',
              error: `skip:${skipReason}`,
              transform_version: config.transform.version,
            });
          }
        }
      }
    }
    this.stageMs.transform += performance.now() - tfStart;
    if (dlqBatch.length > 0) await this.d.dlq.add(dlqBatch);

    await this.respectBackpressure();

    if (rows.length > 0) {
      const seq = state.batchSeq++;
      const p = this.insertOrBisect(chunk, stagingTable, rows, srcs, seq, clog)
        .then((r) => { state.docsDlq += r.dlqd; })
        .catch((err) => { if (!state.firstError) state.firstError = err as Error; });
      inflight.push(p);
      if (inflight.length >= config.ledger.insertInflight) {
        const iStart = performance.now();
        await inflight.shift();
        this.stageMs.insert += performance.now() - iStart;
      }
    }
  }

  /**
   * Insert a batch; on a PERMANENT error, bisect (halve and retry each half,
   * still with transient-retry protection) until the offending documents are
   * isolated, then DLQ them with their raw source docs. Transient errors
   * exhaust the retry policy and propagate (chunk redo).
   */
  private async insertOrBisect(
    chunk: ChunkDoc,
    stagingTable: string,
    rows: OutputRow[],
    srcs: SourceDocument[],
    seq: number,
    clog: Logger,
    depth = 0,
  ): Promise<{ inserted: number; dlqd: number }> {
    const { retryPolicy, staging, dlq, config } = this.d;
    try {
      await retryPolicy.execute(
        () => staging.insertBatch(
          stagingTable,
          rows,
          `mig:${this.runId}:${chunk._id}:${seq}:${depth}:${rows.length}`,
          `mig__${shortHash(`${chunk._id}:${seq}:${depth}:${rows.length}`)}`,
        ),
        `chunk-${chunk.idx}-batch-${seq}-d${depth}`,
        clog,
        undefined,
        classifyError,
      );
      this.totalRowsInserted += rows.length;
      return { inserted: rows.length, dlqd: 0 };
    } catch (err) {
      if (classifyError(err) !== 'permanent') throw err;

      if (rows.length === 1) {
        await dlq.add([{
          run_id: this.runId,
          collection: chunk.collection,
          chunk_id: chunk._id,
          source_id: String(srcs[0]._id),
          raw_doc: srcs[0] as Record<string, unknown>,
          reason: 'insert_rejected',
          error: (err as Error).message.slice(0, 1_000),
          transform_version: config.transform.version,
        }]);
        return { inserted: 0, dlqd: 1 };
      }

      if (depth === BISECT_LOG_THRESHOLD) {
        clog.warn({ batch: seq, size: rows.length }, 'Permanent insert error — bisecting to isolate offending docs');
      }
      const mid = Math.ceil(rows.length / 2);
      const left = await this.insertOrBisect(chunk, stagingTable, rows.slice(0, mid), srcs.slice(0, mid), seq, clog, depth + 1);
      const right = await this.insertOrBisect(chunk, stagingTable, rows.slice(mid), srcs.slice(mid), seq, clog, depth + 1);
      return { inserted: left.inserted + right.inserted, dlqd: left.dlqd + right.dlqd };
    }
  }

  // -------------------------------------------------------------------------
  // Promotion
  // -------------------------------------------------------------------------

  private async promoteChunk(chunk: ChunkDoc, log: Logger): Promise<void> {
    const { ledger, staging } = this.d;
    const stagingTable = chunk.staging_table!;

    const aStart = performance.now();
    const partitions = await staging.listPartitions(stagingTable);
    await ledger.transition(chunk._id, ['written', 'attaching'], 'attaching', { partitions, staging_table: stagingTable });

    await this.finishAttaching({ ...chunk, partitions, attached: chunk.attached ?? [] }, log);
    this.stageMs.attach += performance.now() - aStart;
  }

  private async finishAttaching(chunk: ChunkDoc, log: Logger): Promise<void> {
    const { ledger, staging } = this.d;
    const stagingTable = chunk.staging_table!;
    const attachedSet = new Set(chunk.attached);
    let method: 'attach' | 'insert_select' = chunk.attach_method ?? 'attach';

    const remaining = chunk.partitions.filter((p) => !attachedSet.has(p));
    for (const partitionId of remaining) {
      // Verify-then-attach: never attach a partition whose rows are already
      // live. Checked by staged row ids — precise for THIS chunk even when
      // sibling collections share the month partition and cd window (a
      // window-count here once skipped attaches on multi-collection runs).
      const already = await staging.countLiveByStagedIds(stagingTable, partitionId);
      if (already > 0) {
        await ledger.recordAttached(chunk._id, partitionId);
        continue;
      }
      try {
        await staging.attachPartition(stagingTable, partitionId);
      } catch (err) {
        if (attachedSet.size === 0 && remaining[0] === partitionId) {
          log.warn({ err: (err as Error).message }, 'ATTACH unavailable — falling back to INSERT SELECT');
          await staging.insertSelect(stagingTable);
          method = 'insert_select';
          for (const p of chunk.partitions) await ledger.recordAttached(chunk._id, p);
          break;
        }
        throw err; // partial attach + failure → stays 'attaching', recovery resumes
      }
      await ledger.recordAttached(chunk._id, partitionId);
    }

    await ledger.transition(chunk._id, 'attaching', 'done', { attach_method: method });
    await staging.dropStaging(stagingTable);
    this.chunksDone++;
  }

  /** Delete the live rows of a collection's null-cd docs, precisely by id. */
  /**
   * Purge a chunk's live rows by their Mongo ids (paged). Fallback for
   * unresolvable collections in multi-collection runs, where a cd-window
   * DELETE would also hit sibling collections' rows.
   */
  private async purgeWindowByIds(collection: string, lowerCd: number, upperCd: number): Promise<void> {
    const db = this.d.mongoReader.getDatabase();
    const cursor = db.collection(collection).find(
      { cd: { $gte: new Date(lowerCd), $lt: new Date(upperCd) } },
      { projection: { _id: 1, cd: 1 } },
    ).batchSize(10_000);
    let pairs: Array<{ id: string; cdMs: number }> = [];
    for await (const doc of cursor) {
      pairs.push({ id: String(doc._id), cdMs: (doc.cd as Date).getTime() });
      if (pairs.length >= 10_000) { await this.d.staging.deleteLiveByPairs(pairs); pairs = []; }
    }
    await this.d.staging.deleteLiveByPairs(pairs);
  }

  private async purgeNullCdRows(collection: string): Promise<void> {
    const { MongoClient } = await import('mongodb');
    const mc = new MongoClient(this.d.config.source.uri);
    try {
      await mc.connect();
      const coll = mc.db(this.d.config.source.db).collection(collection);
      // The sweep wrote these rows with cd derived from ts (the transform's
      // fallback) — reconstruct the same pairs so the purge is provenance-
      // exact and can never touch a live row sharing an id.
      const cursor = coll.find(
        { $or: [{ cd: null }, { cd: { $exists: false } }] },
        { projection: { _id: 1, ts: 1 } },
      ).batchSize(10_000);
      let batch: Array<{ id: string; cdMs: number }> = [];
      for await (const doc of cursor) {
        const tsMillis = toEpochMillis(doc.ts);
        if (tsMillis === null || tsMillis <= 0) continue; // never transformed → never inserted
        batch.push({ id: String(doc._id), cdMs: clampDateTime64(tsMillis) });
        if (batch.length >= 10_000) { await this.d.staging.deleteLiveByPairs(batch); batch = []; }
      }
      if (batch.length > 0) await this.d.staging.deleteLiveByPairs(batch);
    } finally {
      await mc.close().catch(() => {});
    }
  }

  /**
   * Double-probe the source for writes: snapshot per-collection newest cd +
   * estimated count, wait, snapshot again. Any advance means old ingestion
   * is still running. Public-ish for tests (delay injectable).
   */
  async probeSourceFrozen(
    db: ReturnType<MongoReader['getDatabase']>,
    collections: string[],
    probeMs: number,
    wait: (ms: number) => Promise<void> = (ms) => sleep(ms),
  ): Promise<{ frozen: boolean; grew: string[]; probeMs: number }> {
    const snapshot = async (): Promise<Map<string, { maxCd: number; est: number }>> => {
      const out = new Map<string, { maxCd: number; est: number }>();
      for (const name of collections) {
        const [top] = await db.collection(name).find({ cd: { $type: 'date' } })
          .sort({ cd: -1 }).limit(1).project({ cd: 1 }).toArray();
        out.set(name, {
          maxCd: top?.cd instanceof Date ? top.cd.getTime() : 0,
          est: await db.collection(name).estimatedDocumentCount(),
        });
      }
      return out;
    };
    const before = await snapshot();
    await wait(probeMs);
    const after = await snapshot();
    const grew: string[] = [];
    for (const [name, b] of before) {
      const a = after.get(name)!;
      if (a.maxCd > b.maxCd || a.est > b.est) grew.push(name);
    }
    return { frozen: grew.length === 0, grew, probeMs };
  }

  /** Drop staging tables orphaned by crash-between-done-and-drop. */
  private async sweepOrphanStaging(collection: string): Promise<void> {
    if (this.dryRun) return;
    const prefix = `${this.d.config.target.table}__stg_${shortHash(`${this.runId}:${collection}`)}_`;
    const orphans = await this.d.staging.listStagingTables(prefix).catch(() => [] as string[]);
    for (const t of orphans) {
      await this.d.staging.dropStaging(t).catch(() => {});
    }
    if (orphans.length > 0) this.logger.info({ orphans: orphans.length }, 'Dropped orphaned staging tables');
  }

  // -------------------------------------------------------------------------
  // Circuit breaker bookkeeping
  // -------------------------------------------------------------------------

  private noteChunkFailure(log: Logger): void {
    this.chunksFailed++;
    this.consecutiveFailed++;
    if (this.consecutiveFailed >= this.d.config.ledger.breakerConsecutive) {
      log.error(
        { consecutiveFailed: this.consecutiveFailed },
        'Circuit breaker: consecutive chunk failures — pausing engine',
      );
      this.pause();
    }
  }

  // -------------------------------------------------------------------------
  // Invariant monitor (background spot checks — never on the hot path)
  // -------------------------------------------------------------------------

  private startInvariantMonitor(): void {
    const intervalMs = this.d.config.ledger.monitorIntervalMs;
    if (intervalMs <= 0) return;
    this.monitorTimer = setInterval(() => {
      this.runInvariantCheck().catch((err) =>
        this.logger.warn({ err: (err as Error).message }, 'Invariant check failed to run'));
    }, intervalMs);
    this.monitorTimer.unref?.();
  }

  private async runInvariantCheck(): Promise<void> {
    if (!this.currentCollection) return;
    const done = await this.d.ledger.listByStatus(this.runId, this.currentCollection, 'done');
    if (done.length === 0) return;
    // Null-cd rows carry cd values derived from ts, which land inside regular
    // chunks' cd windows — with a null-cd sweep present, exact equality per
    // window is not a valid invariant; missing rows still are.
    const hasNullCd = done.some((c) => this.isNullCdChunk(c));
    const samples = done
      .filter((c) => !this.isNullCdChunk(c))
      .sort(() => Math.random() - 0.5)
      .slice(0, 5);
    for (const chunk of samples) {
      const scope = this.scopeOf(chunk);
      // Sibling collections overlap in cd — an unscoped window count is only
      // meaningful when this collection is the whole table.
      if (!scope && this.multiCollection) continue;
      const live = await this.d.staging.countLiveInCdRange(chunk.lower_cd, chunk.upper_cd, scope);
      const violated = hasNullCd ? live < chunk.rows_expected : live !== chunk.rows_expected;
      if (violated) {
        this.logger.error(
          { chunk: chunk._id, live, expected: chunk.rows_expected },
          'INVARIANT VIOLATION: live-table count disagrees with verified chunk — pausing engine',
        );
        await this.d.ledger.transition(chunk._id, 'done', 'failed', {
          last_error: `invariant violation: live=${live} expected=${chunk.rows_expected}`,
        });
        this.pause();
        return;
      }
    }
    this.logger.debug({ sampled: samples.length }, 'Invariant spot check passed');
  }

  // -------------------------------------------------------------------------
  // Operator: retry failed chunks
  // -------------------------------------------------------------------------

  /**
   * Reset all failed chunks of this run back to pending and resume.
   * If a failed chunk had already been (partially) promoted — e.g. flagged by
   * the invariant monitor — its live-table cd window is purged first so the
   * redo starts clean and verify-then-attach behaves correctly.
   * (Null-cd sweep chunks have no cd window and are reset without a purge —
   * their id-based attach check tolerates partial presence.)
   */
  async retryFailed(): Promise<{ retried: number }> {
    const { ledger, staging } = this.d;
    let retried = 0;
    const collections = new Set<string>();
    const failedAll: ChunkDoc[] = [];
    // Failed chunks may span collections; statusCounts is global, listByStatus per collection.
    const counts = await ledger.statusCounts(this.runId);
    if ((counts.failed ?? 0) > 0) {
      const all = await ledger.listAll(this.runId);
      for (const c of all) if (c.status === 'failed') { collections.add(c.collection); failedAll.push(c as ChunkDoc); }
    }
    const collectionsNeedingSweepReset = new Set<string>();
    for (const chunk of failedAll) {
      if (!this.isNullCdChunk(chunk as ChunkDoc) && !this.dryRun) {
        // Purge must never touch sibling collections' rows in the same cd
        // window: scope by (a, e) when the collection resolves; otherwise
        // purge precisely by the window's Mongo ids.
        const scope = this.scopeOf(chunk as ChunkDoc);
        if (scope || !this.multiCollection) {
          await staging.deleteLiveCdRange(chunk.lower_cd, chunk.upper_cd, scope);
        } else {
          await this.purgeWindowByIds(chunk.collection, chunk.lower_cd, chunk.upper_cd);
        }
        collectionsNeedingSweepReset.add(chunk.collection);
      }
      const reset = await ledger.transition(chunk._id, 'failed', 'pending', {
        pod_id: null,
        staging_table: null,
        partitions: [],
        attached: [],
        attach_method: null,
        attempts: 0,
        last_error: null,
      });
      if (reset) retried++;
    }

    // A regular chunk's cd-window purge also deletes any null-cd sweep rows
    // whose derived cd fell inside that window — reset the sweep too, purging
    // its remaining rows precisely by id (it has no cd window of its own).
    for (const collection of collectionsNeedingSweepReset) {
      const sentinel = await ledger.getSentinel(this.runId, collection);
      if (!sentinel || sentinel.status !== 'done') continue;
      await this.purgeNullCdRows(collection);
      await ledger.transition(sentinel._id, 'done', 'pending', {
        pod_id: null, staging_table: null, partitions: [], attached: [],
        attach_method: null, attempts: 0, last_error: 'reset alongside regular-chunk retry (cd-window purge overlaps sweep rows)',
      });
      this.logger.info({ collection }, 'Null-cd sweep reset alongside regular-chunk retry');
    }
    this.consecutiveFailed = 0;
    this.resume();
    this.logger.info({ retried }, 'Failed chunks reset to pending');
    return { retried };
  }

  // -------------------------------------------------------------------------
  // DLQ replay
  // -------------------------------------------------------------------------

  /**
   * Replay pending DLQ entries: re-transform the stored raw docs under the
   * CURRENT transform version and insert them directly into the live table.
   * Safe to run anytime after the affected chunks are done.
   */
  async replayDlq(): Promise<{ replayed: number; stillFailing: number }> {
    const { dlq, staging, retryPolicy, config } = this.d;
    let replayed = 0;
    let stillFailing = 0;

    // Keyset drain: pages of 500 by _id so a large DLQ is fully processed
    // (a plain limited fetch silently replayed only the first page). Entries
    // that fail again keep status pending but sort behind the advancing
    // cursor, so the loop always terminates.
    let afterId: string | null = null;
    for (;;) {
      const batch = await dlq.listPendingAfter(this.runId, afterId, 500);
      if (batch.length === 0) break;
      afterId = batch[batch.length - 1]._id;
      const batchKey = batch[0]._id;
      const rows: OutputRow[] = [];
      const ids: string[] = [];
      for (const entry of batch) {
        const defaults = this.d.hashResolver.resolveCollectionName(entry.collection, config.source.collectionPrefix) ?? undefined;
        const { row } = transformDocument(entry.raw_doc as SourceDocument, defaults, this.coercions);
        if (row) { rows.push(row); ids.push(entry._id); }
        else {
          await dlq.recordRetryError(entry._id, 'still fails transform under ' + config.transform.version);
          stillFailing++;
        }
      }
      if (rows.length === 0) continue;
      try {
        await retryPolicy.execute(
          () => staging.insertIntoLive(rows, `dlqreplay:${batchKey}`),
          `dlq-replay-${batchKey}`,
          this.logger,
          undefined,
          classifyError,
        );
        await dlq.markResolved(ids, config.transform.version);
        replayed += rows.length;
      } catch (err) {
        // Isolate row-level failures within the replay batch too.
        for (let j = 0; j < rows.length; j++) {
          try {
            await staging.insertIntoLive([rows[j]], `dlqreplay:${batchKey}:${j}`);
            await dlq.markResolved([ids[j]], config.transform.version);
            replayed++;
          } catch (rowErr) {
            await dlq.recordRetryError(ids[j], (rowErr as Error).message.slice(0, 1_000));
            stillFailing++;
          }
        }
        void err;
      }
    }

    this.logger.info({ replayed, stillFailing }, 'DLQ replay complete');
    return { replayed, stillFailing };
  }

  // -------------------------------------------------------------------------
  // Self-service: index building (UI action with progress)
  // -------------------------------------------------------------------------

  private indexBuild: { running: boolean; total: number; done: string[]; current: string | null; error: string | null } =
    { running: false, total: 0, done: [], current: null, error: null };

  /** Kick off {cd,_id} index builds on all collections missing it (background). */
  async startIndexBuilds(): Promise<{ started: boolean; missing: number }> {
    if (this.indexBuild.running) return { started: false, missing: this.indexBuild.total - this.indexBuild.done.length };
    const db = this.d.mongoReader.getDatabase();
    const collections = await discoverCollections(db, this.d.config.source.collectionPrefix, this.logger);
    const missing: string[] = [];
    for (const name of collections) {
      const idx = await db.collection(name).indexes().catch(() => []);
      const has = idx.some((i) => (i.key as Record<string, unknown>).cd !== undefined && (i.key as Record<string, unknown>)._id !== undefined);
      if (!has) missing.push(name);
    }
    if (missing.length === 0) return { started: false, missing: 0 };

    this.indexBuild = { running: true, total: missing.length, done: [], current: null, error: null };
    void (async () => {
      for (const name of missing) {
        this.indexBuild.current = name;
        try {
          await db.collection(name).createIndex({ cd: 1, _id: 1 });
          this.indexBuild.done.push(name);
        } catch (err) {
          this.indexBuild.error = `${name}: ${(err as Error).message}`;
          break;
        }
      }
      this.indexBuild.running = false;
      this.indexBuild.current = null;
    })();
    return { started: true, missing: missing.length };
  }

  /** Index-build progress incl. live server-side build progress via $currentOp. */
  async indexBuildProgress(): Promise<Record<string, unknown>> {
    let serverOps: Array<{ collection: string; pct: number | null; msg: string }> = [];
    try {
      const adminDb = this.d.mongoReader.getDatabase().client.db('admin');
      const ops = await adminDb
        .aggregate([
          { $currentOp: { allUsers: true } },
          { $match: { 'command.createIndexes': { $exists: true } } },
          { $project: { command: 1, progress: 1, msg: 1 } },
        ])
        .toArray();
      serverOps = ops.map((o: Record<string, unknown>) => ({
        collection: String((o.command as Record<string, unknown>)?.createIndexes ?? '?'),
        pct: o.progress && (o.progress as Record<string, number>).total
          ? Math.round(((o.progress as Record<string, number>).done / (o.progress as Record<string, number>).total) * 100)
          : null,
        msg: String(o.msg ?? ''),
      }));
    } catch { /* $currentOp may need privileges — progress is then best-effort */ }
    return { ...this.indexBuild, serverOps };
  }

  // -------------------------------------------------------------------------
  // Self-service: preflight & verification
  // -------------------------------------------------------------------------

  /**
   * Environment/readiness checks for the guided UI. Read-only; safe to run
   * anytime (uses the raw Db handle, never mutates reader state).
   */
  async preflight(): Promise<Record<string, unknown>> {
    const { config, mongoReader, staging, ledger } = this.d;
    const checks: Array<{ id: string; label: string; status: 'pass' | 'warn' | 'fail'; detail: string }> = [];

    // MongoDB source
    let collections: string[] = [];
    try {
      const db = mongoReader.getDatabase();
      collections = await discoverCollections(db, config.source.collectionPrefix, this.logger);
      checks.push({ id: 'mongo', label: 'MongoDB source reachable', status: 'pass', detail: `${collections.length} drill collection(s) found` });
      let indexed = 0;
      let totalDocs = 0;
      for (const name of collections) {
        const idx = await db.collection(name).indexes().catch(() => []);
        const has = idx.some((i) => (i.key as Record<string, unknown>).cd !== undefined && (i.key as Record<string, unknown>)._id !== undefined);
        if (has) indexed++;
        totalDocs += await db.collection(name).estimatedDocumentCount().catch(() => 0);
      }
      checks.push({
        id: 'index',
        label: '{cd,_id} index on all collections',
        status: indexed === collections.length ? 'pass' : 'warn',
        detail: indexed === collections.length
          ? 'all indexed'
          : `${collections.length - indexed} collection(s) missing the index — the service builds it automatically, but pre-building avoids a long pause (see Guide step 2)`,
      });
      checks.push({ id: 'docs', label: 'Estimated documents to migrate', status: 'pass', detail: `${totalDocs.toLocaleString('en-US')} (estimate; can be off after an unclean mongod shutdown — chunk sizing is span-guarded)` });
      // cd should essentially always exist; the sweep handles outliers, last.
      let nullCd = 0;
      for (const name of collections) {
        nullCd += await db.collection(name).countDocuments({ cd: null }).catch(() => 0);
      }
      // Clock sanity: every provenance decision rests on live cd (insert
      // time) being newer than all source cd. A source containing cd values
      // at/over ClickHouse's present means skewed clocks or a source that is
      // still ingesting — both invalidate the boundary.
      let maxSourceCd = 0;
      for (const name of collections) {
        const [top] = await db.collection(name).find({ cd: { $type: 'date' } })
          .sort({ cd: -1 }).limit(1).project({ cd: 1 }).toArray();
        if (top?.cd instanceof Date) maxSourceCd = Math.max(maxSourceCd, top.cd.getTime());
      }
      const chNow = await this.d.staging.serverNowMs().catch(() => 0);
      const skewOk = maxSourceCd === 0 || chNow === 0 || maxSourceCd < chNow - 60_000;
      checks.push({
        id: 'clock',
        label: 'Source frozen & clocks sane (cd boundary)',
        status: skewOk ? 'pass' : 'fail',
        detail: skewOk
          ? `newest source cd ${maxSourceCd ? new Date(maxSourceCd).toISOString() : 'n/a'} is safely behind ClickHouse server time`
          : `newest source cd ${new Date(maxSourceCd).toISOString()} is within 60s of ClickHouse server time (${new Date(chNow).toISOString()}) — source still ingesting or clocks skewed; the migrated/live cd boundary is NOT trustworthy yet`,
      });
      // Old ingestion stopped? Probe twice: if any collection's newest cd or
      // estimated count advances between probes, the source is still being
      // written — cutover step 'stop old ingestion' has not happened.
      const frozen = await this.probeSourceFrozen(db, collections, 4_000);
      checks.push({
        id: 'frozen',
        label: 'Old ingestion stopped (source frozen)',
        status: frozen.frozen ? 'pass' : 'fail',
        detail: frozen.frozen
          ? `no writes observed during a ${Math.round(frozen.probeMs / 1000)}s probe`
          : `STILL RECEIVING WRITES: ${frozen.grew.join(', ')} — stop old ingestion before migrating (works the same for new-cluster and same-cluster setups)`,
      });

      // New ingestion flowing? Post-cutover rows carry recent cd. Zero recent
      // rows is a warning, not a failure — traffic may legitimately be zero,
      // or preflight may be running before the SDK flip (both topologies).
      try {
        const recent = await this.d.staging.countRecentLive(15);
        checks.push({
          id: 'live-ingest',
          label: 'New ingestion flowing into ClickHouse',
          status: recent > 0 ? 'pass' : 'warn',
          detail: recent > 0
            ? `${recent.toLocaleString('en-US')} rows ingested in the last 15 min`
            : 'no rows with recent cd in the last 15 min — either the SDK flip has not happened yet or traffic is zero; fine to migrate, but verify live ingestion separately',
        });
      } catch { /* table missing — already reported by the table check */ }

      checks.push({
        id: 'nullcd',
        label: 'Documents without cd (outliers)',
        status: nullCd === 0 ? 'pass' : 'warn',
        detail: nullCd === 0
          ? 'none — every document carries cd'
          : `${nullCd.toLocaleString('en-US')} — a dedicated sweep chunk migrates them, strictly after all regular chunks`,
      });
    } catch (err) {
      checks.push({ id: 'mongo', label: 'MongoDB source reachable', status: 'fail', detail: (err as Error).message });
    }

    // Replica set: reading from secondaries offloads the primary during the
    // days-long scan. (After cutover the source is frozen, so secondary reads
    // are exact.)
    try {
      const hello = await mongoReader.getDatabase().admin().command({ hello: 1 });
      if (hello.setName) {
        const onPrimary = config.source.readPreference === 'primary';
        checks.push({
          id: 'replicaset',
          label: `Replica set detected (${hello.setName})`,
          status: onPrimary ? 'warn' : 'pass',
          detail: onPrimary
            ? `MONGO_READ_PREFERENCE=primary was forced by env — remove it to let the engine auto-select secondaryPreferred (it does this by default on replica sets; source is frozen, so secondary reads are exact)`
            : `read preference: ${config.source.readPreference}${config.source.readPreferenceAuto ? ' (auto-selected — replica set detected)' : ''}`,
        });
      }
    } catch { /* standalone or no permission — nothing to suggest */ }

    // Disk headroom — the #1 preventable mid-migration incident.
    try {
      const dbStats = await mongoReader.getDatabase().stats();
      if (dbStats.fsTotalSize) {
        const freePct = Math.round(((dbStats.fsTotalSize - dbStats.fsUsedSize) / dbStats.fsTotalSize) * 100);
        checks.push({
          id: 'mongo-disk',
          label: 'MongoDB disk headroom',
          status: freePct < 10 ? 'fail' : freePct < 20 ? 'warn' : 'pass',
          detail: `${freePct}% free`,
        });
      }
    } catch { /* stats not available */ }
    const chDisk = await staging.diskSpace();
    if (chDisk && chDisk.totalBytes > 0) {
      const freePct = Math.round((chDisk.freeBytes / chDisk.totalBytes) * 100);
      checks.push({
        id: 'ch-disk',
        label: 'ClickHouse disk headroom',
        status: freePct < 10 ? 'fail' : freePct < 20 ? 'warn' : 'pass',
        detail: `${freePct}% free (${(chDisk.freeBytes / 1e9).toFixed(1)} GB) — needs room for staging + the migrated data (~10-20% of the Mongo size after compression)`,
      });
    }

    // ClickHouse target
    const target = await staging.targetTableInfo();
    checks.push({
      id: 'clickhouse',
      label: `ClickHouse target table (${config.target.db}.${config.target.table})`,
      status: target.exists ? 'pass' : 'fail',
      detail: target.exists ? `exists, ${target.rows.toLocaleString('en-US')} rows` : 'table not found — create it (or start the new stack) before migrating',
    });

    // Dedup canary
    checks.push({
      id: 'dedup',
      label: 'Insert-dedup canary',
      status: staging.dedupWorks === null ? 'warn' : staging.dedupWorks ? 'pass' : 'warn',
      detail: staging.dedupWorks === null
        ? 'not probed yet (runs at migration start)'
        : staging.dedupWorks ? 'dedup token verified working on this target'
          : 'dedup token inert on this target — safe (chunk redo covers it), but ambiguous insert retries may need chunk redo',
    });

    // Dry run
    const dryCounts = await ledger.statusCounts(`${this.d.config.ledger.runId}-dry`);
    const dryTotal = Object.values(dryCounts).reduce((a, b) => a + b, 0);
    checks.push({
      id: 'dryrun',
      label: 'Dry run (sampled rehearsal)',
      status: dryTotal > 0 && (dryCounts.done ?? 0) === dryTotal ? 'pass' : 'warn',
      detail: dryTotal === 0
        ? 'not run yet — start once with DRY_RUN=1 and review the report (Guide step 3)'
        : `${dryCounts.done ?? 0}/${dryTotal} sampled chunks done`,
    });

    return { engineStatus: this.status, checks };
  }

  /**
   * Full migration verification: every done chunk's live-table count checked
   * against its verified expectation, plus table totals. Exact, minutes at
   * most — run before sign-off or any time trust is in question.
   */
  async verifyMigration(): Promise<Record<string, unknown>> {
    const { ledger, staging } = this.d;
    const all = await ledger.listAll(this.runId);
    const byCollection = new Map<string, boolean>();
    for (const c of all) {
      if (this.isNullCdChunk(c as ChunkDoc)) byCollection.set(c.collection, true);
    }

    let checked = 0;
    let unscopedSkipped = 0;
    const collectionCount = new Set(all.map((c) => c.collection)).size;
    const mismatches: Array<{ chunk: string; expected: number; live: number }> = [];
    const targets = all.filter((chunk) => chunk.status === 'done' && !this.isNullCdChunk(chunk as ChunkDoc));
    this.verifyProgress.running = true;
    this.verifyProgress.total = targets.length;
    this.verifyProgress.checked = 0;
    this.verifyProgress.phase = 'recounting chunk windows';
    try {
      // Bounded concurrency: each window count is minmax-pruned and cheap,
      // but a 10TB run has tens of thousands of them — sequential would take
      // hours, unbounded would hammer ClickHouse.
      const CONCURRENCY = 8;
      let cursor = 0;
      await Promise.all(Array.from({ length: CONCURRENCY }, async () => {
        for (;;) {
          const i = cursor++;
          if (i >= targets.length) return;
          const chunk = targets[i];
          const scope = this.scopeOf(chunk as ChunkDoc);
          if (!scope && collectionCount > 1) { unscopedSkipped++; continue; }
          const live = await staging.countLiveInCdRange(chunk.lower_cd, chunk.upper_cd, scope);
          const relaxed = byCollection.get(chunk.collection) === true;
          const bad = relaxed ? live < chunk.rows_expected : live !== chunk.rows_expected;
          checked++;
          this.verifyProgress.checked = checked;
          if (bad) mismatches.push({ chunk: chunk._id, expected: chunk.rows_expected, live });
        }
      }));

      this.verifyProgress.phase = 'scanning for duplicates (per partition)';

    // Duplicate detection + attribution, partition by partition — exact for
    // every duplicate class we act on (copies of the same document share
    // their ts month) and memory-bounded on billion-row tables, unlike a
    // global uniqExact/GROUP BY. Attribution by cd against the ledger's
    // end-of-migrated-data boundary:
    //   0 copies below → live at-least-once artifact (nightly job cleans)
    //   1 copy below   → cross-cutover SDK retry (benign, reported)
    //   2+ copies below → migration defect; verification fails.
    const boundaryMs = all.reduce((m, c) => Math.max(m, c.upper_cd), 0);
    const dup = await staging.duplicateStats(boundaryMs);
    let migrationDuplicates = 0;
    const duplicateSample = dup.sample.map((d) => {
      if (d.migratedCopies >= 2) migrationDuplicates++;
      return {
        _id: d._id,
        copies: d.copies,
        migratedCopies: d.migratedCopies,
        minCd: new Date(d.min_cd_ms).toISOString(),
        maxCd: new Date(d.max_cd_ms).toISOString(),
        verdict: d.migratedCopies === 0
          ? 'live at-least-once artifact (nightly dedup job cleans these)'
          : d.migratedCopies === 1
            ? 'cross-cutover retry duplicate (event reached both stacks — one benign live copy)'
            : 'MIGRATION DEFECT: same document migrated more than once — investigate',
      };
    });

    return {
      ok: mismatches.length === 0 && migrationDuplicates === 0,
      checkedChunks: checked,
      unscopedSkipped,
      mismatches,
      table: { rows: dup.rows, distinctIds: dup.rows - dup.duplicates, duplicates: dup.duplicates },
      duplicateSample,
      migrationDuplicates,
    };
    } finally {
      this.verifyProgress.running = false;
      this.verifyProgress.phase = '';
    }
  }

  // -------------------------------------------------------------------------
  // Stats & report
  // -------------------------------------------------------------------------

  getStats(): LedgerEngineStats {
    // Freeze the clock at completion: docs/second is the run's average
    // afterwards, not a number decaying while the finished engine idles.
    const endMs = this.finishedAt > 0 ? this.finishedAt : Date.now();
    const elapsedSec = this.startedAt > 0 ? (endMs - this.startedAt) / 1000 : 0;
    return {
      engine: 'ledger',
      runId: this.runId,
      podId: this.podId,
      status: this.status,
      fatalError: this.fatalError,
      dryRun: this.dryRun,
      currentCollection: this.currentCollection,
      currentChunk: this.currentChunk,
      totalDocsRead: this.totalDocsRead,
      totalDocsSkipped: this.totalDocsSkipped,
      totalRowsInserted: this.totalRowsInserted,
      totalDocsDlq: this.totalDocsDlq,
      totalCoercions: this.coercions.getTotal(),
      chunksDone: this.chunksDone,
      chunksFailed: this.chunksFailed,
      docsPerSecond: elapsedSec > 0 ? this.totalDocsRead / elapsedSec : 0,
      stageMs: {
        read: Math.round(this.stageMs.read),
        transform: Math.round(this.stageMs.transform),
        insert: Math.round(this.stageMs.insert),
        verify: Math.round(this.stageMs.verify),
        attach: Math.round(this.stageMs.attach),
        pressureWait: Math.round(this.stageMs.pressureWait),
      },
      dedupWorks: this.d.staging.dedupWorks,
      chunkStatusCounts: this.lastStatusCounts,
    };
  }

  /** Data-quality report: coercions, skips, DLQ — the dry-run/final artifact. */
  async getReport(): Promise<Record<string, unknown>> {
    return {
      runId: this.runId,
      dryRun: this.dryRun,
      status: this.status,
      chunkStatusCounts: await this.d.ledger.statusCounts(this.runId),
      skipsByReason: this.skips.getCounts(),
      coercions: this.coercions.getReport(),
      dlq: {
        byStatus: await this.d.dlq.countByStatus(this.runId),
        topErrors: await this.d.dlq.topErrors(this.runId),
      },
    };
  }
}
