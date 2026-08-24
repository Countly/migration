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
  pauseReason: string | null;
  sourceShrankChunks: number;
  cdUpperBoundMs: number | null;
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

/** Thrown when a stalled worker discovers its lease was reclaimed. */
class ClaimLostError extends Error {
  constructor(chunkId: string) { super(`claim lost: ${chunkId}`); this.name = 'ClaimLostError'; }
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
  /** Set while a chunk is processing; page loop calls it to abort fast on claim loss. */
  private assertClaimHook: (() => void) | null = null;
  /** Live progress of a running verify (billion-scale runs take minutes). */
  readonly verifyProgress = { running: false, checked: 0, total: 0, phase: '' };
  private stopping = false;
  private paused = false;
  private currentCollection: string | null = null;
  private currentChunk: string | null = null;
  private startedAt = 0;
  private consecutiveFailed = 0;
  private sourceShrankChunks = 0;
  private streakHadPermanent = false;
  private pauseReason: 'operator' | 'breaker-transient' | 'breaker-data' | null = null;
  private probeOkStreak = 0;
  private autoResuming = false;
  private resumeProbeTimer: NodeJS.Timeout | null = null;
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
  pause(reason: 'operator' | 'breaker-transient' | 'breaker-data' = 'operator'): void {
    this.paused = true;
    this.pauseReason = reason;
    if (this.status === 'running') this.status = 'paused';
  }

  resume(): void {
    this.paused = false;
    this.pauseReason = null;
    // clean slate: without this, one stray failure after resume re-trips
    // the breaker instantly (the streak counter would still be at max)
    this.consecutiveFailed = 0;
    this.streakHadPermanent = false;
    this.probeOkStreak = 0;
    if (this.status === 'paused') this.status = 'running';
  }
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

    // Transient-outage self-healing: only acts while paused with reason
    // 'breaker-transient' (backend outage tripped the failure breaker) —
    // every other pause stays owned by the operator.
    this.resumeProbeTimer = setInterval(() => {
      void this.autoResumeProbe();
    }, 15_000);
    this.resumeProbeTimer.unref?.();

    if (this.dryRun) {
      await this.d.staging.createDryRunTable();
      this.logger.warn(
        { samplePct: config.ledger.dryRunSamplePct },
        'DRY RUN: sampled rehearsal against a Null-engine clone — nothing is stored, nothing is promoted',
      );
    } else {
      await this.d.staging.runDedupCanary();
      await this.checkDlqPressure(this.logger, true); // inherited mass-DLQ pauses a resumed run too
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

    // ── MAP + DRAIN, with TOP-UP passes ──────────────────────────────────
    // Cut every collection's chunk grid upfront so claiming can be GLOBAL:
    // pods reserve the next available chunk anywhere, spilling into the next
    // collection the moment the current one has nothing claimable — instead
    // of convoying on one collection (the many-small-collections killer).
    // After draining, re-map: data that arrived in the OLD pipeline after
    // mapping gets delta chunks appended (and newly created collections get
    // discovered). Frozen source → one extra cheap pass; live source → the
    // run keeps chasing the delta until the source is actually frozen,
    // which makes bulk-before-cutover + final-drain a supported flow.
    // Bound resolution: env wins when present; otherwise the run-config
    // stored bound (the dashboard "apply detected boundary" path) is
    // adopted — re-read at EVERY pass so running pods pick it up live.
    const envBound = config.ledger.cdUpperBoundMs;

    let mapPass = 0;
    for (;;) {
      if (this.stopping) break;
      const storedBound = await this.d.ledger.getStoredBound(this.runId).catch(() => null);
      if (storedBound !== null) {
        if (envBound !== null && envBound !== storedBound) {
          const msg = `bound conflict: LEDGER_CD_UPPER_BOUND=${envBound} but the run stores ${storedBound} — refusing to guess with duplication at stake`;
          this.logger.error(msg);
          this.status = 'failed';
          this.fatalError = msg;
          return;
        }
        if (config.ledger.cdUpperBoundMs === null) {
          this.logger.warn({ bound: new Date(storedBound).toISOString() }, 'Adopted stored run bound (applied via dashboard)');
        }
        config.ledger.cdUpperBoundMs = storedBound;
        // Self-healing prune: a pod whose UNBOUNDED map pass raced the
        // apply click may have appended chunks past the bound after the
        // apply-time prune ran. Every adopting pod re-prunes, so stragglers
        // die within one pass cycle. A non-pending chunk past the bound at
        // this point is a real incident — logged loudly, never swallowed.
        await this.d.ledger.pruneBeyondBound(this.runId, storedBound).then((r) => {
          if (r.deleted > 0 || r.clamped > 0) {
            this.logger.warn({ ...r }, 'Pruned chunks beyond the adopted bound (raced an unbounded map pass)');
          }
        }).catch((err) => {
          this.logger.error({ err: (err as Error).message }, 'Chunks BEYOND the bound have already executed — post-bound data may be duplicated; purge/retry those chunks');
        });
      }
      let newChunks = 0;
      if (mapPass > 0) {
        collections = await discoverCollections(db, config.source.collectionPrefix, this.logger).catch(() => collections);
        collections = collections.filter((name) => {
          const d2 = this.d.hashResolver.resolveCollectionName(name, config.source.collectionPrefix);
          return !(d2 && skipEventNames.has(d2.e));
        });
      }
      for (const collection of collections) {
        if (this.stopping) break;
        newChunks += await this.mapCollection(collection);
      }
      if (mapPass === 0) {
        this.logger.info({ mapped: this.collectionDefaults.size }, 'All collections mapped — global claiming starts');
        await this.recoverChunks(null, this.logger);
      } else if (newChunks > 0) {
        this.logger.info({ pass: mapPass, newChunks }, 'Top-up pass found new data — draining delta');
      }
      if (mapPass > 0 && newChunks === 0) break; // stable: no delta anywhere
      await this.globalClaimLoop();
      if (this.stopping) break;
      mapPass++;
    }

    // ── SWEEP + FINISH ────────────────────────────────────────────────────
    let boundCollection: string | null = null;
    for (;;) {
      if (this.stopping) break;
      while (this.paused && !this.stopping) await sleep(1_000);
      if (this.stopping) break;
      await this.reclaimExpiredLeases(null, this.logger);

      const chunk = await this.d.ledger.claimNextGlobal(this.runId, this.podId, config.ledger.leaseSec);
      if (!chunk) {
        const remaining = await this.d.ledger.countRegularNonTerminal(this.runId);
        if (remaining === 0) break;
        if (!config.worker.enabled) {
          const orphans = await this.d.ledger.findRecoverable(this.runId, null, true);
          for (const orphan of orphans) await this.recoverOne(orphan, this.logger, true);
          continue;
        }
        this.logger.info({ waitingOn: remaining }, 'No claimable chunks; waiting on other pods (reclaim on lease expiry)');
        await sleep(Math.min((config.ledger.leaseSec * 1000) / 2, 15_000));
        continue;
      }

      const log = this.logger.child({ collection: chunk.collection });
      if (chunk.attempts > MAX_CHUNK_ATTEMPTS && this.isSplittable(chunk)) {
        const parts = await this.d.ledger.splitChunk(chunk, 4);
        log.warn({ chunk: chunk._id, attempts: chunk.attempts, parts }, 'Chunk exhausted crash-retries — split into sub-chunks (poison-pill hunt)');
        continue;
      }
      if (chunk.attempts > MAX_CHUNK_ATTEMPTS) {
        await this.d.ledger.transition(chunk._id, 'in_progress', 'failed', {
          last_error: `exceeded ${MAX_CHUNK_ATTEMPTS} attempts (crash quarantine — inspect source docs in this cd window)`,
        }, { podId: this.podId, attempts: chunk.attempts });
        this.noteChunkFailure(log);
        continue;
      }

      if (chunk.collection !== boundCollection) {
        await this.d.mongoReader.switchCollection(chunk.collection);
        boundCollection = chunk.collection;
        this.currentCollection = chunk.collection;
      }
      await this.processChunk(chunk, this.collectionDefaults.get(chunk.collection), log);
      this.lastStatusCounts = await this.d.ledger.statusCounts(this.runId);
    }

    // ── SWEEP PHASE (null-cd sentinels, per collection, after its regulars) ─
    if (!this.stopping) await this.runSweepPhase();

    if (!this.stopping) {
      for (const collection of this.collectionDefaults.keys()) {
        await this.sweepOrphanStaging(collection);
      }
      this.lastStatusCounts = await this.d.ledger.statusCounts(this.runId);
    }
    this.currentCollection = null;

    if (this.monitorTimer) clearInterval(this.monitorTimer);
    if (this.resumeProbeTimer) clearInterval(this.resumeProbeTimer);
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

  /** Collection identity cache (transform defaults) built during mapping. */
  private readonly collectionDefaults = new Map<string, CollectionDefaults | undefined>();

  /**
   * Map one collection: ensure the read index (join semantics), probe cd
   * bounds, cut the chunk grid (+ null-cd sentinel), persist it idempotently.
   * No copying happens here — claims are global afterwards.
   */
  private async mapCollection(collection: string): Promise<number> {
    const { config, mongoReader, ledger } = this.d;
    this.currentCollection = collection; // mapping progress visible in UI
    const log = this.logger.child({ collection });

    await mongoReader.switchCollection(collection);
    if (!(await mongoReader.hasRequiredIndex(collection))) {
      log.info('Building {cd:1,_id:1} index (this can take a long time on large collections — progress in the UI)');
    }
    await mongoReader.ensureIndex(collection);

    const defaults = this.d.hashResolver.resolveCollectionName(collection, config.source.collectionPrefix) ?? undefined;
    this.collectionDefaults.set(collection, defaults);

    const bound = config.ledger.cdUpperBoundMs;
    const lower = await mongoReader.getLowerBound();
    let upper = await mongoReader.getUpperBound();
    if (bound !== null && lower && lower.cd >= bound) {
      // Collection born after the mirror checkpoint: every doc is the
      // mirror's responsibility. Nothing to map here.
      log.info({ bound: new Date(bound).toISOString() }, 'Collection entirely beyond the cd bound — mirror territory, skipping');
      return 0;
    }
    if (bound !== null && upper && upper.cd >= bound) {
      upper = { ...upper, cd: bound - 1 };
    }
    if (!lower || !upper) {
      log.info('Collection empty (no cd-bearing docs), skipping');
      if (!this.dryRun && (await mongoReader.hasNullCdDocuments())) {
        return ledger.initChunks(this.runId, collection, [{ lowerCd: -1, upperCd: 0 }], config.transform.version, defaults ? chScopeOf(defaults) : null);
      }
      return 0;
    }

    // A clamped span makes the whole-collection estimate wrong for chunk
    // sizing — one indexed count below the bound is cheap and exact.
    const estimated = bound !== null && upper.cd === bound - 1
      ? await this.d.mongoReader.getDatabase().collection(collection)
          .countDocuments({ cd: { $lt: new Date(bound) } })
      : await mongoReader.getEstimatedCount();
    let bounds = computeChunkBounds(lower.cd, upper.cd, estimated, config.ledger.chunkDocsTarget, config.ledger.maxChunkDays);

    // Dry run: keep every k-th chunk so old and new data shapes are both covered.
    if (this.dryRun) {
      const k = Math.max(1, Math.ceil(100 / config.ledger.dryRunSamplePct));
      bounds = bounds.filter((_, i) => i % k === 0);
    }

    // Null-cd sweep chunk: documents with no `cd` value are invisible to the
    // cd-bounded chunks — they get one dedicated chunk, paged by `_id`.
    // Sentinel bounds {-1, 0} mark it; the sweep phase runs it only after
    // this collection's regulars are terminal.
    if (!this.dryRun && (await mongoReader.hasNullCdDocuments())) {
      bounds.push({ lowerCd: -1, upperCd: 0 });
      log.info('Collection has null-cd documents — added null-cd sweep chunk');
    }

    const created = await ledger.initChunks(this.runId, collection, bounds, config.transform.version, defaults ? chScopeOf(defaults) : null);
    if (created > 0) {
      log.info({ estimated, chunks: bounds.length, created, scoped: !!defaults, dryRun: this.dryRun }, 'Chunk list ready');
      return created;
    }

    // Collection already mapped (resume / later pass): TOP-UP. Old ingestion
    // assigns cd at write time, so data that arrived after mapping lands
    // strictly BEYOND the mapped upper bound — never inside existing windows.
    // Append delta chunks for [mapped upper, current max cd]; idx continues,
    // so the delta (newest data) is claimed first within the collection.
    if (this.dryRun) return 0;
    if (bound !== null) {
      // Mirror-first: data past the checkpoint belongs to the mirror —
      // top-up must never chase it (that is the whole point of the bound).
      return 0;
    }
    const hw = await ledger.regularHighWater(this.runId, collection);
    if (!hw || hw.maxUpperCd <= 0) return 0;
    if (upper.cd + 1 <= hw.maxUpperCd) return 0; // nothing new
    const deltaCount = await mongoReader.getDatabase().collection(collection)
      .countDocuments({ cd: { $gte: new Date(hw.maxUpperCd) } });
    if (deltaCount === 0) return 0;
    const deltaBounds = computeChunkBounds(hw.maxUpperCd, upper.cd, deltaCount, config.ledger.chunkDocsTarget, config.ledger.maxChunkDays);
    const appended = await ledger.appendChunks(
      this.runId, collection, deltaBounds, hw.maxIdx + 1, config.transform.version, defaults ? chScopeOf(defaults) : null,
    );
    log.info({ appended, deltaDocs: deltaCount, from: new Date(hw.maxUpperCd).toISOString(), to: new Date(upper.cd).toISOString() },
      'Top-up: delta chunks appended for data that arrived after mapping');
    return appended;
  }

  /** Drain every claimable regular chunk anywhere in the run. */
  private async globalClaimLoop(): Promise<void> {
    const { config } = this.d;
    let boundCollection: string | null = null;
    for (;;) {
      if (this.stopping) return;
      while (this.paused && !this.stopping) await sleep(1_000);
      if (this.stopping) return;
      await this.reclaimExpiredLeases(null, this.logger);

      const chunk = await this.d.ledger.claimNextGlobal(this.runId, this.podId, config.ledger.leaseSec);
      if (!chunk) {
        const remaining = await this.d.ledger.countRegularNonTerminal(this.runId);
        if (remaining === 0) return;
        if (!config.worker.enabled) {
          const orphans = await this.d.ledger.findRecoverable(this.runId, null, true);
          for (const orphan of orphans) await this.recoverOne(orphan, this.logger, true);
          continue;
        }
        this.logger.info({ waitingOn: remaining }, 'No claimable chunks; waiting on other pods (reclaim on lease expiry)');
        await sleep(Math.min((config.ledger.leaseSec * 1000) / 2, 15_000));
        continue;
      }

      const log = this.logger.child({ collection: chunk.collection });
      if (chunk.attempts > MAX_CHUNK_ATTEMPTS && this.isSplittable(chunk)) {
        const parts = await this.d.ledger.splitChunk(chunk, 4);
        log.warn({ chunk: chunk._id, attempts: chunk.attempts, parts }, 'Chunk exhausted crash-retries — split into sub-chunks (poison-pill hunt)');
        continue;
      }
      if (chunk.attempts > MAX_CHUNK_ATTEMPTS) {
        await this.d.ledger.transition(chunk._id, 'in_progress', 'failed', {
          last_error: `exceeded ${MAX_CHUNK_ATTEMPTS} attempts (crash quarantine — inspect source docs in this cd window)`,
        }, { podId: this.podId, attempts: chunk.attempts });
        this.noteChunkFailure(log);
        continue;
      }

      if (chunk.collection !== boundCollection) {
        await this.d.mongoReader.switchCollection(chunk.collection);
        boundCollection = chunk.collection;
        this.currentCollection = chunk.collection;
      }
      await this.processChunk(chunk, this.collectionDefaults.get(chunk.collection), log);
      this.lastStatusCounts = await this.d.ledger.statusCounts(this.runId);
    }
  }

  /**
   * Null-cd sweeps: each collection's sentinel runs strictly after that
   * collection's regular chunks are terminal (its rows carry ts-derived cd
   * inside regular windows and would poison verify-then-attach). By the time
   * this phase starts, this pod saw zero claimable regulars globally; other
   * pods may still hold leases, so each sentinel re-checks its own gate.
   */
  private async runSweepPhase(): Promise<void> {
    const { config, ledger } = this.d;
    for (;;) {
      if (this.stopping) return;
      while (this.paused && !this.stopping) await sleep(1_000);

      const sentinels = await ledger.listPendingSentinels(this.runId);
      let claimedAny = false;
      for (const sentinel of sentinels) {
        if (this.stopping) return;
        // Barrier-lite against a mapper racing us: re-probe THIS collection
        // for delta right before its sweep. If new data appended (or another
        // pod's fresh append is visible), regulars go first — skip the
        // sentinel this round. (Since promotion pair-checks staged rows,
        // even a lost race here degrades to harmless idempotent redo, not
        // data loss — this probe is defense-in-depth for ordering.)
        const appended = await this.mapCollection(sentinel.collection).catch(() => 0);
        if (appended > 0) { await this.globalClaimLoop(); continue; }
        if ((await ledger.countRegularNonTerminal(this.runId, sentinel.collection)) > 0) continue;
        const chunk = await ledger.claimById(sentinel._id, this.podId, config.ledger.leaseSec);
        if (!chunk) continue;
        claimedAny = true;
        const log = this.logger.child({ collection: chunk.collection });
        if (chunk.attempts > MAX_CHUNK_ATTEMPTS) {
          await ledger.transition(chunk._id, 'in_progress', 'failed', {
            last_error: `exceeded ${MAX_CHUNK_ATTEMPTS} attempts (crash quarantine — inspect source docs in this cd window)`,
          });
          this.noteChunkFailure(log);
          continue;
        }
        await this.d.mongoReader.switchCollection(chunk.collection);
        this.currentCollection = chunk.collection;
        await this.processChunk(chunk, this.collectionDefaults.get(chunk.collection), log);
        this.lastStatusCounts = await ledger.statusCounts(this.runId);
      }
      if (claimedAny) continue;

      // Nothing claimable: complete when NOTHING is non-terminal anywhere.
      const counts = await ledger.statusCounts(this.runId);
      const nonTerminal = (counts.pending ?? 0) + (counts.in_progress ?? 0) + (counts.written ?? 0) + (counts.attaching ?? 0);
      if (nonTerminal === 0) return;
      if (!config.worker.enabled) {
        const orphans = await ledger.findRecoverable(this.runId, null, true);
        for (const orphan of orphans) await this.recoverOne(orphan, this.logger, true);
        continue;
      }
      await this.reclaimExpiredLeases(null, this.logger);
      await sleep(Math.min((config.ledger.leaseSec * 1000) / 2, 15_000));
    }
  }

  // -------------------------------------------------------------------------
  // Recovery & multi-pod lease reclaim
  // -------------------------------------------------------------------------

  private async recoverChunks(collection: string | null, log: Logger): Promise<void> {
    const includeAll = !this.d.config.worker.enabled;
    const recoverable = await this.d.ledger.findRecoverable(this.runId, collection, includeAll);
    for (const chunk of recoverable) {
      await this.recoverOne(chunk, log, includeAll);
    }
  }

  /** Periodic tick (multi-pod): reclaim chunks whose owner's lease expired. */
  private async reclaimExpiredLeases(collection: string | null, log: Logger): Promise<void> {
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

  private async recoverOne(chunk: ChunkDoc, log: Logger, ignoreLease = false): Promise<void> {
    const { ledger, staging } = this.d;
    // Single-winner recovery: reclaim atomically starts a new claim
    // generation. Concurrent recoverers race here and exactly one proceeds;
    // a zombie ex-owner is fenced out of every subsequent ledger mutation
    // (and of the mutating side of attach healing) by the attempts bump.
    const mine = await ledger.reclaim(chunk._id, chunk.status, this.podId, this.d.config.ledger.leaseSec, ignoreLease);
    if (!mine) return; // another pod reclaimed it (or the state moved on)
    const fence = { podId: this.podId, attempts: mine.attempts };
    const stagingTable = mine.staging_table;
    log.info({ chunk: mine._id, status: chunk.status, gen: mine.attempts }, 'Recovering chunk');

    try {
      if (chunk.status === 'in_progress') {
        // Mid-copy crash: never reconstruct — drop and redo.
        if (stagingTable) await staging.dropStaging(stagingTable);
        await ledger.transition(mine._id, 'in_progress', 'pending', { staging_table: null, pod_id: null }, fence);
        return;
      }
      if (chunk.status === 'written') {
        const count = stagingTable ? await staging.countRows(stagingTable).catch(() => -1) : -1;
        if (count === mine.rows_expected && count >= 0) {
          await this.promoteChunk(mine, log);
        } else {
          if (stagingTable) await staging.dropStaging(stagingTable);
          await ledger.transition(mine._id, 'written', 'pending', { staging_table: null, pod_id: null }, fence);
        }
        return;
      }
      if (chunk.status === 'attaching') {
        // The one state where blind retry is unsafe (double-attach
        // duplicates) — finishAttaching pair-accounts every partition.
        if (!stagingTable) {
          await ledger.transition(mine._id, 'attaching', 'pending', { staging_table: null, pod_id: null }, fence);
          return;
        }
        await this.finishAttaching(mine, log);
      }
    } catch (err) {
      if (err instanceof ClaimLostError) return; // lost the chunk mid-recovery — not ours anymore
      // A failed recovery attempt must never kill the pod (field crash:
      // ECONNREFUSED from a recovery-path ClickHouse call during an outage
      // propagated up through the claim loop). The chunk stays claimed by
      // us; when our lease expires it becomes recoverable again — retried
      // on a later tick, healed by the usual machinery.
      log.warn({ chunk: mine._id, err: (err as Error).message }, 'Recovery attempt failed — will retry after lease expiry');
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

  private stagingName(collection: string, idx: number, gen?: number): string {
    // gen = claim generation (attempts): a stalled worker resumed after
    // reclamation writes to ITS OWN table, never the new owner's. Orphaned
    // generations are swept by the prefix-based orphan sweep.
    const base = `${this.d.config.target.table}__stg_${shortHash(`${this.runId}:${collection}`)}_${idx}`;
    return gen !== undefined ? `${base}_g${gen}` : base;
  }

  private async processChunk(
    chunk: ChunkDoc,
    defaults: CollectionDefaults | undefined,
    log: Logger,
  ): Promise<void> {
    const { config, mongoReader, ledger, staging } = this.d;
    this.currentChunk = chunk._id;
    const fence = { podId: this.podId, attempts: chunk.attempts };
    const stagingTable = this.dryRun ? staging.dryRunTable : this.stagingName(chunk.collection, chunk.idx, chunk.attempts);
    const clog = log.child({ chunk: chunk.idx, staging: stagingTable });

    let claimLost = false;
    const heartbeat = setInterval(() => {
      ledger.heartbeat(chunk._id, this.podId, config.ledger.leaseSec, chunk.attempts)
        .then((owned) => { if (!owned) claimLost = true; })
        .catch(() => {});
    }, Math.min(Math.max(10_000, (config.ledger.leaseSec * 1000) / 3), Math.max(500, (config.ledger.leaseSec * 1000) / 2)));
    const assertClaim = (): void => {
      if (claimLost) throw new ClaimLostError(chunk._id);
    };
    this.assertClaimHook = assertClaim;

    try {
      if (!this.dryRun) {
        await staging.createStaging(stagingTable);
      }
      const owned = await ledger.transition(chunk._id, 'in_progress', 'in_progress', { staging_table: stagingTable }, fence);
      if (!owned) throw new ClaimLostError(chunk._id);

      const result = await this.copyChunk(chunk, stagingTable, defaults, clog);

      this.totalDocsRead += result.docsRead;
      this.totalDocsSkipped += result.docsSkipped;
      this.totalDocsDlq += result.docsDlq;

      assertClaim();
      const rowsExpected = result.docsRead - result.docsSkipped - result.docsDlq;
      const toWritten = await ledger.transition(chunk._id, 'in_progress', 'written', {
        docs_read: result.docsRead,
        docs_skipped: result.docsSkipped,
        rows_expected: rowsExpected,
      }, fence);
      if (!toWritten) throw new ClaimLostError(chunk._id);

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
        }, fence);
        this.noteChunkFailure(clog, 'permanent');
        this.pause('breaker-data');
        return;
      }

      if (this.dryRun) {
        // Null-engine target: nothing stored, nothing to verify or promote.
        await ledger.transition(chunk._id, 'written', 'done', { attach_method: null }, fence);
        this.chunksDone++;
        this.consecutiveFailed = 0;
        this.streakHadPermanent = false;
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
        }, fence);
        return;
      }

      await this.promoteChunk(
        { ...chunk, staging_table: stagingTable, rows_expected: rowsExpected, docs_read: result.docsRead, docs_skipped: result.docsSkipped },
        clog,
      );
      this.consecutiveFailed = 0;
      this.streakHadPermanent = false;

      // Per-commit under-read guard: the tally-independent check. Everything
      // else compares against docs READ; this one asks the SOURCE how many
      // docs the window holds. A silently truncated cursor (tally == staging
      // == live, all short) is caught here, at commit time, for the price of
      // one indexed count (~1% of chunk duration).
      if (config.ledger.sourceCountCheck && !this.isNullCdChunk(chunk)) {
        await this.sourceCountGuard(chunk, result.docsRead, clog);
      }

      clog.info(
        { docsRead: result.docsRead, docsSkipped: result.docsSkipped, dlq: result.docsDlq, rowsExpected },
        'Chunk done',
      );

      await this.checkDlqPressure(clog);
    } catch (err) {
      if (err instanceof ClaimLostError) {
        // Our lease was reclaimed while we were stalled — the chunk belongs
        // to someone else now. Abandon quietly: no transitions, and drop
        // only OUR generation's staging table.
        clog.warn({ chunk: chunk._id }, 'Claim lost (lease reclaimed while stalled) — abandoning chunk');
        if (!this.dryRun) await staging.dropStaging(stagingTable).catch(() => {});
        return;
      }
      const error = err as Error;
      const isPermanent = classifyError(err) === 'permanent';
      clog.error({ error: error.message, isPermanent }, 'Chunk failed');
      if (!this.dryRun) await staging.dropStaging(stagingTable).catch(() => {});
      const target = isPermanent || chunk.attempts >= MAX_CHUNK_ATTEMPTS ? 'failed' : 'pending';
      await ledger.transition(chunk._id, ['in_progress', 'written'], target, {
        staging_table: null,
        pod_id: null,
        last_error: error.message.slice(0, 500),
      }, fence);
      if (target === 'failed') this.noteChunkFailure(clog, isPermanent ? 'permanent' : 'transient');
    } finally {
      clearInterval(heartbeat);
      this.assertClaimHook = null;
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
    this.assertClaimHook?.();
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
    const fence = chunk.pod_id ? { podId: chunk.pod_id, attempts: chunk.attempts } : undefined;
    const moved = await ledger.transition(chunk._id, ['written', 'attaching'], 'attaching', { partitions, staging_table: stagingTable }, fence);
    if (!moved) throw new ClaimLostError(chunk._id);

    await this.finishAttaching({ ...chunk, partitions, attached: chunk.attached ?? [] }, log);
    this.stageMs.attach += performance.now() - aStart;
  }

  private async finishAttaching(chunk: ChunkDoc, log: Logger): Promise<void> {
    const { ledger, staging } = this.d;
    const fence = chunk.pod_id ? { podId: chunk.pod_id, attempts: chunk.attempts } : undefined;
    const stagingTable = chunk.staging_table!;
    const attachedSet = new Set(chunk.attached);
    let method: 'attach' | 'insert_select' = chunk.attach_method ?? 'attach';

    const remaining = chunk.partitions.filter((p) => !attachedSet.has(p));
    for (const partitionId of remaining) {
      // Verify-then-attach with EXACT pair accounting. `live` counts rows of
      // the live table matching this partition's staged (_id, cd) pairs —
      // precise for THIS chunk even when sibling collections share the month
      // partition and cd window:
      //   live === staged → already fully attached (crash landed between
      //                     ATTACH and recordAttached) — record, move on
      //   live === 0      → normal path — attach
      //   anything else   → double-attach (two recoverers raced pre-reclaim,
      //                     or a zombie attached concurrently) or a partial
      //                     promotion — heal: delete the matched pairs, then
      //                     attach fresh. Every mutating heal re-asserts
      //                     ownership first, so a fenced-out actor can never
      //                     issue the DELETE.
      const staged = await staging.countPartitionRows(stagingTable, partitionId);
      if (staged === 0) {
        await ledger.recordAttached(chunk._id, partitionId, fence);
        continue;
      }
      const live = await staging.countLiveMatchingStaged(stagingTable, partitionId);
      if (live === staged) {
        await ledger.recordAttached(chunk._id, partitionId, fence);
        continue;
      }
      if (live !== 0) {
        const owned = await ledger.transition(chunk._id, 'attaching', 'attaching', {}, fence);
        if (!owned) throw new ClaimLostError(chunk._id);
        log.error({ partition: partitionId, staged, live }, 'Pair-count anomaly (double-attach or partial promotion) — healing: delete matched pairs, attach fresh');
        await staging.deleteLiveMatchingStaged(stagingTable, partitionId);
      }
      try {
        await staging.attachPartition(stagingTable, partitionId);
      } catch (err) {
        if (attachedSet.size === 0 && remaining[0] === partitionId) {
          log.warn({ err: (err as Error).message }, 'ATTACH unavailable — falling back to INSERT SELECT');
          await staging.insertSelect(stagingTable);
          method = 'insert_select';
          for (const p of chunk.partitions) await ledger.recordAttached(chunk._id, p, fence);
          break;
        }
        throw err; // partial attach + failure → stays 'attaching', recovery resumes
      }
      // Post-attach re-count: a concurrent actor attaching between our count
      // and our ATTACH shows up as live > staged — heal once more.
      const after = await staging.countLiveMatchingStaged(stagingTable, partitionId);
      if (after > staged) {
        const owned = await ledger.transition(chunk._id, 'attaching', 'attaching', {}, fence);
        if (!owned) throw new ClaimLostError(chunk._id);
        log.error({ partition: partitionId, staged, after }, 'Concurrent double-attach detected post-attach — healing');
        await staging.deleteLiveMatchingStaged(stagingTable, partitionId);
        await staging.attachPartition(stagingTable, partitionId);
      }
      await ledger.recordAttached(chunk._id, partitionId, fence);
    }

    await ledger.transition(chunk._id, 'attaching', 'done', { attach_method: method }, fence);
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

  /**
   * cls semantics: only failures KNOWN to be data-permanent pass
   * 'permanent' (the classifier at the failure site decides). Unknown-cause
   * failures (crash quarantine) default to 'transient': if the streak has
   * no known-permanent failure, the pause arms the auto-resume probe — an
   * infra outage self-heals, while genuine poison re-fails through the
   * classified path on the next attempt and re-pauses as breaker-data.
   */
  private noteChunkFailure(log: Logger, cls: 'transient' | 'permanent' = 'transient'): void {
    this.chunksFailed++;
    this.consecutiveFailed++;
    if (cls === 'permanent') this.streakHadPermanent = true;
    if (this.consecutiveFailed >= this.d.config.ledger.breakerConsecutive) {
      const reason = this.streakHadPermanent ? 'breaker-data' : 'breaker-transient';
      log.error(
        { consecutiveFailed: this.consecutiveFailed, reason },
        'Circuit breaker: consecutive chunk failures — pausing engine' +
        (reason === 'breaker-transient' ? ' (transient causes — auto-resume armed, probing backends)' : ''),
      );
      this.pause(reason);
    }
  }

  /**
   * While paused by TRANSIENT failures (backend outage), probe both
   * backends; two consecutive healthy probes re-queue the failed chunks and
   * resume. Data pauses (breaker-data, operator, monitor, DLQ guard) never
   * auto-resume.
   */
  private async autoResumeProbe(): Promise<void> {
    if (!this.paused || this.pauseReason !== 'breaker-transient' || this.autoResuming) return;
    try {
      await this.d.staging.serverNowMs();
      await this.d.ledger.countForRun(this.runId);
      this.probeOkStreak++;
    } catch {
      this.probeOkStreak = 0;
      return;
    }
    if (this.probeOkStreak < 2) return;
    this.autoResuming = true;
    try {
      const { retried } = await this.retryFailed();
      this.logger.warn({ retried }, 'Backends healthy after transient-failure pause — auto-resumed, failed chunks re-queued');
      this.resume();
    } catch (err) {
      this.logger.warn({ err: (err as Error).message }, 'Auto-resume attempt failed — will re-probe');
      this.probeOkStreak = 0;
    } finally {
      this.autoResuming = false;
    }
  }

  /**
   * Post-commit under-read guard. Direction matters:
   *  - source > read (UNDER-read): the window holds docs the cursor never
   *    saw — the data-loss signal this guard exists for. Chunk fails, retry
   *    redoes the window.
   *  - source < read (source SHRANK): docs were deleted between our read
   *    and this recount. Field case: Mongo's retention TTL reaper deleting
   *    at the retention horizon while the migration passes it — every
   *    window one-retention-period old shrinks slightly, forever, so
   *    failing here would flap on retry chasing a moving target. The
   *    migration is a snapshot: it keeps what existed at read time; the
   *    source-vs-live audit reports the drift separately.
   */
  private async sourceCountGuard(chunk: ChunkDoc, docsRead: number, clog: Logger): Promise<void> {
    const srcCount = await this.d.mongoReader.getDatabase().collection(chunk.collection)
      .countDocuments({ cd: { $gte: new Date(chunk.lower_cd), $lt: new Date(chunk.upper_cd) } });
    if (srcCount === docsRead) return;
    if (srcCount < docsRead) {
      this.sourceShrankChunks++;
      clog.warn(
        { sourceCount: srcCount, docsRead, deleted: docsRead - srcCount },
        'Source shrank after read (retention TTL / deletions) — chunk stays done; migrated set is the snapshot at read time',
      );
      return;
    }
    clog.error(
      { sourceCount: srcCount, docsRead },
      'SOURCE-COUNT MISMATCH (under-read): window holds docs the read never saw — flagging chunk for redo',
    );
    await this.d.ledger.transition(chunk._id, 'done', 'failed', {
      last_error: `source-count mismatch: source=${srcCount} read=${docsRead} — under-read; retry redoes the window`,
    });
    this.noteChunkFailure(clog, 'permanent');
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
        this.pause('breaker-data');
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
   * Global DLQ mass guard. The per-chunk circuit breaker (5% of one chunk)
   * never trips on EVENLY-SPREAD failure — 1% of every chunk on a 10B-doc
   * migration would silently accumulate ~100M raw docs. When total pending
   * crosses the threshold, pause: that scale of DLQ means a systematic
   * problem to fix in the transform/source, not data to collect.
   */
  async checkDlqPressure(log: Logger, force = false): Promise<boolean> {
    const threshold = this.d.config.ledger.dlqPauseThreshold;
    if (threshold <= 0 || this.paused || this.dryRun) return false;
    if (!force && this.totalDocsDlq + this.totalDocsSkipped < threshold) return false; // cheap in-process pre-filter
    const counts = await this.d.dlq.countByStatus(this.runId);
    if ((counts.pending ?? 0) >= threshold) {
      const storage = await this.d.dlq.storageStats().catch(() => null);
      log.error(
        {
          pending: counts.pending,
          threshold,
          dlqStorageMB: storage ? Math.round(storage.dlqBytes / 1e6) : null,
          manifestDbDiskFreePct: storage?.diskFreePct ?? null,
        },
        'DLQ MASS GUARD: pending dead-letter docs crossed the threshold — pausing. ' +
        'This is a systematic problem: fix the cause, then either Retry failed chunks ' +
        '(redo re-reads the source) or Replay DLQ. Raise LEDGER_DLQ_PAUSE_THRESHOLD only ' +
        'if the disk numbers above say you can afford to keep collecting.',
      );
      this.pause('breaker-data');
      return true;
    }
    return false;
  }

  /** Live progress of a running sampled content audit. */
  readonly contentAuditProgress = {
    running: false, sampled: 0, matched: 0,
    mismatches: [] as Array<{ _id: string; collection: string; kind: string; fields?: string[] }>,
  };

  /**
   * Sampled doc-per-doc audit — the answer to what count-based verification
   * cannot see: the right NUMBER of wrong rows. Random source documents are
   * re-transformed and compared field-by-field against their live rows.
   * Scalar columns compare exactly; JSON columns (sg/up/custom/cmp) compare
   * by top-level key set (ClickHouse's JSON type normalizes value encodings,
   * so value-level equality there belongs to the differential harness, which
   * pins the transform itself).
   */
  async contentAudit(samplesPerCollection = 500): Promise<{
    sampled: number; matched: number; missing: number; different: number;
    mismatches: Array<{ _id: string; collection: string; kind: string; fields?: string[] }>;
  }> {
    const { config, staging } = this.d;
    const p = this.contentAuditProgress;
    p.running = true; p.sampled = 0; p.matched = 0; p.mismatches = [];
    try {
      const db = this.d.mongoReader.getDatabase();
      let collections = await discoverCollections(db, config.source.collectionPrefix, this.logger);
      const skipEventNames = new Set(['[CLY]_apm_device', '[CLY]_apm_network']);
      collections = collections.filter((name) => {
        const defaults = this.d.hashResolver.resolveCollectionName(name, config.source.collectionPrefix);
        return !(defaults && skipEventNames.has(defaults.e));
      });

      let missing = 0, different = 0;
      for (const collection of collections) {
        const defaults = this.d.hashResolver.resolveCollectionName(collection, config.source.collectionPrefix) ?? undefined;
        const coll = db.collection(collection);
        const [lowDoc] = await coll.find({ cd: { $type: 'date' } }).sort({ cd: 1 }).limit(1).project({ cd: 1 }).toArray();
        const [highDoc] = await coll.find({ cd: { $type: 'date' } }).sort({ cd: -1 }).limit(1).project({ cd: 1 }).toArray();
        if (!lowDoc || !highDoc) continue;
        const lo = (lowDoc.cd as Date).getTime(), hi = (highDoc.cd as Date).getTime();

        // K random cd probe points, a small run of docs from each — cheap
        // index-served sampling without $sample's whole-collection scan.
        const RUN_LEN = 25;
        const probes = Math.max(1, Math.ceil(samplesPerCollection / RUN_LEN));
        const docs: Record<string, unknown>[] = [];
        for (let k = 0; k < probes; k++) {
          const at = new Date(lo + Math.floor(((k + 0.5) / probes) * (hi - lo)));
          const page = await coll.find({ cd: { $gte: at } }).sort({ cd: 1, _id: 1 }).limit(RUN_LEN).toArray();
          docs.push(...(page as Record<string, unknown>[]));
        }

        const expected = new Map<string, OutputRow>();
        for (const doc of docs) {
          const { row } = transformDocument(doc as SourceDocument, defaults);
          if (row) expected.set(row._id, row);
        }
        if (expected.size === 0) continue;
        const expCds = [...expected.values()].map((r) => Date.parse(r.cd.replace(' ', 'T') + 'Z'));
        const live = await staging.fetchRowsByIds(
          [...expected.keys()],
          { loMs: Math.min(...expCds), hiMs: Math.max(...expCds) },
        );

        for (const [id, exp] of expected) {
          p.sampled++;
          const got = live.get(id);
          if (!got || String(got.cd_txt) !== exp.cd) {
            missing++;
            if (p.mismatches.length < 100) p.mismatches.push({ _id: id, collection, kind: 'missing (no live row with this (_id, cd))' });
            continue;
          }
          const bad: string[] = [];
          const eq = (a: unknown, b: unknown): boolean => (a ?? null) === (b ?? null);
          if (!eq(got.a, exp.a)) bad.push('a');
          if (!eq(got.e, exp.e)) bad.push('e');
          if (!eq(got.n, exp.n)) bad.push('n');
          if (!eq(got.uid, exp.uid)) bad.push('uid');
          if (!eq(got.uid_canon, exp.uid_canon)) bad.push('uid_canon');
          if (!eq(got.did, exp.did)) bad.push('did');
          if (!eq(got.lsid, exp.lsid)) bad.push('lsid');
          if (String(got.ts_txt) !== exp.ts) bad.push('ts');
          if (Number(got.c) !== exp.c) bad.push('c');
          if (Number(got.s) !== exp.s) bad.push('s');
          if (Number(got.dur) !== exp.dur) bad.push('dur');
          for (const jf of ['sg', 'up', 'custom', 'cmp'] as const) {
            const g = got[jf], x = exp[jf];
            const gKeys = g && typeof g === 'object' ? Object.keys(g as object).sort().join(',') : '';
            const xKeys = x && typeof x === 'object' ? Object.keys(x as object).sort().join(',') : '';
            if (gKeys !== xKeys) bad.push(jf + ':keys');
          }
          if (bad.length > 0) {
            different++;
            if (p.mismatches.length < 100) p.mismatches.push({ _id: id, collection, kind: 'field mismatch', fields: bad });
          } else {
            p.matched++;
          }
        }
      }
      return { sampled: p.sampled, matched: p.matched, missing, different, mismatches: p.mismatches };
    } finally {
      p.running = false;
    }
  }

  /** Live progress of a running DLQ replay (large queues take a while). */
  readonly replayProgress = { running: false, processed: 0, replayed: 0, stillFailing: 0, alreadyLive: 0 };

  /**
   * Replay pending DLQ entries: re-transform the stored raw docs under the
   * CURRENT transform version and insert them directly into the live table.
   * Safe to run anytime after the affected chunks are done — entries whose
   * rows are ALREADY live (e.g. a chunk redo with a fixed transform migrated
   * them from the source first) are marked resolved without inserting, so
   * redo-then-replay cannot duplicate.
   */
  async replayDlq(): Promise<{ replayed: number; stillFailing: number; alreadyLive: number }> {
    const { dlq, staging, retryPolicy, config } = this.d;
    // Dry run must never write the live table: replay rehearses against the
    // Null-engine table (full parse/type validation, nothing stored) —
    // field bug: a dry-run replay wrote real rows that the actual run would
    // then duplicate.
    const replayTarget = this.dryRun ? staging.dryRunTable : undefined;
    if (this.dryRun) await staging.ensureDryRunTable();
    let replayed = 0;
    let stillFailing = 0;
    let alreadyLive = 0;
    this.replayProgress.running = true;
    Object.assign(this.replayProgress, { processed: 0, replayed: 0, stillFailing: 0, alreadyLive: 0 });
    try {

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
      this.replayProgress.processed += batch.length;
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

      // Skip rows already live as (_id, cd) pairs — a chunk redo with a
      // fixed transform migrates DLQ'd docs from the source; replaying them
      // on top would duplicate. Marked resolved: the doc IS migrated.
      if (rows.length > 0 && !this.dryRun) {
        const cdMsOf = (r: OutputRow): number => Date.parse(r.cd.replace(' ', 'T') + 'Z');
        const cdVals = rows.map(cdMsOf);
        const liveCd = await staging.fetchLiveCdByIds(
          rows.map((r) => r._id),
          { loMs: Math.min(...cdVals), hiMs: Math.max(...cdVals) },
        );
        const keep: OutputRow[] = [];
        const keepIds: string[] = [];
        const resolvedIds: string[] = [];
        for (let j = 0; j < rows.length; j++) {
          const cdMs = Date.parse(rows[j].cd.replace(' ', 'T') + 'Z');
          if (liveCd.get(rows[j]._id) === cdMs) { resolvedIds.push(ids[j]); }
          else { keep.push(rows[j]); keepIds.push(ids[j]); }
        }
        if (resolvedIds.length > 0) {
          await dlq.markResolved(resolvedIds, config.transform.version + ' (already live — no insert)');
          alreadyLive += resolvedIds.length;
        }
        rows.length = 0; rows.push(...keep);
        ids.length = 0; ids.push(...keepIds);
      }
      if (rows.length === 0) { this.syncReplayProgress(replayed, stillFailing, alreadyLive); continue; }
      try {
        await retryPolicy.execute(
          () => staging.insertIntoLive(rows, `dlqreplay:${batchKey}`, replayTarget),
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
            await staging.insertIntoLive([rows[j]], `dlqreplay:${batchKey}:${j}`, replayTarget);
            await dlq.markResolved([ids[j]], config.transform.version);
            replayed++;
          } catch (rowErr) {
            await dlq.recordRetryError(ids[j], (rowErr as Error).message.slice(0, 1_000));
            stillFailing++;
          }
        }
        void err;
      }
      this.syncReplayProgress(replayed, stillFailing, alreadyLive);
    }

    this.logger.info({ replayed, stillFailing, alreadyLive }, 'DLQ replay complete');
    return { replayed, stillFailing, alreadyLive };
    } finally {
      this.replayProgress.running = false;
    }
  }

  private syncReplayProgress(replayed: number, stillFailing: number, alreadyLive: number): void {
    this.replayProgress.replayed = replayed;
    this.replayProgress.stillFailing = stillFailing;
    this.replayProgress.alreadyLive = alreadyLive;
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
      const boundMs = config.ledger.cdUpperBoundMs;
      if (boundMs !== null) {
        // Mirror-first: the source is EXPECTED to keep growing — the trust
        // boundary is the checkpoint bound, which must be safely in the past.
        const boundOk = chNow === 0 || boundMs < chNow - 60_000;
        checks.push({
          id: 'clock',
          label: 'Mirror checkpoint bound sane',
          status: boundOk ? 'pass' : 'fail',
          detail: boundOk
            ? `migrating cd < ${new Date(boundMs).toISOString()} only — everything at/after is the mirror's (source keeps growing, as expected in mirror-first mode)`
            : `LEDGER_CD_UPPER_BOUND ${new Date(boundMs).toISOString()} is not safely in the past vs ClickHouse time ${new Date(chNow).toISOString()} — set it to the mirror's recorded checkpoint`,
        });
      } else {
        const skewOk = maxSourceCd === 0 || chNow === 0 || maxSourceCd < chNow - 60_000;
        checks.push({
          id: 'clock',
          label: 'Source frozen & clocks sane (cd boundary)',
          status: skewOk ? 'pass' : 'fail',
          detail: skewOk
            ? `newest source cd ${maxSourceCd ? new Date(maxSourceCd).toISOString() : 'n/a'} is safely behind ClickHouse server time`
            : `newest source cd ${new Date(maxSourceCd).toISOString()} is within 60s of ClickHouse server time (${new Date(chNow).toISOString()}) — source still ingesting or clocks skewed; the migrated/live cd boundary is NOT trustworthy yet`,
        });
      }
      // Old ingestion stopped? Probe twice: if any collection's newest cd or
      // estimated count advances between probes, the source is still being
      // written — cutover step 'stop old ingestion' has not happened.
      if (config.ledger.cdUpperBoundMs !== null) {
        checks.push({
          id: 'frozen',
          label: 'Old ingestion running (mirror-first mode)',
          status: 'pass',
          detail: 'source keeps receiving writes by design — the mirror carries everything at/after the checkpoint; this migration only touches cd below it',
        });
      } else {
        const frozen = await this.probeSourceFrozen(db, collections, 4_000);
        checks.push({
          id: 'frozen',
          label: 'Old ingestion stopped (source frozen)',
          status: frozen.frozen ? 'pass' : 'fail',
          detail: frozen.frozen
            ? `no writes observed during a ${Math.round(frozen.probeMs / 1000)}s probe`
            : `STILL RECEIVING WRITES: ${frozen.grew.join(', ')} — stop old ingestion before migrating (works the same for new-cluster and same-cluster setups)`,
        });
      }

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
      pauseReason: this.pauseReason,
      sourceShrankChunks: this.sourceShrankChunks,
      cdUpperBoundMs: this.d.config.ledger.cdUpperBoundMs,
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
