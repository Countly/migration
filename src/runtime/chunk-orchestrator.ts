/**
 * ChunkOrchestrator — the `ledger` engine.
 *
 * Work model: each collection is split into cd-bounded chunks (sized by
 * estimated doc count). Per chunk: claim (atomic, leased, newest-first) →
 * copy into a per-chunk staging table (pipelined reads + a small window of
 * concurrent synchronous inserts) → verify (read tally vs exact ClickHouse
 * count) → promote (verify-then-ATTACH per partition, INSERT SELECT
 * fallback) → drop staging → done.
 *
 * Recovery model: the ledger is a worklist, never blindly trusted.
 *   in_progress → drop staging, redo whole chunk
 *   written     → recount staging; matches → promote, else drop + redo
 *   attaching   → per partition: already in live table? record : attach
 * No Redis anywhere; MongoDB (ledger) + ClickHouse are the only dependencies.
 */

import type { Logger } from 'pino';
import type { Config } from '../config/schema.ts';
import type { MongoReader } from '../source/mongo-reader.ts';
import type { HashResolver, CollectionDefaults } from '../transform/hash-resolver.ts';
import type { RetryPolicy } from './retry-policy.ts';
import { LedgerStore, type ChunkDoc } from '../state/ledger-store.ts';
import { StagingManager } from '../target/staging-manager.ts';
import { transformBatch, type OutputRow } from '../transform/normalize.ts';
import { SkipCounter } from '../transform/skip-reasons.ts';
import { classifyError } from './error-classifier.ts';
import { discoverCollections } from '../source/discover-collections.ts';
import type { Cursor } from '../types/cursor.ts';
import { createHash } from 'node:crypto';

export interface ChunkOrchestratorDeps {
  config: Config;
  logger: Logger;
  mongoReader: MongoReader;
  ledger: LedgerStore;
  staging: StagingManager;
  retryPolicy: RetryPolicy;
  hashResolver: HashResolver;
}

export interface LedgerEngineStats {
  engine: 'ledger';
  runId: string;
  podId: string;
  status: string;
  currentCollection: string | null;
  currentChunk: string | null;
  totalDocsRead: number;
  totalDocsSkipped: number;
  totalRowsInserted: number;
  chunksDone: number;
  chunksFailed: number;
  docsPerSecond: number;
  stageMs: { read: number; transform: number; insert: number; verify: number; attach: number };
  dedupWorks: boolean | null;
  chunkStatusCounts: Record<string, number>;
}

const MAX_CHUNK_ATTEMPTS = 3;

function shortHash(s: string): string {
  return createHash('sha1').update(s).digest('hex').slice(0, 8);
}

export class ChunkOrchestrator {
  private readonly d: ChunkOrchestratorDeps;
  private readonly logger: Logger;
  private readonly runId: string;
  private readonly podId: string;

  private status = 'idle';
  private stopping = false;
  private currentCollection: string | null = null;
  private currentChunk: string | null = null;
  private startedAt = 0;

  private totalDocsRead = 0;
  private totalDocsSkipped = 0;
  private totalRowsInserted = 0;
  private chunksDone = 0;
  private chunksFailed = 0;
  private stageMs = { read: 0, transform: 0, insert: 0, verify: 0, attach: 0 };
  private lastStatusCounts: Record<string, number> = {};

  constructor(deps: ChunkOrchestratorDeps) {
    this.d = deps;
    this.logger = deps.logger.child({ component: 'ChunkOrchestrator' });
    this.runId = deps.config.ledger.runId;
    this.podId = deps.config.worker.podId;
  }

  stopAfterChunk(): void {
    this.stopping = true;
  }

  getStatus(): string {
    return this.status;
  }

  async run(): Promise<void> {
    this.status = 'running';
    this.startedAt = Date.now();
    const { config } = this.d;

    await this.d.staging.runDedupCanary();

    const db = this.d.mongoReader.getDatabase();
    let collections = await discoverCollections(db, config.source.collectionPrefix, this.logger);

    // Same APM filtering as the classic engine
    const skipEventNames = new Set(['[CLY]_apm_device', '[CLY]_apm_network']);
    collections = collections.filter((name) => {
      const defaults = this.d.hashResolver.resolveCollectionName(name, config.source.collectionPrefix);
      return !(defaults && skipEventNames.has(defaults.e));
    });

    this.logger.info({ collections: collections.length, runId: this.runId }, 'Ledger engine starting');

    for (const collection of collections) {
      if (this.stopping) break;
      await this.processCollection(collection);
    }

    this.status = this.stopping ? 'stopped' : 'completed';
    this.logger.info(
      {
        status: this.status,
        chunksDone: this.chunksDone,
        chunksFailed: this.chunksFailed,
        totalDocsRead: this.totalDocsRead,
        totalRowsInserted: this.totalRowsInserted,
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
    const chunkCount = Math.max(1, Math.min(50_000, Math.ceil(estimated / config.ledger.chunkDocsTarget)));
    const spanMs = upper.cd + 1 - lower.cd;
    const bounds: Array<{ lowerCd: number; upperCd: number }> = [];
    for (let i = 0; i < chunkCount; i++) {
      const lo = lower.cd + Math.floor((spanMs * i) / chunkCount);
      const hi = i === chunkCount - 1 ? upper.cd + 1 : lower.cd + Math.floor((spanMs * (i + 1)) / chunkCount);
      if (hi > lo) bounds.push({ lowerCd: lo, upperCd: hi });
    }

    const created = await ledger.initChunks(this.runId, collection, bounds, config.transform.version);
    log.info({ estimated, chunks: bounds.length, created }, 'Chunk list ready');

    const defaults = this.d.hashResolver.resolveCollectionName(collection, config.source.collectionPrefix) ?? undefined;

    await this.recoverChunks(collection, defaults, log);

    // Work loop: claim newest-first until nothing is pending
    for (;;) {
      if (this.stopping) return;
      const chunk = await ledger.claimNext(this.runId, collection, this.podId, config.ledger.leaseSec);
      if (!chunk) break;
      if (chunk.attempts > MAX_CHUNK_ATTEMPTS) {
        await ledger.transition(chunk._id, 'in_progress', 'failed', {
          last_error: `exceeded ${MAX_CHUNK_ATTEMPTS} attempts`,
        });
        this.chunksFailed++;
        continue;
      }
      await this.processChunk(chunk, defaults, log);
      this.lastStatusCounts = await ledger.statusCounts(this.runId, collection);
    }

    this.lastStatusCounts = await ledger.statusCounts(this.runId, collection);
    log.info({ statusCounts: this.lastStatusCounts }, 'Collection complete');
    this.currentCollection = null;
  }

  // -------------------------------------------------------------------------
  // Startup / lease recovery
  // -------------------------------------------------------------------------

  private async recoverChunks(
    collection: string,
    defaults: CollectionDefaults | undefined,
    log: Logger,
  ): Promise<void> {
    const { ledger, staging } = this.d;
    // Single-pod default: recover everything non-terminal. Multi-pod: only expired leases.
    const includeAll = !this.d.config.worker.enabled;
    const recoverable = await ledger.findRecoverable(this.runId, collection, includeAll);

    for (const chunk of recoverable) {
      const stagingTable = chunk.staging_table ?? this.stagingName(collection, chunk.idx);
      log.info({ chunk: chunk._id, status: chunk.status }, 'Recovering chunk');

      if (chunk.status === 'in_progress') {
        // Mid-copy crash: never reconstruct — drop and redo.
        await staging.dropStaging(stagingTable);
        await ledger.transition(chunk._id, 'in_progress', 'pending', { staging_table: null, pod_id: null });
        continue;
      }

      if (chunk.status === 'written') {
        // Copy finished but promotion never started: recount, then promote or redo.
        const count = await staging.countRows(stagingTable).catch(() => -1);
        if (count === chunk.rows_expected && count >= 0) {
          await this.promoteChunk({ ...chunk, staging_table: stagingTable }, log);
        } else {
          await staging.dropStaging(stagingTable);
          await ledger.transition(chunk._id, 'written', 'pending', { staging_table: null, pod_id: null });
        }
        continue;
      }

      if (chunk.status === 'attaching') {
        // The one state where blind retry is unsafe (double-attach duplicates):
        // verify per partition before attaching what remains.
        await this.finishAttaching({ ...chunk, staging_table: stagingTable }, log);
      }
    }
    void defaults; // reserved for future recovery-time re-transform checks
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
    const { config, mongoReader, ledger, staging, retryPolicy } = this.d;
    this.currentChunk = chunk._id;
    const stagingTable = this.stagingName(chunk.collection, chunk.idx);
    const clog = log.child({ chunk: chunk.idx, staging: stagingTable });

    const heartbeat = setInterval(() => {
      ledger.heartbeat(chunk._id, this.podId, config.ledger.leaseSec).catch(() => {});
    }, Math.max(10_000, (config.ledger.leaseSec * 1000) / 3));

    try {
      await staging.createStaging(stagingTable);
      await ledger.transition(chunk._id, 'in_progress', 'in_progress', { staging_table: stagingTable });

      const skips = new SkipCounter();
      const upperBound: Cursor = { cd: chunk.upper_cd, id: '' };
      let cursor: Cursor | null = { cd: chunk.lower_cd, id: '' };
      let docsRead = 0;
      let batchSeq = 0;
      let firstError: Error | null = null;

      const inflight: Promise<void>[] = [];
      const pushInsert = (rows: OutputRow[]) => {
        const seq = batchSeq++;
        const p = retryPolicy
          .execute(
            () => staging.insertBatch(
              stagingTable,
              rows,
              `mig:${this.runId}:${chunk._id}:${seq}`,
              `mig__${shortHash(chunk._id)}__${seq}`,
            ),
            `chunk-${chunk.idx}-batch-${seq}`,
            clog,
            undefined,
            classifyError,
          )
          .then(() => {
            this.totalRowsInserted += rows.length;
          })
          .catch((err) => {
            if (!firstError) firstError = err as Error;
          });
        inflight.push(p);
      };

      // Pipelined read: prefetch the next page while transforming/inserting.
      // Track the cursor each read was issued with: readPage's min() bound is
      // INCLUSIVE, so every page after the first re-returns the previous
      // page's last doc — it must be dropped or it lands twice. (The classic
      // engine has this exact off-by-one; see the A/B findings.)
      const issueRead = (cur: Cursor | null) => ({
        curId: cur && cur.id !== '' ? cur.id : null,
        promise: mongoReader.readPage(cur, upperBound, config.source.mongoPageSize),
      });
      const t0 = performance.now();
      let tRead = 0;
      let pending = issueRead(cursor);
      for (;;) {
        const rStart = performance.now();
        const page = await pending.promise;
        tRead += performance.now() - rStart;
        if (page.docs.length === 0) break;

        let docs = page.docs;
        if (pending.curId !== null && String(docs[0]?._id) === pending.curId) {
          docs = docs.slice(1);
        }

        docsRead += docs.length;
        cursor = page.lastCursor;
        const isLast = page.docs.length < config.source.mongoPageSize;
        if (!isLast && !firstError) {
          pending = issueRead(cursor);
        }

        const tfStart = performance.now();
        const { rows } = transformBatch(docs, skips, defaults);
        this.stageMs.transform += performance.now() - tfStart;

        if (rows.length > 0) {
          pushInsert(rows);
          if (inflight.length >= config.ledger.insertInflight) {
            const iStart = performance.now();
            await inflight.shift();
            this.stageMs.insert += performance.now() - iStart;
          }
        }

        if (isLast || firstError) break;
      }
      this.stageMs.read += tRead;

      const iStart = performance.now();
      await Promise.all(inflight);
      this.stageMs.insert += performance.now() - iStart;

      const docsSkipped = skips.getTotal();
      this.totalDocsRead += docsRead;
      this.totalDocsSkipped += docsSkipped;

      if (firstError) {
        throw firstError;
      }

      const rowsExpected = docsRead - docsSkipped;
      await ledger.transition(chunk._id, 'in_progress', 'written', {
        docs_read: docsRead,
        docs_skipped: docsSkipped,
        rows_expected: rowsExpected,
      });

      // Verify: read tally vs exact ClickHouse count
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
        { ...chunk, staging_table: stagingTable, rows_expected: rowsExpected, docs_read: docsRead, docs_skipped: docsSkipped },
        clog,
      );

      clog.info(
        { docsRead, docsSkipped, rowsExpected, elapsedMs: Math.round(performance.now() - t0) },
        'Chunk done',
      );
    } catch (err) {
      const error = err as Error;
      const isPermanent = classifyError(err) === 'permanent';
      clog.error({ error: error.message, isPermanent }, 'Chunk failed');
      await staging.dropStaging(stagingTable).catch(() => {});
      // Permanent data errors won't fix themselves — mark failed for the
      // operator (future: bisection + DLQ). Transient: back to pending.
      const target = isPermanent || chunk.attempts >= MAX_CHUNK_ATTEMPTS ? 'failed' : 'pending';
      await ledger.transition(chunk._id, ['in_progress', 'written'], target, {
        staging_table: null,
        pod_id: null,
        last_error: error.message.slice(0, 500),
      });
      if (target === 'failed') this.chunksFailed++;
    } finally {
      clearInterval(heartbeat);
      this.currentChunk = null;
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

  /** Attach all not-yet-attached partitions, verify-then-attach, then finalize. */
  private async finishAttaching(chunk: ChunkDoc, log: Logger): Promise<void> {
    const { ledger, staging } = this.d;
    const stagingTable = chunk.staging_table!;
    const attachedSet = new Set(chunk.attached);
    let method: 'attach' | 'insert_select' = chunk.attach_method ?? 'attach';

    const remaining = chunk.partitions.filter((p) => !attachedSet.has(p));
    for (const partitionId of remaining) {
      // Verify-then-attach: if rows for this partition∩chunk already exist in
      // the live table, a previous attempt attached it — never attach twice.
      const already = await staging.countLiveInChunkPartition(partitionId, chunk.lower_cd, chunk.upper_cd);
      if (already > 0) {
        await ledger.recordAttached(chunk._id, partitionId);
        continue;
      }
      try {
        await staging.attachPartition(stagingTable, partitionId);
      } catch (err) {
        if (attachedSet.size === 0 && remaining[0] === partitionId) {
          // Nothing attached yet — safe to fall back to a full copy.
          log.warn({ err: (err as Error).message }, 'ATTACH unavailable — falling back to INSERT SELECT');
          await staging.insertSelect(stagingTable);
          method = 'insert_select';
          for (const p of chunk.partitions) await ledger.recordAttached(chunk._id, p);
          break;
        }
        throw err; // partial attach + failure → keep 'attaching', recovery resumes it
      }
      await ledger.recordAttached(chunk._id, partitionId);
    }

    await ledger.transition(chunk._id, 'attaching', 'done', { attach_method: method });
    await staging.dropStaging(stagingTable);
    this.chunksDone++;
  }

  // -------------------------------------------------------------------------
  // Stats
  // -------------------------------------------------------------------------

  getStats(): LedgerEngineStats {
    const elapsedSec = this.startedAt > 0 ? (Date.now() - this.startedAt) / 1000 : 0;
    return {
      engine: 'ledger',
      runId: this.runId,
      podId: this.podId,
      status: this.status,
      currentCollection: this.currentCollection,
      currentChunk: this.currentChunk,
      totalDocsRead: this.totalDocsRead,
      totalDocsSkipped: this.totalDocsSkipped,
      totalRowsInserted: this.totalRowsInserted,
      chunksDone: this.chunksDone,
      chunksFailed: this.chunksFailed,
      docsPerSecond: elapsedSec > 0 ? this.totalDocsRead / elapsedSec : 0,
      stageMs: {
        read: Math.round(this.stageMs.read),
        transform: Math.round(this.stageMs.transform),
        insert: Math.round(this.stageMs.insert),
        verify: Math.round(this.stageMs.verify),
        attach: Math.round(this.stageMs.attach),
      },
      dedupWorks: this.d.staging.dedupWorks,
      chunkStatusCounts: this.lastStatusCounts,
    };
  }
}
