/**
 * LedgerStore — the chunk checklist for the `ledger` engine.
 *
 * One MongoDB document per chunk of work. This is the ONLY progress state the
 * ledger engine keeps (no Redis): transitions happen a few times per chunk
 * (~30-60 min of work), and recovery never trusts the ledger blindly — it
 * verifies chunks against actual staging-table counts (see ChunkOrchestrator).
 *
 * Chunk lifecycle:
 *   pending → in_progress → written → attaching → done
 *                       ↘ failed (operator-visible, redo via reset)
 *
 * Claiming is an atomic findOneAndUpdate with a lease; a pod that dies simply
 * lets its lease expire and another pod reclaims the chunk (drop staging, redo).
 */

import { MongoClient, type Collection } from 'mongodb';
import type { Logger } from 'pino';

export type ChunkStatus = 'pending' | 'in_progress' | 'written' | 'attaching' | 'done' | 'failed' | 'superseded';

export interface ChunkDoc {
  _id: string;                 // `${runId}:${collection}:${idx}`
  run_id: string;
  collection: string;
  // Collection identity in ClickHouse terms. Hashed collections map 1:1 to an
  // (app, event) pair — every cd-window query against the LIVE table must be
  // scoped by these, because collections overlap in wall-clock time and the
  // live table holds them all. Null for unresolvable/base collections.
  scope_a: string | null;
  scope_e: string | null;
  scope_n: string | null;         // set for custom events (e='[CLY]_custom')
  idx: number;
  lower_cd: number;            // inclusive, epoch ms
  upper_cd: number;            // exclusive, epoch ms
  status: ChunkStatus;
  pod_id: string | null;
  lease_until: Date | null;
  staging_table: string | null;
  docs_read: number;
  docs_skipped: number;
  rows_expected: number;
  partitions: string[];        // partition ids discovered in staging at attach time
  attached: string[];          // partition ids confirmed attached to the live table
  attach_method: 'attach' | 'insert_select' | null;
  attempts: number;
  last_error: string | null;
  transform_version: string;
  updated_at: Date;
}

export class LedgerStore {
  private client: MongoClient;
  private coll: Collection<ChunkDoc> | null = null;
  private readonly logger: Logger;
  private readonly dbName: string;
  private readonly collectionName: string;

  constructor(uri: string, dbName: string, logger: Logger, collectionName = 'mig_ranges') {
    this.client = new MongoClient(uri);
    this.dbName = dbName;
    this.collectionName = collectionName;
    this.logger = logger.child({ component: 'LedgerStore' });
  }

  async connect(): Promise<void> {
    await this.client.connect();
    this.coll = this.client.db(this.dbName).collection<ChunkDoc>(this.collectionName);
    await this.coll.createIndex({ run_id: 1, collection: 1, status: 1, idx: -1 });
    // Global claim order: next available chunk across ALL collections
    await this.coll.createIndex({ run_id: 1, status: 1, collection: 1, idx: -1 });
    this.logger.info({ db: this.dbName, collection: this.collectionName }, 'LedgerStore connected');
  }

  async close(): Promise<void> {
    await this.client.close();
  }

  private c(): Collection<ChunkDoc> {
    if (!this.coll) throw new Error('LedgerStore not connected');
    return this.coll;
  }

  /**
   * Aggregated run summary — the UI's primary data source, so the dashboard
   * stays O(collections), not O(chunks) (a 10TB run can have tens of
   * thousands of chunks; shipping them all every 2s does not scale).
   */
  async summarize(runId: string): Promise<{
    total: number;
    byStatus: Record<string, number>;
    docsDone: number;
    perCollection: Array<{ collection: string; byStatus: Record<string, number>; docsDone: number; doneDocsRead: number; nonDoneRowsExpected: number }>;
  }> {
    const rows = await this.c().aggregate<{
      _id: { c: string; s: string }; n: number; docsDone: number; docsRead: number; nonDoneExpected: number;
    }>([
      { $match: { run_id: runId } },
      { $group: {
        _id: { c: '$collection', s: '$status' },
        n: { $sum: 1 },
        docsDone: { $sum: { $cond: [{ $eq: ['$status', 'done'] }, '$rows_expected', 0] } },
        docsRead: { $sum: { $cond: [{ $eq: ['$status', 'done'] }, '$docs_read', 0] } },
        nonDoneExpected: { $sum: { $cond: [{ $in: ['$status', ['pending', 'in_progress', 'written', 'attaching', 'failed']] }, '$rows_expected', 0] } },
      } },
    ]).toArray();
    const perColl = new Map<string, { collection: string; byStatus: Record<string, number>; docsDone: number; doneDocsRead: number; nonDoneRowsExpected: number }>();
    const byStatus: Record<string, number> = {};
    let total = 0, docsDone = 0;
    for (const r of rows) {
      const e = perColl.get(r._id.c) ?? { collection: r._id.c, byStatus: {}, docsDone: 0, doneDocsRead: 0, nonDoneRowsExpected: 0 };
      e.byStatus[r._id.s] = (e.byStatus[r._id.s] ?? 0) + r.n;
      e.docsDone += r.docsDone;
      e.doneDocsRead += r.docsRead;
      e.nonDoneRowsExpected += r.nonDoneExpected;
      perColl.set(r._id.c, e);
      byStatus[r._id.s] = (byStatus[r._id.s] ?? 0) + r.n;
      total += r.n;
      docsDone += r.docsDone;
    }
    return { total, byStatus, docsDone, perCollection: [...perColl.values()].sort((a, b) => a.collection.localeCompare(b.collection)) };
  }

  /** Non-terminal + failed chunk details, capped — the interesting ones on huge runs. */
  async listActive(runId: string, limit = 500): Promise<ChunkDoc[]> {
    return this.c()
      .find({ run_id: runId, status: { $in: ['pending', 'in_progress', 'written', 'attaching', 'failed'] } })
      .sort({ collection: 1, idx: 1 })
      .limit(limit)
      .toArray() as never;
  }

  /** Rebuild support: replace this run's entire ledger with regenerated chunks. */
  async replaceAllForRun(runId: string, docs: ChunkDoc[]): Promise<number> {
    await this.c().deleteMany({ run_id: runId });
    if (docs.length > 0) await this.c().insertMany(docs, { ordered: false });
    return docs.length;
  }

  async countForRun(runId: string): Promise<number> {
    return this.c().countDocuments({ run_id: runId });
  }

  /**
   * Idempotently create the chunk list for a collection. If any chunks
   * already exist for (runId, collection) this is a no-op — resume keeps
   * whatever bounds were originally cut.
   */
  async initChunks(
    runId: string,
    collection: string,
    bounds: Array<{ lowerCd: number; upperCd: number }>,
    transformVersion: string,
    scope?: { a: string; e: string; n?: string } | null,
  ): Promise<number> {
    const existing = await this.c().countDocuments({ run_id: runId, collection }, { limit: 1 });
    if (existing > 0) return 0;

    const now = new Date();
    const docs: ChunkDoc[] = bounds.map((b, idx) => ({
      _id: `${runId}:${collection}:${idx}`,
      run_id: runId,
      collection,
      scope_a: scope?.a ?? null,
      scope_e: scope?.e ?? null,
      scope_n: scope?.n ?? null,
      idx,
      lower_cd: b.lowerCd,
      upper_cd: b.upperCd,
      status: 'pending',
      pod_id: null,
      lease_until: null,
      staging_table: null,
      docs_read: 0,
      docs_skipped: 0,
      rows_expected: 0,
      partitions: [],
      attached: [],
      attach_method: null,
      attempts: 0,
      last_error: null,
      transform_version: transformVersion,
      updated_at: now,
    }));

    // The FIRST document is the reservation, exactly as in appendChunks:
    // pods that probed a live source at different instants compute DIFFERENT
    // grids, and unordered insertMany with swallowed duplicate keys would
    // interleave them into overlapping/gapping windows. insertOne on idx 0
    // (same _id for every racer) lets exactly one grid stand; the loser
    // returns 0 and falls into mapCollection's top-up path, which appends
    // any genuine delta beyond the winner's upper bound.
    try {
      await this.c().insertOne(docs[0]);
    } catch (err: unknown) {
      if ((err as { code?: number }).code === 11000) return 0; // another pod won the map
      throw err;
    }
    if (docs.length > 1) {
      // ordered:true so a crash mid-insert leaves a contiguous PREFIX of the
      // grid — resume then heals the remainder through the top-up path
      // (delta from the prefix's upper bound). An unordered partial insert
      // could leave holes no later pass would refill.
      try {
        await this.c().insertMany(docs.slice(1), { ordered: true });
      } catch (err: unknown) {
        if ((err as { code?: number }).code !== 11000) throw err;
      }
    }
    return docs.length;
  }

  /**
   * Top-up support: highest regular idx and upper_cd for a collection —
   * the append point for delta chunks (data that arrived after mapping).
   */
  async regularHighWater(runId: string, collection: string): Promise<{ maxIdx: number; maxUpperCd: number } | null> {
    const [top] = await this.c()
      .find({ run_id: runId, collection })
      .sort({ idx: -1 }).limit(1).project({ idx: 1 }).toArray();
    if (!top) return null;
    const [upper] = await this.c()
      .find({ run_id: runId, collection, lower_cd: { $gte: 0 } })
      .sort({ upper_cd: -1 }).limit(1).project({ upper_cd: 1 }).toArray();
    return { maxIdx: top.idx as number, maxUpperCd: (upper?.upper_cd as number) ?? 0 };
  }

  /**
   * Append delta chunks after the existing grid (idx continues). The FIRST
   * document is the reservation: it is inserted alone, and a duplicate key
   * there means another pod won the append for this startIdx — we return 0
   * and the caller re-probes on its next pass. This serializes concurrent
   * appends without a lock: racing pods that observed different source
   * maxima can no longer interleave two different grids into overlapping
   * windows.
   */
  async appendChunks(
    runId: string,
    collection: string,
    bounds: Array<{ lowerCd: number; upperCd: number }>,
    startIdx: number,
    transformVersion: string,
    scope?: { a: string; e: string; n?: string } | null,
  ): Promise<number> {
    if (bounds.length === 0) return 0;
    const now = new Date();
    const docs: ChunkDoc[] = bounds.map((b, i) => ({
      _id: `${runId}:${collection}:${startIdx + i}`,
      run_id: runId,
      collection,
      scope_a: scope?.a ?? null,
      scope_e: scope?.e ?? null,
      scope_n: scope?.n ?? null,
      idx: startIdx + i,
      lower_cd: b.lowerCd,
      upper_cd: b.upperCd,
      status: 'pending',
      pod_id: null,
      lease_until: null,
      staging_table: null,
      docs_read: 0,
      docs_skipped: 0,
      rows_expected: 0,
      partitions: [],
      attached: [],
      attach_method: null,
      attempts: 0,
      last_error: null,
      transform_version: transformVersion,
      updated_at: now,
    }));
    // The FIRST document is the reservation: racing pods that observed
    // different source maxima compute the same startIdx, so exactly one
    // insertOne wins — the loser aborts with 0 and re-probes next pass.
    // Without this, unordered insertMany with swallowed duplicate keys let
    // two different grids interleave into OVERLAPPING windows (= docs
    // migrated twice).
    try {
      await this.c().insertOne(docs[0]);
    } catch (err: unknown) {
      if ((err as { code?: number }).code === 11000) return 0; // lost the race
      throw err;
    }
    if (docs.length > 1) {
      try {
        await this.c().insertMany(docs.slice(1), { ordered: false });
      } catch (err: unknown) {
        if ((err as { code?: number }).code !== 11000) throw err;
      }
    }
    return docs.length;
  }

  /**
   * Atomically claim the next pending REGULAR chunk anywhere in the run —
   * collections in order, newest data first within each. Pods drain one
   * collection together and spill into the next the moment nothing is
   * claimable, so many-small-collection datasets parallelize across pods
   * instead of convoying (sentinels are never claimed here; the sweep phase
   * takes them per collection once its regulars are terminal).
   */
  async claimNextGlobal(runId: string, podId: string, leaseSec: number): Promise<ChunkDoc | null> {
    return this.c().findOneAndUpdate(
      { run_id: runId, status: 'pending', lower_cd: { $gte: 0 } },
      {
        $set: {
          status: 'in_progress',
          pod_id: podId,
          lease_until: new Date(Date.now() + leaseSec * 1000),
          updated_at: new Date(),
        },
        $inc: { attempts: 1 },
      },
      { sort: { collection: 1, idx: -1 }, returnDocument: 'after' },
    );
  }

  /** Pending null-cd sweep chunks (sentinel bounds {-1, 0}) across the run. */
  async listPendingSentinels(runId: string): Promise<ChunkDoc[]> {
    return this.c().find({ run_id: runId, status: 'pending', lower_cd: -1 }).toArray();
  }

  /** Guarded claim of one specific chunk (sweep phase). */
  async claimById(chunkId: string, podId: string, leaseSec: number): Promise<ChunkDoc | null> {
    return this.c().findOneAndUpdate(
      { _id: chunkId, status: 'pending' },
      {
        $set: {
          status: 'in_progress',
          pod_id: podId,
          lease_until: new Date(Date.now() + leaseSec * 1000),
          updated_at: new Date(),
        },
        $inc: { attempts: 1 },
      },
      { returnDocument: 'after' },
    );
  }

  /**
   * Atomically claim the next pending chunk, newest data first (highest idx).
   */
  async claimNext(
    runId: string,
    collection: string,
    podId: string,
    leaseSec: number,
    excludeSentinel = false,
  ): Promise<ChunkDoc | null> {
    const filter: Record<string, unknown> = { run_id: runId, collection, status: 'pending' };
    // The null-cd sweep (sentinel bounds lower_cd=-1) must run strictly AFTER
    // all regular chunks: its rows carry cd derived from ts, which lands
    // inside regular chunks' cd windows and would poison their
    // verify-then-attach checks.
    if (excludeSentinel) filter.lower_cd = { $gte: 0 };
    return this.c().findOneAndUpdate(
      filter,
      {
        $set: {
          status: 'in_progress',
          pod_id: podId,
          lease_until: new Date(Date.now() + leaseSec * 1000),
          updated_at: new Date(),
        },
        $inc: { attempts: 1 },
      },
      { sort: { idx: -1 }, returnDocument: 'after' },
    );
  }

  /**
   * Extend the lease of a chunk this pod is working on. Returns false when
   * the claim no longer belongs to this pod+generation — the worker was
   * stalled past its lease and someone reclaimed; it must abandon the chunk.
   */
  async heartbeat(chunkId: string, podId: string, leaseSec: number, attempts?: number): Promise<boolean> {
    const filter: Record<string, unknown> = { _id: chunkId, pod_id: podId, status: { $in: ['in_progress', 'written', 'attaching'] } };
    if (attempts !== undefined) filter.attempts = attempts;
    const res = await this.c().updateOne(
      filter,
      { $set: { lease_until: new Date(Date.now() + leaseSec * 1000), updated_at: new Date() } },
    );
    return res.matchedCount > 0;
  }

  /**
   * Guarded state transition. Returns the updated doc or null when the guard
   * failed (someone else moved the chunk — treat as lost claim).
   */
  async transition(
    chunkId: string,
    from: ChunkStatus | ChunkStatus[],
    to: ChunkStatus,
    patch: Partial<ChunkDoc> = {},
    fence?: { podId: string; attempts: number },
  ): Promise<ChunkDoc | null> {
    const fromArr = Array.isArray(from) ? from : [from];
    const filter: Record<string, unknown> = { _id: chunkId, status: { $in: fromArr } };
    // Claim fencing: a stalled worker that resumes after its lease was
    // reclaimed must not be able to move the NEW owner's claim. attempts
    // increments atomically on every claim, so (pod_id, attempts) uniquely
    // identifies one claim generation.
    if (fence) { filter.pod_id = fence.podId; filter.attempts = fence.attempts; }
    return this.c().findOneAndUpdate(
      filter,
      { $set: { ...patch, status: to, updated_at: new Date() } },
      { returnDocument: 'after' },
    );
  }

  /** Append one attached partition id (crash-safe attach progress). */
  async recordAttached(chunkId: string, partitionId: string, fence?: { podId: string; attempts: number }): Promise<void> {
    const filter: Record<string, unknown> = { _id: chunkId };
    if (fence) { filter.pod_id = fence.podId; filter.attempts = fence.attempts; }
    await this.c().updateOne(
      filter,
      { $addToSet: { attached: partitionId }, $set: { updated_at: new Date() } },
    );
  }

  /**
   * Chunks needing recovery: leases expired mid-work, or non-terminal states
   * left behind by a crashed pod (when includeAll, e.g. single-pod startup).
   */
  async findRecoverable(runId: string, collection: string | null, includeAll: boolean): Promise<ChunkDoc[]> {
    const nonTerminal: ChunkStatus[] = ['in_progress', 'written', 'attaching'];
    const filter: Record<string, unknown> = includeAll
      ? { run_id: runId, status: { $in: nonTerminal } }
      : { run_id: runId, status: { $in: nonTerminal }, lease_until: { $lt: new Date() } };
    if (collection !== null) filter.collection = collection;
    return this.c().find(filter).toArray();
  }

  async listByStatus(runId: string, collection: string, status: ChunkStatus): Promise<ChunkDoc[]> {
    return this.c().find({ run_id: runId, collection, status }).toArray();
  }

  /** Status → count map for progress reporting. */
  async statusCounts(runId: string, collection?: string): Promise<Record<string, number>> {
    const match: Record<string, unknown> = { run_id: runId };
    if (collection) match.collection = collection;
    const rows = await this.c()
      .aggregate<{ _id: string; n: number }>([
        { $match: match },
        { $group: { _id: '$status', n: { $sum: 1 } } },
      ])
      .toArray();
    return Object.fromEntries(rows.map((r) => [r._id, r.n]));
  }

  /**
   * Poison-pill quarantine: replace a chunk that keeps crashing the process
   * with `parts` fresh sub-chunks over its cd span. Repeated splitting
   * converges on a tiny window around the poison document. The original
   * chunk becomes `superseded` (terminal).
   */
  async splitChunk(chunk: ChunkDoc, parts: number): Promise<number> {
    const maxDoc = await this.c()
      .find({ run_id: chunk.run_id, collection: chunk.collection })
      .sort({ idx: -1 }).limit(1).project({ idx: 1 }).toArray();
    const baseIdx = (maxDoc[0]?.idx ?? 0) + 1;

    const span = chunk.upper_cd - chunk.lower_cd;
    const now = new Date();
    const subs: ChunkDoc[] = [];
    for (let i = 0; i < parts; i++) {
      const lo = chunk.lower_cd + Math.floor((span * i) / parts);
      const hi = i === parts - 1 ? chunk.upper_cd : chunk.lower_cd + Math.floor((span * (i + 1)) / parts);
      if (hi <= lo) continue;
      subs.push({
        _id: `${chunk.run_id}:${chunk.collection}:${baseIdx + i}`,
        run_id: chunk.run_id,
        collection: chunk.collection,
        scope_a: chunk.scope_a ?? null,
        scope_e: chunk.scope_e ?? null,
        scope_n: chunk.scope_n ?? null,
        idx: baseIdx + i,
        lower_cd: lo,
        upper_cd: hi,
        status: 'pending',
        pod_id: null,
        lease_until: null,
        staging_table: null,
        docs_read: 0,
        docs_skipped: 0,
        rows_expected: 0,
        partitions: [],
        attached: [],
        attach_method: null,
        attempts: 0,
        last_error: null,
        transform_version: chunk.transform_version,
        updated_at: now,
      });
    }
    await this.c().insertMany(subs, { ordered: false });
    await this.transition(chunk._id, ['in_progress', 'failed'], 'superseded', {
      last_error: `split into ${subs.length} sub-chunks (idx ${baseIdx}..${baseIdx + subs.length - 1}) after repeated crashes`,
    });
    return subs.length;
  }

  /** All chunks of a run (dashboard feed) — trimmed projection, idx order. */
  async listAll(runId: string): Promise<Array<Pick<ChunkDoc,
    '_id' | 'collection' | 'scope_a' | 'scope_e' | 'scope_n' | 'idx' | 'status' | 'lower_cd' | 'upper_cd' |
    'docs_read' | 'docs_skipped' | 'rows_expected' | 'attempts' | 'last_error' | 'pod_id' | 'updated_at'>>> {
    return this.c()
      .find(
        { run_id: runId },
        { projection: { collection: 1, scope_a: 1, scope_e: 1, scope_n: 1, idx: 1, status: 1, lower_cd: 1, upper_cd: 1,
          docs_read: 1, docs_skipped: 1, rows_expected: 1, attempts: 1, last_error: 1, pod_id: 1, updated_at: 1 } },
      )
      .sort({ collection: 1, idx: 1 })
      .toArray() as never;
  }

  /** Non-terminal REGULAR (non-sentinel) chunks — gates the null-cd sweep. */
  async countRegularNonTerminal(runId: string, collection?: string): Promise<number> {
    const filter: Record<string, unknown> = {
      run_id: runId,
      lower_cd: { $gte: 0 },
      status: { $in: ['pending', 'in_progress', 'written', 'attaching'] },
    };
    if (collection !== undefined) filter.collection = collection;
    return this.c().countDocuments(filter);
  }

  /** The null-cd sentinel chunk of a collection, if any. */
  async getSentinel(runId: string, collection: string): Promise<ChunkDoc | null> {
    return this.c().findOne({ run_id: runId, collection, lower_cd: -1, upper_cd: 0 });
  }

  /** Per-pod activity summary (Pods panel): who did what, who is alive. */
  async podActivity(runId: string): Promise<Array<{ pod: string; done: number; active: number; lastSeen: Date | null }>> {
    const rows = await this.c()
      .aggregate<{ _id: string; done: number; active: number; lastSeen: Date }>([
        { $match: { run_id: runId, pod_id: { $ne: null } } },
        { $group: {
          _id: '$pod_id',
          done: { $sum: { $cond: [{ $eq: ['$status', 'done'] }, 1, 0] } },
          active: { $sum: { $cond: [{ $in: ['$status', ['in_progress', 'written', 'attaching']] }, 1, 0] } },
          lastSeen: { $max: '$updated_at' },
        } },
        { $sort: { done: -1 } },
      ])
      .toArray();
    return rows.map((r) => ({ pod: r._id, done: r.done, active: r.active, lastSeen: r.lastSeen ?? null }));
  }

  /** Sum of expected rows for done chunks — used by full re-verification. */
  async expectedRows(runId: string, collection: string): Promise<number> {
    const rows = await this.c()
      .aggregate<{ total: number }>([
        { $match: { run_id: runId, collection, status: 'done' } },
        { $group: { _id: null, total: { $sum: '$rows_expected' } } },
      ])
      .toArray();
    return rows[0]?.total ?? 0;
  }
}
