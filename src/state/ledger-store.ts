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
  /** Fencing generation: every RESTORE bumps it, and prune mutations are predicated on the generation they snapshotted — a zombie prune resuming after a marker takeover carries stale generations and matches nothing. Absent = 0. */
  fence_gen?: number | null;
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
    /** Cluster truth — each pod's in-memory skip counter only knows its own share. */
    docsSkipped: number;
    perCollection: Array<{ collection: string; byStatus: Record<string, number>; docsDone: number; doneDocsRead: number; nonDoneRowsExpected: number }>;
  }> {
    const rows = await this.c().aggregate<{
      _id: { c: string; s: string }; n: number; docsDone: number; docsRead: number; nonDoneExpected: number; docsSkipped: number;
    }>([
      { $match: { run_id: runId } },
      { $group: {
        _id: { c: '$collection', s: '$status' },
        n: { $sum: 1 },
        docsDone: { $sum: { $cond: [{ $eq: ['$status', 'done'] }, '$rows_expected', 0] } },
        docsRead: { $sum: { $cond: [{ $eq: ['$status', 'done'] }, '$docs_read', 0] } },
        nonDoneExpected: { $sum: { $cond: [{ $in: ['$status', ['pending', 'in_progress', 'written', 'attaching', 'failed']] }, '$rows_expected', 0] } },
        docsSkipped: { $sum: '$docs_skipped' },
      } },
    ]).toArray();
    const perColl = new Map<string, { collection: string; byStatus: Record<string, number>; docsDone: number; doneDocsRead: number; nonDoneRowsExpected: number }>();
    const byStatus: Record<string, number> = {};
    let total = 0, docsDone = 0, docsSkipped = 0;
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
      docsSkipped += r.docsSkipped;
    }
    return { total, byStatus, docsDone, docsSkipped, perCollection: [...perColl.values()].sort((a, b) => a.collection.localeCompare(b.collection)) };
  }

  /** Non-terminal + failed chunk details, capped — the interesting ones on huge runs. */
  async listActive(runId: string, limit = 500): Promise<ChunkDoc[]> {
    // recently-done chunks stay listed for 2 min: on huge runs the map only
    // shows this active window, and completions VANISHING instead of
    // turning green read as "weird animation" in the field
    return this.c()
      .find({
        run_id: runId,
        $or: [
          { status: { $in: ['pending', 'in_progress', 'written', 'attaching', 'failed'] } },
          { status: 'done', updated_at: { $gt: new Date(Date.now() - 120_000) } },
        ],
      })
      .sort({ collection: 1, idx: 1 })
      .limit(limit)
      .toArray() as never;
  }

  /**
   * The "tape": every chunk in GLOBAL CLAIM ORDER (collection asc, idx
   * desc — the exact order claimNextGlobal serves). The chunk map renders
   * a row-aligned window over it so the migration cursor stays visually
   * anchored and the view advances line by line, never cell by cell.
   */
  async findFrontier(runId: string): Promise<ChunkDoc | null> {
    return this.c().findOne(
      { run_id: runId, status: { $in: ['pending', 'in_progress', 'written', 'attaching'] } },
      { sort: { collection: 1, idx: -1 } },
    );
  }

  /** Tape position of a chunk = how many chunks precede it in claim order. */
  async countTapeBefore(runId: string, chunk: { collection: string; idx: number }): Promise<number> {
    return this.c().countDocuments({
      run_id: runId,
      $or: [
        { collection: { $lt: chunk.collection } },
        { collection: chunk.collection, idx: { $gt: chunk.idx } },
      ],
    });
  }

  async tapeSlice(runId: string, skip: number, limit: number): Promise<ChunkDoc[]> {
    return this.c()
      .find({ run_id: runId })
      .sort({ collection: 1, idx: -1 })
      .skip(skip)
      .limit(limit)
      .toArray() as never;
  }

  /** All failed chunks (capped) — the failed table must never depend on the map window. */
  async listFailed(runId: string, limit = 200): Promise<ChunkDoc[]> {
    return this.c()
      .find({ run_id: runId, status: 'failed' })
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
    // map-vs-apply race: this pass may have read an older (or no) bound —
    // re-check the CURRENT one and clean our own delta before anyone claims
    await this.prunePendingBeyondStoredBound(runId);
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
    // map-vs-apply race: this pass may have read an older (or no) bound —
    // re-check the CURRENT one and clean our own delta before anyone claims
    await this.prunePendingBeyondStoredBound(runId);
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
   * Run-level stored bound (mig_run_config): the "apply the detected
   * boundary from the dashboard" path. Env (LEDGER_CD_UPPER_BOUND) always
   * wins as the source of truth when present; this store only fills in
   * when env is unset, and pods re-read it at every map pass.
   */
  private rc(): Collection<{
    _id: string; cd_upper_bound_ms: number; set_at: Date; set_by: string;
    bound_token?: string;
    apply_in_progress_token?: string; apply_in_progress_at?: Date;
    start_gate_open?: boolean; start_gate_opened_at?: Date; start_gate_opened_by?: string;
    unbounded_ok?: boolean; unbounded_ok_by?: string; unbounded_ok_at?: Date;
    maintenance_op?: string; maintenance_token?: string; maintenance_at?: Date;
  }> {
    if (!this.coll) throw new Error('LedgerStore not connected');
    return this.client.db(this.dbName).collection('mig_run_config');
  }

  /**
   * Map-time document estimates: the FIXED denominator for progress. The
   * live "to go" used to be re-estimated from moving averages and would
   * fluctuate (even increase); freezing the total at mapping makes
   * progress a true countdown. Top-up appends $inc by an exact delta count
   * — the total growing then is honest (new data really arrived).
   */
  private est(): Collection<{ _id: string; run_id: string; collection: string; estimated: number }> {
    if (!this.coll) throw new Error('LedgerStore not connected');
    return this.client.db(this.dbName).collection('mig_collection_est');
  }

  async setCollectionEstimate(runId: string, collection: string, estimated: number): Promise<void> {
    await this.est().updateOne(
      { _id: `${runId}:${collection}` },
      { $set: { run_id: runId, collection, estimated } },
      { upsert: true },
    );
  }

  async incCollectionEstimate(runId: string, collection: string, delta: number): Promise<void> {
    await this.est().updateOne(
      { _id: `${runId}:${collection}` },
      { $inc: { estimated: delta }, $setOnInsert: { run_id: runId, collection } },
      { upsert: true },
    );
  }

  async sumEstimates(runId: string): Promise<number | null> {
    const rows = await this.est().aggregate<{ total: number }>([
      { $match: { run_id: runId } },
      { $group: { _id: null, total: { $sum: '$estimated' } } },
    ]).toArray();
    return rows.length > 0 ? rows[0].total : null; // null = run mapped before this feature
  }

  /** First-ever start and first full completion — the run's wall-clock story. */
  async markRunStarted(runId: string): Promise<void> {
    await this.rc().updateOne(
      { _id: runId },
      { $setOnInsert: { run_started_at: new Date() } as never },
      { upsert: true },
    );
    // $setOnInsert misses the case where the doc exists (e.g. a stored
    // bound was applied before the first pod started) — pipeline-update
    // fills it exactly once either way
    await this.rc().updateOne(
      { _id: runId },
      [{ $set: { run_started_at: { $ifNull: ['$run_started_at', '$$NOW'] } } }] as never,
    );
  }

  async markRunCompleted(runId: string): Promise<void> {
    await this.rc().updateOne(
      { _id: runId },
      [{ $set: { run_completed_at: { $ifNull: ['$run_completed_at', '$$NOW'] } } }] as never,
      { upsert: true },
    );
  }

  async getRunTimes(runId: string): Promise<{ startedAtMs: number | null; completedAtMs: number | null }> {
    const doc = await this.rc().findOne({ _id: runId }) as unknown as { run_started_at?: Date; run_completed_at?: Date } | null;
    return {
      startedAtMs: doc?.run_started_at ? doc.run_started_at.getTime() : null,
      completedAtMs: doc?.run_completed_at ? doc.run_completed_at.getTime() : null,
    };
  }

  /**
   * Start gate (mig_run_config): with LEDGER_START_PAUSED set, pods hold
   * before mapping until this is opened once for the run. It lives with the
   * run rather than in one pod's memory, so a single Start covers every pod
   * (including ones that join afterwards) and survives restarts.
   */
  async isStartGateOpen(runId: string): Promise<boolean> {
    const doc = await this.rc().findOne({ _id: runId });
    return doc?.start_gate_open === true;
  }

  async openStartGate(runId: string, openedBy: string): Promise<void> {
    await this.rc().updateOne(
      { _id: runId },
      [{ $set: {
        start_gate_open: true,
        start_gate_opened_at: { $ifNull: ['$start_gate_opened_at', '$$NOW'] },
        start_gate_opened_by: { $ifNull: ['$start_gate_opened_by', openedBy] },
      } }] as never,
      { upsert: true },
    );
  }

  async getStoredBound(runId: string): Promise<number | null> {
    const doc = await this.rc().findOne({ _id: runId });
    return doc?.cd_upper_bound_ms ?? null;
  }

  /** Bound plus its ownership token — fence mutations record the token so a rolled-back apply can restore exactly what its bound caused. */
  async getStoredBoundInfo(runId: string): Promise<{ boundMs: number; token: string | null } | null> {
    const doc = await this.rc().findOne({ _id: runId });
    if (doc?.cd_upper_bound_ms === undefined) return null;
    return { boundMs: doc.cd_upper_bound_ms, token: doc.bound_token ?? null };
  }

  /** One read for the post-claim fence: the bound, its token, and whether an apply is mid-flight (provisional grid — claims must not hold anything). */
  async getBoundState(runId: string): Promise<{ boundMs: number | null; token: string | null; applying: boolean }> {
    const doc = await this.rc().findOne({ _id: runId });
    // a marker older than 10 min is a crashed apply — never let it stall the run
    const applying = !!doc?.apply_in_progress_token
      && (doc.apply_in_progress_at?.getTime() ?? 0) > Date.now() - 600_000;
    return { boundMs: doc?.cd_upper_bound_ms ?? null, token: doc?.bound_token ?? null, applying };
  }

  /** Token-scoped heartbeat: keep a LEGITIMATE long apply's marker alive (huge-grid prunes, MongoDB stalls). False = the token was taken over — the apply must abort. */
  async renewApplyMarker(runId: string, token: string): Promise<boolean> {
    const res = await this.rc().updateOne(
      { _id: runId, apply_in_progress_token: token },
      { $set: { apply_in_progress_at: new Date() } },
    );
    return res.matchedCount > 0;
  }

  /**
   * CAS-acquire the CLUSTER-WIDE maintenance reservation: final check and
   * dedupe are destructive-vs-audit exclusive, and pods route their HTTP
   * requests independently — a process-local lock cannot see the other
   * pod's operation. Stale after 10 min without renewal (running ops
   * heartbeat); a crashed holder's reservation is taken over then.
   */
  async acquireMaintenance(runId: string, op: string, token: string): Promise<{ acquired: boolean; holder?: string }> {
    const staleBefore = new Date(Date.now() - 600_000);
    try {
      const res = await this.rc().updateOne(
        {
          _id: runId,
          $or: [
            { maintenance_token: { $exists: false } },
            { maintenance_at: { $lt: staleBefore } },
          ],
        },
        { $set: { maintenance_op: op, maintenance_token: token, maintenance_at: new Date() } },
        { upsert: true },
      );
      if (res.matchedCount > 0 || (res.upsertedCount ?? 0) === 1) return { acquired: true };
    } catch (err) {
      if ((err as { code?: number }).code !== 11000) throw err;
    }
    const doc = await this.rc().findOne({ _id: runId });
    return { acquired: false, holder: doc?.maintenance_op ?? 'unknown' };
  }

  /** Token-scoped heartbeat for the maintenance reservation. */
  async renewMaintenance(runId: string, token: string): Promise<boolean> {
    const res = await this.rc().updateOne(
      { _id: runId, maintenance_token: token },
      { $set: { maintenance_at: new Date() } },
    );
    return res.matchedCount > 0;
  }

  /** Release the maintenance reservation — only its own token can. */
  async releaseMaintenance(runId: string, token: string): Promise<void> {
    await this.rc().updateOne(
      { _id: runId, maintenance_token: token },
      { $unset: { maintenance_op: '', maintenance_token: '', maintenance_at: '' } },
    );
  }

  /**
   * ACQUIRE the apply marker — compare-and-set: succeeds only when no live
   * marker exists (absent, or stale past the 10-minute crash expiry), so two
   * applies can never interleave and one's clear can never expose the
   * other's provisional grid. The fence releases every claim while any live
   * marker is set.
   */
  async acquireApplyMarker(runId: string, token: string): Promise<boolean> {
    const staleBefore = new Date(Date.now() - 600_000);
    try {
      const res = await this.rc().updateOne(
        {
          _id: runId,
          $or: [
            { apply_in_progress_token: { $exists: false } },
            { apply_in_progress_at: { $lt: staleBefore } },
          ],
        },
        { $set: { apply_in_progress_token: token, apply_in_progress_at: new Date() } },
        { upsert: true },
      );
      return res.matchedCount > 0 || (res.upsertedCount ?? 0) === 1;
    } catch (err) {
      // duplicate key on upsert = a live marker exists on the doc
      if ((err as { code?: number }).code === 11000) return false;
      throw err;
    }
  }

  /** Clear only this apply's marker (token-scoped) — a competing apply's marker survives. */
  async clearApplyMarker(runId: string, token: string): Promise<boolean> {
    const res = await this.rc().updateOne(
      { _id: runId, apply_in_progress_token: token },
      { $unset: { apply_in_progress_token: '', apply_in_progress_at: '' } },
    );
    return res.matchedCount > 0;
  }

  /** Cluster-wide operator answer to the startup guard: "nothing mirrors traffic — run unbounded". */
  async getUnboundedAck(runId: string): Promise<boolean> {
    const doc = await this.rc().findOne({ _id: runId });
    return doc?.unbounded_ok === true;
  }

  async setUnboundedAck(runId: string, by: string): Promise<void> {
    await this.rc().updateOne(
      { _id: runId },
      { $set: { unbounded_ok: true, unbounded_ok_by: by, unbounded_ok_at: new Date() } },
      { upsert: true },
    );
  }

  /**
   * Durable change marker for a run's chunk state: any claim, completion,
   * retry or remap moves it — including work that starts AND finishes
   * between two snapshots (which an activeClaims poll would never see).
   */
  async runFingerprint(runId: string): Promise<string> {
    const [row] = await this.c().aggregate<{ n: number; done: number; maxU: Date | null }>([
      { $match: { run_id: runId } },
      { $group: {
        _id: null, n: { $sum: 1 },
        done: { $sum: { $cond: [{ $eq: ['$status', 'done'] }, 1, 0] } },
        maxU: { $max: '$updated_at' },
      } },
    ]).toArray();
    return row ? `${row.n}:${row.done}:${row.maxU ? row.maxU.getTime() : 0}` : '0:0:0';
  }

  /** Supersede a chunk this pod holds — the bound says it must never be read. The bound's token is recorded so a rolled-back apply can restore exactly its own casualties. */
  async supersede(chunkId: string, podId: string, boundToken?: string | null): Promise<void> {
    await this.c().updateOne(
      { _id: chunkId, pod_id: podId },
      { $set: { status: 'superseded', pod_id: null, lease_until: null, updated_at: new Date(), ...(boundToken ? { superseded_by_token: boundToken } : {}) } as never },
    );
  }

  /** Bring back the chunks a specific bound's fence superseded — its apply rolled back, so no bound governs them any more. */
  async restoreSuperseded(runId: string, boundToken: string): Promise<number> {
    const res = await this.c().updateMany(
      { run_id: runId, status: 'superseded', superseded_by_token: boundToken } as never,
      { $set: { status: 'pending', pod_id: null, lease_until: null, updated_at: new Date() }, $unset: { superseded_by_token: '' } } as never,
    );
    return res.modifiedCount ?? 0;
  }

  /** Release a claim untouched (status back to pending) — used when configuration cannot be read. */
  async releaseClaim(chunkId: string, podId: string): Promise<void> {
    await this.c().updateOne(
      { _id: chunkId, pod_id: podId, status: 'in_progress' },
      { $set: { status: 'pending', pod_id: null, lease_until: null, updated_at: new Date() } },
    );
  }

  /** Clamp a chunk's upper edge to the bound (claimed straddler). */
  async clampUpper(chunkId: string, boundMs: number): Promise<void> {
    await this.c().updateOne({ _id: chunkId }, { $set: { upper_cd: boundMs, updated_at: new Date() } });
  }

  /**
   * Self-heal for the map-vs-apply race: a map pass that read no bound (or
   * an older one) may insert chunks a just-applied bound forbids. Called by
   * the chunk-insert paths AFTER inserting: re-reads the CURRENT stored
   * bound and prunes/clamps pending chunks beyond it, so a stale pass
   * cleans up its own delta before the claim loop can drain it.
   */
  async prunePendingBeyondStoredBound(runId: string): Promise<number> {
    // While an apply is mid-flight the stored bound may be PROVISIONAL and
    // this cleanup keeps no receipt — a rollback could never restore what it
    // deletes (and a collection whose regulars all died here while its
    // null-cd sentinel survived would never remap). The marker is acquired
    // BEFORE the store, so applying=false means the bound read is settled;
    // deferring costs nothing: the apply's own prune, the post-claim fence,
    // and the next map pass all cover the interim.
    const state = await this.getBoundState(runId);
    if (state.applying) return 0;
    const bound = state.boundMs;
    if (bound === null) return 0;
    const del = await this.c().deleteMany({ run_id: runId, lower_cd: { $gte: bound }, status: 'pending' });
    await this.c().updateMany(
      { run_id: runId, lower_cd: { $gte: 0, $lt: bound }, upper_cd: { $gt: bound }, status: 'pending' },
      { $set: { upper_cd: bound, updated_at: new Date() } },
    );
    return del.deletedCount ?? 0;
  }

  /** Roll back a bound whose post-store verification failed — apply must never leave a half-applied bound behind. */
  async clearStoredBound(runId: string): Promise<void> {
    await this.rc().updateOne({ _id: runId }, { $unset: { cd_upper_bound_ms: '', set_at: '', set_by: '' } });
  }

  /**
   * Compare-and-set: store the bound only if the current stored value still
   * equals what the caller validated against. Returns an OWNERSHIP TOKEN:
   * the rollback predicate matches the token, not the value, so an
   * identical-value re-apply by someone else (value-ABA) is never unwound
   * by this caller's rollback.
   */
  async setStoredBoundIf(runId: string, boundMs: number, setBy: string, expectedPrior: number | null, mintedToken?: string): Promise<string | null> {
    // the caller may mint the token BEFORE the write: on a lost
    // acknowledgement it still knows what to roll back by
    const token = mintedToken ?? `${setBy}:${Date.now()}:${Math.random().toString(36).slice(2, 10)}`;
    if (expectedPrior === null) {
      const res = await this.rc().updateOne(
        { _id: runId, cd_upper_bound_ms: { $exists: false } },
        { $set: { cd_upper_bound_ms: boundMs, set_at: new Date(), set_by: setBy, bound_token: token } },
        { upsert: true },
      ).catch((err: unknown) => {
        // duplicate-key on upsert = the doc appeared with a bound mid-flight
        if ((err as { code?: number }).code === 11000) return { matchedCount: 0, upsertedCount: 0 };
        throw err;
      });
      return (res.matchedCount > 0 || (res as { upsertedCount?: number }).upsertedCount === 1) ? token : null;
    }
    const res = await this.rc().updateOne(
      { _id: runId, cd_upper_bound_ms: expectedPrior },
      { $set: { cd_upper_bound_ms: boundMs, set_at: new Date(), set_by: setBy, bound_token: token } },
    );
    return res.matchedCount > 0 ? token : null;
  }

  /** Unwind ONLY the store identified by the token — restores the prior value or clears. Returns false if someone else's store governs now. */
  async rollbackStoredBound(runId: string, token: string, priorBound: number | null): Promise<boolean> {
    const res = priorBound === null
      ? await this.rc().updateOne(
        { _id: runId, bound_token: token },
        { $unset: { cd_upper_bound_ms: '', set_at: '', set_by: '', bound_token: '' } },
      )
      : await this.rc().updateOne(
        { _id: runId, bound_token: token },
        { $set: { cd_upper_bound_ms: priorBound, set_at: new Date(), set_by: 'rollback', bound_token: `rollback:${token}` } },
      );
    return res.matchedCount > 0;
  }

  /** Clear the bound only if it still holds the value this caller stored — a rollback must never clobber a bound another apply won meanwhile. */
  async clearStoredBoundIf(runId: string, expected: number): Promise<boolean> {
    const res = await this.rc().updateOne(
      { _id: runId, cd_upper_bound_ms: expected },
      { $unset: { cd_upper_bound_ms: '', set_at: '', set_by: '' } },
    );
    return res.matchedCount > 0;
  }

  async setStoredBound(runId: string, boundMs: number, setBy: string): Promise<void> {
    await this.rc().updateOne(
      { _id: runId },
      { $set: { cd_upper_bound_ms: boundMs, set_at: new Date(), set_by: setBy } },
      { upsert: true },
    );
  }

  /**
   * Applying a bound to an ALREADY-MAPPED run: the grid was cut without it,
   * so pending chunks past the bound must go. Regular pending chunks fully
   * beyond are deleted; a pending straddler is clamped to end AT the bound.
   * Refuses when any non-pending chunk reaches past the bound — that data
   * (possibly) already moved and needs purge tooling, not a config flip.
   */
  async pruneBeyondBound(runId: string, boundMs: number, receiptSink?: (r: { deletedChunks: ChunkDoc[]; clampedChunks: Array<{ _id: string; upper_cd: number; fence_gen?: number | null }> }) => void | Promise<void>, ownerToken?: string): Promise<{
    deleted: number; clamped: number;
    /** What the prune changed, verbatim — a raced apply restores it. */
    restore: { deletedChunks: ChunkDoc[]; clampedChunks: Array<{ _id: string; upper_cd: number; fence_gen?: number | null }> };
  }> {
    const busy = await this.c().countDocuments({
      run_id: runId, lower_cd: { $gte: 0 }, upper_cd: { $gt: boundMs },
      status: { $nin: ['pending'] },
    });
    if (busy > 0) {
      throw new Error(`${busy} non-pending chunk(s) already reach past the bound — their windows may hold migrated post-bound data; purge/retry them first`);
    }
    // snapshot EVERYTHING first, hand the receipt to the caller, and only
    // then write: a destructive write whose acknowledgement is lost must
    // still be restorable by the caller
    const deletedChunks = await this.c()
      .find({ run_id: runId, lower_cd: { $gte: boundMs }, status: 'pending' })
      .toArray();
    const clampedChunks = (await this.c()
      .find(
        { run_id: runId, lower_cd: { $gte: 0, $lt: boundMs }, upper_cd: { $gt: boundMs }, status: 'pending' },
        { projection: { _id: 1, upper_cd: 1, fence_gen: 1 } },
      )
      .toArray()).map((c) => ({ _id: String(c._id), upper_cd: c.upper_cd, fence_gen: (c.fence_gen as number | undefined) ?? null }));
    // awaited: a sink that persists the receipt durably must finish BEFORE
    // the destructive writes below — its failure aborts the prune untouched
    await receiptSink?.({ deletedChunks, clampedChunks });
    // Ownership fence: a pod that stalled past the marker expiry INSIDE this
    // call must not resume writing after a takeover recovered its journal —
    // the renewal doubles as the check and shrinks the zombie window from
    // the whole prune to the instant before each destructive write.
    if (ownerToken !== undefined && !(await this.renewApplyMarker(runId, ownerToken))) {
      throw new Error('the apply marker was taken over — prune aborted before its destructive delete');
    }
    // FENCED per chunk: each delete is predicated on the fence generation
    // the snapshot saw (null matches the absent field). A restore bumps the
    // generation, so a zombie prune resuming these writes after a takeover
    // recovered its journal deletes NOTHING the restore brought back — the
    // check-then-write pair is atomic per document, not merely adjacent.
    const del = deletedChunks.length === 0 ? { deletedCount: 0 } : await this.c().bulkWrite(
      deletedChunks.map((c) => ({
        deleteOne: { filter: { _id: c._id, status: 'pending', fence_gen: (c.fence_gen as number | undefined) ?? null } },
      })),
      { ordered: false },
    );
    // clamp ONLY the snapshotted ids: a straddler inserted after the
    // snapshot must not be modified outside the receipt (a rollback would
    // leave it truncated under a rejected bound) — the insert-path
    // self-prune and the post-claim fence own anything newer
    if (ownerToken !== undefined && !(await this.renewApplyMarker(runId, ownerToken))) {
      throw new Error('the apply marker was taken over — prune aborted before its destructive clamp');
    }
    // clamps are fenced the same way, and BUMP the generation themselves so
    // a zombie's re-clamp with the snapshotted generation misses
    const clamp = clampedChunks.length === 0 ? { modifiedCount: 0 } : await this.c().bulkWrite(
      clampedChunks.map((c) => ({
        updateOne: {
          filter: { _id: c._id, status: 'pending', fence_gen: c.fence_gen ?? null },
          update: { $set: { upper_cd: boundMs, updated_at: new Date() }, $inc: { fence_gen: 1 } },
        },
      })),
      { ordered: false },
    );
    return { deleted: del.deletedCount ?? 0, clamped: clamp.modifiedCount ?? 0, restore: { deletedChunks, clampedChunks } };
  }

  /**
   * Undo a prune whose apply raced — re-insert deleted pending chunks,
   * un-clamp straddlers (only while still pending). When another apply WON
   * meanwhile, its bound governs: chunks at/beyond it stay pruned and
   * straddlers stay clamped to it, so a losing rollback can never resurrect
   * what the winning bound removed.
   */
  async restorePrune(
    restore: { deletedChunks: ChunkDoc[]; clampedChunks: Array<{ _id: string; upper_cd: number; fence_gen?: number | null }> },
    currentBoundMs: number | null = null,
  ): Promise<void> {
    // every restored document carries a BUMPED fence generation: the pruner
    // whose receipt this is predicated its writes on the generation it
    // snapshotted, so a zombie resuming those writes after this restore
    // matches nothing
    const insertable = (currentBoundMs === null
      ? restore.deletedChunks
      : restore.deletedChunks.filter((c) => c.lower_cd < currentBoundMs).map((c) => (
        c.upper_cd > currentBoundMs ? { ...c, upper_cd: currentBoundMs } : c
      ))).map((c) => ({ ...c, fence_gen: ((c.fence_gen as number | undefined) ?? 0) + 1 }));
    if (insertable.length > 0) {
      try {
        await this.c().insertMany(insertable, { ordered: false });
      } catch (err) {
        // re-inserting is idempotent — chunks already present are fine; any
        // OTHER failure means the grid was NOT restored and must propagate
        const e = err as { code?: number; writeErrors?: Array<{ code?: number }> };
        // when per-write errors exist THEY are the truth — a top-level 11000
        // can front a mixed batch where other writes failed for real reasons
        const we = e.writeErrors ?? [];
        const dupOnly = we.length > 0 ? we.every((w) => w.code === 11000) : e.code === 11000;
        if (!dupOnly) throw err;
      }
    }
    for (const c of restore.clampedChunks) {
      const upper = currentBoundMs !== null ? Math.min(c.upper_cd, currentBoundMs) : c.upper_cd;
      // $inc bumps past the pruner's snapshotted generation — its zombie
      // re-clamp then matches nothing
      await this.c().updateOne({ _id: c._id, status: 'pending' }, { $set: { upper_cd: upper, updated_at: new Date() }, $inc: { fence_gen: 1 } });
    }
  }

  /** Prune journal (mig_prune_journal): receipts persisted BEFORE each destructive prune. */
  private pj(): Collection<{
    run_id: string; token: string; created_at: Date;
    receipt: { deletedChunks: ChunkDoc[]; clampedChunks: Array<{ _id: string; upper_cd: number }> };
  }> {
    if (!this.coll) throw new Error('LedgerStore not connected');
    return this.client.db(this.dbName).collection('mig_prune_journal');
  }

  /**
   * Persist a prune receipt durably — called by the sink BEFORE the prune's
   * destructive writes. PAGED: a large grid's receipt must never approach
   * the 16MiB BSON document limit (which would fail every apply attempt
   * before pruning), so deleted chunks split across journal documents.
   * Clamped straddlers (at most one per collection) ride in the first page.
   */
  async journalPruneReceipt(runId: string, token: string, receipt: { deletedChunks: ChunkDoc[]; clampedChunks: Array<{ _id: string; upper_cd: number }> }): Promise<void> {
    const PAGE = 5_000;
    const { deletedChunks, clampedChunks } = receipt;
    if (deletedChunks.length === 0 && clampedChunks.length === 0) return;
    const docs: Array<{ run_id: string; token: string; created_at: Date; receipt: { deletedChunks: ChunkDoc[]; clampedChunks: Array<{ _id: string; upper_cd: number }> } }> = [];
    for (let i = 0; i < Math.max(1, Math.ceil(deletedChunks.length / PAGE)); i++) {
      docs.push({
        run_id: runId, token, created_at: new Date(),
        receipt: { deletedChunks: deletedChunks.slice(i * PAGE, (i + 1) * PAGE), clampedChunks: i === 0 ? clampedChunks : [] },
      });
    }
    await this.pj().insertMany(docs);
  }

  /** Number of prune-journal entries for a run — non-zero means unsettled destructive work. */
  async countPruneJournal(runId: string): Promise<number> {
    return this.pj().countDocuments({ run_id: runId });
  }

  /** Remove an apply's journal entries once its outcome is settled (committed or fully rolled back). */
  async clearPruneJournal(runId: string, token: string): Promise<void> {
    await this.pj().deleteMany({ run_id: runId, token });
  }

  /**
   * Restore orphaned prune journal entries — receipts whose apply died
   * between the prune and a settled outcome. Restoration happens under the
   * GOVERNING bound (env else stored), which makes it safe to run against
   * ANY leftover entry: a journal whose apply actually committed its bound
   * restores nothing (every pruned chunk is at/beyond that bound), while a
   * crashed pre-commit apply gets its chunks back in full. Entries owned by
   * a LIVE (non-stale) apply marker are someone's in-flight work — skipped.
   * Runs at engine startup and before every new apply.
   */
  async recoverPruneJournal(runId: string, envBoundMs: number | null = null): Promise<{ recovered: number; skippedLiveApply: number }> {
    const entries = await this.pj().find({ run_id: runId }).sort({ created_at: -1 }).toArray();
    if (entries.length === 0) return { recovered: 0, skippedLiveApply: 0 };
    const rc = await this.rc().findOne({ _id: runId });
    const markerLive = rc?.apply_in_progress_token && rc.apply_in_progress_at
      && rc.apply_in_progress_at.getTime() >= Date.now() - 600_000
      ? rc.apply_in_progress_token : null;
    const governing = envBoundMs ?? await this.getStoredBound(runId);
    let recovered = 0, skippedLiveApply = 0;
    for (const entry of entries) {
      if (markerLive !== null && entry.token === markerLive) { skippedLiveApply++; continue; }
      await this.restorePrune(entry.receipt, governing);
      // by _id, never by (token, created_at): a paged receipt's documents
      // share both, and deleting a DIFFERENT page than the one just restored
      // would lose it forever if the process dies before its turn
      await this.pj().deleteOne({ _id: entry._id });
      recovered++;
    }
    return { recovered, skippedLiveApply };
  }

  /**
   * Atomically take over a recoverable chunk. Single-winner: the status and
   * expired-lease filter mean that when several pods spot the same chunk,
   * exactly one reclaim succeeds — the losers get null and walk away
   * (without this, two recoverers could both run the attaching path and
   * double-attach the same staging partition). $inc attempts starts a NEW
   * claim generation, so every fenced mutation still held by the previous
   * owner (a zombie that resumes later) is rejected from here on.
   * ignoreLease is for single-pod mode only, where a fresh process recovers
   * its own predecessor's chunks without waiting out their leases.
   */
  async reclaim(chunkId: string, fromStatus: ChunkStatus, podId: string, leaseSec: number, ignoreLease = false): Promise<ChunkDoc | null> {
    const filter: Record<string, unknown> = { _id: chunkId, status: fromStatus };
    if (!ignoreLease) filter.lease_until = { $lt: new Date() };
    return this.c().findOneAndUpdate(
      filter as never,
      {
        $set: { pod_id: podId, lease_until: new Date(Date.now() + leaseSec * 1000), updated_at: new Date() },
        $inc: { attempts: 1 },
      },
      { returnDocument: 'after' },
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

  /**
   * Pods holding LIVE claims: non-terminal chunks whose lease has not
   * expired. This is the guard for destructive/ambiguous operator actions
   * (rebuild, audits): a crashed pod's stale claims must NOT block them —
   * its leases expire — while a genuinely working pod must.
   */
  async activeClaims(runId: string, excludePod?: string): Promise<Array<{ pod: string; count: number }>> {
    const match: Record<string, unknown> = {
      run_id: runId,
      status: { $in: ['in_progress', 'written', 'attaching'] },
      lease_until: { $gt: new Date() },
      pod_id: { $ne: null },
    };
    const rows = await this.c()
      .aggregate<{ _id: string; count: number }>([
        { $match: match },
        { $group: { _id: '$pod_id', count: { $sum: 1 } } },
      ])
      .toArray();
    return rows
      .filter((r) => r._id !== excludePod)
      .map((r) => ({ pod: r._id, count: r.count }));
  }

  /**
   * Cluster-wide throughput from the shared ledger: docs read by chunks
   * that finished in the last windowSec, across ALL pods. The in-memory
   * docsPerSecond each pod reports covers only itself — with N pods the
   * dashboard undercounted by ~N× (field-reported: UI said 10k docs/s
   * while 4 pods actually moved ~39k).
   */
  async clusterRate(runId: string, windowSec: number): Promise<{ docsPerSecond: number; pods: number; windowSec: number }> {
    const since = new Date(Date.now() - windowSec * 1000);
    const rows = await this.c()
      .aggregate<{ docs: number; pods: string[] }>([
        { $match: { run_id: runId, status: 'done', updated_at: { $gte: since } } },
        { $group: { _id: null, docs: { $sum: '$docs_read' }, pods: { $addToSet: '$pod_id' } } },
      ])
      .toArray();
    const docs = rows[0]?.docs ?? 0;
    const pods = (rows[0]?.pods ?? []).filter(Boolean).length;
    return { docsPerSecond: docs / windowSec, pods, windowSec };
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
