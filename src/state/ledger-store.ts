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
   * Idempotently create the chunk list for a collection. If any chunks
   * already exist for (runId, collection) this is a no-op — resume keeps
   * whatever bounds were originally cut.
   */
  async initChunks(
    runId: string,
    collection: string,
    bounds: Array<{ lowerCd: number; upperCd: number }>,
    transformVersion: string,
  ): Promise<number> {
    const existing = await this.c().countDocuments({ run_id: runId, collection }, { limit: 1 });
    if (existing > 0) return 0;

    const now = new Date();
    const docs: ChunkDoc[] = bounds.map((b, idx) => ({
      _id: `${runId}:${collection}:${idx}`,
      run_id: runId,
      collection,
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

    try {
      await this.c().insertMany(docs, { ordered: false });
    } catch (err: unknown) {
      // Duplicate keys mean another pod initialized concurrently — fine.
      if ((err as { code?: number }).code !== 11000) throw err;
    }
    return docs.length;
  }

  /**
   * Atomically claim the next pending chunk, newest data first (highest idx).
   */
  async claimNext(
    runId: string,
    collection: string,
    podId: string,
    leaseSec: number,
  ): Promise<ChunkDoc | null> {
    return this.c().findOneAndUpdate(
      { run_id: runId, collection, status: 'pending' },
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

  /** Extend the lease of a chunk this pod is working on. */
  async heartbeat(chunkId: string, podId: string, leaseSec: number): Promise<void> {
    await this.c().updateOne(
      { _id: chunkId, pod_id: podId, status: { $in: ['in_progress', 'written', 'attaching'] } },
      { $set: { lease_until: new Date(Date.now() + leaseSec * 1000), updated_at: new Date() } },
    );
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
  ): Promise<ChunkDoc | null> {
    const fromArr = Array.isArray(from) ? from : [from];
    return this.c().findOneAndUpdate(
      { _id: chunkId, status: { $in: fromArr } },
      { $set: { ...patch, status: to, updated_at: new Date() } },
      { returnDocument: 'after' },
    );
  }

  /** Append one attached partition id (crash-safe attach progress). */
  async recordAttached(chunkId: string, partitionId: string): Promise<void> {
    await this.c().updateOne(
      { _id: chunkId },
      { $addToSet: { attached: partitionId }, $set: { updated_at: new Date() } },
    );
  }

  /**
   * Chunks needing recovery: leases expired mid-work, or non-terminal states
   * left behind by a crashed pod (when includeAll, e.g. single-pod startup).
   */
  async findRecoverable(runId: string, collection: string, includeAll: boolean): Promise<ChunkDoc[]> {
    const nonTerminal: ChunkStatus[] = ['in_progress', 'written', 'attaching'];
    const filter = includeAll
      ? { run_id: runId, collection, status: { $in: nonTerminal } }
      : { run_id: runId, collection, status: { $in: nonTerminal }, lease_until: { $lt: new Date() } };
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
    '_id' | 'collection' | 'idx' | 'status' | 'lower_cd' | 'upper_cd' |
    'docs_read' | 'docs_skipped' | 'rows_expected' | 'attempts' | 'last_error' | 'pod_id' | 'updated_at'>>> {
    return this.c()
      .find(
        { run_id: runId },
        { projection: { collection: 1, idx: 1, status: 1, lower_cd: 1, upper_cd: 1,
          docs_read: 1, docs_skipped: 1, rows_expected: 1, attempts: 1, last_error: 1, pod_id: 1, updated_at: 1 } },
      )
      .sort({ collection: 1, idx: 1 })
      .toArray() as never;
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
