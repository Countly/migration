/**
 * DlqStore — the dead-letter queue for the ledger engine.
 *
 * One MongoDB document per source document that could not be migrated,
 * carrying the FULL RAW source doc — that is what makes the pile replayable
 * later (after a transform fix) without ever re-reading the customer's
 * source collection.
 */

import { MongoClient, type Collection } from 'mongodb';
import { toEpochMillis, clampDateTime64 } from '../transform/validators.ts';
import type { Logger } from 'pino';

export type DlqReason = 'insert_rejected' | 'transform_error' | 'skipped';
export type DlqStatus = 'pending' | 'resolved' | 'waived';

export interface DlqDoc {
  _id: string;                    // `${runId}:${sourceId}`
  run_id: string;
  collection: string;
  chunk_id: string;
  source_id: string;
  raw_doc: Record<string, unknown>;
  reason: DlqReason;
  error: string;
  transform_version: string;      // version that failed
  /**
   * The doc's cd (or ts-derived fallback) in epoch ms, when parseable —
   * lets audits attribute unmigrated docs to their cd window, so a window
   * whose shortfall is exactly its pending/waived DLQ docs is not flagged
   * as a disagreement. Null when the doc has no usable cd/ts (those can't
   * land in any window anyway).
   */
  cd_ms: number | null;
  status: DlqStatus;
  resolved_by_version: string | null;
  created_at: Date;
  updated_at: Date;
}

export class DlqStore {
  private client: MongoClient;
  private coll: Collection<DlqDoc> | null = null;
  private readonly logger: Logger;
  private readonly dbName: string;

  constructor(uri: string, dbName: string, logger: Logger) {
    this.client = new MongoClient(uri);
    this.dbName = dbName;
    this.logger = logger.child({ component: 'DlqStore' });
  }

  async connect(): Promise<void> {
    await this.client.connect();
    this.coll = this.client.db(this.dbName).collection<DlqDoc>('mig_dlq_docs');
    await this.coll.createIndex({ run_id: 1, status: 1 });
    await this.coll.createIndex({ run_id: 1, collection: 1, cd_ms: 1 });
    this.logger.info({ db: this.dbName }, 'DlqStore connected');
  }

  async close(): Promise<void> {
    await this.client.close();
  }

  private c(): Collection<DlqDoc> {
    if (!this.coll) throw new Error('DlqStore not connected');
    return this.coll;
  }

  async add(entries: Array<Omit<DlqDoc, '_id' | 'status' | 'resolved_by_version' | 'created_at' | 'updated_at' | 'cd_ms'> & { cd_ms?: number | null }>): Promise<void> {
    if (entries.length === 0) return;
    const now = new Date();
    const deriveCdMs = (raw: Record<string, unknown>): number | null => {
      const cd = toEpochMillis(raw.cd);
      if (cd !== null && cd > 0) return clampDateTime64(cd);
      const ts = toEpochMillis(raw.ts);
      return ts !== null && ts > 0 ? clampDateTime64(ts) : null;
    };
    const docs: DlqDoc[] = entries.map((e) => ({
      cd_ms: e.cd_ms !== undefined ? e.cd_ms : deriveCdMs(e.raw_doc),
      ...e,
      _id: `${e.run_id}:${e.source_id}`,
      status: 'pending',
      resolved_by_version: null,
      created_at: now,
      updated_at: now,
    }));
    try {
      await this.c().insertMany(docs, { ordered: false });
    } catch (err: unknown) {
      if ((err as { code?: number }).code !== 11000) throw err; // re-DLQ of same doc is fine
    }
  }

  /** Keyset page for full-drain loops: pending entries with _id > after. */
  async listPendingAfter(runId: string, afterId: string | null, limit = 500): Promise<DlqDoc[]> {
    const filter: Record<string, unknown> = { run_id: runId, status: 'pending' };
    if (afterId) filter._id = { $gt: afterId };
    return this.c().find(filter).sort({ _id: 1 }).limit(limit).toArray();
  }

  async listPending(runId: string, limit = 10_000, skip = 0): Promise<DlqDoc[]> {
    // Stable order so pagination pages don't shuffle between polls
    return this.c().find({ run_id: runId, status: 'pending' }).sort({ _id: 1 }).skip(skip).limit(limit).toArray();
  }

  /** Disk cost of the DLQ (raw docs are stored whole) + manifest-DB headroom. */
  async storageStats(): Promise<{ dlqBytes: number; dlqDocs: number; diskFreePct: number | null }> {
    const db = this.client.db(this.dbName);
    let dlqBytes = 0, dlqDocs = 0, diskFreePct: number | null = null;
    try {
      const cs = await db.command({ collStats: 'mig_dlq_docs' });
      dlqBytes = cs.storageSize ?? 0;
      dlqDocs = cs.count ?? 0;
    } catch { /* collection may not exist yet */ }
    try {
      const ds = await db.stats();
      if (ds.fsTotalSize) diskFreePct = Math.round(((ds.fsTotalSize - ds.fsUsedSize) / ds.fsTotalSize) * 100);
    } catch { /* no permission — omit */ }
    return { dlqBytes, dlqDocs, diskFreePct };
  }

  /**
   * Known-unmigrated docs (pending/waived — anything not resolved) whose cd
   * falls in a window. Audits subtract these: their absence from the live
   * table is accounted for, not a disagreement. Entries written before the
   * cd_ms field (or with unparseable cd/ts) can't be attributed and count 0.
   */
  async countUnresolvedInWindow(runId: string, collection: string, lowerCdMs: number, upperCdMs: number): Promise<number> {
    return this.c().countDocuments({
      run_id: runId, collection,
      cd_ms: { $gte: lowerCdMs, $lt: upperCdMs },
      status: { $ne: 'resolved' },
    });
  }

  async countByStatus(runId: string): Promise<Record<string, number>> {
    const rows = await this.c()
      .aggregate<{ _id: string; n: number }>([
        { $match: { run_id: runId } },
        { $group: { _id: '$status', n: { $sum: 1 } } },
      ])
      .toArray();
    return Object.fromEntries(rows.map((r) => [r._id, r.n]));
  }

  async topErrors(runId: string, limit = 10): Promise<Array<{ error: string; n: number }>> {
    const rows = await this.c()
      .aggregate<{ _id: string; n: number }>([
        { $match: { run_id: runId, status: 'pending' } },
        { $group: { _id: '$error', n: { $sum: 1 } } },
        { $sort: { n: -1 } },
        { $limit: limit },
      ])
      .toArray();
    return rows.map((r) => ({ error: r._id, n: r.n }));
  }

  async markResolved(ids: string[], version: string): Promise<void> {
    if (ids.length === 0) return;
    await this.c().updateMany(
      { _id: { $in: ids } },
      { $set: { status: 'resolved', resolved_by_version: version, updated_at: new Date() } },
    );
  }

  /** Update error on a still-failing pending entry (replay attempt failed again). */
  async recordRetryError(id: string, error: string): Promise<void> {
    await this.c().updateOne({ _id: id }, { $set: { error, updated_at: new Date() } });
  }

  /**
   * Waive pending entries: the explicit operator decision that these docs
   * will NOT be migrated. The raw docs stay in the DLQ permanently as the
   * record of what was excluded — waived is terminal, but reversible (an
   * operator can flip back to pending and replay after a later fix).
   * With no ids given, waives everything currently pending for the run.
   */
  async waive(runId: string, ids?: string[]): Promise<number> {
    const filter: Record<string, unknown> = { run_id: runId, status: 'pending' };
    if (ids && ids.length > 0) filter._id = { $in: ids };
    const res = await this.c().updateMany(filter, { $set: { status: 'waived', updated_at: new Date() } });
    return res.modifiedCount;
  }
}
