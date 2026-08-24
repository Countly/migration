/**
 * Mirror state: one document per run in the manifest DB. Holds the change
 * stream's resume token (crash-safe continuation), the checkpoint T0 (the
 * cluster time recorded when the mirror FIRST started — the value the bulk
 * migration must use as LEDGER_CD_UPPER_BOUND), and running counters.
 *
 * The checkpoint is written exactly once ($setOnInsert): restarting the
 * mirror never moves the boundary.
 */
import { MongoClient, type Collection, type Document } from 'mongodb';
import type { Logger } from 'pino';

export interface MirrorState {
  _id: string; // runId
  resume_token: Document | null;
  checkpoint_ms: number;
  started_at: Date;
  last_event_ms: number | null;
  last_flush_at: Date | null;
  docs_mirrored: number;
  docs_skipped: number;
  docs_dlq: number;
  non_insert_ops: number;
  at_head: boolean;
  updated_at: Date;
}

export class MirrorStore {
  private readonly client: MongoClient;
  private readonly dbName: string;
  private coll: Collection<MirrorState> | null = null;
  private readonly logger: Logger;

  constructor(uri: string, dbName: string, logger: Logger) {
    this.client = new MongoClient(uri);
    this.dbName = dbName;
    this.logger = logger.child({ component: 'MirrorStore' });
  }

  async connect(): Promise<void> {
    await this.client.connect();
    this.coll = this.client.db(this.dbName).collection<MirrorState>('mig_mirror_state');
  }

  async close(): Promise<void> {
    await this.client.close();
  }

  private c(): Collection<MirrorState> {
    if (!this.coll) throw new Error('MirrorStore not connected');
    return this.coll;
  }

  async load(runId: string): Promise<MirrorState | null> {
    return this.c().findOne({ _id: runId });
  }

  /** First start only: records the checkpoint. Restarts keep the original. */
  async init(runId: string, checkpointMs: number): Promise<MirrorState> {
    await this.c().updateOne(
      { _id: runId },
      {
        $setOnInsert: {
          resume_token: null, checkpoint_ms: checkpointMs, started_at: new Date(),
          last_event_ms: null, last_flush_at: null,
          docs_mirrored: 0, docs_skipped: 0, docs_dlq: 0, non_insert_ops: 0,
          at_head: false,
          updated_at: new Date(),
        },
      },
      { upsert: true },
    );
    return (await this.load(runId))!;
  }

  async progress(
    runId: string,
    resumeToken: Document,
    deltas: { mirrored: number; skipped: number; dlq: number; nonInsert: number },
    lastEventMs: number | null,
  ): Promise<void> {
    await this.c().updateOne(
      { _id: runId },
      {
        $set: {
          resume_token: resumeToken,
          last_flush_at: new Date(),
          at_head: false, // flushing = we had backlog
          updated_at: new Date(),
          ...(lastEventMs !== null ? { last_event_ms: lastEventMs } : {}),
        },
        $inc: {
          docs_mirrored: deltas.mirrored,
          docs_skipped: deltas.skipped,
          docs_dlq: deltas.dlq,
          non_insert_ops: deltas.nonInsert,
        },
      },
    );
  }

  /**
   * Idle heartbeat: an at-head mirror flushes nothing, so without this the
   * dashboard would read a quiet night as "stale" and show ever-growing
   * "lag" that is really just time-since-last-event.
   */
  async touch(runId: string, atHead: boolean): Promise<void> {
    await this.c().updateOne(
      { _id: runId },
      { $set: { at_head: atHead, updated_at: new Date() } },
    );
  }
}
