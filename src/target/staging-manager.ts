/**
 * StagingManager — ClickHouse side of the `ledger` engine.
 *
 * Owns per-chunk staging tables: create (clone of the live table), insert
 * (SYNCHRONOUS — errors surface to the caller, unlike the classic engine's
 * fire-and-forget async inserts), count (exact, metadata-served), promote
 * (verify-then-ATTACH per partition, with INSERT SELECT fallback), drop.
 *
 * Also runs the startup dedup canary: insert_deduplication_token only works
 * when the target engine supports a dedup window (Replicated* by default;
 * plain MergeTree needs non_replicated_deduplication_window > 0). The canary
 * measures this instead of assuming.
 */

import { createClient, type ClickHouseClient } from '@clickhouse/client';
import type { Logger } from 'pino';
import type { OutputRow } from '../transform/normalize.ts';

export interface StagingManagerConfig {
  url: string;
  database: string;
  table: string; // live target table
  username: string;
  password: string;
  queryTimeoutMs: number;
}

export class StagingManager {
  private client: ClickHouseClient | null = null;
  private readonly logger: Logger;
  private readonly config: StagingManagerConfig;
  public dedupWorks: boolean | null = null;

  constructor(config: StagingManagerConfig, logger: Logger) {
    this.config = config;
    this.logger = logger.child({ component: 'StagingManager' });
  }

  async connect(): Promise<void> {
    this.client = createClient({
      url: this.config.url,
      database: this.config.database,
      username: this.config.username,
      password: this.config.password,
      compression: { request: true },
      // Synchronous inserts: an acked insert is parsed, validated, and visible.
      clickhouse_settings: {
        date_time_input_format: 'best_effort',
        optimize_on_insert: 0,
      },
      request_timeout: this.config.queryTimeoutMs,
    });
    await this.client.ping();
    this.logger.info('StagingManager connected (sync inserts)');
  }

  async close(): Promise<void> {
    if (this.client) {
      await this.client.close();
      this.client = null;
    }
  }

  private ch(): ClickHouseClient {
    if (!this.client) throw new Error('StagingManager not connected');
    return this.client;
  }

  private fq(table: string): string {
    return `\`${this.config.database}\`.\`${table}\``;
  }

  // -------------------------------------------------------------------------
  // Dedup canary
  // -------------------------------------------------------------------------

  /**
   * Empirically verify whether insert_deduplication_token is honored on a
   * clone of the live table. Logs loudly either way; the result is exposed
   * so operators can see it in /stats.
   */
  async runDedupCanary(): Promise<boolean> {
    const canary = `${this.config.table}_mig_canary`;
    const row = [{ _id: 'canary', a: 'canary', e: 'canary', n: 'canary', uid: 'canary', did: '',
      ts: '2000-01-01 00:00:00.000', up: {}, sg: {}, c: 1, s: 0, dur: 0, cd: '2000-01-01 00:00:00.000' }];
    try {
      await this.ch().command({ query: `DROP TABLE IF EXISTS ${this.fq(canary)}` });
      await this.ch().command({ query: `CREATE TABLE ${this.fq(canary)} AS ${this.fq(this.config.table)}` });
      await this.applyDedupWindow(canary);
      for (let i = 0; i < 2; i++) {
        await this.ch().insert({
          table: canary,
          values: row,
          format: 'JSONEachRow',
          clickhouse_settings: { insert_deduplication_token: 'mig-canary' },
        });
      }
      const n = await this.countRows(canary);
      this.dedupWorks = n === 1;
    } catch (err) {
      this.logger.warn({ err: (err as Error).message }, 'Dedup canary failed to run — assuming dedup inert');
      this.dedupWorks = false;
    } finally {
      await this.ch().command({ query: `DROP TABLE IF EXISTS ${this.fq(canary)}` }).catch(() => {});
    }
    if (this.dedupWorks) {
      this.logger.info('Dedup canary: insert_deduplication_token WORKS on this target');
    } else {
      this.logger.warn(
        'Dedup canary: dedup token is INERT on this target — within-chunk duplicate protection ' +
        'relies on chunk redo semantics only (safe, but ambiguous insert retries may duplicate ' +
        'rows inside a chunk until it is verified)',
      );
    }
    return this.dedupWorks;
  }

  /** Best-effort: give a staging table a dedup window so tokens work on plain MergeTree. */
  private async applyDedupWindow(table: string): Promise<void> {
    try {
      await this.ch().command({
        query: `ALTER TABLE ${this.fq(table)} MODIFY SETTING non_replicated_deduplication_window = 100`,
      });
    } catch {
      // Replicated / Shared engines reject or don't need this — token works there natively.
    }
  }

  // -------------------------------------------------------------------------
  // Dry-run target (Null engine: full parse/type validation, nothing stored)
  // -------------------------------------------------------------------------

  get dryRunTable(): string {
    return `${this.config.table}_dryrun`;
  }

  async createDryRunTable(): Promise<void> {
    await this.ch().command({ query: `DROP TABLE IF EXISTS ${this.fq(this.dryRunTable)}` });
    await this.ch().command({
      query: `CREATE TABLE ${this.fq(this.dryRunTable)} AS ${this.fq(this.config.table)} ENGINE = Null`,
    });
    this.logger.info({ table: this.dryRunTable }, 'Dry-run Null-engine table created');
  }

  /** Direct insert into the live table (DLQ replay path). */
  async insertIntoLive(rows: OutputRow[], dedupToken: string): Promise<void> {
    await this.ch().insert({
      table: this.config.table,
      values: rows,
      format: 'JSONEachRow',
      clickhouse_settings: { insert_deduplication_token: dedupToken },
    });
  }

  // -------------------------------------------------------------------------
  // Staging table lifecycle
  // -------------------------------------------------------------------------

  async createStaging(stagingTable: string): Promise<void> {
    await this.ch().command({ query: `DROP TABLE IF EXISTS ${this.fq(stagingTable)}` });
    await this.ch().command({
      query: `CREATE TABLE ${this.fq(stagingTable)} AS ${this.fq(this.config.table)}`,
    });
    await this.applyDedupWindow(stagingTable);
  }

  async dropStaging(stagingTable: string): Promise<void> {
    await this.ch().command({ query: `DROP TABLE IF EXISTS ${this.fq(stagingTable)}` });
  }

  async insertBatch(
    stagingTable: string,
    rows: OutputRow[],
    dedupToken: string,
    queryId: string,
  ): Promise<void> {
    await this.ch().insert({
      table: stagingTable,
      values: rows,
      format: 'JSONEachRow',
      clickhouse_settings: { insert_deduplication_token: dedupToken },
      query_id: queryId,
    });
  }

  /** Exact row count (metadata-served on MergeTree — instant). */
  async countRows(table: string): Promise<number> {
    const res = await this.ch().query({
      query: `SELECT count() AS c FROM ${this.fq(table)}`,
      format: 'JSONEachRow',
    });
    const rows = await res.json<{ c: string }>();
    return Number(rows[0]?.c ?? 0);
  }

  // -------------------------------------------------------------------------
  // Promotion (verify-then-attach)
  // -------------------------------------------------------------------------

  /** Active partition ids of a staging table. */
  async listPartitions(stagingTable: string): Promise<string[]> {
    const res = await this.ch().query({
      query: `SELECT DISTINCT partition_id FROM system.parts
              WHERE database = {db:String} AND table = {table:String} AND active
              ORDER BY partition_id`,
      query_params: { db: this.config.database, table: stagingTable },
      format: 'JSONEachRow',
    });
    const rows = await res.json<{ partition_id: string }>();
    return rows.map((r) => r.partition_id);
  }

  /**
   * Rows already present in the LIVE table for this partition within the
   * chunk's cd bounds. Used by attach-recovery: historical cd ranges contain
   * only migrated rows, so >0 here means "this partition was already attached".
   */
  async countLiveInChunkPartition(partitionId: string, lowerCdMs: number, upperCdMs: number): Promise<number> {
    const res = await this.ch().query({
      query: `SELECT count() AS c FROM ${this.fq(this.config.table)}
              WHERE _partition_id = {pid:String}
                AND cd >= fromUnixTimestamp64Milli({lo:Int64})
                AND cd <  fromUnixTimestamp64Milli({hi:Int64})`,
      query_params: { pid: partitionId, lo: lowerCdMs, hi: upperCdMs },
      format: 'JSONEachRow',
    });
    const rows = await res.json<{ c: string }>();
    return Number(rows[0]?.c ?? 0);
  }

  /**
   * Attach-recovery check for chunks WITHOUT a usable cd window (the null-cd
   * sweep): are any of this staging partition's row ids already live?
   */
  async countLiveByStagedIds(stagingTable: string, partitionId: string): Promise<number> {
    const res = await this.ch().query({
      query: `SELECT count() AS c FROM ${this.fq(this.config.table)}
              WHERE _partition_id = {pid:String}
                AND _id IN (SELECT _id FROM ${this.fq(stagingTable)} WHERE _partition_id = {pid:String} LIMIT 100)`,
      query_params: { pid: partitionId },
      format: 'JSONEachRow',
    });
    const rows = await res.json<{ c: string }>();
    return Number(rows[0]?.c ?? 0);
  }

  /**
   * Attach one partition of a staging table into the live table.
   * Parts-level (no rewrite). Throws on failure — caller decides fallback.
   */
  async attachPartition(stagingTable: string, partitionId: string): Promise<void> {
    await this.ch().command({
      query: `ALTER TABLE ${this.fq(this.config.table)} ATTACH PARTITION ID '${partitionId}' FROM ${this.fq(stagingTable)}`,
    });
  }

  /**
   * Fallback promotion when ATTACH is unavailable (e.g. engine/settings
   * mismatch in some environments): copy rows. Slower but always works.
   */
  async insertSelect(stagingTable: string): Promise<void> {
    await this.ch().command({
      query: `INSERT INTO ${this.fq(this.config.table)} SELECT * FROM ${this.fq(stagingTable)}`,
    });
  }

  /** Grouped verification: rows in the live table within given cd bounds. */
  async countLiveInCdRange(lowerCdMs: number, upperCdMs: number): Promise<number> {
    const res = await this.ch().query({
      query: `SELECT count() AS c FROM ${this.fq(this.config.table)}
              WHERE cd >= fromUnixTimestamp64Milli({lo:Int64})
                AND cd <  fromUnixTimestamp64Milli({hi:Int64})`,
      query_params: { lo: lowerCdMs, hi: upperCdMs },
      format: 'JSONEachRow',
    });
    const rows = await res.json<{ c: string }>();
    return Number(rows[0]?.c ?? 0);
  }
}
