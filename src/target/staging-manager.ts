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
    // Per-process name: concurrent pods each probe their own canary table
    // (a shared name races on create/drop and false-flags dedup as inert).
    const canary = `${this.config.table}_mig_canary_${process.pid}`;
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

  /** Ensure the Null-engine rehearsal table exists without dropping it
   * (replay can run beside the main dry-run loop). */
  async ensureDryRunTable(): Promise<void> {
    await this.ch().command({
      query: `CREATE TABLE IF NOT EXISTS ${this.fq(this.dryRunTable)} AS ${this.fq(this.config.table)} ENGINE = Null`,
    });
  }

  /** Direct insert into the live table (DLQ replay path) — or, in dry-run
   * mode, into the Null-engine rehearsal table via targetTable. */
  async insertIntoLive(rows: OutputRow[], dedupToken: string, targetTable?: string): Promise<void> {
    await this.ch().insert({
      table: targetTable ?? this.config.table,
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
   * Attach-recovery check: are any of this staging partition's rows already
   * live? Matches (_id, cd) PAIRS — exact provenance with no schema changes:
   * an id alone is ambiguous (a cross-cutover SDK retry lands the same _id
   * in the same ts-month partition), but the retry copy's cd is stamped at
   * post-cutover insert time and can never equal the staged row's historical
   * cd. Also precise across sibling collections sharing the partition.
   */
  async countPartitionRows(table: string, partitionId: string): Promise<number> {
    const res = await this.ch().query({
      query: `SELECT count() AS c FROM ${this.fq(table)} WHERE _partition_id = {pid:String}`,
      query_params: { pid: partitionId },
      format: 'JSONEachRow',
    });
    const rows = await res.json<{ c: string }>();
    return Number(rows[0]?.c ?? 0);
  }

  /**
   * EXACT count of live rows matching one staging partition's (_id, cd)
   * pairs. The attach path compares this against the partition's staged row
   * count: equal = already attached, zero = safe to attach, anything else =
   * double-attach or partial promotion that needs healing.
   */
  async countLiveMatchingStaged(stagingTable: string, partitionId: string): Promise<number> {
    const res = await this.ch().query({
      query: `SELECT count() AS c FROM ${this.fq(this.config.table)}
              WHERE _partition_id = {pid:String}
                AND (_id, cd) IN (SELECT _id, cd FROM ${this.fq(stagingTable)} WHERE _partition_id = {pid:String})`,
      query_params: { pid: partitionId },
      format: 'JSONEachRow',
    });
    const rows = await res.json<{ c: string }>();
    return Number(rows[0]?.c ?? 0);
  }

  /**
   * Heal a double-attach: remove ALL live copies matching this staging
   * partition's (_id, cd) pairs — the caller re-attaches afterwards so
   * exactly one copy remains. Provenance-exact: rows sharing an _id but a
   * different cd (live traffic, cross-cutover retries) are never touched.
   */
  async deleteLiveMatchingStaged(stagingTable: string, partitionId: string): Promise<void> {
    await this.ch().command({
      query: `DELETE FROM ${this.fq(this.config.table)}
              WHERE _partition_id = {pid:String}
                AND (_id, cd) IN (SELECT _id, cd FROM ${this.fq(stagingTable)} WHERE _partition_id = {pid:String})`,
      query_params: { pid: partitionId },
    });
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

  /**
   * Purge the live table's rows in a chunk's cd window (lightweight DELETE).
   * Used when retrying a chunk that was already (partially) promoted — redo
   * must start from a clean window or verify-then-attach would skip it.
   */
  private scopeSql(scope?: { a: string; e: string; n?: string } | null): string {
    if (!scope) return '';
    return 'AND a = {sa:String} AND e = {se:String}' + (scope.n !== undefined ? ' AND n = {sn:String}' : '');
  }

  private scopeParams(scope?: { a: string; e: string; n?: string } | null): Record<string, string> {
    if (!scope) return {};
    return { sa: scope.a, se: scope.e, ...(scope.n !== undefined ? { sn: scope.n } : {}) };
  }

  async deleteLiveCdRange(lowerCdMs: number, upperCdMs: number, scope?: { a: string; e: string; n?: string } | null): Promise<void> {
    await this.ch().command({
      query: `DELETE FROM ${this.fq(this.config.table)}
              WHERE cd >= fromUnixTimestamp64Milli({lo:Int64})
                AND cd <  fromUnixTimestamp64Milli({hi:Int64})
                ${this.scopeSql(scope)}`,
      query_params: { lo: lowerCdMs, hi: upperCdMs, ...this.scopeParams(scope) },
    });
  }

  /** ClickHouse disk headroom (preflight). */
  async diskSpace(): Promise<{ freeBytes: number; totalBytes: number } | null> {
    try {
      const res = await this.ch().query({
        query: `SELECT sum(free_space) AS f, sum(total_space) AS t FROM system.disks`,
        format: 'JSONEachRow',
      });
      const rows = await res.json<{ f: string; t: string }>();
      return { freeBytes: Number(rows[0]?.f ?? 0), totalBytes: Number(rows[0]?.t ?? 0) };
    } catch { return null; }
  }


  /** Rows live ingestion wrote recently (preflight: is new ingestion flowing?). */
  async countRecentLive(minutes: number): Promise<number> {
    const res = await this.ch().query({
      query: `SELECT count() AS c FROM ${this.fq(this.config.table)}
              WHERE cd >= now64(3) - INTERVAL {m:UInt32} MINUTE`,
      query_params: { m: minutes },
      format: 'JSONEachRow',
    });
    const rows = await res.json<{ c: string }>();
    return Number(rows[0]?.c ?? 0);
  }

  /**
   * Full rows for the sampled content audit, keyed by _id. Timestamps come
   * back in the same 'YYYY-MM-DD hh:mm:ss.SSS' text form the transform
   * emits, so scalar comparison is direct string/number equality.
   */
  async fetchRowsByIds(ids: string[], cdBounds?: { loMs: number; hiMs: number }): Promise<Map<string, Record<string, unknown>>> {
    const bound = cdBounds
      ? 'AND cd >= fromUnixTimestamp64Milli({blo:Int64}) AND cd <= fromUnixTimestamp64Milli({bhi:Int64})'
      : '';
    const out = new Map<string, Record<string, unknown>>();
    for (let i = 0; i < ids.length; i += 5_000) {
      const page = ids.slice(i, i + 5_000);
      const res = await this.ch().query({
        query: `SELECT _id, a, e, n, uid, uid_canon, did, lsid,
                       toString(ts) AS ts_txt, toString(cd) AS cd_txt,
                       c, s, dur, up, sg, custom, cmp
                FROM ${this.fq(this.config.table)} WHERE _id IN {ids:Array(String)} ${bound}`,
        query_params: { ids: page, ...(cdBounds ? { blo: cdBounds.loMs, bhi: cdBounds.hiMs } : {}) },
        format: 'JSONEachRow',
      });
      for (const r of await res.json<Record<string, unknown>>()) out.set(String(r._id), r);
    }
    return out;
  }

  /** ClickHouse server wall-clock (preflight clock-skew check). */
  async serverNowMs(): Promise<number> {
    const res = await this.ch().query({
      query: `SELECT toUnixTimestamp64Milli(now64(3)) AS n`,
      format: 'JSONEachRow',
    });
    const rows = await res.json<{ n: string }>();
    return Number(rows[0]?.n ?? 0);
  }

  /**
   * Exact duplicate statistics, memory-bounded for billion-row tables:
   * scans partition by partition (legitimate duplicate copies always share
   * their ts month — same document ⇒ same ts ⇒ same partition — so
   * per-partition GROUP BY is exact for every duplicate class we act on,
   * while a global uniqExact/GROUP BY over billions of ids is not safe).
   */
  async duplicateStats(boundaryMs: number, sampleLimit = 20): Promise<{
    rows: number; duplicates: number;
    sample: Array<{ _id: string; copies: number; migratedCopies: number; min_cd_ms: number; max_cd_ms: number }>;
  }> {
    const parts = await this.ch().query({
      query: `SELECT partition_id AS partition, sum(rows) AS r FROM system.parts
              WHERE database = {db:String} AND table = {t:String} AND active
              GROUP BY partition_id ORDER BY partition_id`,
      query_params: { db: this.config.database, t: this.config.table },
      format: 'JSONEachRow',
    });
    const partitions = await parts.json<{ partition: string; r: string }>();
    let rows = 0, duplicates = 0;
    const sample: Array<{ _id: string; copies: number; migratedCopies: number; min_cd_ms: number; max_cd_ms: number }> = [];
    for (const p of partitions) {
      rows += Number(p.r);
      const res = await this.ch().query({
        query: `SELECT _id, count() AS c, countIf(cd < fromUnixTimestamp64Milli({b:Int64})) AS mc,
                       toUnixTimestamp64Milli(min(cd)) AS lo, toUnixTimestamp64Milli(max(cd)) AS hi,
                       sum(c - 1) OVER () AS excess
                FROM (SELECT _id, cd FROM ${this.fq(this.config.table)} WHERE _partition_id = {p:String})
                GROUP BY _id HAVING c > 1
                ORDER BY mc DESC, c DESC LIMIT {lim:UInt32}`,
        query_params: { b: boundaryMs, p: p.partition, lim: sampleLimit },
        format: 'JSONEachRow',
        clickhouse_settings: { max_bytes_before_external_group_by: '4000000000' },
      });
      const groups = await res.json<{ _id: string; c: string; mc: string; lo: string; hi: string; excess: string }>();
      if (groups.length > 0) duplicates += Number(groups[0].excess);
      for (const g of groups) {
        if (sample.length >= sampleLimit) break;
        sample.push({ _id: g._id, copies: Number(g.c), migratedCopies: Number(g.mc), min_cd_ms: Number(g.lo), max_cd_ms: Number(g.hi) });
      }
    }
    return { rows, duplicates, sample };
  }


  /** Does the live target table exist / how many rows does it hold? */
  async targetTableInfo(): Promise<{ exists: boolean; rows: number }> {
    try {
      const rows = await this.countRows(this.config.table);
      return { exists: true, rows };
    } catch {
      return { exists: false, rows: 0 };
    }
  }

  /**
   * Precise purge by (_id, cd) pairs — used where no cd window exists (the
   * null-cd sweep) or the collection is unresolvable. Pair matching means a
   * live cross-cutover retry copy (same _id, post-cutover cd) is untouchable.
   */
  async deleteLiveByPairs(pairs: Array<{ id: string; cdMs: number }>): Promise<void> {
    if (pairs.length === 0) return;
    // Two parallel arrays zipped server-side — the HTTP interface cannot
    // parse a JS array-of-arrays as Array(Tuple(...)). The cd min/max bound
    // lets the mutation prune to the pairs' partitions instead of scanning
    // the whole table.
    const lo = Math.min(...pairs.map((p) => p.cdMs));
    const hi = Math.max(...pairs.map((p) => p.cdMs));
    await this.ch().command({
      query: `DELETE FROM ${this.fq(this.config.table)}
              WHERE cd >= fromUnixTimestamp64Milli({blo:Int64}) AND cd <= fromUnixTimestamp64Milli({bhi:Int64})
                AND (_id, toUnixTimestamp64Milli(cd)) IN (
                SELECT arrayJoin(arrayZip({ids:Array(String)}, {cds:Array(Int64)}))
              )`,
      query_params: { ids: pairs.map((p) => p.id), cds: pairs.map((p) => p.cdMs), blo: lo, bhi: hi },
    });
  }

  /** Staging tables left behind by crashes (crash between done and drop). */
  async listStagingTables(prefix: string): Promise<string[]> {
    const res = await this.ch().query({
      query: `SELECT name FROM system.tables WHERE database = {db:String} AND name LIKE {p:String}`,
      query_params: { db: this.config.database, p: `${prefix}%` },
      format: 'JSONEachRow',
    });
    const rows = await res.json<{ name: string }>();
    return rows.map((r) => r.name);
  }

  /** Grouped verification: rows in the live table within given cd bounds. */
  async countLiveInCdRange(lowerCdMs: number, upperCdMs: number, scope?: { a: string; e: string; n?: string } | null): Promise<number> {
    const res = await this.ch().query({
      query: `SELECT count() AS c FROM ${this.fq(this.config.table)}
              WHERE cd >= fromUnixTimestamp64Milli({lo:Int64})
                AND cd <  fromUnixTimestamp64Milli({hi:Int64})
                ${this.scopeSql(scope)}`,
      query_params: { lo: lowerCdMs, hi: upperCdMs, ...this.scopeParams(scope) },
      format: 'JSONEachRow',
    });
    const rows = await res.json<{ c: string }>();
    return Number(rows[0]?.c ?? 0);
  }

  /**
   * Which of these ids exist in the live table, and at what cd? Paged IN
   * queries; used by ledger rebuild to attribute null-cd sweep rows (their
   * cd is ts-derived and lands inside regular chunks' windows).
   */
  async fetchLiveCdByIds(ids: string[], cdBounds?: { loMs: number; hiMs: number }): Promise<Map<string, number>> {
    // _id is not in the ORDER BY — without cd bounds this is a full-column
    // scan on a 10B-row table. Callers know their rows' cd values; pass them.
    const bound = cdBounds
      ? 'AND cd >= fromUnixTimestamp64Milli({blo:Int64}) AND cd <= fromUnixTimestamp64Milli({bhi:Int64})'
      : '';
    const out = new Map<string, number>();
    for (let i = 0; i < ids.length; i += 10_000) {
      const page = ids.slice(i, i + 10_000);
      const res = await this.ch().query({
        query: `SELECT _id, toUnixTimestamp64Milli(cd) AS cd_ms FROM ${this.fq(this.config.table)}
                WHERE _id IN {ids:Array(String)} ${bound}`,
        query_params: { ids: page, ...(cdBounds ? { blo: cdBounds.loMs, bhi: cdBounds.hiMs } : {}) },
        format: 'JSONEachRow',
      });
      const rows = await res.json<{ _id: string; cd_ms: string }>();
      for (const r of rows) out.set(r._id, Number(r.cd_ms));
    }
    return out;
  }
}
