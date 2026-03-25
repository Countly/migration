import type { ClickHouseClient } from '@clickhouse/client';
import type { Logger } from 'pino';

export interface BackpressureConfig {
  enabled: boolean;
  partsToThrowInsert: number;
  maxPartsInTotal: number;
  partitionPctHigh: number;
  partitionPctLow: number;
  totalPctHigh: number;
  totalPctLow: number;
  pollIntervalMs: number;
  maxPauseEpisodeMs: number;
  /** Optional RSS soft limit in bytes; when process RSS >= this value, backpressure triggers. */
  rssSoftLimitBytes?: number;
}

export interface PressureState {
  activePartsTotal: number;
  maxPartsInPartition: number;
  mergesInFlight: number;
  partitionPressure: number;
  totalPressure: number;
  compactionStalled: boolean;
  rssExceeded: boolean;
  /** True when disk check is skipped; requires system.disks query to implement. */
  diskCheckSkipped: boolean;
  shouldPause: boolean;
  canResume: boolean;
  pauseReason: string | null;
}

export interface ServerMergeTreeLimits {
  partsToThrowInsert: number;
  partsToDelayInsert: number;
  maxPartsInTotal: number;
  inactivePartsToThrowInsert: number;
  inactivePartsToDelayInsert: number;
}

export class ClickHousePressure {
  private readonly client: ClickHouseClient;
  private readonly config: BackpressureConfig;
  private readonly logger: Logger;

  constructor(client: ClickHouseClient, config: BackpressureConfig, logger: Logger) {
    this.client = client;
    this.config = config;
    this.logger = logger.child({ component: 'clickhouse-pressure' });
  }

  /**
   * Query the actual MergeTree settings from the ClickHouse server.
   * Returns the server-side limits that govern when inserts are rejected or delayed.
   */
  static async fetchServerLimits(
    client: ClickHouseClient,
    logger: Logger,
  ): Promise<ServerMergeTreeLimits> {
    const result = await client.query({
      query: `
        SELECT name, value
        FROM system.merge_tree_settings
        WHERE name IN (
          'parts_to_throw_insert',
          'parts_to_delay_insert',
          'max_parts_in_total',
          'inactive_parts_to_throw_insert',
          'inactive_parts_to_delay_insert'
        )
      `,
      format: 'JSONEachRow',
    });

    const rows = await result.json<{ name: string; value: string }>();
    const map: Record<string, number> = {};
    for (const row of rows) {
      map[row.name] = Number(row.value);
    }

    const limits: ServerMergeTreeLimits = {
      partsToThrowInsert: map['parts_to_throw_insert'] ?? 300,
      partsToDelayInsert: map['parts_to_delay_insert'] ?? 150,
      maxPartsInTotal: map['max_parts_in_total'] ?? 100000,
      inactivePartsToThrowInsert: map['inactive_parts_to_throw_insert'] ?? 0,
      inactivePartsToDelayInsert: map['inactive_parts_to_delay_insert'] ?? 0,
    };

    logger.info({ limits }, 'Fetched ClickHouse merge_tree_settings');
    return limits;
  }

  async sample(database: string, table: string): Promise<PressureState> {
    const results = await Promise.allSettled([
      this.queryActivePartsTotal(database, table),
      this.queryMaxPartsInPartition(database, table),
      this.queryMergesInFlight(database, table),
    ]);

    const activePartsTotal = results[0].status === 'fulfilled' ? results[0].value : 0;
    const maxPartsInPartition = results[1].status === 'fulfilled' ? results[1].value : 0;
    const mergesInFlight = results[2].status === 'fulfilled' ? results[2].value : 0;

    const allFailed = results.every(r => r.status === 'rejected');
    if (allFailed) {
      const errors = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected');
      this.logger.error({ errors: errors.map(e => e.reason?.message ?? String(e.reason)) }, 'all pressure queries failed — treating as pressured');
      return {
        activePartsTotal: 0,
        maxPartsInPartition: 0,
        mergesInFlight: 0,
        partitionPressure: 1,
        totalPressure: 1,
        compactionStalled: false,
        rssExceeded: false,
        diskCheckSkipped: true,
        shouldPause: true,
        canResume: false,
        pauseReason: 'all_pressure_queries_failed',
      };
    }

    if (results.some(r => r.status === 'rejected')) {
      const errors = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected');
      this.logger.warn({ errors: errors.map(e => e.reason?.message ?? String(e.reason)) }, 'some pressure queries failed, using fallback values');
    }

    const partitionPressure = maxPartsInPartition / this.config.partsToThrowInsert;
    const totalPressure = activePartsTotal / this.config.maxPartsInTotal;

    // §11.6 – compaction stalled: parts are elevated but no merges are running
    const compactionStalled =
      mergesInFlight === 0 &&
      maxPartsInPartition > this.config.partsToThrowInsert * 0.25;

    // §11.6 – RSS soft limit check
    const { rssSoftLimitBytes } = this.config;
    const rssExceeded =
      rssSoftLimitBytes !== undefined &&
      rssSoftLimitBytes > 0 &&
      process.memoryUsage().rss >= rssSoftLimitBytes;

    const shouldPause =
      partitionPressure >= this.config.partitionPctHigh ||
      totalPressure >= this.config.totalPctHigh ||
      compactionStalled ||
      rssExceeded;

    const canResume =
      partitionPressure < this.config.partitionPctLow &&
      totalPressure < this.config.totalPctLow &&
      !compactionStalled &&
      !rssExceeded;

    const reasons: string[] = [];
    if (partitionPressure >= this.config.partitionPctHigh) {
      reasons.push(
        `partitionPressure ${partitionPressure.toFixed(3)} >= ${this.config.partitionPctHigh}`,
      );
    }
    if (totalPressure >= this.config.totalPctHigh) {
      reasons.push(
        `totalPressure ${totalPressure.toFixed(3)} >= ${this.config.totalPctHigh}`,
      );
    }
    if (compactionStalled) {
      reasons.push('compaction_stalled');
    }
    if (rssExceeded) {
      reasons.push(`rss_exceeded (limit=${rssSoftLimitBytes})`);
    }
    const pauseReason = reasons.length > 0 ? reasons.join('; ') : null;

    const state: PressureState = {
      activePartsTotal,
      maxPartsInPartition,
      mergesInFlight,
      partitionPressure,
      totalPressure,
      compactionStalled,
      rssExceeded,
      // minFreeDiskBytes is configured but requires a system.disks query to check
      diskCheckSkipped: true,
      shouldPause,
      canResume,
      pauseReason,
    };

    this.logger.debug({ state }, 'pressure sample');

    return state;
  }

  private async queryActivePartsTotal(database: string, table: string): Promise<number> {
    const result = await this.client.query({
      query: `
        SELECT count() AS active_parts_total
        FROM system.parts
        WHERE active = 1 AND database = {db:String} AND table = {table:String}
      `,
      query_params: { db: database, table },
      format: 'JSONEachRow',
    });

    const rows = await result.json<{ active_parts_total: string }>();
    return Number(rows[0]?.active_parts_total ?? 0);
  }

  private async queryMaxPartsInPartition(database: string, table: string): Promise<number> {
    const result = await this.client.query({
      query: `
        SELECT max(parts) AS max_parts_in_partition
        FROM (
          SELECT partition, count() AS parts
          FROM system.parts
          WHERE active = 1 AND database = {db:String} AND table = {table:String}
          GROUP BY partition
        )
      `,
      query_params: { db: database, table },
      format: 'JSONEachRow',
    });

    const rows = await result.json<{ max_parts_in_partition: string }>();
    return Number(rows[0]?.max_parts_in_partition ?? 0);
  }

  private async queryMergesInFlight(database: string, table: string): Promise<number> {
    const result = await this.client.query({
      query: `
        SELECT count() AS merges_in_flight
        FROM system.merges
        WHERE database = {db:String} AND table = {table:String}
      `,
      query_params: { db: database, table },
      format: 'JSONEachRow',
    });

    const rows = await result.json<{ merges_in_flight: string }>();
    return Number(rows[0]?.merges_in_flight ?? 0);
  }
}
