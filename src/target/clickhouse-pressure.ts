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

export class ClickHousePressure {
  private readonly client: ClickHouseClient;
  private readonly config: BackpressureConfig;
  private readonly logger: Logger;

  constructor(client: ClickHouseClient, config: BackpressureConfig, logger: Logger) {
    this.client = client;
    this.config = config;
    this.logger = logger.child({ component: 'clickhouse-pressure' });
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
