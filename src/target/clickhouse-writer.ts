import { createClient, type ClickHouseClient } from '@clickhouse/client';
import type { Logger } from 'pino';
import type { OutputRow } from '../transform/normalize.ts';

export interface ClickHouseWriterConfig {
  url: string;
  database: string;
  table: string;
  username: string;
  password: string;
  queryTimeoutMs: number;
  useDedupToken: boolean;
}

export interface InsertBatchParams {
  runId: string;
  batchSeq: number;
  rows: OutputRow[];
}

export interface InsertResult {
  insertMs: number;
  rowsInserted: number;
}

export class ClickHouseWriter {
  private readonly config: ClickHouseWriterConfig;
  private readonly logger: Logger;
  private client: ClickHouseClient | null = null;
  private _connected: boolean = false;

  constructor(config: ClickHouseWriterConfig, logger: Logger) {
    this.config = config;
    this.logger = logger.child({ component: 'clickhouse-writer' });
  }

  async connect(): Promise<void> {
    this.logger.info({ url: this.config.url, database: this.config.database }, 'connecting to ClickHouse');

    this.client = createClient({
      url: this.config.url,
      database: this.config.database,
      username: this.config.username,
      password: this.config.password,
      compression: {
        request: true,
      },
      clickhouse_settings: {
        date_time_input_format: 'best_effort',
        optimize_on_insert: 0,
      },
      request_timeout: this.config.queryTimeoutMs,
    });

    await this.client.ping();
    this._connected = true;

    this.logger.info('connected to ClickHouse');
  }

  async insertBatch(params: InsertBatchParams): Promise<InsertResult> {
    if (!this.client) {
      throw new Error('ClickHouseWriter is not connected. Call connect() first.');
    }

    const { runId, batchSeq, rows } = params;
    const queryId = `mig__${runId}__${batchSeq}`;
    const dedupToken = `mig:${runId}:${batchSeq}`;

    this.logger.debug(
      { queryId, rowCount: rows.length },
      'inserting batch',
    );

    const start = performance.now();

    const clickhouseSettings: Record<string, string> = {};
    if (this.config.useDedupToken) {
      clickhouseSettings.insert_deduplication_token = dedupToken;
    }

    await this.client.insert({
      table: this.config.table,
      values: rows,
      format: 'JSONEachRow',
      clickhouse_settings: clickhouseSettings,
      query_id: queryId,
    });

    const insertMs = performance.now() - start;

    this.logger.debug(
      { queryId, insertMs: Math.round(insertMs), rowsInserted: rows.length },
      'batch inserted',
    );

    return {
      insertMs,
      rowsInserted: rows.length,
    };
  }

  isConnected(): boolean {
    return this._connected;
  }

  async close(): Promise<void> {
    if (this.client) {
      this.logger.info('closing ClickHouse connection');
      await this.client.close();
      this.client = null;
      this._connected = false;
    }
  }
}
