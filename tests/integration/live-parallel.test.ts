/**
 * Migration under CONCURRENT live ingestion.
 *
 * The cutover-first playbook means live ingestion writes into drill_events
 * the whole time the migrator runs. This suite runs a continuous writer
 * (live-shaped rows: cd = now, mixed apps INCLUDING the same (a,e,n) scope
 * the migrator is copying) against the live table during a full migration,
 * with the invariant monitor enabled at a hot interval, and asserts:
 *
 *  - migrated counts are exact (live rows never leak into window checks)
 *  - every live-written row survives (no purge/attach path touches them)
 *  - the invariant monitor never trips (no false violation from live rows)
 *  - verify passes and attributes any duplicate correctly
 *  - ATTACH lands into the CURRENT month partition while the writer is
 *    inserting into it (docs with recent ts but historical cd share the
 *    partition with live traffic — the hot-partition case).
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pino from 'pino';
import { createHash } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { createClient, type ClickHouseClient } from '@clickhouse/client';

import { LedgerStore } from '../../src/state/ledger-store.ts';
import { DlqStore } from '../../src/state/dlq-store.ts';
import { StagingManager } from '../../src/target/staging-manager.ts';
import { MongoReader } from '../../src/source/mongo-reader.ts';
import { RetryPolicy } from '../../src/runtime/retry-policy.ts';
import { HashResolver } from '../../src/transform/hash-resolver.ts';
import { ChunkOrchestrator } from '../../src/runtime/chunk-orchestrator.ts';
import { loadConfig } from '../../src/config/loader.ts';

const MONGO_URI = 'mongodb://localhost:27017/?directConnection=true';
const CH_URL = process.env.TEST_CLICKHOUSE_URL ?? 'http://localhost:8123';
const CH_PASSWORD = process.env.TEST_CLICKHOUSE_PASSWORD ?? '';
const DB = 'test_mig_live';
const RUN = 'live-1';
const logger = pino({ level: 'silent' });

const APP = 'app_live_test';
const EV = 'checkout';
const COLL = `drill_events${createHash('sha1').update(EV + APP).digest('hex')}`;
const DOCS = 30_000;
const BASE = Date.UTC(2026, 2, 1);

describe('migration under concurrent live ingestion', () => {
  let ch: ClickHouseClient;
  let mc: MongoClient;
  let orchestrator: ChunkOrchestrator;
  const closers: Array<() => Promise<void>> = [];

  beforeAll(async () => {
    mc = new MongoClient(MONGO_URI);
    await mc.connect();
    await mc.db(DB).dropDatabase();
    await mc.db(`${DB}_countly`).dropDatabase();
    await mc.db(`${DB}_countly`).collection('apps').insertOne({ _id: APP } as never);
    await mc.db(`${DB}_countly`).collection('events').insertOne({ _id: APP, list: [EV] } as never);

    ch = createClient({ url: CH_URL, password: CH_PASSWORD });
    await ch.command({ query: `CREATE DATABASE IF NOT EXISTS ${DB}` });
    await ch.command({ query: `DROP TABLE IF EXISTS ${DB}.drill_events` });
    await ch.command({
      query: `CREATE TABLE ${DB}.drill_events (
        \`a\` LowCardinality(String), \`e\` LowCardinality(String), \`n\` String,
        \`uid\` String, \`uid_canon\` Nullable(String), \`did\` String, \`lsid\` Nullable(String),
        \`_id\` String, \`ts\` DateTime64(3), \`up\` JSON(max_dynamic_paths = 32),
        \`custom\` Nullable(JSON(max_dynamic_paths = 0)), \`cmp\` Nullable(JSON(max_dynamic_paths = 0)),
        \`sg\` JSON(max_dynamic_paths = 0), \`c\` UInt32, \`s\` Float64, \`dur\` Float64,
        \`lu\` Nullable(DateTime64(3)), \`cd\` DateTime64(3) DEFAULT now64(3))
      ENGINE = MergeTree PARTITION BY toYYYYMM(ts, 'UTC') ORDER BY (a, e, n, ts)`,
    });

    // Source: most docs historical; the newest slice carries RECENT ts with
    // historical cd, so its chunk attaches into the CURRENT month partition —
    // the same partition live traffic inserts into.
    const coll = mc.db(DB).collection(COLL);
    const hotTsBase = Date.now() - 60 * 60_000;
    let docs: Record<string, unknown>[] = [];
    for (let i = 0; i < DOCS; i++) {
      const historical = i < DOCS - 2_000;
      const ts = historical ? BASE + i * 60_000 : hotTsBase + (i - (DOCS - 2_000)) * 100;
      const cd = BASE + i * 60_000; // cd strictly historical for ALL docs
      docs.push({ _id: `m_${i}`, uid: String(i % 200), did: `d${i}`, ts, cd: new Date(cd), sg: { v: i }, c: 1 });
      if (docs.length === 5_000) { await coll.insertMany(docs as never[]); docs = []; }
    }
    if (docs.length) await coll.insertMany(docs as never[]);
    await coll.createIndex({ cd: 1, _id: 1 });

    process.env.SERVICE_NAME = 'live-e2e';
    process.env.MONGO_URI = MONGO_URI;
    process.env.MONGO_DB = DB;
    process.env.MONGO_COUNTLY_DB = `${DB}_countly`;
    process.env.MANIFEST_DB = DB;
    process.env.CLICKHOUSE_URL = CH_URL;
    process.env.CLICKHOUSE_PASSWORD = CH_PASSWORD;
    process.env.CLICKHOUSE_DB = DB;
    process.env.LEDGER_RUN_ID = RUN;
    process.env.LEDGER_CHUNK_DOCS_TARGET = '4000';
    process.env.MONGO_PAGE_SIZE = '1000';
    // Monitor HOT: every 150ms it spot-checks done chunks against the live
    // table while the writer runs — a live row leaking into a window count
    // would trip it and pause the engine (which the test would catch below).
    process.env.LEDGER_MONITOR_INTERVAL_MS = '150';
    process.env.BACKPRESSURE_ENABLED = 'false';
    const config = loadConfig();

    const mongoReader = new MongoReader({
      uri: MONGO_URI, database: DB, readPreference: 'primary', readConcern: 'local',
      retryReads: true, appName: 'live-e2e', cursorBatchSize: 500, maxTimeMs: 60_000,
    }, logger);
    const ledger = new LedgerStore(MONGO_URI, DB, logger);
    const dlq = new DlqStore(MONGO_URI, DB, logger);
    const staging = new StagingManager({
      url: CH_URL, database: DB, table: 'drill_events', username: 'default', password: CH_PASSWORD, queryTimeoutMs: 60_000,
    }, logger);
    const hashResolver = new HashResolver({ uri: MONGO_URI, countlyDb: `${DB}_countly` }, logger);

    await mongoReader.connect();
    await ledger.connect();
    await dlq.connect();
    await staging.connect();
    await hashResolver.build();
    closers.push(() => mongoReader.close(), () => ledger.close(), () => dlq.close(), () => staging.close(), () => hashResolver.close());

    orchestrator = new ChunkOrchestrator({
      config, logger, mongoReader, ledger, dlq, staging,
      retryPolicy: new RetryPolicy({ maxRetries: 2, baseDelayMs: 50, maxDelayMs: 200 }), hashResolver,
    });
  }, 120_000);

  afterAll(async () => {
    for (const close of closers) await close().catch(() => {});
    await ch.command({ query: `DROP DATABASE IF EXISTS ${DB}` }).catch(() => {});
    await ch.close();
    await mc.db(DB).dropDatabase().catch(() => {});
    await mc.db(`${DB}_countly`).dropDatabase().catch(() => {});
    await mc.close();
  });

  it('migrates exactly while live rows pour into the same table (and same hot partition)', async () => {
    // Live writer: 40-row batches every 30ms — half into the SAME (a,e,n)
    // scope the migrator is copying (the adversarial case for window
    // counts), half into another app. cd left to the column default (insert
    // time), ts = now → current month partition, shared with the migrated
    // hot slice.
    let liveWritten = 0;
    let writerError: Error | null = null;
    let stop = false;
    const writer = (async () => {
      while (!stop) {
        const now = Date.now();
        const rows = Array.from({ length: 40 }, (_, j) => ({
          a: j % 2 === 0 ? APP : 'other_app',
          e: '[CLY]_custom',
          n: j % 2 === 0 ? EV : 'signup',
          uid: `live_u${j}`, did: 'live_d',
          _id: `live_${now}_${liveWritten + j}`,
          ts: new Date(now).toISOString().replace('T', ' ').replace('Z', ''),
          up: {}, sg: {}, c: 1, s: 0, dur: 0,
        }));
        try {
          await ch.insert({ table: `${DB}.drill_events`, values: rows, format: 'JSONEachRow' });
          liveWritten += rows.length;
        } catch (e) { writerError = e as Error; stop = true; }
        await new Promise((r) => setTimeout(r, 30));
      }
    })();

    await new Promise((r) => setTimeout(r, 200)); // writer running before migration starts
    await orchestrator.run();
    await new Promise((r) => setTimeout(r, 300)); // writer keeps going after completion
    stop = true;
    await writer;

    expect(writerError).toBeNull();
    expect(liveWritten).toBeGreaterThan(1_000); // the writer genuinely ran throughout

    // Engine finished cleanly: monitor never tripped (a trip pauses + flags)
    const stats = orchestrator.getStats();
    expect(stats.status).toBe('completed');
    expect(stats.chunksFailed).toBe(0);

    // Migrated data exact: every source doc present exactly once
    const mig = await ch.query({
      query: `SELECT count() AS c, uniqExact(_id) AS u FROM ${DB}.drill_events WHERE _id LIKE 'm_%'`,
      format: 'JSONEachRow',
    });
    const [m] = await mig.json<{ c: string; u: string }>();
    expect(Number(m.c)).toBe(DOCS);
    expect(Number(m.u)).toBe(DOCS);

    // Every live-written row survived migration untouched
    const live = await ch.query({
      query: `SELECT count() AS c FROM ${DB}.drill_events WHERE _id LIKE 'live_%'`,
      format: 'JSONEachRow',
    });
    expect(Number((await live.json<{ c: string }>())[0].c)).toBe(liveWritten);

    // The hot slice really did land in the live-traffic partition
    const hot = await ch.query({
      query: `SELECT countIf(_id LIKE 'm_%') AS mig, countIf(_id LIKE 'live_%') AS live
              FROM ${DB}.drill_events WHERE _partition_id = toString(toYYYYMM(now(), 'UTC'))`,
      format: 'JSONEachRow',
    });
    const [h] = await hot.json<{ mig: string; live: string }>();
    expect(Number(h.mig)).toBe(2_000);
    expect(Number(h.live)).toBeGreaterThan(0);

    // Full verification passes with live data present
    const verify = await orchestrator.verifyMigration();
    expect(verify.ok).toBe(true);
    expect((verify.mismatches as unknown[]).length).toBe(0);
  }, 180_000);
});
