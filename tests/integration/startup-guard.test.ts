/**
 * Unbounded-with-live-target startup guard — the missing-bound mistake made
 * impossible to make silently. Pinned here:
 *
 *  - a FRESH run against a ClickHouse that is already receiving live data,
 *    with no cd bound set, HOLDS before mapping (pauseReason boundary-unset)
 *  - a plain Resume does not answer the mirror question: the run re-holds
 *  - applying a bound releases the hold and the run respects it
 *  - the explicit no-mirror ack (allow-unbounded) releases the hold and the
 *    run proceeds unbounded — including on a re-run over already-migrated
 *    data (idempotent redo across runs)
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
const DB = 'test_mig_guard';
const logger = pino({ level: 'silent' });

const APP = 'app_guard';
const EV = 'views';
const COLL = `drill_events${createHash('sha1').update(EV + APP).digest('hex')}`;
const HIST = 800;
const POST = 50;
const FLIP = Date.now() - 20 * 60_000; // tee flip 20 min ago
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

describe('unbounded-with-live-target startup guard', () => {
  let ch: ClickHouseClient;
  let mc: MongoClient;
  let ledger: LedgerStore;
  let dlqStore: DlqStore;
  let staging: StagingManager;
  let hashResolver: HashResolver;
  const closers: Array<() => Promise<void>> = [];

  const mkOrchestrator = async (runId: string, podId: string): Promise<ChunkOrchestrator> => {
    Object.assign(process.env, {
      SERVICE_NAME: 'guard-test',
      MONGO_URI, MONGO_DB: DB, MONGO_COUNTLY_DB: `${DB}_countly`, MANIFEST_DB: DB,
      CLICKHOUSE_URL: CH_URL, CLICKHOUSE_PASSWORD: CH_PASSWORD, CLICKHOUSE_DB: DB,
      LEDGER_RUN_ID: runId, LEDGER_CHUNK_DOCS_TARGET: '400', MONGO_PAGE_SIZE: '200',
      LEDGER_MONITOR_INTERVAL_MS: '0', BACKPRESSURE_ENABLED: 'false',
      MULTI_POD_ENABLED: 'false', POD_ID: podId,
    });
    delete process.env.LEDGER_CD_UPPER_BOUND;
    delete process.env.LEDGER_UNBOUNDED_OK;
    delete process.env.LEDGER_START_PAUSED;
    const config = loadConfig();
    const mongoReader = new MongoReader({
      uri: MONGO_URI, database: DB, readPreference: 'primary', readConcern: 'local',
      retryReads: true, appName: podId, cursorBatchSize: 500, maxTimeMs: 60_000,
    }, logger);
    await mongoReader.connect();
    closers.push(() => mongoReader.close());
    return new ChunkOrchestrator({
      config, logger, mongoReader, ledger, dlq: dlqStore, staging,
      retryPolicy: new RetryPolicy({ maxRetries: 3, baseDelayMs: 100, maxDelayMs: 500 }), hashResolver,
    });
  };

  const waitFor = async (cond: () => boolean, ms: number): Promise<void> => {
    const until = Date.now() + ms;
    while (!cond() && Date.now() < until) await sleep(300);
    expect(cond()).toBe(true);
  };

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

    // source: history before the flip + the mirror's post-flip re-ingested docs
    const docs: Record<string, unknown>[] = [];
    for (let i = 0; i < HIST; i++) {
      const t = FLIP - (HIST - i) * 60_000;
      docs.push({ _id: `h_${i}`, uid: String(i % 20), did: `d${i}`, ts: t, cd: new Date(t), sg: { v: i }, c: 1 });
    }
    for (let i = 0; i < POST; i++) {
      const t = FLIP + i * 1_000;
      docs.push({ _id: `post_${i}`, uid: 'p', did: 'd', ts: t, cd: new Date(t), sg: {}, c: 1 });
    }
    await mc.db(DB).collection(COLL).insertMany(docs as never[]);
    await mc.db(DB).collection(COLL).createIndex({ cd: 1, _id: 1 });

    // the guard's trigger: the target is ALREADY receiving live traffic
    const nowIso = (ms: number): string => new Date(ms).toISOString().replace('T', ' ').replace('Z', '');
    await ch.insert({
      table: `${DB}.drill_events`, format: 'JSONEachRow',
      values: Array.from({ length: 5 }, (_, i) => ({
        a: 'live_app', e: '[CLY]_custom', n: 'live', uid: 'u', did: 'd', _id: `live_${i}`,
        ts: nowIso(Date.now() - 5 * 60_000 + i), cd: nowIso(Date.now() - 5 * 60_000 + i),
        up: {}, sg: {}, c: 1, s: 0, dur: 0,
      })),
    });

    ledger = new LedgerStore(MONGO_URI, DB, logger);
    dlqStore = new DlqStore(MONGO_URI, DB, logger);
    staging = new StagingManager({
      url: CH_URL, database: DB, table: 'drill_events', username: 'default', password: CH_PASSWORD, queryTimeoutMs: 60_000,
    }, logger);
    hashResolver = new HashResolver({ uri: MONGO_URI, countlyDb: `${DB}_countly` }, logger);
    await ledger.connect();
    await dlqStore.connect();
    await staging.connect();
    await hashResolver.build();
    closers.push(() => ledger.close(), () => dlqStore.close(), () => staging.close(), () => hashResolver.close());
  }, 120_000);

  afterAll(async () => {
    for (const close of closers) await close().catch(() => {});
    await ch.command({ query: `DROP DATABASE IF EXISTS ${DB}` }).catch(() => {});
    await ch.close();
    await mc.db(DB).dropDatabase().catch(() => {});
    await mc.db(`${DB}_countly`).dropDatabase().catch(() => {});
    await mc.close();
  }, 60_000);

  const chCount = async (where: string): Promise<number> => {
    const res = await ch.query({ query: `SELECT count() AS n FROM ${DB}.drill_events WHERE ${where}`, format: 'JSONEachRow' });
    return Number((await res.json<{ n: string }>())[0].n);
  };

  it('holds a fresh unbounded run, ignores plain Resume, and releases when a bound is applied', async () => {
    const orch = await mkOrchestrator('guard-1', 'guard-pod-1');
    const done = orch.run();

    await waitFor(() => orch.getStats().status === 'paused' && orch.getStats().pauseReason === 'boundary-unset', 20_000);

    // Resume without answering the mirror question → re-held
    orch.resume();
    await sleep(4_500);
    expect(orch.getStats().status).toBe('paused');
    expect(orch.getStats().pauseReason).toBe('boundary-unset');

    // applying a bound answers it — the run releases AND respects the bound
    await ledger.setStoredBound('guard-1', FLIP, 'guard-test');
    await done;
    const stats = orch.getStats();
    expect(stats.status).toBe('completed');
    expect(stats.cdUpperBoundMs).toBe(FLIP);
    expect(await chCount("_id LIKE 'h_%'")).toBe(HIST);
    expect(await chCount("_id LIKE 'post_%'")).toBe(0); // post-flip mirror copies never migrated
  }, 120_000);

  it('the explicit no-mirror ack releases the hold and the run proceeds unbounded', async () => {
    const orch = await mkOrchestrator('guard-2', 'guard-pod-2');
    const done = orch.run();
    await waitFor(() => orch.getStats().status === 'paused' && orch.getStats().pauseReason === 'boundary-unset', 20_000);

    await ledger.setUnboundedAck('guard-2', 'guard-test');
    await done;
    expect(orch.getStats().status).toBe('completed');
    expect(orch.getStats().cdUpperBoundMs).toBeNull();
    // unbounded, as declared: the post-flip docs are migrated this time
    expect(await chCount("_id LIKE 'post_%'")).toBe(POST);
    expect(await chCount("_id LIKE 'h_%'")).toBe(HIST); // idempotent redo — no duplicates
    // a completed decision never re-arms: a third fresh-looking pod for the
    // same run sails through (statusCounts > 0 short-circuits anyway)
  }, 180_000);
});
