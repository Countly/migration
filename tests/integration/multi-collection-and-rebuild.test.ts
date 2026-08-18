/**
 * Multi-collection correctness + ledger rebuild.
 *
 * Production Countly stores drill events in MANY hashed collections
 * (drill_events{sha1(event+app)}), all overlapping in wall-clock time, while
 * ClickHouse holds them in ONE table. Every cd-window query against the live
 * table must therefore be scoped to the chunk's (a, e) — these tests pin that
 * (a window purge or count without scope silently corrupts sibling
 * collections), and exercise the ledger rebuild that regenerates mig_ranges
 * from data when progress state is lost.
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
import { rebuildLedger, newRebuildProgress } from '../../src/runtime/ledger-rebuild.ts';
import { loadConfig } from '../../src/config/loader.ts';
import type { Config } from '../../src/config/schema.ts';

const MONGO_URI = 'mongodb://localhost:27017/?directConnection=true';
const CH_URL = 'http://localhost:8123';
const DB = 'test_mig_multi';
const RUN = 'multi-1';
const logger = pino({ level: 'silent' });

const APP = 'app_alpha';
const EV1 = 'purchase';
const EV2 = 'page_view';
const hash = (ev: string) => createHash('sha1').update(ev + APP).digest('hex');
const COLL1 = `drill_events${hash(EV1)}`;
const COLL2 = `drill_events${hash(EV2)}`;
const DOCS_PER_COLL = 600;
const BASE = Date.UTC(2026, 5, 1);

describe('multi-collection scoping + ledger rebuild', () => {
  let ch: ClickHouseClient;
  let mc: MongoClient;
  let orchestrator: ChunkOrchestrator;
  let ledger: LedgerStore;
  let hashResolver: HashResolver;
  let config: Config;
  const closers: Array<() => Promise<void>> = [];

  const liveCount = async (e: string): Promise<number> => {
    const res = await ch.query({
      query: `SELECT count() AS c FROM ${DB}.drill_events WHERE a = '${APP}' AND e = '[CLY]_custom' AND n = '${e}'`,
      format: 'JSONEachRow',
    });
    return Number((await res.json<{ c: string }>())[0].c);
  };
  const totals = async (): Promise<{ t: number; u: number }> => {
    const res = await ch.query({
      query: `SELECT count() AS t, uniqExact(_id) AS u FROM ${DB}.drill_events`,
      format: 'JSONEachRow',
    });
    const [row] = await res.json<{ t: string; u: string }>();
    return { t: Number(row.t), u: Number(row.u) };
  };

  beforeAll(async () => {
    mc = new MongoClient(MONGO_URI);
    await mc.connect();
    await mc.db(DB).dropDatabase();
    await mc.db(`${DB}_countly`).dropDatabase();

    // Countly meta so the resolver maps both hashed collections to (a, e)
    await mc.db(`${DB}_countly`).collection('apps').insertOne({ _id: APP } as never);
    await mc.db(`${DB}_countly`).collection('events').insertOne({ _id: APP, list: [EV1, EV2] } as never);

    ch = createClient({ url: CH_URL });
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

    // Both collections cover the SAME wall-clock range — the production shape
    // that makes unscoped cd-window queries dangerous. Docs in the hashed
    // collections carry no a/e (implicit in the collection name).
    for (const [coll, tag] of [[COLL1, 'p'], [COLL2, 'v']] as const) {
      const docs: Record<string, unknown>[] = [];
      for (let i = 0; i < DOCS_PER_COLL; i++) {
        const ts = BASE + i * 60_000;
        docs.push({ _id: `${tag}_${i}`, uid: String(i % 40), did: `d${i}`, ts, cd: new Date(ts), sg: { v: i }, c: 1 });
      }
      const c = mc.db(DB).collection(coll);
      await c.insertMany(docs as never[]);
      await c.createIndex({ cd: 1, _id: 1 });
    }
    // Null-cd outliers in collection 1, ts INSIDE the regular range — their
    // sweep rows land inside regular windows and must not confuse rebuild.
    await mc.db(DB).collection(COLL1).insertMany([
      { _id: 'nocd_p_1', uid: 'u1', did: 'd', ts: BASE + 90_000 },
      { _id: 'nocd_p_2', uid: 'u2', did: 'd', ts: BASE + 150_000, cd: null },
    ] as never[]);

    process.env.SERVICE_NAME = 'multi-e2e';
    process.env.MONGO_URI = MONGO_URI;
    process.env.MONGO_DB = DB;
    process.env.MONGO_COUNTLY_DB = `${DB}_countly`;
    process.env.MANIFEST_DB = DB;
    process.env.CLICKHOUSE_URL = CH_URL;
    process.env.CLICKHOUSE_DB = DB;
    process.env.LEDGER_RUN_ID = RUN;
    process.env.LEDGER_CHUNK_DOCS_TARGET = '250';
    process.env.LEDGER_MONITOR_INTERVAL_MS = '0';
    process.env.BACKPRESSURE_ENABLED = 'false';
    config = loadConfig();

    const mongoReader = new MongoReader({
      uri: MONGO_URI, database: DB, readPreference: 'primary', readConcern: 'local',
      retryReads: true, appName: 'multi-e2e', cursorBatchSize: 500, maxTimeMs: 60_000,
    }, logger);
    ledger = new LedgerStore(MONGO_URI, DB, logger);
    const dlq = new DlqStore(MONGO_URI, DB, logger);
    const staging = new StagingManager({
      url: CH_URL, database: DB, table: 'drill_events', username: 'default', password: '', queryTimeoutMs: 60_000,
    }, logger);
    const retryPolicy = new RetryPolicy({ maxRetries: 2, baseDelayMs: 50, maxDelayMs: 200 });
    hashResolver = new HashResolver({ uri: MONGO_URI, countlyDb: `${DB}_countly` }, logger);

    await mongoReader.connect();
    await ledger.connect();
    await dlq.connect();
    await staging.connect();
    await hashResolver.build();
    closers.push(() => mongoReader.close(), () => ledger.close(), () => dlq.close(), () => staging.close(), () => hashResolver.close());

    orchestrator = new ChunkOrchestrator({
      config, logger, mongoReader, ledger, dlq, staging, retryPolicy, hashResolver,
    });
  }, 60_000);

  afterAll(async () => {
    for (const close of closers) await close().catch(() => {});
    if (process.env.KEEP_TEST_STATE) { await ch.close(); await mc.close(); return; }
    await ch.command({ query: `DROP DATABASE IF EXISTS ${DB}` }).catch(() => {});
    await ch.close();
    await mc.db(DB).dropDatabase().catch(() => {});
    await mc.db(`${DB}_countly`).dropDatabase().catch(() => {});
    await mc.close();
  });

  it('migrates overlapping collections exactly; chunks carry (a,e) scope; scoped verify passes', async () => {
    await orchestrator.run();

    expect(await liveCount(EV1)).toBe(DOCS_PER_COLL + 2); // + 2 null-cd sweep docs
    expect(await liveCount(EV2)).toBe(DOCS_PER_COLL);
    const { t, u } = await totals();
    expect(t).toBe(DOCS_PER_COLL * 2 + 2);
    expect(u).toBe(t); // zero duplicates

    const all = await ledger.listAll(RUN);
    expect(all.every((c) => c.scope_a === APP)).toBe(true);
    expect(all.every((c) => c.scope_e === '[CLY]_custom')).toBe(true); // custom events: e is the bucket, n the name
    expect(new Set(all.map((c) => c.scope_n))).toEqual(new Set([EV1, EV2]));

    // Pre-scoping, overlapping windows made per-chunk verify count BOTH
    // collections' rows → guaranteed mismatch. Scoped, it must be clean.
    const verify = await orchestrator.verifyMigration();
    expect(verify.ok).toBe(true);
    expect((verify.mismatches as unknown[]).length).toBe(0);
    expect(verify.unscopedSkipped).toBe(0);
  }, 120_000);

  it('retryFailed purges ONLY the failed chunk\'s collection — siblings in the same window are untouched', async () => {
    const all = await ledger.listAll(RUN);
    const victim = all.find((c) => c.collection === COLL1 && c.status === 'done' && c.lower_cd >= 0)!;
    await ledger.transition(victim._id, 'done', 'failed', { last_error: 'test: simulated invariant flag' });

    await orchestrator.retryFailed();

    // THE regression: before scoping, this purge deleted COLL2's rows in the
    // same cd window. COLL2 must be complete while COLL1's window is redone.
    expect(await liveCount(EV2)).toBe(DOCS_PER_COLL);

    await orchestrator.run();
    expect(await liveCount(EV1)).toBe(DOCS_PER_COLL + 2);
    const { t, u } = await totals();
    expect(u).toBe(t);
    expect(t).toBe(DOCS_PER_COLL * 2 + 2);
  }, 120_000);

  it('rebuilds a lost ledger from data: all-done run, post-cutover live rows ignored', async () => {
    // Disaster: the ledger vanishes. Meanwhile live ingestion (post-cutover)
    // keeps writing rows with NEWER cd than anything in the frozen source.
    await mc.db(DB).collection('mig_ranges').deleteMany({});
    await ch.command({
      query: `INSERT INTO ${DB}.drill_events (a, e, n, uid, did, _id, ts, cd)
              VALUES ('${APP}', '[CLY]_custom', '${EV1}', 'u_live', 'd_live', 'live_1', ${BASE + 40 * 86_400_000}, fromUnixTimestamp64Milli(${BASE + 40 * 86_400_000}))
                   , ('${APP}', '[CLY]_custom', '${EV2}', 'u_live', 'd_live', 'live_2', ${BASE + 41 * 86_400_000}, fromUnixTimestamp64Milli(${BASE + 41 * 86_400_000}))`,
    });

    const progress = newRebuildProgress();
    progress.status = 'running';
    await rebuildLedger({ config, logger, ledger, hashResolver, progress });

    const all = await ledger.listAll(RUN);
    expect(all.length).toBeGreaterThan(0);
    expect(all.every((c) => c.status === 'done')).toBe(true);
    expect(all.every((c) => c.scope_a === APP)).toBe(true);

    const sentinel = all.find((c) => c.collection === COLL1 && c.lower_cd === -1)!;
    expect(sentinel.status).toBe('done');
    expect(sentinel.rows_expected).toBe(2);

    const s1 = progress.summary.find((c) => c.collection === COLL1)!;
    expect(s1.scoped).toBe(true);
    expect(s1.nullCdDocs).toBe(2);
    expect(s1.nullCdSwept).toBe(2);
    // sweep rows subtracted per window; post-cutover rows outside all windows
    expect(s1.mongoDocs).toBe(DOCS_PER_COLL);
    expect(s1.liveRows).toBe(DOCS_PER_COLL);

    // Resuming after a rebuild that found everything done copies nothing new
    await orchestrator.run();
    const { t, u } = await totals();
    expect(t).toBe(DOCS_PER_COLL * 2 + 2 + 2); // + the 2 live rows
    expect(u).toBe(t);
  }, 120_000);

  it('rebuild flags a half-migrated window as failed; retry + resume heal it exactly', async () => {
    // Lose the ledger AND part of one window's rows (e.g. the crash that
    // took the ledger also lost a ClickHouse part).
    await mc.db(DB).collection('mig_ranges').deleteMany({});
    const goneIds = Array.from({ length: 50 }, (_, i) => `'p_${i + 10}'`).join(',');
    await ch.command({ query: `DELETE FROM ${DB}.drill_events WHERE _id IN (${goneIds})` });

    const progress = newRebuildProgress();
    progress.status = 'running';
    await rebuildLedger({ config, logger, ledger, hashResolver, progress });

    const all = await ledger.listAll(RUN);
    const failed = all.filter((c) => c.status === 'failed');
    expect(failed.length).toBe(1);
    expect(failed[0].collection).toBe(COLL1);
    expect(failed[0].last_error).toContain('rebuilt from data');
    // every other window checked out
    expect(all.filter((c) => c.status === 'done').length).toBe(all.length - 1);

    await orchestrator.retryFailed();
    await orchestrator.run();

    expect(await liveCount(EV1)).toBe(DOCS_PER_COLL + 2 + 1); // + live_1
    expect(await liveCount(EV2)).toBe(DOCS_PER_COLL + 1);     // + live_2
    const { t, u } = await totals();
    expect(u).toBe(t);

    const verify = await orchestrator.verifyMigration();
    expect(verify.ok).toBe(true);
  }, 120_000);
});
