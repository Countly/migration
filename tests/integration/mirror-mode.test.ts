/**
 * Mirror mode: live change-stream replication for the mirror-first playbook
 * (customer keeps the old arch authoritative; the new arch receives a live
 * copy; bulk migration backfills history up to the mirror's checkpoint).
 *
 * Covered here:
 *  - inserts mirrored with PRESERVED (_id, cd) identity, across hashed and
 *    base-format collections, resolver refresh for collections created
 *    after startup, non-insert ops counted (not mirrored)
 *  - stop/restart resumes from the saved token: writes during downtime are
 *    not lost; deliberate token REWIND redelivers and converges (pair-check)
 *  - THE SCENARIO: mirror running + source still growing + bulk migration
 *    bounded at the mirror checkpoint → the whole timeline lands exactly
 *    once, chunks never cross the bound, verify and the source audit pass
 *    over the combined mirrored+migrated table.
 *
 * Requires a replica-set MongoDB (change streams) — skipped on standalone.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pino from 'pino';
import { createHash } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { createClient, type ClickHouseClient } from '@clickhouse/client';

import { MirrorEngine } from '../../src/runtime/mirror-engine.ts';
import { MirrorStore } from '../../src/state/mirror-store.ts';
import { LedgerStore } from '../../src/state/ledger-store.ts';
import { DlqStore } from '../../src/state/dlq-store.ts';
import { StagingManager } from '../../src/target/staging-manager.ts';
import { MongoReader } from '../../src/source/mongo-reader.ts';
import { RetryPolicy } from '../../src/runtime/retry-policy.ts';
import { HashResolver } from '../../src/transform/hash-resolver.ts';
import { ChunkOrchestrator } from '../../src/runtime/chunk-orchestrator.ts';
import { rebuildLedger, newRebuildProgress } from '../../src/runtime/ledger-rebuild.ts';
import { loadConfig } from '../../src/config/loader.ts';

const MONGO_URI = 'mongodb://localhost:27017/?directConnection=true';
const CH_URL = process.env.TEST_CLICKHOUSE_URL ?? 'http://localhost:8123';
const CH_PASSWORD = process.env.TEST_CLICKHOUSE_PASSWORD ?? '';
const DB = 'test_mig_mirror';
const RUN = 'mirror-1';
const logger = pino({ level: 'silent' });

const APP = 'app_mirror';
const EV_A = 'taps';
const EV_LATE = 'late_event'; // meta created AFTER the mirror starts
const collOf = (ev: string): string => `drill_events${createHash('sha1').update(ev + APP).digest('hex')}`;
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

// change streams need a replica set — detect before defining the suite
const probe = new MongoClient(MONGO_URI);
let isReplicaSet = false;
try {
  await probe.connect();
  isReplicaSet = !!(await probe.db('admin').command({ hello: 1 })).setName;
} finally {
  await probe.close();
}

const CH_TABLE = `CREATE TABLE ${DB}.drill_events (
  \`a\` LowCardinality(String), \`e\` LowCardinality(String), \`n\` String,
  \`uid\` String, \`uid_canon\` Nullable(String), \`did\` String, \`lsid\` Nullable(String),
  \`_id\` String, \`ts\` DateTime64(3), \`up\` JSON(max_dynamic_paths = 32),
  \`custom\` Nullable(JSON(max_dynamic_paths = 0)), \`cmp\` Nullable(JSON(max_dynamic_paths = 0)),
  \`sg\` JSON(max_dynamic_paths = 0), \`c\` UInt32, \`s\` Float64, \`dur\` Float64,
  \`lu\` Nullable(DateTime64(3)), \`cd\` DateTime64(3) DEFAULT now64(3))
ENGINE = MergeTree PARTITION BY toYYYYMM(ts, 'UTC') ORDER BY (a, e, n, ts)`;

describe.skipIf(!isReplicaSet)('mirror mode (change-stream replication)', () => {
  let ch: ClickHouseClient;
  let mc: MongoClient;
  let staging: StagingManager;
  let dlqStore: DlqStore;
  let mirrorStore: MirrorStore;
  let hashResolver: HashResolver;
  let ledger: LedgerStore;
  const closers: Array<() => Promise<void>> = [];

  const baseEnv: Record<string, string> = {
    SERVICE_NAME: 'mirror-test',
    MONGO_URI, MONGO_DB: DB, MONGO_COUNTLY_DB: `${DB}_countly`, MANIFEST_DB: DB,
    CLICKHOUSE_URL: CH_URL, CLICKHOUSE_PASSWORD: CH_PASSWORD, CLICKHOUSE_DB: DB,
    LEDGER_RUN_ID: RUN,
    LEDGER_CHUNK_DOCS_TARGET: '500',
    MONGO_PAGE_SIZE: '250',
    LEDGER_MONITOR_INTERVAL_MS: '0',
    BACKPRESSURE_ENABLED: 'false',
    MIRROR_BATCH_DOCS: '200',
    MIRROR_BATCH_MS: '150',
  };

  const chCount = async (where = '1'): Promise<{ c: number; u: number }> => {
    const res = await ch.query({
      query: `SELECT count() AS c, uniqExact(_id) AS u FROM ${DB}.drill_events WHERE ${where}`,
      format: 'JSONEachRow',
    });
    const [r] = await res.json<{ c: string; u: string }>();
    return { c: Number(r.c), u: Number(r.u) };
  };
  const waitFor = async (cond: () => Promise<boolean>, ms = 30_000): Promise<void> => {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      if (await cond()) return;
      await sleep(250);
    }
    throw new Error('waitFor timeout');
  };

  const newEngine = (): MirrorEngine => {
    for (const [k, v] of Object.entries(baseEnv)) process.env[k] = v;
    process.env.MIRROR_MODE = 'true';
    delete process.env.LEDGER_CD_UPPER_BOUND;
    const config = loadConfig();
    return new MirrorEngine({ config, logger, staging, dlq: dlqStore, mirrorStore, hashResolver });
  };

  beforeAll(async () => {
    mc = new MongoClient(MONGO_URI);
    await mc.connect();
    await mc.db(DB).dropDatabase();
    await mc.db(`${DB}_countly`).dropDatabase();
    await mc.db(`${DB}_countly`).collection('apps').insertOne({ _id: APP } as never);
    await mc.db(`${DB}_countly`).collection('events').insertOne({ _id: APP, list: [EV_A] } as never);

    ch = createClient({ url: CH_URL, password: CH_PASSWORD });
    await ch.command({ query: `CREATE DATABASE IF NOT EXISTS ${DB}` });
    await ch.command({ query: `DROP TABLE IF EXISTS ${DB}.drill_events` });
    await ch.command({ query: CH_TABLE });

    staging = new StagingManager({
      url: CH_URL, database: DB, table: 'drill_events', username: 'default', password: CH_PASSWORD, queryTimeoutMs: 60_000,
    }, logger);
    dlqStore = new DlqStore(MONGO_URI, DB, logger);
    mirrorStore = new MirrorStore(MONGO_URI, DB, logger);
    ledger = new LedgerStore(MONGO_URI, DB, logger);
    hashResolver = new HashResolver({ uri: MONGO_URI, countlyDb: `${DB}_countly` }, logger);
    await staging.connect();
    await dlqStore.connect();
    await mirrorStore.connect();
    await ledger.connect();
    await hashResolver.build();
    closers.push(() => staging.close(), () => dlqStore.close(), () => mirrorStore.close(), () => ledger.close(), () => hashResolver.close());
  }, 120_000);

  afterAll(async () => {
    for (const close of closers) await close().catch(() => {});
    await ch.command({ query: `DROP DATABASE IF EXISTS ${DB}` }).catch(() => {});
    await ch.close();
    await mc.db(DB).dropDatabase().catch(() => {});
    await mc.db(`${DB}_countly`).dropDatabase().catch(() => {});
    await mc.close();
  });

  it('mirrors inserts exactly; resumes across restart; token rewind converges', async () => {
    let engine = newEngine();
    let runP = engine.run();
    await waitFor(async () => (await mirrorStore.load(RUN)) !== null);
    const t0 = (await mirrorStore.load(RUN))!.checkpoint_ms;
    expect(t0).toBeGreaterThan(0);

    // hashed collection (identity from the collection name)
    const collA = mc.db(DB).collection(collOf(EV_A));
    const now = Date.now();
    await collA.insertMany(Array.from({ length: 300 }, (_, i) => ({
      _id: `a_${i}`, uid: String(i % 20), did: `d${i}`, ts: now + i, cd: new Date(now + i), sg: { v: i }, c: 1,
    })) as never[]);
    // base collection (new format: identity embedded in the doc)
    await mc.db(DB).collection('drill_events').insertMany(Array.from({ length: 200 }, (_, i) => ({
      _id: `b_${i}`, a: APP, e: '[CLY]_custom', n: 'embedded_ev',
      uid: String(i % 20), did: `d${i}`, ts: now + i, cd: new Date(now + i), sg: { v: i }, c: 1,
    })) as never[]);

    await waitFor(async () => (await chCount()).c >= 500);
    let tot = await chCount();
    expect(tot).toEqual({ c: 500, u: 500 });

    // identity preserved end-to-end: (_id, cd) of a sampled row matches Mongo
    const res = await ch.query({
      query: `SELECT _id, toUnixTimestamp64Milli(cd) AS cdms, a, e, n FROM ${DB}.drill_events WHERE _id = 'a_7'`,
      format: 'JSONEachRow',
    });
    const [row] = await res.json<{ _id: string; cdms: string; a: string; e: string; n: string }>();
    expect(Number(row.cdms)).toBe(now + 7);
    expect([row.a, row.e, row.n]).toEqual([APP, '[CLY]_custom', EV_A]);

    // a collection whose meta appears only AFTER the mirror started:
    // the engine must refresh the resolver instead of skipping forever
    await mc.db(`${DB}_countly`).collection('events').updateOne(
      { _id: APP } as never, { $push: { list: EV_LATE } } as never);
    await mc.db(DB).collection(collOf(EV_LATE)).insertMany(Array.from({ length: 50 }, (_, i) => ({
      _id: `late_${i}`, uid: 'u', did: 'd', ts: now + i, cd: new Date(now + i), sg: {}, c: 1,
    })) as never[]);
    await waitFor(async () => (await chCount(`n = '${EV_LATE}'`)).c === 50);

    // non-insert ops are counted, never mirrored, and the stream survives
    await collA.updateOne({ _id: 'a_0' } as never, { $set: { uid: 'merged' } } as never);
    await collA.deleteOne({ _id: 'a_1' } as never);
    await waitFor(async () => ((await mirrorStore.load(RUN))!.non_insert_ops ?? 0) >= 2);
    tot = await chCount();
    expect(tot).toEqual({ c: 550, u: 550 }); // CH untouched by update/delete
    // restore a_1 in Mongo (identical identity) so later Mongo-vs-CH totals
    // reconcile; the mirror redelivers it and the pair-check must SKIP it
    await collA.insertOne({ _id: 'a_1', uid: '1', did: 'd1', ts: now + 1, cd: new Date(now + 1), sg: { v: 1 }, c: 1 } as never);
    await sleep(600);
    expect(await chCount()).toEqual({ c: 550, u: 550 }); // no duplicate from the re-insert

    // stop; write while the mirror is DOWN; restart → resume, no loss
    const tokenBeforeDowntime = (await mirrorStore.load(RUN))!.resume_token;
    engine.stop();
    await runP;
    await collA.insertMany(Array.from({ length: 150 }, (_, i) => ({
      _id: `down_${i}`, uid: 'u', did: 'd', ts: now + i, cd: new Date(now + i), sg: {}, c: 1,
    })) as never[]);
    engine = newEngine();
    runP = engine.run();
    await waitFor(async () => (await chCount()).c >= 700);
    expect(await chCount()).toEqual({ c: 700, u: 700 });

    // deliberate REWIND: force redelivery of everything since before the
    // downtime — the pair-check must converge, never duplicate
    engine.stop();
    await runP;
    await mc.db(DB).collection('mig_mirror_state').updateOne(
      { _id: RUN } as never, { $set: { resume_token: tokenBeforeDowntime } } as never);
    engine = newEngine();
    runP = engine.run();
    await waitFor(async () => {
      const st = await mirrorStore.load(RUN);
      return !!st?.last_flush_at && Date.now() - st.last_flush_at.getTime() < 1_000;
    });
    await sleep(1_000); // let any (wrong) duplicates land
    expect(await chCount()).toEqual({ c: 700, u: 700 });

    engine.stop();
    await runP;
  }, 120_000);

  it('THE SCENARIO: mirror + source still growing + bulk migration bounded at the checkpoint = exact timeline', async () => {
    // history that predates the mirror
    const collA = mc.db(DB).collection(collOf(EV_A));
    const BASE = Date.UTC(2026, 0, 1);
    let docs: Record<string, unknown>[] = [];
    for (let i = 0; i < 6_000; i++) {
      docs.push({ _id: `hist_${i}`, uid: String(i % 50), did: `d${i}`, ts: BASE + i * 60_000, cd: new Date(BASE + i * 60_000), sg: { v: i }, c: 1 });
      if (docs.length === 2_000) { await collA.insertMany(docs as never[]); docs = []; }
    }
    if (docs.length) await collA.insertMany(docs as never[]);
    await collA.createIndex({ cd: 1, _id: 1 });
    await mc.db(DB).collection('drill_events').createIndex({ cd: 1, _id: 1 });
    await mc.db(DB).collection(collOf(EV_LATE)).createIndex({ cd: 1, _id: 1 });

    const engine = newEngine();
    const runP = engine.run();
    await waitFor(async () => (await mirrorStore.load(RUN)) !== null);
    const t0 = (await mirrorStore.load(RUN))!.checkpoint_ms;

    // old ingestion keeps writing THROUGH the bulk migration
    let liveWritten = 0;
    let stopWriter = false;
    const writer = (async () => {
      while (!stopWriter) {
        const nowMs = Date.now();
        await collA.insertMany(Array.from({ length: 25 }, (_, i) => ({
          _id: `live_${nowMs}_${liveWritten + i}`, uid: 'lu', did: 'ld',
          ts: nowMs, cd: new Date(nowMs), sg: {}, c: 1,
        })) as never[]);
        liveWritten += 25;
        await sleep(80);
      }
    })();

    // bulk migration, bounded at the mirror checkpoint
    for (const [k, v] of Object.entries(baseEnv)) process.env[k] = v;
    process.env.MIRROR_MODE = 'false';
    process.env.LEDGER_CD_UPPER_BOUND = String(t0);
    process.env.MULTI_POD_ENABLED = 'false';
    process.env.POD_ID = 'bulk-pod';
    const config = loadConfig();
    const mongoReader = new MongoReader({
      uri: MONGO_URI, database: DB, readPreference: 'primary', readConcern: 'local',
      retryReads: true, appName: 'bulk-pod', cursorBatchSize: 500, maxTimeMs: 60_000,
    }, logger);
    await mongoReader.connect();
    closers.push(() => mongoReader.close());
    const orchestrator = new ChunkOrchestrator({
      config, logger, mongoReader, ledger, dlq: dlqStore, staging,
      retryPolicy: new RetryPolicy({ maxRetries: 3, baseDelayMs: 100, maxDelayMs: 500 }), hashResolver,
    });
    await orchestrator.run();
    expect(orchestrator.getStats().status).toBe('completed');

    // the ledger never crossed the checkpoint
    const chunks = await mc.db(DB).collection('mig_ranges').find({ run_id: RUN, lower_cd: { $gte: 0 } } as never).toArray();
    expect(chunks.length).toBeGreaterThan(0);
    for (const c of chunks) expect(c.upper_cd).toBeLessThanOrEqual(t0);

    // stop old ingestion; let the mirror drain
    stopWriter = true;
    await writer;
    const mongoTotal = async (): Promise<number> => {
      let n = 0;
      for (const name of [collOf(EV_A), collOf(EV_LATE), 'drill_events']) {
        n += await mc.db(DB).collection(name).countDocuments();
      }
      return n;
    };
    const expected = await mongoTotal();
    await waitFor(async () => (await chCount()).c === expected, 60_000);
    const tot = await chCount();
    expect(tot, `mirrored+migrated vs source (liveWritten=${liveWritten})`).toEqual({ c: expected, u: expected });

    // the whole timeline — mirrored AND migrated — passes verify + audit
    const verify = await orchestrator.verifyMigration();
    expect(verify.ok, JSON.stringify(verify.mismatches).slice(0, 2000)).toBe(true);
    const prog = newRebuildProgress();
    await rebuildLedger({ config, logger, ledger, dlq: dlqStore, hashResolver, progress: prog, checkOnly: true });
    expect(prog.mismatchedWindows.length, JSON.stringify(prog.mismatchedWindows).slice(0, 2000)).toBe(0);
    expect(prog.deletionDriftWindows.length).toBe(0);

    engine.stop();
    await runP;
  }, 300_000);
});
