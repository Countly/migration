/**
 * LEDGER_CD_UPPER_BOUND — the duplication guard for tee-mirrored cutovers.
 *
 * Field scenario: the old cluster stays authoritative and KEEPS receiving
 * traffic; nginx tees the same requests to the new architecture, which
 * re-ingests them with its own logic (and its own _id/cd identities). Any
 * old-cluster doc at/after the tee flip that the bulk migration copies
 * would therefore be an UNDETECTABLE duplicate of its re-ingested twin —
 * the (_id, cd) machinery cannot pair them. The bound makes post-flip data
 * structurally invisible to the migration:
 *
 *  - the mapper never cuts a window at/past the bound
 *  - top-up is disabled entirely (no chasing new data)
 *  - collections born after the bound are skipped
 *  - re-running map passes on a growing source appends NOTHING
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

const MONGO_URI = 'mongodb://localhost:27017/?directConnection=true';
const CH_URL = process.env.TEST_CLICKHOUSE_URL ?? 'http://localhost:8123';
const CH_PASSWORD = process.env.TEST_CLICKHOUSE_PASSWORD ?? '';
const DB = 'test_mig_bound';
const RUN = 'bound-1';
const logger = pino({ level: 'silent' });

const APP = 'app_bound';
const EV = 'orders';
const COLL = `drill_events${createHash('sha1').update(EV + APP).digest('hex')}`;
const HIST = 5_000;
const BASE = Date.UTC(2026, 3, 1);
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

describe('cd upper bound (tee-mirror duplication guard)', () => {
  let ch: ClickHouseClient;
  let mc: MongoClient;
  let ledger: LedgerStore;
  let dlqStore: DlqStore;
  let staging: StagingManager;
  let hashResolver: HashResolver;
  const closers: Array<() => Promise<void>> = [];
  const BOUND = Date.now() - 60_000; // "tee flip" happened a minute ago

  const mkOrchestrator = async (podId: string): Promise<ChunkOrchestrator> => {
    Object.assign(process.env, {
      SERVICE_NAME: 'bound-test',
      MONGO_URI, MONGO_DB: DB, MONGO_COUNTLY_DB: `${DB}_countly`, MANIFEST_DB: DB,
      CLICKHOUSE_URL: CH_URL, CLICKHOUSE_PASSWORD: CH_PASSWORD, CLICKHOUSE_DB: DB,
      LEDGER_RUN_ID: RUN, LEDGER_CHUNK_DOCS_TARGET: '500', MONGO_PAGE_SIZE: '250',
      LEDGER_MONITOR_INTERVAL_MS: '0', BACKPRESSURE_ENABLED: 'false',
      MULTI_POD_ENABLED: 'false', POD_ID: podId,
      LEDGER_CD_UPPER_BOUND: String(BOUND),
    });
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

    // history strictly before the bound + a slice already past it (data
    // that arrived between the tee flip and the migration starting)
    const coll = mc.db(DB).collection(COLL);
    const docs: Record<string, unknown>[] = [];
    for (let i = 0; i < HIST; i++) {
      const t = BASE + i * 60_000;
      docs.push({ _id: `h_${i}`, uid: String(i % 40), did: `d${i}`, ts: t, cd: new Date(t), sg: { v: i }, c: 1 });
    }
    for (let i = 0; i < 300; i++) {
      const t = BOUND + i * 100; // at/after the flip — tee territory
      docs.push({ _id: `post_${i}`, uid: 'p', did: 'd', ts: t, cd: new Date(t), sg: {}, c: 1 });
    }
    await coll.insertMany(docs as never[]);
    await coll.createIndex({ cd: 1, _id: 1 });

    // a collection born entirely after the flip — must be skipped whole
    const born = mc.db(DB).collection('drill_events');
    await born.insertMany(Array.from({ length: 100 }, (_, i) => ({
      _id: `born_${i}`, a: APP, e: '[CLY]_custom', n: 'new_ev',
      uid: 'u', did: 'd', ts: BOUND + i, cd: new Date(BOUND + i), sg: {}, c: 1,
    })) as never[]);
    await born.createIndex({ cd: 1, _id: 1 });

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
  });

  it('migrates only cd < bound while the source keeps growing; re-map appends nothing', async () => {
    // old ingestion keeps writing post-flip data THROUGH the migration
    let stopWriter = false;
    let liveWritten = 0;
    const writer = (async () => {
      const coll = mc.db(DB).collection(COLL);
      while (!stopWriter) {
        const nowMs = Date.now();
        await coll.insertMany(Array.from({ length: 20 }, (_, i) => ({
          _id: `live_${nowMs}_${liveWritten + i}`, uid: 'lu', did: 'ld',
          ts: nowMs, cd: new Date(nowMs), sg: {}, c: 1,
        })) as never[]);
        liveWritten += 20;
        await sleep(60);
      }
    })();

    const orchestrator = await mkOrchestrator('bound-pod');
    await orchestrator.run();
    stopWriter = true;
    await writer;
    expect(orchestrator.getStats().status).toBe('completed');
    expect(liveWritten).toBeGreaterThan(0);

    // exactly the pre-flip history — nothing at/after the bound, ever
    const res = await ch.query({
      query: `SELECT count() AS c, uniqExact(_id) AS u,
                     countIf(_id LIKE 'post\\_%' OR _id LIKE 'live\\_%' OR _id LIKE 'born\\_%') AS leaked,
                     max(toUnixTimestamp64Milli(cd)) AS maxcd
              FROM ${DB}.drill_events`,
      format: 'JSONEachRow',
    });
    const [r] = await res.json<{ c: string; u: string; leaked: string; maxcd: string }>();
    expect(Number(r.leaked), 'post-flip docs must never migrate').toBe(0);
    expect(Number(r.c)).toBe(HIST);
    expect(Number(r.u)).toBe(HIST);
    expect(Number(r.maxcd)).toBeLessThan(BOUND);

    // ledger discipline: no window touches the bound; born-after collection unmapped
    const chunks = await mc.db(DB).collection('mig_ranges').find({ run_id: RUN } as never).toArray();
    expect(chunks.length).toBeGreaterThan(0);
    for (const c of chunks) {
      expect(c.upper_cd).toBeLessThanOrEqual(BOUND);
      expect(c.collection).toBe(COLL); // 'drill_events' (born post-flip) never mapped
    }

    // a SECOND full run over the (still larger) source: top-up is disabled,
    // the grid is untouched, and not one extra row lands
    const again = await mkOrchestrator('bound-pod-2');
    await again.run();
    expect(again.getStats().status).toBe('completed');
    const after = await mc.db(DB).collection('mig_ranges').countDocuments({ run_id: RUN } as never);
    expect(after).toBe(chunks.length);
    const res2 = await ch.query({ query: `SELECT count() AS c FROM ${DB}.drill_events`, format: 'JSONEachRow' });
    expect(Number((await res2.json<{ c: string }>())[0].c)).toBe(HIST);

    // frozen progress denominator: the bound-clamped map counted the span
    // EXACTLY, so the stored estimate equals the migrated history and the
    // second run (which appended nothing) did not disturb it
    expect(await ledger.sumEstimates(RUN)).toBe(HIST);

    // verify + source audit stay green with a growing source: post-bound
    // windows derived from the source show live=0 (pending), never defects
    const verify = await orchestrator.verifyMigration();
    expect(verify.ok, JSON.stringify(verify.mismatches).slice(0, 2000)).toBe(true);
    const config = loadConfig();
    const prog = newRebuildProgress();
    await rebuildLedger({ config, logger, ledger, dlq: dlqStore, hashResolver, progress: prog, checkOnly: true });
    expect(prog.mismatchedWindows.length, JSON.stringify(prog.mismatchedWindows).slice(0, 2000)).toBe(0);
    expect(prog.deletionDriftWindows.length).toBe(0);
  }, 180_000);
});
