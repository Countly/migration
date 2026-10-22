/**
 * Index-build progress reporting — the run's own mapping-time wait on the
 * {cd:1,_id:1} index is visible, not only builds started from the button.
 * Pinned here:
 *
 *  - while mapping blocks on ensureIndex, autoIndexState / index-progress
 *    name the collection and its position in the map pass
 *  - the state clears once the index is ready, and the run completes with
 *    the index built on every collection
 *  - the state is set around ensureIndex even when the index is LISTED
 *    (an in-progress build lists as present but still blocks)
 *  - the button path builds missing indexes and reports completion
 *  - $currentOp works under the default readConcern 'majority' (it only
 *    accepts 'local'; the failure used to be swallowed into "no progress")
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
const DB = 'test_mig_idxprog';
const logger = pino({ level: 'silent' });

const APP = 'app_idx';
const EVENTS = ['views', 'clicks'];
const collOf = (ev: string): string => `drill_events${createHash('sha1').update(ev + APP).digest('hex')}`;
const COLLS = EVENTS.map(collOf);
const DOCS = 300;
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

const hasCdIdIndex = async (mc: MongoClient, coll: string): Promise<boolean> =>
  (await mc.db(DB).collection(coll).indexes()).some((i) => i.name === 'cd_1__id_1');

describe('index-build progress', () => {
  let ch: ClickHouseClient;
  let mc: MongoClient;
  let ledger: LedgerStore;
  let dlqStore: DlqStore;
  let staging: StagingManager;
  let hashResolver: HashResolver;
  const closers: Array<() => Promise<void>> = [];

  const mkOrchestrator = async (runId: string): Promise<{ orch: ChunkOrchestrator; reader: MongoReader }> => {
    Object.assign(process.env, {
      SERVICE_NAME: 'idxprog-test',
      MONGO_URI, MONGO_DB: DB, MONGO_COUNTLY_DB: `${DB}_countly`, MANIFEST_DB: DB,
      CLICKHOUSE_URL: CH_URL, CLICKHOUSE_PASSWORD: CH_PASSWORD, CLICKHOUSE_DB: DB,
      LEDGER_RUN_ID: runId, LEDGER_CHUNK_DOCS_TARGET: '400', MONGO_PAGE_SIZE: '200',
      LEDGER_MONITOR_INTERVAL_MS: '0', BACKPRESSURE_ENABLED: 'false',
      MULTI_POD_ENABLED: 'false', POD_ID: `${runId}-pod`, LEDGER_UNBOUNDED_OK: '1',
    });
    delete process.env.LEDGER_CD_UPPER_BOUND;
    delete process.env.LEDGER_START_PAUSED;
    const config = loadConfig();
    const reader = new MongoReader({
      // 'majority' is the MONGO_READ_CONCERN default — $currentOp refuses it
      // unless the query pins 'local' itself
      uri: MONGO_URI, database: DB, readPreference: 'primary', readConcern: 'majority',
      retryReads: true, appName: runId, cursorBatchSize: 500, maxTimeMs: 60_000,
    }, logger);
    await reader.connect();
    closers.push(() => reader.close());
    const orch = new ChunkOrchestrator({
      config, logger, mongoReader: reader, ledger, dlq: dlqStore, staging,
      retryPolicy: new RetryPolicy({ maxRetries: 3, baseDelayMs: 100, maxDelayMs: 500 }), hashResolver,
    });
    return { orch, reader };
  };

  /** Hold every ensureIndex until released — a build that takes as long as the test wants. */
  const gateEnsureIndex = (reader: MongoReader): { release: () => void; entered: string[] } => {
    const entered: string[] = [];
    let open: () => void = () => {};
    let gate = new Promise<void>((r) => { open = r; });
    const real = reader.ensureIndex.bind(reader);
    reader.ensureIndex = async (coll: string): Promise<void> => {
      entered.push(coll);
      await gate;
      await real(coll);
    };
    return {
      entered,
      release: () => { open(); gate = new Promise<void>((r) => { open = r; }); },
    };
  };

  const waitFor = async (cond: () => boolean, ms: number): Promise<void> => {
    const until = Date.now() + ms;
    while (!cond() && Date.now() < until) await sleep(100);
    expect(cond()).toBe(true);
  };

  const seed = async (): Promise<void> => {
    await mc.db(DB).dropDatabase();
    const base = Date.now() - 30 * 86_400_000;
    for (const coll of COLLS) {
      await mc.db(DB).collection(coll).insertMany(Array.from({ length: DOCS }, (_, i) => {
        const t = base + i * 60_000;
        return { _id: `${coll.slice(-6)}_${i}`, uid: String(i % 10), did: `d${i}`, ts: t, cd: new Date(t), sg: {}, c: 1 };
      }) as never[]);
    }
    await ch.command({ query: `TRUNCATE TABLE ${DB}.drill_events` });
  };

  beforeAll(async () => {
    mc = new MongoClient(MONGO_URI);
    await mc.connect();
    await mc.db(`${DB}_countly`).dropDatabase();
    await mc.db(`${DB}_countly`).collection('apps').insertOne({ _id: APP } as never);
    await mc.db(`${DB}_countly`).collection('events').insertOne({ _id: APP, list: EVENTS } as never);

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

  it('reports the collection the run is waiting on, per collection, and clears when the index is ready', async () => {
    await seed();
    const { orch, reader } = await mkOrchestrator('idx-auto');
    const gate = gateEnsureIndex(reader);
    expect(orch.autoIndexState()).toBeNull();
    const done = orch.run();

    const seen: Array<{ collection: string; position: number | null; of: number | null }> = [];
    for (let n = 1; n <= COLLS.length; n++) {
      await waitFor(() => gate.entered.length === n, 30_000);
      const state = orch.autoIndexState();
      expect(state).not.toBeNull();
      expect(state!.collection).toBe(gate.entered[n - 1]);
      expect(state!.startedAtMs).toBeLessThanOrEqual(Date.now());
      const progress = await orch.indexBuildProgress();
      expect(progress.auto).toEqual(state);
      expect(progress.phases).toBe(4);
      expect(Array.isArray(progress.serverOps)).toBe(true);
      expect(progress.serverOpsError).toBeNull();
      seen.push({ collection: state!.collection, position: state!.position, of: state!.of });
      gate.release();
    }
    expect(seen.map((s) => [s.position, s.of])).toEqual([[1, 2], [2, 2]]);
    expect(new Set(seen.map((s) => s.collection))).toEqual(new Set(COLLS));

    // top-up / sweep re-probes pass through ensureIndex again — keep releasing
    const releaser = setInterval(() => gate.release(), 50);
    await done;
    clearInterval(releaser);
    expect(orch.getStats().status).toBe('completed');
    expect(orch.autoIndexState()).toBeNull();
    expect((await orch.indexBuildProgress()).auto).toBeNull();
    for (const coll of COLLS) expect(await hasCdIdIndex(mc, coll)).toBe(true);
  }, 120_000);

  it('sets the state around ensureIndex even when the index is already listed', async () => {
    await seed();
    for (const coll of COLLS) await mc.db(DB).collection(coll).createIndex({ cd: 1, _id: 1 });
    const { orch, reader } = await mkOrchestrator('idx-listed');
    const gate = gateEnsureIndex(reader);
    const done = orch.run();
    await waitFor(() => gate.entered.length === 1, 30_000);
    expect(orch.autoIndexState()).toMatchObject({ collection: gate.entered[0], position: 1, of: 2 });
    const releaser = setInterval(() => gate.release(), 50);
    await done;
    clearInterval(releaser);
    expect(orch.getStats().status).toBe('completed');
    expect(orch.autoIndexState()).toBeNull();
  }, 120_000);

  it('builds missing indexes from the button and reports completion', async () => {
    await seed();
    const { orch } = await mkOrchestrator('idx-button');
    const started = await orch.startIndexBuilds();
    expect(started).toEqual({ started: true, missing: COLLS.length, inProgress: 0 });
    const until = Date.now() + 30_000;
    let progress = await orch.indexBuildProgress();
    while (progress.running && Date.now() < until) { await sleep(100); progress = await orch.indexBuildProgress(); }
    expect(progress).toMatchObject({ running: false, total: COLLS.length, error: null, current: null, auto: null });
    expect(new Set(progress.done as string[])).toEqual(new Set(COLLS));
    for (const coll of COLLS) expect(await hasCdIdIndex(mc, coll)).toBe(true);

    // everything indexed, nothing in flight → nothing to do
    expect(await orch.startIndexBuilds()).toEqual({ started: false, missing: 0, inProgress: 0 });
  }, 120_000);
});
