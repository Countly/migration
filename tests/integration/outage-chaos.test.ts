/**
 * Backing-service outage chaos: the migration keeps its consistency
 * guarantees when MONGO and CLICKHOUSE themselves go away mid-run — not
 * just when migration pods die (pod-chaos covers that).
 *
 * Runs against DEDICATED throwaway containers (never the shared dev-stack
 * services), so it is env-gated: CHAOS_OUTAGE=1 npx vitest run <this file>.
 *
 * Outage script while two workers migrate:
 *   1. `docker restart` ClickHouse mid-copy  — hard crash: connections
 *      reset, in-flight inserts fail, retry backoff must absorb or park.
 *   2. `docker pause` MongoDB for ~6s        — the nastier hang case:
 *      connections freeze (no errors), cursors stall, heartbeats stall.
 *      Because the LEDGER freezes for every pod equally, no lease can be
 *      stolen during the outage — a property worth proving, not assuming.
 *
 * Convergence: workers may exit nonzero or park chunks as failed — the
 * documented operator flow (respawn + retryFailed) must heal to an EXACT
 * end state: every doc once, verify + source audit pass, no debris.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { spawn, execSync, type ChildProcess } from 'node:child_process';
import path from 'node:path';
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

const ENABLED = process.env.CHAOS_OUTAGE === '1';

const MONGO_CTR = 'mig-chaos-mongo';
const CH_CTR = 'mig-chaos-ch';
const MONGO_URI = 'mongodb://localhost:27117/?directConnection=true';
const CH_URL = 'http://localhost:8223';
const CH_PASSWORD = 'chaos-pass';
const DB = 'test_mig_outage';
const RUN = 'outage-1';
const logger = pino({ level: 'silent' });

const APP = 'app_outage';
const EVENTS = ['payments', 'sessions'];
const collOf = (ev: string): string => `drill_events${createHash('sha1').update(ev + APP).digest('hex')}`;
const DOCS_EACH = 30_000;
const NULL_CD_DOCS = 20;
const BASE = Date.UTC(2026, 2, 1);

const sh = (cmd: string): string => execSync(cmd, { encoding: 'utf-8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

describe.skipIf(!ENABLED)('backing-service outage chaos (CHAOS_OUTAGE=1, dedicated containers)', () => {
  let ch: ClickHouseClient;
  let mc: MongoClient;
  let ledger: LedgerStore;
  let dlqStore: DlqStore;
  let orchestrator: ChunkOrchestrator; // verify/audit/heal only
  let hashResolver: HashResolver;
  let config: Config;
  const closers: Array<() => Promise<void>> = [];

  const WORKER = path.resolve(process.cwd(), 'tests/chaos/worker.ts');
  const baseEnv: Record<string, string> = {
    SERVICE_NAME: 'outage-chaos',
    MONGO_URI, MONGO_DB: DB, MONGO_COUNTLY_DB: `${DB}_countly`, MANIFEST_DB: DB,
    CLICKHOUSE_URL: CH_URL, CLICKHOUSE_PASSWORD: CH_PASSWORD, CLICKHOUSE_DB: DB,
    LEDGER_RUN_ID: RUN,
    LEDGER_CHUNK_DOCS_TARGET: '1000',
    MONGO_PAGE_SIZE: '250', // slow the copy so outages land mid-chunk
    LEDGER_LEASE_SEC: '3',
    LEDGER_MONITOR_INTERVAL_MS: '0',
    BACKPRESSURE_ENABLED: 'false',
    MULTI_POD_ENABLED: 'true',
    LOG_LEVEL: 'fatal',
    CHAOS_LOG_LEVEL: 'warn', // worker pino level — stderrTail captures it
    // prod-like backoff — the point of this suite: multi-second blips must
    // be absorbed by retries, not instantly park chunks
    CLICKHOUSE_MAX_RETRIES: '8',
    CLICKHOUSE_RETRY_BASE_DELAY_MS: '500',
    CLICKHOUSE_RETRY_MAX_DELAY_MS: '5000',
  };

  const stderrTail = new Map<string, string>();
  const spawnWorker = (podId: string): ChildProcess => {
    const child = spawn(process.execPath, ['--experimental-strip-types', WORKER], {
      env: { ...process.env, ...baseEnv, POD_ID: podId },
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    let tail = '';
    child.stderr!.on('data', (d: Buffer) => { tail = (tail + d.toString()).slice(-4000); });
    child.on('exit', () => stderrTail.set(podId, tail));
    return child;
  };
  const waitExit = (p: ChildProcess): Promise<{ code: number | null; signal: string | null }> =>
    new Promise((resolve) => {
      if (p.exitCode !== null || p.signalCode !== null) {
        resolve({ code: p.exitCode, signal: p.signalCode });
        return;
      }
      p.on('exit', (code, signal) => resolve({ code, signal }));
    });

  beforeAll(async () => {
    // dedicated throwaway services — NEVER the shared dev-stack containers
    sh(`docker rm -f ${MONGO_CTR} ${CH_CTR} 2>/dev/null || true`);
    const mongoImage = process.env.CHAOS_MONGO_IMAGE ?? 'mirror.gcr.io/library/mongo:8';
    const chImage = process.env.CHAOS_CH_IMAGE ?? 'mirror.gcr.io/clickhouse/clickhouse-server:26.4';
    sh(`docker run -d --name ${MONGO_CTR} -p 27117:27017 ${mongoImage}`);
    sh(`docker run -d --name ${CH_CTR} -p 8223:8123 -e CLICKHOUSE_PASSWORD=${CH_PASSWORD} ${chImage}`);

    // readiness
    for (let i = 0; ; i++) {
      try {
        mc = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 2_000 });
        await mc.connect();
        break;
      } catch (e) {
        if (i > 30) throw e;
        await sleep(1_000);
      }
    }
    for (let i = 0; ; i++) {
      try {
        ch = createClient({ url: CH_URL, password: CH_PASSWORD });
        await ch.command({ query: 'SELECT 1' });
        break;
      } catch (e) {
        if (i > 30) throw e;
        await sleep(1_000);
      }
    }

    await mc.db(`${DB}_countly`).collection('apps').insertOne({ _id: APP } as never);
    await mc.db(`${DB}_countly`).collection('events').insertOne({ _id: APP, list: EVENTS } as never);
    await ch.command({ query: `CREATE DATABASE IF NOT EXISTS ${DB}` });
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

    for (const ev of EVENTS) {
      const coll = mc.db(DB).collection(collOf(ev));
      let docs: Record<string, unknown>[] = [];
      for (let j = 0; j < DOCS_EACH; j++) {
        const t = BASE + j * 60_000;
        docs.push({ _id: `${ev}_${j}`, uid: String(j % 100), did: `d${j}`, ts: t, cd: new Date(t), sg: { v: j }, c: 1 });
        if (docs.length === 5_000) { await coll.insertMany(docs as never[]); docs = []; }
      }
      if (docs.length) await coll.insertMany(docs as never[]);
      await coll.createIndex({ cd: 1, _id: 1 });
    }
    const nullDocs = Array.from({ length: NULL_CD_DOCS }, (_, j) => ({
      _id: `${EVENTS[0]}_nocd_${j}`, uid: `nu${j}`, did: 'd', ts: BASE + j * 120_000, sg: { v: j }, c: 1,
      ...(j % 2 === 1 ? { cd: null } : {}),
    }));
    await mc.db(DB).collection(collOf(EVENTS[0])).insertMany(nullDocs as never[]);

    for (const [k, v] of Object.entries(baseEnv)) process.env[k] = v;
    process.env.POD_ID = 'outage-verify';
    config = loadConfig();

    const mongoReader = new MongoReader({
      uri: MONGO_URI, database: DB, readPreference: 'primary', readConcern: 'local',
      retryReads: true, appName: 'outage-verify', cursorBatchSize: 500, maxTimeMs: 60_000,
    }, logger);
    ledger = new LedgerStore(MONGO_URI, DB, logger);
    dlqStore = new DlqStore(MONGO_URI, DB, logger);
    const staging = new StagingManager({
      url: CH_URL, database: DB, table: 'drill_events', username: 'default', password: CH_PASSWORD, queryTimeoutMs: 60_000,
    }, logger);
    hashResolver = new HashResolver({ uri: MONGO_URI, countlyDb: `${DB}_countly` }, logger);
    await mongoReader.connect();
    await ledger.connect();
    await dlqStore.connect();
    await staging.connect();
    await hashResolver.build();
    closers.push(() => mongoReader.close(), () => ledger.close(), () => dlqStore.close(), () => staging.close(), () => hashResolver.close());

    orchestrator = new ChunkOrchestrator({
      config, logger, mongoReader, ledger, dlq: dlqStore, staging,
      retryPolicy: new RetryPolicy({ maxRetries: 3, baseDelayMs: 200, maxDelayMs: 1_000 }), hashResolver,
    });
  }, 300_000);

  afterAll(async () => {
    for (const close of closers) await close().catch(() => {});
    await ch?.close().catch(() => {});
    await mc?.close().catch(() => {});
    sh(`docker rm -f ${MONGO_CTR} ${CH_CTR} 2>/dev/null || true`);
  });

  it('survives a ClickHouse restart and a MongoDB freeze mid-run, exactly', async () => {
    const w1 = spawnWorker('outage-pod-1');
    const w2 = spawnWorker('outage-pod-2');

    // let both pods get into the copy phase
    await sleep(4_000);

    console.log('[outage] restarting ClickHouse (hard crash: connections reset)');
    sh(`docker restart ${CH_CTR}`);

    await sleep(8_000); // CH back up; workers retrying/recovering

    console.log('[outage] pausing MongoDB for 6s (hang: connections freeze)');
    sh(`docker pause ${MONGO_CTR}`);
    await sleep(6_000);
    sh(`docker unpause ${MONGO_CTR}`);
    console.log('[outage] MongoDB unpaused');

    // Progress watchdog: workers that survive the outages should keep
    // completing chunks. If the done-count stalls for 90s, dump state and
    // SIGKILL them (operationally a wedged pod gets killed; but a wedge
    // here is also a liveness BUG we want surfaced, hence the loud dump).
    let wedged = false;
    let lastDone = -1;
    let stalledSince = Date.now();
    const bothExited = Promise.all([waitExit(w1), waitExit(w2)]);
    for (;;) {
      const raced = await Promise.race([bothExited.then(() => 'exited'), sleep(10_000).then(() => 'tick')]);
      if (raced === 'exited') break;
      const counts = await ledger.statusCounts(RUN).catch(() => ({} as Record<string, number>));
      const done = counts.done ?? 0;
      console.log('[outage] watch: counts', JSON.stringify(counts));
      if (done !== lastDone) { lastDone = done; stalledSince = Date.now(); }
      else if (Date.now() - stalledSince > 90_000) {
        wedged = true;
        console.log('[outage] WEDGED: no chunk completed in 90s — killing workers');
        console.log('[outage] pod-1 stderr tail:', stderrTail.get('outage-pod-1')?.slice(-1500) ?? '(live)');
        console.log('[outage] pod-2 stderr tail:', stderrTail.get('outage-pod-2')?.slice(-1500) ?? '(live)');
        w1.kill('SIGKILL'); w2.kill('SIGKILL');
        break;
      }
    }
    const [e1, e2] = await Promise.all([waitExit(w1), waitExit(w2)]);
    console.log('[outage] workers exited', e1, e2);
    console.log('[outage] pod-1 stderr:', (stderrTail.get('outage-pod-1') ?? '').slice(-1500));
    console.log('[outage] pod-2 stderr:', (stderrTail.get('outage-pod-2') ?? '').slice(-1500));

    // SELF-HEALING is the assertion, not just eventual exactness: the
    // transient-classified breaker pause must auto-resume once the backends
    // are healthy — workers finish on their own, the watchdog never fires.
    expect(wedged, 'workers wedged: breaker paused without auto-resume').toBe(false);
    expect(e1.code, `pod-1 stderr: ${(stderrTail.get('outage-pod-1') ?? '').slice(-800)}`).toBe(0);
    expect(e2.code, `pod-2 stderr: ${(stderrTail.get('outage-pod-2') ?? '').slice(-800)}`).toBe(0);

    // Drain + heal: the documented operator flow. Workers hit by the outage
    // may have exited nonzero or parked chunks as failed — respawn and
    // retry until the ledger is fully terminal with zero failures.
    for (let round = 0; round < 4; round++) {
      const counts = await ledger.statusCounts(RUN);
      const nonTerminal = (counts.pending ?? 0) + (counts.in_progress ?? 0) + (counts.written ?? 0) + (counts.attaching ?? 0);
      if (nonTerminal === 0 && (counts.failed ?? 0) === 0) break;
      if ((counts.failed ?? 0) > 0) {
        const { retried } = await orchestrator.retryFailed();
        console.log(`[outage] round ${round}: retried ${retried} failed chunks`);
      }
      const finisher = spawnWorker(`outage-final-${round}`);
      const guard = setTimeout(() => finisher.kill('SIGKILL'), 180_000);
      const exit = await waitExit(finisher);
      clearTimeout(guard);
      console.log(`[outage] finisher round ${round} exited`, exit, 'counts', await ledger.statusCounts(RUN));
    }

    // ── End state: exactness despite both backing services failing ──────
    const counts = await ledger.statusCounts(RUN);
    expect(counts.failed ?? 0, JSON.stringify(counts)).toBe(0);
    expect((counts.pending ?? 0) + (counts.in_progress ?? 0) + (counts.written ?? 0) + (counts.attaching ?? 0)).toBe(0);

    const sourceTotal = EVENTS.length * DOCS_EACH + NULL_CD_DOCS;
    const totals = await ch.query({
      query: `SELECT count() AS t, uniqExact(_id) AS u FROM ${DB}.drill_events`,
      format: 'JSONEachRow',
    });
    const [tot] = await totals.json<{ t: string; u: string }>();
    expect(Number(tot.t), `stderr p1: ${stderrTail.get('outage-pod-1')?.slice(-500)}`).toBe(sourceTotal);
    expect(Number(tot.u)).toBe(sourceTotal);

    const verify = await orchestrator.verifyMigration();
    expect(verify.ok, JSON.stringify(verify.mismatches).slice(0, 2000)).toBe(true);

    const prog = newRebuildProgress();
    await rebuildLedger({ config, logger, ledger, dlq: dlqStore, hashResolver, progress: prog, checkOnly: true });
    expect(prog.mismatchedWindows.length).toBe(0);

    const debris = await ch.query({
      query: `SELECT name FROM system.tables WHERE database = '${DB}' AND name LIKE '%__stg_%'`,
      format: 'JSONEachRow',
    });
    expect((await debris.json<{ name: string }>()).map((r) => r.name)).toEqual([]);
    expect((await dlqStore.listPendingAfter(RUN, null, 10)).length).toBe(0);
  }, 600_000);
});
