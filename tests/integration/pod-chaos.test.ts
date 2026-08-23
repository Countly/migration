/**
 * Pod chaos: real worker PROCESSES are spawned and SIGKILLed at random
 * moments across every stage — mapping, staging copy, verify, the
 * post-ATTACH/pre-record torn-commit window (forced deterministically in
 * phase A), lease reclamation, sweep — until a final undisturbed worker
 * completes the run. The assertions that matter are END-STATE ONLY:
 *
 *  - every source doc lands exactly once (count + uniqExact, per collection)
 *  - the ledger is fully terminal with zero failed chunks
 *  - verify, the source-recount audit, and the content sample audit all pass
 *  - no staging debris, no unresolved DLQ entries
 *
 * Kill timing is seeded but real interleavings differ run to run — by
 * design: the invariants must hold under EVERY interleaving. Crash
 * quarantine (a chunk SIGKILLed > MAX_CHUNK_ATTEMPTS times) is legal and
 * healed through the documented operator flow (retryFailed), bounded here.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { spawn, type ChildProcess } from 'node:child_process';
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

const MONGO_URI = 'mongodb://localhost:27017/?directConnection=true';
const CH_URL = process.env.TEST_CLICKHOUSE_URL ?? 'http://localhost:8123';
const CH_PASSWORD = process.env.TEST_CLICKHOUSE_PASSWORD ?? '';
const DB = 'test_mig_chaos';
const RUN = 'chaos-1';
const logger = pino({ level: 'silent' });

const APP = 'app_chaos';
const EVENTS = ['orders', 'clicks', 'installs'];
const collOf = (ev: string): string => `drill_events${createHash('sha1').update(ev + APP).digest('hex')}`;
const DOCS_EACH = 12_000;
const NULL_CD_DOCS = 40; // in collection 0 only — exercises the sweep under chaos
const BASE = Date.UTC(2026, 2, 1);

/** Deterministic RNG so a failing seed can be replayed. */
function mulberry32(seed: number): () => number {
  return () => {
    seed |= 0; seed = (seed + 0x6D2B79F5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

describe('pod chaos: random SIGKILL across all stages, exact end state', () => {
  let ch: ClickHouseClient;
  let mc: MongoClient;
  let ledger: LedgerStore;
  let dlqStore: DlqStore;
  let orchestrator: ChunkOrchestrator; // verify/audit/heal only — never runs chunks
  let hashResolver: HashResolver;
  let config: Config;
  const closers: Array<() => Promise<void>> = [];
  const rng = mulberry32(0x5eed);

  const WORKER = path.resolve(process.cwd(), 'tests/chaos/worker.ts');
  const baseEnv: Record<string, string> = {
    SERVICE_NAME: 'chaos',
    MONGO_URI, MONGO_DB: DB, MONGO_COUNTLY_DB: `${DB}_countly`, MANIFEST_DB: DB,
    CLICKHOUSE_URL: CH_URL, CLICKHOUSE_PASSWORD: CH_PASSWORD, CLICKHOUSE_DB: DB,
    LEDGER_RUN_ID: RUN,
    LEDGER_CHUNK_DOCS_TARGET: '400',
    MONGO_PAGE_SIZE: '200',
    LEDGER_LEASE_SEC: '2', // dead pods' leases recover in seconds
    LEDGER_MONITOR_INTERVAL_MS: '0',
    BACKPRESSURE_ENABLED: 'false',
    MULTI_POD_ENABLED: 'true',
    LOG_LEVEL: 'fatal',
  };

  const stderrTail = new Map<string, string>();
  const spawnWorker = (podId: string, extraEnv: Record<string, string> = {}): ChildProcess => {
    const child = spawn(process.execPath, ['--experimental-strip-types', WORKER], {
      env: { ...process.env, ...baseEnv, POD_ID: podId, ...extraEnv },
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    let tail = '';
    child.stderr!.on('data', (d: Buffer) => { tail = (tail + d.toString()).slice(-4000); });
    child.on('exit', () => stderrTail.set(podId, tail));
    return child;
  };
  const waitExit = (p: ChildProcess): Promise<{ code: number | null; signal: string | null }> =>
    new Promise((resolve) => {
      // the process may have exited before we subscribe (phase B awaits
      // workers sequentially) — 'exit' would then never fire for us
      if (p.exitCode !== null || p.signalCode !== null) {
        resolve({ code: p.exitCode, signal: p.signalCode });
        return;
      }
      p.on('exit', (code, signal) => resolve({ code, signal }));
    });
  const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

  const nonTerminal = async (): Promise<number> => {
    const counts = await ledger.statusCounts(RUN);
    return (counts.pending ?? 0) + (counts.in_progress ?? 0) + (counts.written ?? 0) + (counts.attaching ?? 0);
  };

  beforeAll(async () => {
    mc = new MongoClient(MONGO_URI);
    await mc.connect();
    await mc.db(DB).dropDatabase();
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

    for (const ev of EVENTS) {
      const coll = mc.db(DB).collection(collOf(ev));
      let docs: Record<string, unknown>[] = [];
      for (let j = 0; j < DOCS_EACH; j++) {
        const t = BASE + j * 60_000;
        docs.push({ _id: `${ev}_${j}`, uid: String(j % 100), did: `d${j}`, ts: t, cd: new Date(t), sg: { v: j }, c: 1 });
        if (docs.length === 4_000) { await coll.insertMany(docs as never[]); docs = []; }
      }
      if (docs.length) await coll.insertMany(docs as never[]);
      await coll.createIndex({ cd: 1, _id: 1 });
    }
    // Null-cd docs (both shapes: field absent and cd: null), ts inside range
    const nullDocs: Record<string, unknown>[] = [];
    for (let j = 0; j < NULL_CD_DOCS; j++) {
      const base = { _id: `${EVENTS[0]}_nocd_${j}`, uid: `nu${j}`, did: 'd', ts: BASE + j * 120_000, sg: { v: j }, c: 1 };
      nullDocs.push(j % 2 === 0 ? base : { ...base, cd: null });
    }
    await mc.db(DB).collection(collOf(EVENTS[0])).insertMany(nullDocs as never[]);

    // in-test components for verify/audit/heal (this orchestrator never
    // executes chunks — spawned workers do all the migrating)
    for (const [k, v] of Object.entries(baseEnv)) process.env[k] = v;
    process.env.POD_ID = 'chaos-verify';
    config = loadConfig();

    const mongoReader = new MongoReader({
      uri: MONGO_URI, database: DB, readPreference: 'primary', readConcern: 'local',
      retryReads: true, appName: 'chaos-verify', cursorBatchSize: 500, maxTimeMs: 60_000,
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
      retryPolicy: new RetryPolicy({ maxRetries: 2, baseDelayMs: 50, maxDelayMs: 200 }), hashResolver,
    });
  }, 180_000);

  afterAll(async () => {
    for (const close of closers) await close().catch(() => {});
    await ch.command({ query: `DROP DATABASE IF EXISTS ${DB}` }).catch(() => {});
    await ch.close();
    await mc.db(DB).dropDatabase().catch(() => {});
    await mc.db(`${DB}_countly`).dropDatabase().catch(() => {});
    await mc.close();
  });

  it('random pod kills at every stage still end in an exact, fully audited state', async () => {
    // ── Phase A: forced torn commit — SIGKILL between ATTACH and record ──
    console.log('[chaos] phase A: spawning torn worker');
    const torn = spawnWorker('pod-torn', { CHAOS_CRASH_AFTER_ATTACH: '1' });
    const tornExit = await waitExit(torn);
    console.log('[chaos] phase A: torn worker exited', tornExit);
    expect(tornExit.signal, `stderr: ${stderrTail.get('pod-torn')}`).toBe('SIGKILL');
    // rows are live, the ledger doesn't know: at least one chunk is torn
    const tornChunks = await mc.db(DB).collection('mig_ranges')
      .countDocuments({ run_id: RUN, status: 'attaching' } as never);
    expect(tornChunks).toBeGreaterThanOrEqual(1);

    // ── Phase B: random kill/respawn cycles, 2 pods each ─────────────────
    let cycles = 0;
    for (; cycles < 8; cycles++) {
      const nt = await nonTerminal();
      console.log(`[chaos] phase B cycle ${cycles}: nonTerminal=${nt}`);
      if (nt === 0) break;
      const workers: Array<{ child: ChildProcess; timer: NodeJS.Timeout }> = [];
      for (let w = 0; w < 2; w++) {
        const child = spawnWorker(`pod-c${cycles}-${w}`);
        const delay = 700 + Math.floor(rng() * 2200);
        const timer = setTimeout(() => child.kill('SIGKILL'), delay);
        workers.push({ child, timer });
      }
      for (const { child, timer } of workers) {
        await waitExit(child);
        clearTimeout(timer);
      }
    }

    // ── Phase C: undisturbed drain + quarantine healing ──────────────────
    let healed = 0;
    for (let round = 0; round < 3; round++) {
      const finisher = spawnWorker(`pod-final-${round}`);
      const guard = setTimeout(() => finisher.kill('SIGKILL'), 180_000);
      const exit = await waitExit(finisher);
      clearTimeout(guard);
      console.log(`[chaos] phase C round ${round}: exit`, exit, 'counts', await ledger.statusCounts(RUN));
      expect(exit.code, `finisher round ${round} exited abnormally; stderr: ${stderrTail.get(`pod-final-${round}`)}`).toBe(0);
      const counts = await ledger.statusCounts(RUN);
      if ((counts.failed ?? 0) === 0 && (await nonTerminal()) === 0) break;
      // crash quarantine (same chunk SIGKILLed > max attempts) — documented
      // operator flow: retry failed chunks, run again
      const { retried } = await orchestrator.retryFailed();
      healed += retried;
      await sleep(100);
    }

    // ── End state: the only assertions that matter ───────────────────────
    const counts = await ledger.statusCounts(RUN);
    expect(counts.failed ?? 0).toBe(0);
    expect(await nonTerminal()).toBe(0);
    expect(counts.done ?? 0).toBeGreaterThan(0);

    const sourceTotal = EVENTS.length * DOCS_EACH + NULL_CD_DOCS;
    const totals = await ch.query({
      query: `SELECT count() AS t, uniqExact(_id) AS u FROM ${DB}.drill_events`,
      format: 'JSONEachRow',
    });
    const [tot] = await totals.json<{ t: string; u: string }>();

    if (Number(tot.t) !== sourceTotal) {
      // Diagnose before failing: which rows duplicated, and which chunk owns them
      const dupq = await ch.query({
        query: `SELECT _id, count() AS c, min(toUnixTimestamp64Milli(cd)) AS cdlo, max(toUnixTimestamp64Milli(cd)) AS cdhi
                FROM ${DB}.drill_events GROUP BY _id HAVING c > 1 ORDER BY _id LIMIT 6`,
        format: 'JSONEachRow',
      });
      const dups = await dupq.json<{ _id: string; c: string; cdlo: string; cdhi: string }>();
      const dtot = await ch.query({
        query: `SELECT count() AS n FROM (SELECT _id FROM ${DB}.drill_events GROUP BY _id HAVING count() > 1)`,
        format: 'JSONEachRow',
      });
      console.log('[chaos] DUP total ids:', (await dtot.json<{ n: string }>())[0].n, 'sample:', JSON.stringify(dups));
      if (dups.length > 0) {
        const cd = Number(dups[0].cdlo);
        const covering = await mc.db(DB).collection('mig_ranges')
          .find({ run_id: RUN, lower_cd: { $lte: cd }, upper_cd: { $gt: cd } } as never).toArray();
        console.log('[chaos] covering chunks for first dup:', JSON.stringify(covering, null, 1).slice(0, 3000));
      }
    }
    expect(Number(tot.t), `total rows (cycles=${cycles}, healedChunks=${healed})`).toBe(sourceTotal);
    expect(Number(tot.u)).toBe(sourceTotal); // every doc exactly once

    for (const ev of EVENTS) {
      const per = await ch.query({
        query: `SELECT count() AS c FROM ${DB}.drill_events WHERE startsWith(_id, '${ev}_')`,
        format: 'JSONEachRow',
      });
      const expected = DOCS_EACH + (ev === EVENTS[0] ? NULL_CD_DOCS : 0);
      expect(Number((await per.json<{ c: string }>())[0].c), `collection ${ev}`).toBe(expected);
    }

    // full verification + both audits pass on the chaos-built table
    const verify = await orchestrator.verifyMigration();
    expect(verify.ok, JSON.stringify(verify.mismatches).slice(0, 2000)).toBe(true);

    const prog = newRebuildProgress();
    await rebuildLedger({ config, logger, ledger, dlq: dlqStore, hashResolver, progress: prog, checkOnly: true });
    expect(prog.mismatchedWindows.length, JSON.stringify(prog.mismatchedWindows).slice(0, 2000)).toBe(0);

    const audit = await orchestrator.contentAudit(300);
    expect(audit.missing).toBe(0);
    expect(audit.different).toBe(0);

    // no staging debris, no unresolved DLQ
    const debris = await ch.query({
      query: `SELECT name FROM system.tables WHERE database = '${DB}' AND name LIKE '%__stg_%'`,
      format: 'JSONEachRow',
    });
    expect((await debris.json<{ name: string }>()).map((r) => r.name)).toEqual([]);
    expect((await dlqStore.listPendingAfter(RUN, null, 10)).length).toBe(0);
  }, 420_000);
});
