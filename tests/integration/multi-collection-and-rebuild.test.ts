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
const CH_URL = process.env.TEST_CLICKHOUSE_URL ?? 'http://localhost:8123';
const CH_PASSWORD = process.env.TEST_CLICKHOUSE_PASSWORD ?? '';
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
  let staging: StagingManager;
  let dlqStore: DlqStore;
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
    process.env.CLICKHOUSE_PASSWORD = CH_PASSWORD;
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
    dlqStore = new DlqStore(MONGO_URI, DB, logger);
    const dlq = dlqStore;
    staging = new StagingManager({
      url: CH_URL, database: DB, table: 'drill_events', username: 'default', password: CH_PASSWORD, queryTimeoutMs: 60_000,
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
    await rebuildLedger({ config, logger, ledger, dlq: dlqStore, hashResolver, progress });

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
    await rebuildLedger({ config, logger, ledger, dlq: dlqStore, hashResolver, progress });

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

  it('verify attributes duplicates by cd boundary: live artifact / cross-cutover retry / migration defect', async () => {
    const nowMs = Date.now();
    // A duplicate always shares its ts with the original (a retry RESENDS the
    // same event) — that shared ts month is what makes per-partition
    // duplicate scanning exact.
    const mk = (id: string, tsMs: number, cdMs: number) =>
      `('${APP}', '[CLY]_custom', '${EV1}', 'u_dup', 'd_dup', '${id}', ${tsMs}, fromUnixTimestamp64Milli(${cdMs}))`;
    // 1) live at-least-once redelivery: same _id twice, NO migrated copy
    await ch.command({
      query: `INSERT INTO ${DB}.drill_events (a, e, n, uid, did, _id, ts, cd)
              VALUES ${mk('redelivered_1', nowMs, nowMs)}, ${mk('redelivered_1', nowMs, nowMs + 500)}`,
    });
    // 2) cross-cutover SDK retry: same event ts, post-cutover cd
    await ch.command({
      query: `INSERT INTO ${DB}.drill_events (a, e, n, uid, did, _id, ts, cd)
              VALUES ${mk('p_5', BASE + 5 * 60_000, nowMs)}`,
    });

    let verify = await orchestrator.verifyMigration();
    expect(verify.table.duplicates).toBe(2);
    expect(verify.migrationDuplicates).toBe(0);
    expect(verify.ok).toBe(true); // neither is a migration defect
    const sample = (verify.duplicateSample as Array<{ _id: string; migratedCopies: number; verdict: string }>);
    const byId = new Map(sample.map((d) => [d._id, d]));
    expect(byId.get('redelivered_1')!.migratedCopies).toBe(0);
    expect(byId.get('redelivered_1')!.verdict).toContain('live at-least-once artifact');
    expect(byId.get('p_5')!.migratedCopies).toBe(1);
    expect(byId.get('p_5')!.verdict).toContain('cross-cutover retry');

    // 3) a REAL migration defect looks like: two copies BELOW the boundary
    // (both written by migration — same doc migrated twice)
    await ch.command({
      query: `INSERT INTO ${DB}.drill_events (a, e, n, uid, did, _id, ts, cd)
              VALUES ('${APP}', '[CLY]_custom', '${EV1}', 'u_dup', 'd_dup', 'p_6', ${BASE + 6 * 60_000}, fromUnixTimestamp64Milli(${BASE + 6 * 60_000 + 1}))`,
    });
    verify = await orchestrator.verifyMigration();
    expect(verify.migrationDuplicates).toBe(1);
    expect(verify.ok).toBe(false);

    await ch.command({ query: `DELETE FROM ${DB}.drill_events WHERE _id = 'redelivered_1' OR (_id = 'p_5' AND cd >= fromUnixTimestamp64Milli(${nowMs})) OR (_id = 'p_6' AND cd = fromUnixTimestamp64Milli(${BASE + 6 * 60_000 + 1}))` });
  }, 60_000);

  it('probeSourceFrozen detects writes landing between probes', async () => {
    const db = mc.db(DB);
    // frozen: nothing changes during the wait
    const still = await orchestrator.probeSourceFrozen(db as never, [COLL1], 10);
    expect(still.frozen).toBe(true);

    // not frozen: a write lands during the wait window
    const busy = await orchestrator.probeSourceFrozen(db as never, [COLL1], 10, async () => {
      await db.collection(COLL1).insertOne({ _id: 'late_arrival', uid: 'u', ts: Date.now(), cd: new Date() } as never);
    });
    expect(busy.frozen).toBe(false);
    expect(busy.grew).toContain(COLL1);
    await db.collection(COLL1).deleteOne({ _id: 'late_arrival' } as never);
  }, 30_000);

  it('DLQ pending listing paginates stably', async () => {
    const { DlqStore } = await import('../../src/state/dlq-store.ts');
    const store = new DlqStore(MONGO_URI, DB, logger);
    await store.connect();
    await store.add(Array.from({ length: 12 }, (_, i) => ({
      run_id: RUN, source_id: `pg_${String(i).padStart(2, '0')}`, collection: COLL1,
      reason: 'skipped' as const, error: 'test', raw_doc: { i }, transform_version: 'v-test',
    })));
    const page1 = await store.listPending(RUN, 5, 0);
    const page2 = await store.listPending(RUN, 5, 5);
    expect(page1.length).toBe(5);
    expect(page2.length).toBe(5);
    expect(new Set([...page1, ...page2].map((d) => d._id)).size).toBe(10); // no overlap
    await store.waive(RUN);
    await store.close();
  }, 30_000);

  it('replay skips DLQ entries whose rows are already live (redo-then-replay cannot duplicate)', async () => {
    const { DlqStore } = await import('../../src/state/dlq-store.ts');
    const store = new DlqStore(MONGO_URI, DB, logger);
    await store.connect();
    // p_100 is already migrated; its DLQ entry simulates a doc that failed
    // once but was later migrated by a chunk redo with a fixed transform.
    const srcTs = BASE + 100 * 60_000;
    await store.add([{
      run_id: RUN, source_id: 'p_100', collection: COLL1, reason: 'insert_rejected',
      error: 'old transform bug', transform_version: 'v-old',
      raw_doc: { _id: 'p_100', uid: '100', did: 'd100', ts: srcTs, cd: new Date(srcTs), sg: { v: 100 }, c: 1 },
    }]);

    const res = await orchestrator.replayDlq();
    expect(res.alreadyLive).toBe(1);
    expect(res.replayed).toBe(0);
    const count = await ch.query({
      query: `SELECT count() AS c FROM ${DB}.drill_events WHERE _id = 'p_100'`, format: 'JSONEachRow',
    });
    expect(Number((await count.json<{ c: string }>())[0].c)).toBe(1); // still exactly one copy
    await store.close();
  }, 60_000);

  it('DLQ mass guard pauses the engine when pending crosses the threshold', async () => {
    const { DlqStore } = await import('../../src/state/dlq-store.ts');
    const store = new DlqStore(MONGO_URI, DB, logger);
    await store.connect();
    await store.add(Array.from({ length: 6 }, (_, i) => ({
      run_id: RUN, source_id: `mass_${i}`, collection: COLL1, reason: 'skipped' as const,
      error: 'systematic', transform_version: 'v-test', raw_doc: { i },
    })));

    const prev = config.ledger.dlqPauseThreshold;
    config.ledger.dlqPauseThreshold = 5;
    try {
      expect(await orchestrator.checkDlqPressure(logger, true)).toBe(true);  // tripped + paused
      expect(await orchestrator.checkDlqPressure(logger, true)).toBe(false); // already paused — idempotent
    } finally {
      config.ledger.dlqPauseThreshold = prev;
      orchestrator.resume();
      await store.waive(RUN);
      await store.close();
    }
  }, 30_000);

  it('audits close the count-blind spots: source recount and sampled content comparison', async () => {
    const { rebuildLedger: rebuild, newRebuildProgress: newProgress } = await import('../../src/runtime/ledger-rebuild.ts');

    // Clean state: both audits pass and the ledger is untouched by checkOnly
    const before = await ledger.listAll(RUN);
    let prog = newProgress();
    await rebuild({ config, logger, ledger, dlq: dlqStore, hashResolver, progress: prog, checkOnly: true });
    expect(prog.mismatchedWindows.length).toBe(0);
    const after = await ledger.listAll(RUN);
    expect(after.map((c) => c._id + c.status).join()).toBe(before.map((c) => c._id + c.status).join());

    let audit = await orchestrator.contentAudit(100);
    expect(audit.sampled).toBeGreaterThan(50);
    expect(audit.missing).toBe(0);
    expect(audit.different).toBe(0);

    // Corrupt one live row the sampler deterministically hits: p_80 gets a
    // wrong uid (same _id and cd — invisible to every count and pair check).
    const srcDoc = await mc.db(DB).collection(COLL1).findOne({ _id: 'p_80' } as never) as Record<string, unknown>;
    const cdMs = (srcDoc.cd as Date).getTime();
    await staging.deleteLiveByPairs([{ id: 'p_80', cdMs }]);
    const { transformDocument } = await import('../../src/transform/normalize.ts');
    const defaults = hashResolver.resolveCollectionName(COLL1, config.source.collectionPrefix) ?? undefined;
    const { row } = transformDocument(srcDoc as never, defaults);
    await staging.insertIntoLive([{ ...row!, uid: 'EVIL' }], 'audit-corrupt');

    audit = await orchestrator.contentAudit(600); // sample densely → must hit p_80
    expect(audit.different).toBeGreaterThanOrEqual(1);
    const hit = audit.mismatches.find((m) => m._id === 'p_80');
    expect(hit?.fields).toContain('uid');
    // counts still agree everywhere — this class is invisible to the source audit
    prog = newProgress();
    await rebuild({ config, logger, ledger, dlq: dlqStore, hashResolver, progress: prog, checkOnly: true });
    expect(prog.mismatchedWindows.length).toBe(0);

    // Now a LOSS: delete the row entirely — source audit flags the window
    await staging.deleteLiveByPairs([{ id: 'p_80', cdMs }]);
    prog = newProgress();
    await rebuild({ config, logger, ledger, dlq: dlqStore, hashResolver, progress: prog, checkOnly: true });
    expect(prog.mismatchedWindows.length).toBe(1);
    expect(prog.mismatchedWindows[0].source - prog.mismatchedWindows[0].live).toBe(1);

    // Restore the true row; both audits green again
    await staging.insertIntoLive([row!], 'audit-restore');
    prog = newProgress();
    await rebuild({ config, logger, ledger, dlq: dlqStore, hashResolver, progress: prog, checkOnly: true });
    expect(prog.mismatchedWindows.length).toBe(0);
    audit = await orchestrator.contentAudit(600);
    expect(audit.missing).toBe(0);
    expect(audit.different).toBe(0);
  }, 120_000);

  it('a waived DLQ doc explains its window shortfall — audit does not cry wolf', async () => {
    const { rebuildLedger: rebuild, newRebuildProgress: newProgress } = await import('../../src/runtime/ledger-rebuild.ts');
    const { transformDocument } = await import('../../src/transform/normalize.ts');

    // Simulate a doc that never migrated because it was DLQ'd and waived:
    // remove its live row and record it as waived with its cd attributed.
    const srcDoc = await mc.db(DB).collection(COLL1).findOne({ _id: 'p_40' } as never) as Record<string, unknown>;
    const cdMs = (srcDoc.cd as Date).getTime();
    await staging.deleteLiveByPairs([{ id: 'p_40', cdMs }]);
    await dlqStore.add([{
      run_id: RUN, source_id: 'p_40', collection: COLL1, chunk_id: 'test', reason: 'insert_rejected',
      error: 'unfixable by decision', transform_version: 'v-test', raw_doc: srcDoc,
    } as never]);
    await dlqStore.waive(RUN, [`${RUN}:p_40`]);

    // Source audit: source > live by exactly the waived doc → NOT a mismatch
    const prog = newProgress();
    await rebuild({ config, logger, ledger, dlq: dlqStore, hashResolver, progress: prog, checkOnly: true });
    expect(prog.mismatchedWindows.length).toBe(0);

    // restore: un-waive bookkeeping + reinsert the true row
    const defaults = hashResolver.resolveCollectionName(COLL1, config.source.collectionPrefix) ?? undefined;
    const { row } = transformDocument(srcDoc as never, defaults);
    await staging.insertIntoLive([row!], 'audit-waive-restore');
    await mc.db(DB).collection('mig_dlq_docs').deleteOne({ _id: `${RUN}:p_40` } as never);
    const clean = newProgress();
    await rebuild({ config, logger, ledger, dlq: dlqStore, hashResolver, progress: clean, checkOnly: true });
    expect(clean.mismatchedWindows.length).toBe(0);
  }, 60_000);

  it('dry-run replay writes to the Null table, never live (field bug)', async () => {
    // A DLQ entry under the DRY run id with a perfectly good raw doc
    const goodDoc = { _id: 'dryreplay_1', uid: 'u', did: 'd', ts: BASE + 1_000, cd: new Date(BASE + 1_000), sg: { v: 1 }, c: 1 };
    await dlqStore.add([{
      run_id: `${RUN}-dry`, source_id: 'dryreplay_1', collection: COLL1, chunk_id: 'test',
      reason: 'insert_rejected', error: 'was broken under v1', transform_version: 'v1', raw_doc: goodDoc,
    } as never]);

    const dryConfig = { ...config, ledger: { ...config.ledger, dryRun: true } };
    const dryOrch = new ChunkOrchestrator({
      config: dryConfig as never, logger,
      mongoReader: new MongoReader({ uri: MONGO_URI, database: DB, readPreference: 'primary', readConcern: 'local', retryReads: true, appName: 'dry-replay', cursorBatchSize: 500, maxTimeMs: 60_000 }, logger),
      ledger, dlq: dlqStore, staging, retryPolicy: new RetryPolicy({ maxRetries: 2, baseDelayMs: 50, maxDelayMs: 200 }), hashResolver,
    });
    await (dryOrch as unknown as { d: { mongoReader: MongoReader } }).d.mongoReader.connect();

    const res = await dryOrch.replayDlq();
    expect(res.replayed).toBe(1); // rehearsal succeeded (Null table accepted it)

    // THE assertion: nothing reached the live table
    const live = await ch.query({
      query: `SELECT count() AS c FROM ${DB}.drill_events WHERE _id = 'dryreplay_1'`, format: 'JSONEachRow',
    });
    expect(Number((await live.json<{ c: string }>())[0].c)).toBe(0);

    const dryPending = await dlqStore.listPending(`${RUN}-dry`);
    expect(dryPending.length).toBe(0); // resolved in the dry DLQ

    await (dryOrch as unknown as { d: { mongoReader: MongoReader } }).d.mongoReader.close();
    await mc.db(DB).collection('mig_dlq_docs').deleteMany({ run_id: `${RUN}-dry` } as never);
  }, 60_000);

  it('attach-recovery pair check ignores live copies of the same _id (cross-cutover retry)', async () => {
    // The mixing vector: a crash during attach + an SDK retry that landed the
    // same _id in live (same ts → same month partition). Matching (_id, cd)
    // pairs is exact: the retry copy's cd differs by construction.
    const tsMs = BASE + 42 * 60_000;
    const stagingTable = 'drill_events__stg_precision_test';
    await staging.createStaging(stagingTable);
    await staging.insertBatch(stagingTable, [{
      _id: 'retry_victim', a: APP, e: '[CLY]_custom', n: EV1, uid: 'u', did: 'd',
      ts: new Date(tsMs).toISOString().replace('T', ' ').replace('Z', ''),
      c: 1, s: 0, dur: 0,
      cd: new Date(tsMs).toISOString().replace('T', ' ').replace('Z', ''),
    } as never], 'precision-test', 'precision-q1');
    const [partitionId] = await staging.listPartitions(stagingTable);

    // live retry copy: same _id, same ts (same partition), NOT migrated
    await ch.command({
      query: `INSERT INTO ${DB}.drill_events (a, e, n, uid, did, _id, ts, cd)
              VALUES ('${APP}', '[CLY]_custom', '${EV1}', 'u', 'd', 'retry_victim', ${tsMs}, fromUnixTimestamp64Milli(${Date.now()}))`,
    });
    expect(await staging.countLiveMatchingStaged(stagingTable, partitionId)).toBe(0); // id-only matching: 1 → skipped attach → data loss

    // once the migrated copy IS live, recovery correctly reports it
    await staging.insertIntoLive([{
      _id: 'retry_victim', a: APP, e: '[CLY]_custom', n: EV1, uid: 'u', did: 'd',
      ts: new Date(tsMs).toISOString().replace('T', ' ').replace('Z', ''),
      c: 1, s: 0, dur: 0,
      cd: new Date(tsMs).toISOString().replace('T', ' ').replace('Z', ''),
    } as never], 'precision-test-live');
    expect(await staging.countLiveMatchingStaged(stagingTable, partitionId)).toBe(1);

    await staging.dropStaging(stagingTable);
    await ch.command({ query: `DELETE FROM ${DB}.drill_events WHERE _id = 'retry_victim'` });
  }, 60_000);

  it('fencing: a stalled ex-owner cannot move or heartbeat a reclaimed chunk', async () => {
    const FR = 'fence-run';
    await ledger.initChunks(FR, COLL1, [{ lowerCd: 0, upperCd: 1000 }], 'v2', null);

    // stale pod claims (generation 1) and stalls
    const stale = await ledger.claimById(`${FR}:${COLL1}:0`, 'stale-pod', 1);
    expect(stale?.attempts).toBe(1);
    const staleFence = { podId: 'stale-pod', attempts: stale!.attempts };

    // lease expires; recovery resets; a new pod claims (generation 2)
    await ledger.transition(`${FR}:${COLL1}:0`, 'in_progress', 'pending', { pod_id: null, staging_table: null });
    const fresh = await ledger.claimById(`${FR}:${COLL1}:0`, 'new-pod', 600);
    expect(fresh?.attempts).toBe(2);

    // the stalled worker wakes up: every fenced mutation is rejected
    expect(await ledger.heartbeat(`${FR}:${COLL1}:0`, 'stale-pod', 600, staleFence.attempts)).toBe(false);
    expect(await ledger.transition(`${FR}:${COLL1}:0`, 'in_progress', 'written', {}, staleFence)).toBeNull();

    // the rightful owner's fence works
    const ok = await ledger.transition(`${FR}:${COLL1}:0`, 'in_progress', 'written', {}, { podId: 'new-pod', attempts: 2 });
    expect(ok?.status).toBe('written');
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: FR } as never);
  }, 30_000);

  it('appendChunks: racing pods with different grids — exactly one wins, windows never overlap', async () => {
    const AR = 'append-race-run';
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: AR } as never); // idempotent re-runs
    await ledger.initChunks(AR, COLL1, [{ lowerCd: 0, upperCd: 1000 }], 'v2', null);

    // Pods observed different source maxima and race their appends
    const gridA = [{ lowerCd: 1000, upperCd: 2000 }, { lowerCd: 2000, upperCd: 3000 }];
    const gridB = [{ lowerCd: 1000, upperCd: 1500 }, { lowerCd: 1500, upperCd: 2500 }, { lowerCd: 2500, upperCd: 3500 }];
    const [a, b] = await Promise.all([
      ledger.appendChunks(AR, COLL1, gridA, 1, 'v2', null),
      ledger.appendChunks(AR, COLL1, gridB, 1, 'v2', null),
    ]);
    expect([a, b].filter((x) => x > 0).length, `a=${a} b=${b}`).toBe(1); // exactly one reservation won

    const chunks = await mc.db(DB).collection('mig_ranges')
      .find({ run_id: AR } as never).sort({ idx: 1 }).toArray();
    // one coherent grid: contiguous, non-overlapping
    for (let i = 1; i < chunks.length; i++) {
      expect(chunks[i].lower_cd).toBe(chunks[i - 1].upper_cd);
    }
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: AR } as never);
  }, 30_000);

  it('initChunks: racing pods with DIFFERENT initial grids — exactly one coherent grid stands', async () => {
    const IR = 'init-race-run';
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: IR } as never);
    // Pods that probed a live source at different instants computed different
    // maxima: different cut points, even different chunk counts. Pre-fix,
    // both unordered insertMany calls swallowed duplicate keys and the two
    // grids INTERLEAVED into overlapping windows.
    const gridA = [{ lowerCd: 0, upperCd: 400 }, { lowerCd: 400, upperCd: 700 }, { lowerCd: 700, upperCd: 1000 }];
    const gridB = [{ lowerCd: 0, upperCd: 250 }, { lowerCd: 250, upperCd: 500 }, { lowerCd: 500, upperCd: 750 }, { lowerCd: 750, upperCd: 1100 }];
    const [a, b] = await Promise.all([
      ledger.initChunks(IR, COLL1, gridA, 'v2', null),
      ledger.initChunks(IR, COLL1, gridB, 'v2', null),
    ]);
    expect([a, b].filter((x) => x > 0).length, `a=${a} b=${b}`).toBe(1); // exactly one reservation won
    const winner = a > 0 ? gridA : gridB;
    const chunks = await mc.db(DB).collection('mig_ranges')
      .find({ run_id: IR } as never).sort({ idx: 1 }).toArray();
    expect(chunks.length).toBe(winner.length); // no chunks from the loser's grid
    for (let i = 0; i < chunks.length; i++) {
      expect(chunks[i].lower_cd).toBe(winner[i].lowerCd);
      expect(chunks[i].upper_cd).toBe(winner[i].upperCd);
    }
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: IR } as never);
  }, 30_000);

  it('initChunks: identical grids (frozen source) collapse to a single grid', async () => {
    const IF = 'init-frozen-run';
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: IF } as never);
    const grid = [{ lowerCd: 0, upperCd: 500 }, { lowerCd: 500, upperCd: 1000 }];
    const results = await Promise.all([
      ledger.initChunks(IF, COLL1, grid, 'v2', null),
      ledger.initChunks(IF, COLL1, grid, 'v2', null),
      ledger.initChunks(IF, COLL1, grid, 'v2', null),
    ]);
    expect(results.filter((x) => x > 0).length).toBe(1);
    expect(await mc.db(DB).collection('mig_ranges').countDocuments({ run_id: IF } as never)).toBe(2);
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: IF } as never);
  }, 30_000);

  it('sweep sentinel: two pods race the claim — exactly one wins', async () => {
    const SR = 'sentinel-race-run';
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: SR } as never);
    await ledger.initChunks(SR, COLL1, [{ lowerCd: -1, upperCd: 0 }], 'v2', null);
    const [p1, p2] = await Promise.all([
      ledger.claimById(`${SR}:${COLL1}:0`, 'pod-1', 600),
      ledger.claimById(`${SR}:${COLL1}:0`, 'pod-2', 600),
    ]);
    expect([p1, p2].filter(Boolean).length).toBe(1);
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: SR } as never);
  }, 30_000);

  it('retryFailed double-fire: concurrent operators retry each failed chunk exactly once', async () => {
    // Two dashboards clicking "Retry failed chunks" at once: the from-state
    // guard on failed->pending means exactly one transition wins; the double
    // purge is idempotent (the injected window is empty, year 2100).
    const counts = await ledger.statusCounts(RUN);
    expect(counts.failed ?? 0).toBe(0); // clean baseline before injection
    const fakeId = `${RUN}:fake_retry_coll:0`;
    await mc.db(DB).collection('mig_ranges').insertOne({
      _id: fakeId, run_id: RUN, collection: 'fake_retry_coll',
      scope_a: APP, scope_e: '[CLY]_custom', scope_n: 'zzz_no_such_event',
      idx: 0, lower_cd: 4102444800000, upper_cd: 4102444900000,
      status: 'failed', pod_id: null, lease_until: null, staging_table: null,
      docs_read: 0, docs_skipped: 0, rows_expected: 0, partitions: [], attached: [],
      attach_method: null, attempts: 3, last_error: 'injected', transform_version: 'v2', updated_at: new Date(),
    } as never);
    const [r1, r2] = await Promise.all([orchestrator.retryFailed(), orchestrator.retryFailed()]);
    expect(r1.retried + r2.retried, `r1=${r1.retried} r2=${r2.retried}`).toBe(1);
    const doc = await mc.db(DB).collection('mig_ranges').findOne({ _id: fakeId } as never);
    expect(doc?.status).toBe('pending');
    expect(doc?.attempts).toBe(0);
    await mc.db(DB).collection('mig_ranges').deleteOne({ _id: fakeId } as never);
  }, 30_000);

  it('zombie owner mid-chunk: abandons quietly — drops only its own staging, no transitions', async () => {
    // The full processChunk abandon path (not just store-level fencing): a
    // worker resumes with a STALE claim snapshot after its lease was
    // reclaimed. Its first fenced transition must fail, it must drop only
    // its OWN generation's staging table, and it must leave the chunk
    // exactly as the rightful owner had it.
    const ZR = 'zombie-run';
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: ZR } as never);
    await ledger.initChunks(ZR, COLL1, [{ lowerCd: 0, upperCd: 1000 }], 'v2', null);
    const chunkId = `${ZR}:${COLL1}:0`;

    const orchPod = (orchestrator as unknown as { podId: string }).podId;
    const stale = await ledger.claimById(chunkId, orchPod, 1); // generation 1, then "stalls"
    expect(stale?.attempts).toBe(1);

    // lease expires; recovery resets; another pod claims generation 2
    await ledger.transition(chunkId, 'in_progress', 'pending', { pod_id: null, staging_table: null });
    const fresh = await ledger.claimById(chunkId, 'rightful-pod', 600);
    expect(fresh?.attempts).toBe(2);
    const nameOf = (gen: number): string => (orchestrator as unknown as {
      stagingName: (c: string, i: number, g: number) => string;
    }).stagingName(COLL1, 0, gen);
    await staging.createStaging(nameOf(2)); // the rightful owner's table

    // the zombie wakes up and runs the chunk pipeline on its stale snapshot
    await (orchestrator as unknown as {
      processChunk: (c: unknown, d: unknown, l: unknown) => Promise<void>;
    }).processChunk(stale, undefined, logger);

    // chunk untouched: still owned by the rightful pod at generation 2
    const doc = await mc.db(DB).collection('mig_ranges').findOne({ _id: chunkId } as never);
    expect(doc?.pod_id).toBe('rightful-pod');
    expect(doc?.attempts).toBe(2);
    expect(doc?.status).toBe('in_progress');

    // zombie's staging generation dropped; the rightful owner's intact
    const tables = await ch.query({
      query: `SELECT name FROM system.tables WHERE database = '${DB}' AND name LIKE '%__stg_%'`,
      format: 'JSONEachRow',
    });
    const names = (await tables.json<{ name: string }>()).map((r) => r.name);
    expect(names).not.toContain(nameOf(1));
    expect(names).toContain(nameOf(2));

    await staging.dropStaging(nameOf(2));
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: ZR } as never);
  }, 30_000);

  it('reclaim: two recoverers race an expired chunk — exactly one wins', async () => {
    const RR = 'reclaim-race-run';
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: RR } as never);
    await ledger.initChunks(RR, COLL1, [{ lowerCd: 0, upperCd: 1000 }], 'v2', null);
    const id = `${RR}:${COLL1}:0`;
    await mc.db(DB).collection('mig_ranges').updateOne({ _id: id } as never, {
      $set: { status: 'attaching', pod_id: 'dead-pod', lease_until: new Date(Date.now() - 60_000), attempts: 1 },
    } as never);
    const [r1, r2] = await Promise.all([
      ledger.reclaim(id, 'attaching', 'recoverer-1', 600),
      ledger.reclaim(id, 'attaching', 'recoverer-2', 600),
    ]);
    expect([r1, r2].filter(Boolean).length).toBe(1); // single winner
    const winner = (r1 ?? r2)!;
    expect(winner.attempts).toBe(2); // new claim generation
    // a still-live lease is never reclaimed
    expect(await ledger.reclaim(id, 'attaching', 'recoverer-3', 600)).toBeNull();
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: RR } as never);
  }, 30_000);

  it('double-attached partition heals to exactly one copy during recovery', async () => {
    // The chaos-harness catch, pinned deterministically: a partition that
    // ended up attached TWICE (two recoverers raced before reclaim existed)
    // must converge to exactly one live copy when recovery runs again.
    const HR = 'heal-run';
    const HCOLL = 'drill_events_healcoll';
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: HR } as never);
    const st = 'drill_events__stg_healtest_0_g1';
    await staging.createStaging(st);
    const rows = Array.from({ length: 5 }, (_, i) => ({
      a: APP, e: '[CLY]_custom', n: 'heal_ev', uid: `hu${i}`, did: 'hd',
      _id: `healrow_${i}`, ts: `2031-01-01 00:00:0${i}.000`, cd: `2031-01-01 00:00:0${i}.000`,
      up: {}, sg: {}, c: 1, s: 0, dur: 0,
    }));
    await ch.insert({ table: `${DB}.${st}`, values: rows, format: 'JSONEachRow' });
    const [pid] = await staging.listPartitions(st);
    expect(pid).toBe('203101');

    // the historical bug: the same partition attached twice
    await staging.attachPartition(st, pid);
    await staging.attachPartition(st, pid);
    expect(await staging.countLiveMatchingStaged(st, pid)).toBe(10);

    await mc.db(DB).collection('mig_ranges').insertOne({
      _id: `${HR}:${HCOLL}:0`, run_id: HR, collection: HCOLL,
      scope_a: null, scope_e: null, scope_n: null, idx: 0,
      lower_cd: Date.UTC(2031, 0, 1), upper_cd: Date.UTC(2031, 0, 2),
      status: 'attaching', pod_id: 'dead-pod', lease_until: new Date(Date.now() - 60_000),
      staging_table: st, docs_read: 5, docs_skipped: 0, rows_expected: 5,
      partitions: [pid], attached: [], attach_method: null, attempts: 1,
      last_error: null, transform_version: 'v2', updated_at: new Date(),
    } as never);

    await (orchestrator as unknown as {
      recoverOne: (c: unknown, l: unknown) => Promise<void>;
    }).recoverOne(await mc.db(DB).collection('mig_ranges').findOne({ _id: `${HR}:${HCOLL}:0` } as never), logger);

    const doc = await mc.db(DB).collection('mig_ranges').findOne({ _id: `${HR}:${HCOLL}:0` } as never);
    expect(doc?.status).toBe('done');
    const liveNow = await ch.query({
      query: `SELECT count() AS c, uniqExact(_id) AS u FROM ${DB}.drill_events WHERE startsWith(_id, 'healrow_')`,
      format: 'JSONEachRow',
    });
    const [l] = await liveNow.json<{ c: string; u: string }>();
    expect(Number(l.c)).toBe(5); // healed: exactly one copy of each
    expect(Number(l.u)).toBe(5);

    await ch.command({ query: `DELETE FROM ${DB}.drill_events WHERE startsWith(_id, 'healrow_')` });
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: HR } as never);
  }, 30_000);
});
