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
    const dlq = new DlqStore(MONGO_URI, DB, logger);
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
    await rebuild({ config, logger, ledger, hashResolver, progress: prog, checkOnly: true });
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
    await rebuild({ config, logger, ledger, hashResolver, progress: prog, checkOnly: true });
    expect(prog.mismatchedWindows.length).toBe(0);

    // Now a LOSS: delete the row entirely — source audit flags the window
    await staging.deleteLiveByPairs([{ id: 'p_80', cdMs }]);
    prog = newProgress();
    await rebuild({ config, logger, ledger, hashResolver, progress: prog, checkOnly: true });
    expect(prog.mismatchedWindows.length).toBe(1);
    expect(prog.mismatchedWindows[0].source - prog.mismatchedWindows[0].live).toBe(1);

    // Restore the true row; both audits green again
    await staging.insertIntoLive([row!], 'audit-restore');
    prog = newProgress();
    await rebuild({ config, logger, ledger, hashResolver, progress: prog, checkOnly: true });
    expect(prog.mismatchedWindows.length).toBe(0);
    audit = await orchestrator.contentAudit(600);
    expect(audit.missing).toBe(0);
    expect(audit.different).toBe(0);
  }, 120_000);

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
    expect(await staging.countLiveByStagedIds(stagingTable, partitionId)).toBe(0); // id-only matching: 1 → skipped attach → data loss

    // once the migrated copy IS live, recovery correctly reports it
    await staging.insertIntoLive([{
      _id: 'retry_victim', a: APP, e: '[CLY]_custom', n: EV1, uid: 'u', did: 'd',
      ts: new Date(tsMs).toISOString().replace('T', ' ').replace('Z', ''),
      c: 1, s: 0, dur: 0,
      cd: new Date(tsMs).toISOString().replace('T', ' ').replace('Z', ''),
    } as never], 'precision-test-live');
    expect(await staging.countLiveByStagedIds(stagingTable, partitionId)).toBe(1);

    await staging.dropStaging(stagingTable);
    await ch.command({ query: `DELETE FROM ${DB}.drill_events WHERE _id = 'retry_victim'` });
  }, 60_000);
});
