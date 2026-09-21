/**
 * Tee-boundary auto-detection: rate-shape based, no per-event identity
 * (teed events carry different _id/cd on each side, and device ts is
 * unreliable). Pinned here:
 *
 *  - the ingestion-pause signature (zero-traffic minute on both sides)
 *    is found and the suggested bound lands inside it
 *  - without a gap the anchor is suggested and the ambiguity around it is
 *    QUANTIFIED, never hidden
 *  - a run that already mapped chunks is refused (migrated rows would
 *    poison the ClickHouse anchor) — while sync parity still reports
 *  - sync parity flags an hour where the tee silently dropped traffic
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pino from 'pino';
import { createHash } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { createClient, type ClickHouseClient } from '@clickhouse/client';

import { detectBoundary, newBoundaryProgress, decideAutoApply } from '../../src/runtime/boundary-detector.ts';
import { LedgerStore } from '../../src/state/ledger-store.ts';
import { StagingManager } from '../../src/target/staging-manager.ts';
import { loadConfig } from '../../src/config/loader.ts';
import type { Config } from '../../src/config/schema.ts';

const MONGO_URI = 'mongodb://localhost:27017/?directConnection=true';
const CH_URL = process.env.TEST_CLICKHOUSE_URL ?? 'http://localhost:8123';
const CH_PASSWORD = process.env.TEST_CLICKHOUSE_PASSWORD ?? '';
const DB = 'test_mig_boundary';
const RUN = 'boundary-1';
const logger = pino({ level: 'silent' });

const APP = 'app_tee';
const COLL = `drill_events${createHash('sha1').update('views' + APP).digest('hex')}`;
const MIN = 60_000;

describe('tee-boundary detection + sync parity', () => {
  let ch: ClickHouseClient;
  let mc: MongoClient;
  let ledger: LedgerStore;
  let staging: StagingManager;
  let config: Config;
  const closers: Array<() => Promise<void>> = [];

  // Timeline (all relative to a flip ~3h ago, minute-aligned so bucket
  // edges are deterministic):
  //   [flip-20m, flip-2m)  old-only traffic (pre-tee)
  //   [flip-2m,  flip)     ZERO traffic — the ingestion pause
  //   [flip, now-ish]      teed: both stores, hourly rate 60 docs
  //   one post-flip hour   CH gets only half — simulated tee outage
  const FLIP = Math.floor((Date.now() - 3 * 3_600_000) / MIN) * MIN;
  const DEAD_HOUR = Math.floor((FLIP + 90 * MIN) / 3_600_000) * 3_600_000;

  const chRow = (id: string, cdMs: number): Record<string, unknown> => ({
    a: APP, e: '[CLY]_custom', n: 'views', uid: 'u', did: 'd', _id: id,
    ts: new Date(cdMs).toISOString().replace('T', ' ').replace('Z', ''),
    cd: new Date(cdMs).toISOString().replace('T', ' ').replace('Z', ''),
    up: {}, sg: {}, c: 1, s: 0, dur: 0,
  });

  beforeAll(async () => {
    mc = new MongoClient(MONGO_URI);
    await mc.connect();
    await mc.db(DB).dropDatabase();
    await mc.db(`${DB}_countly`).dropDatabase();
    await mc.db(`${DB}_countly`).collection('apps').insertOne({ _id: APP } as never);
    await mc.db(`${DB}_countly`).collection('events').insertOne({ _id: APP, list: ['views'] } as never);

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

    // pre-tee: old side only, 5 docs/min for 18 minutes, then the 2-min pause
    const mongoDocs: Record<string, unknown>[] = [];
    for (let m = 20; m > 2; m--) {
      for (let i = 0; i < 5; i++) {
        const cd = FLIP - m * MIN + i * 1_000;
        mongoDocs.push({ _id: `pre_${m}_${i}`, uid: 'u', did: 'd', ts: cd, cd: new Date(cd), sg: {}, c: 1 });
      }
    }
    // post-flip teed traffic: SAME events in both stores, DIFFERENT identities
    const chRows: Record<string, unknown>[] = [];
    const teedUntil = Date.now() - 5 * MIN;
    for (let t = FLIP; t < teedUntil; t += MIN) {
      const perMin = 3; // 180/hr — above the parity monitor's 100-doc noise floor
      for (let i = 0; i < perMin; i++) {
        const cdOld = t + i * 700;
        const cdNew = cdOld + 400; // independent server stamps
        mongoDocs.push({ _id: `old_${t}_${i}`, uid: 'u', did: 'd', ts: cdOld, cd: new Date(cdOld), sg: {}, c: 1 });
        // tee outage hour: the secondary silently missed the second half
        const inDeadHour = cdNew >= DEAD_HOUR && cdNew < DEAD_HOUR + 3_600_000;
        const dropped = inDeadHour && (cdNew - DEAD_HOUR) >= 1_800_000;
        if (!dropped) chRows.push(chRow(`new_${t}_${i}`, cdNew));
      }
    }
    await mc.db(DB).collection(COLL).insertMany(mongoDocs as never[]);
    await mc.db(DB).collection(COLL).createIndex({ cd: 1, _id: 1 });
    await ch.insert({ table: `${DB}.drill_events`, values: chRows, format: 'JSONEachRow' });

    Object.assign(process.env, {
      SERVICE_NAME: 'boundary-test',
      MONGO_URI, MONGO_DB: DB, MONGO_COUNTLY_DB: `${DB}_countly`, MANIFEST_DB: DB,
      CLICKHOUSE_URL: CH_URL, CLICKHOUSE_PASSWORD: CH_PASSWORD, CLICKHOUSE_DB: DB,
      LEDGER_RUN_ID: RUN, BACKPRESSURE_ENABLED: 'false', MULTI_POD_ENABLED: 'false',
    });
    delete process.env.LEDGER_CD_UPPER_BOUND;
    config = loadConfig();

    ledger = new LedgerStore(MONGO_URI, DB, logger);
    staging = new StagingManager({
      url: CH_URL, database: DB, table: 'drill_events', username: 'default', password: CH_PASSWORD, queryTimeoutMs: 60_000,
    }, logger);
    await ledger.connect();
    await staging.connect();
    closers.push(() => ledger.close(), () => staging.close());
  }, 120_000);

  afterAll(async () => {
    for (const close of closers) await close().catch(() => {});
    await ch.command({ query: `DROP DATABASE IF EXISTS ${DB}` }).catch(() => {});
    await ch.close();
    await mc.db(DB).dropDatabase().catch(() => {});
    await mc.db(`${DB}_countly`).dropDatabase().catch(() => {});
    await mc.close();
  });

  const run = async (): Promise<ReturnType<typeof newBoundaryProgress>['report']> => {
    const progress = newBoundaryProgress();
    const report = await detectBoundary({
      config, logger, db: mc.db(DB), staging, ledger, progress, bandMinutes: 30,
    });
    expect(progress.collectionsScanned).toBeGreaterThan(0);
    return report;
  };

  it('stored-bound compare-and-set: only the apply that validated against the current value wins', async () => {
    const RUN2 = 'boundary-cas-1';
    const t1 = await ledger.setStoredBoundIf(RUN2, 1_000_000_000_000, 'a', null);
    expect(t1).toBeTruthy();
    expect(await ledger.setStoredBoundIf(RUN2, 1_100_000_000_000, 'b', null)).toBeNull();
    const t2 = await ledger.setStoredBoundIf(RUN2, 1_200_000_000_000, 'c', 1_000_000_000_000);
    expect(t2).toBeTruthy();
    expect(await ledger.setStoredBoundIf(RUN2, 1_300_000_000_000, 'd', 1_000_000_000_000)).toBeNull();
    expect(await ledger.getStoredBound(RUN2)).toBe(1_200_000_000_000);

    // value-ABA: a competing apply re-stores the SAME value under its own
    // token — the earlier owner's rollback must not unwind it
    const t3 = await ledger.setStoredBoundIf(RUN2, 1_200_000_000_000, 'e', 1_200_000_000_000);
    expect(t3).toBeTruthy();
    expect(await ledger.rollbackStoredBound(RUN2, t2 as string, 1_000_000_000_000)).toBe(false);
    expect(await ledger.getStoredBound(RUN2)).toBe(1_200_000_000_000);
    // fence casualties are token-scoped: only the rolled-back bound's
    // superseded chunks come back
    const RUN3b = 'boundary-fence-1';
    const mkc = (idx: number) => ({
      _id: `${RUN3b}:c:${idx}`, run_id: RUN3b, collection: 'c',
      scope_a: 'a', scope_e: 'e', scope_n: null, idx, lower_cd: idx * 100, upper_cd: idx * 100 + 100,
      status: 'in_progress' as const, pod_id: 'p1', lease_until: new Date(Date.now() + 60_000), staging_table: null,
      docs_read: 0, docs_skipped: 0, rows_expected: 0, partitions: [], attached: [],
      attach_method: null, attempts: 0, last_error: null, transform_version: 'v', updated_at: new Date(),
    });
    await ledger.replaceAllForRun(RUN3b, [mkc(1), mkc(2)] as never[]);
    await ledger.supersede(`${RUN3b}:c:1`, 'p1', 'tokenA');
    await ledger.supersede(`${RUN3b}:c:2`, 'p1', 'tokenB');
    expect(await ledger.restoreSuperseded(RUN3b, 'tokenA')).toBe(1);
    const rows3 = await mc.db(DB).collection('mig_ranges').find({ run_id: RUN3b } as never).sort({ idx: 1 }).toArray();
    expect(rows3.map((r) => [r.idx, r.status])).toEqual([[1, 'pending'], [2, 'superseded']]);

    // apply marker: token-scoped set/clear, stale markers ignored
    const RUN4 = 'boundary-marker-1';
    expect(await ledger.acquireApplyMarker(RUN4, 'mtokA')).toBe(true);
    expect((await ledger.getBoundState(RUN4)).applying).toBe(true);
    expect(await ledger.clearApplyMarker(RUN4, 'WRONG')).toBe(false);
    expect((await ledger.getBoundState(RUN4)).applying).toBe(true);
    expect(await ledger.clearApplyMarker(RUN4, 'mtokA')).toBe(true);
    expect((await ledger.getBoundState(RUN4)).applying).toBe(false);

    // marker acquisition is a CAS: one live apply at a time
    const RUN5 = 'boundary-marker-2';
    expect(await ledger.acquireApplyMarker(RUN5, 'a1')).toBe(true);
    expect(await ledger.acquireApplyMarker(RUN5, 'a2')).toBe(false);
    expect(await ledger.clearApplyMarker(RUN5, 'a1')).toBe(true);
    expect(await ledger.acquireApplyMarker(RUN5, 'a2')).toBe(true);
    await ledger.clearApplyMarker(RUN5, 'a2');

    // the CURRENT owner's rollback works
    expect(await ledger.rollbackStoredBound(RUN2, t3 as string, 1_000_000_000_000)).toBe(true);
    expect(await ledger.getStoredBound(RUN2)).toBe(1_000_000_000_000);
  });

  it('prune journal: orphaned receipts restore under the governing bound; live applies are skipped', async () => {
    const mk = (run: string, id: string, lo: number, up: number) => ({
      _id: id, run_id: run, collection: 'c', idx: 0, lower_cd: lo, upper_cd: up,
      status: 'pending', attempts: 0, created_at: new Date(), updated_at: new Date(),
    });
    const ranges = mc.db(DB).collection('mig_ranges');

    // crash BEFORE the bound committed: deleted chunk reinserted, straddler unclamped
    const RJ = 'prune-journal-1';
    await ranges.insertOne(mk(RJ, 'rj:straddle', 50, 100) as never); // on-disk: clamped by the dead apply
    await ledger.journalPruneReceipt(RJ, 'tokDead', {
      deletedChunks: [mk(RJ, 'rj:gone', 150, 200)] as never[],
      clampedChunks: [{ _id: 'rj:straddle', upper_cd: 180 }],
    });
    expect(await ledger.recoverPruneJournal(RJ, null)).toEqual({ recovered: 1, skippedLiveApply: 0 });
    const rows = await ranges.find({ run_id: RJ } as never).sort({ _id: 1 }).toArray();
    expect(rows.map((r) => [r._id, r.lower_cd, r.upper_cd])).toEqual([['rj:gone', 150, 200], ['rj:straddle', 50, 180]]);
    // the journal is empty now — recovery is idempotent
    expect(await ledger.recoverPruneJournal(RJ, null)).toEqual({ recovered: 0, skippedLiveApply: 0 });

    // a LIVE apply's entry is someone's in-flight work — skipped until its marker clears
    await ledger.journalPruneReceipt(RJ, 'tokLive', { deletedChunks: [mk(RJ, 'rj:live', 300, 400)] as never[], clampedChunks: [] });
    expect(await ledger.acquireApplyMarker(RJ, 'tokLive')).toBe(true);
    expect(await ledger.recoverPruneJournal(RJ, null)).toEqual({ recovered: 0, skippedLiveApply: 1 });
    expect(await ranges.countDocuments({ _id: 'rj:live' } as never)).toBe(0);
    expect(await ledger.clearApplyMarker(RJ, 'tokLive')).toBe(true);
    expect(await ledger.recoverPruneJournal(RJ, null)).toEqual({ recovered: 1, skippedLiveApply: 0 });
    expect(await ranges.countDocuments({ _id: 'rj:live' } as never)).toBe(1);

    // huge receipts PAGE across journal documents (16MiB BSON limit) and
    // recover in full
    const RJ3 = 'prune-journal-3';
    const big = Array.from({ length: 5_001 }, (_, i) => mk(RJ3, `rj3:${i}`, i * 10, i * 10 + 9));
    await ledger.journalPruneReceipt(RJ3, 'tokBig', { deletedChunks: big as never[], clampedChunks: [] });
    expect(await ledger.countPruneJournal(RJ3)).toBe(2);
    expect(await ledger.recoverPruneJournal(RJ3, null)).toEqual({ recovered: 2, skippedLiveApply: 0 });
    expect(await ranges.countDocuments({ run_id: RJ3 } as never)).toBe(5_001);
    expect(await ledger.countPruneJournal(RJ3)).toBe(0);

    // a COMMITTED apply's leftover entry restores NOTHING — its own bound filters every chunk out
    const RJ2 = 'prune-journal-2';
    expect(await ledger.setStoredBoundIf(RJ2, 120, 'test', null)).toBeTruthy();
    await ledger.journalPruneReceipt(RJ2, 'tokDone', { deletedChunks: [mk(RJ2, 'rj2:beyond', 130, 200)] as never[], clampedChunks: [] });
    expect(await ledger.recoverPruneJournal(RJ2, null)).toEqual({ recovered: 1, skippedLiveApply: 0 });
    expect(await ranges.countDocuments({ _id: 'rj2:beyond' } as never)).toBe(0);
  });

  it('restorePrune under a winning bound never resurrects what that bound pruned', async () => {
    const RUN3 = 'boundary-restore-1';
    const mk = (idx: number, lo: number, hi: number) => ({
      _id: `${RUN3}:c:${idx}`, run_id: RUN3, collection: 'c',
      scope_a: 'a', scope_e: 'e', scope_n: null, idx, lower_cd: lo, upper_cd: hi,
      status: 'pending' as const, pod_id: null, lease_until: null, staging_table: null,
      docs_read: 0, docs_skipped: 0, rows_expected: 0, partitions: [], attached: [],
      attach_method: null, attempts: 0, last_error: null, transform_version: 'v', updated_at: new Date(),
    });
    // loser's receipt holds chunks at 100–200 and 200–300, straddler originally ending 150
    const receipt = {
      deletedChunks: [mk(1, 100, 200), mk(2, 200, 300)] as never[],
      clampedChunks: [{ _id: `${RUN3}:c:0`, upper_cd: 150 }],
    };
    await ledger.replaceAllForRun(RUN3, [{ ...mk(0, 0, 100), upper_cd: 120 }] as never[]);
    // the winner's bound is 150: chunk 200–300 stays gone, 100–200 comes back clamped to 150
    await ledger.restorePrune(receipt as never, 150);
    const rows = await mc.db(DB).collection('mig_ranges').find({ run_id: RUN3 } as never).sort({ idx: 1 }).toArray();
    expect(rows.map((r) => [r.idx, r.lower_cd, r.upper_cd])).toEqual([[0, 0, 150], [1, 100, 150]]);
  });

  it('finds the ingestion-pause gap and suggests a bound inside it; parity flags the dead hour', async () => {
    const report = (await run())!;
    const d = report.detection;
    expect(d.status).toBe('ok');
    expect(d.method).toBe('gap');
    expect(d.gap).not.toBeNull();
    // the gap is the [FLIP-2m, FLIP) pause; the suggestion sits inside it
    expect(d.gap!.fromMs).toBe(FLIP - 2 * MIN);
    expect(d.gap!.toMs).toBe(FLIP);
    expect(d.suggestedBoundMs!).toBeGreaterThanOrEqual(d.gap!.fromMs);
    expect(d.suggestedBoundMs!).toBeLessThan(d.gap!.toMs);
    expect(d.ambiguousMongoDocs).toBe(0);

    // seam sanity in the minute table: mongo-only before, both after
    const before = d.minutes!.find((m) => m.minuteMs === FLIP - 5 * MIN)!;
    expect(before.mongo).toBeGreaterThan(0);
    expect(before.ch).toBe(0);
    const after = d.minutes!.find((m) => m.minuteMs === FLIP + 5 * MIN)!;
    expect(after.mongo).toBeGreaterThan(0);
    expect(after.ch).toBeGreaterThan(0);

    // sync parity: exactly the tee-outage hour is flagged
    expect(report.sync.status).toBe('ok');
    const flagged = report.sync.hours!.filter((h) => h.flagged);
    expect(flagged.length).toBe(1);
    expect(flagged[0].hourMs).toBe(DEAD_HOUR);
    expect(flagged[0].ch).toBeLessThan(flagged[0].mongo);
  }, 60_000);

  it('without a gap: suggests the anchor and quantifies the ambiguity', async () => {
    // fill the pause with old-side traffic — no clean gap anymore
    const fill: Record<string, unknown>[] = [];
    for (let m = 1; m <= 2; m++) {
      for (let i = 0; i < 5; i++) {
        const cd = FLIP - m * MIN + i * 1_000;
        fill.push({ _id: `fill_${m}_${i}`, uid: 'u', did: 'd', ts: cd, cd: new Date(cd), sg: {}, c: 1 });
      }
    }
    await mc.db(DB).collection(COLL).insertMany(fill as never[]);

    const report = (await run())!;
    const d = report.detection;
    expect(d.status).toBe('ok');
    expect(d.method).toBe('anchor');
    expect(d.ambiguousMongoDocs!).toBeGreaterThan(0); // the stake is a number, not a secret

    await mc.db(DB).collection(COLL).deleteMany({ _id: { $regex: '^fill_' } } as never);
  }, 60_000);

  it('apply-bound: prunes pending chunks past the bound, refuses when executed chunks are there', async () => {
    const AR = 'apply-run';
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: AR } as never);
    // grid mapped WITHOUT a bound: 4 windows; the bound lands inside #2
    await ledger.initChunks(AR, COLL, [
      { lowerCd: 0, upperCd: 100 }, { lowerCd: 100, upperCd: 200 },
      { lowerCd: 200, upperCd: 300 }, { lowerCd: 300, upperCd: 400 },
    ], 'v2', null);
    const B = 150;
    const pruned = await ledger.pruneBeyondBound(AR, B);
    expect(pruned).toMatchObject({ deleted: 2, clamped: 1 }); // #2,#3 gone; #1 clamped
    const left = await mc.db(DB).collection('mig_ranges')
      .find({ run_id: AR } as never).sort({ idx: 1 }).toArray();
    expect(left.map((c) => [c.lower_cd, c.upper_cd])).toEqual([[0, 100], [100, 150]]);

    // an EXECUTED chunk past the bound → hard refusal (data may have moved)
    await mc.db(DB).collection('mig_ranges').updateOne(
      { _id: `${AR}:${COLL}:1` } as never, { $set: { status: 'done', upper_cd: 200 } } as never);
    await expect(ledger.pruneBeyondBound(AR, B)).rejects.toThrow(/non-pending/);
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: AR } as never);
  }, 30_000);

  it('stored bound: a fresh pod adopts it and migrates only below; env conflict is fatal', async () => {
    const SR = 'stored-run';
    const B = FLIP - MIN; // inside the pause gap
    await ledger.setStoredBound(SR, B, 'test');

    // pod with NO env bound: adopts the stored one
    Object.assign(process.env, {
      LEDGER_RUN_ID: SR, LEDGER_CHUNK_DOCS_TARGET: '200', MONGO_PAGE_SIZE: '200',
      MULTI_POD_ENABLED: 'false', POD_ID: 'stored-pod',
    });
    delete process.env.LEDGER_CD_UPPER_BOUND;
    const config2 = loadConfig();
    const { MongoReader } = await import('../../src/source/mongo-reader.ts');
    const { DlqStore } = await import('../../src/state/dlq-store.ts');
    const { RetryPolicy } = await import('../../src/runtime/retry-policy.ts');
    const { HashResolver } = await import('../../src/transform/hash-resolver.ts');
    const { ChunkOrchestrator } = await import('../../src/runtime/chunk-orchestrator.ts');
    const mongoReader = new MongoReader({
      uri: MONGO_URI, database: DB, readPreference: 'primary', readConcern: 'local',
      retryReads: true, appName: 'stored-pod', cursorBatchSize: 500, maxTimeMs: 60_000,
    }, logger);
    const dlq2 = new DlqStore(MONGO_URI, DB, logger);
    const resolver2 = new HashResolver({ uri: MONGO_URI, countlyDb: `${DB}_countly` }, logger);
    await mongoReader.connect(); await dlq2.connect(); await resolver2.build();
    closers.push(() => mongoReader.close(), () => dlq2.close(), () => resolver2.close());
    const orch = new ChunkOrchestrator({
      config: config2, logger, mongoReader, ledger, dlq: dlq2, staging,
      retryPolicy: new RetryPolicy({ maxRetries: 2, baseDelayMs: 50, maxDelayMs: 200 }), hashResolver: resolver2,
    });
    await orch.run();
    expect(orch.getStats().status).toBe('completed');
    expect((orch.getStats() as { cdUpperBoundMs: number | null }).cdUpperBoundMs).toBe(B); // adopted

    // only pre-gap docs migrated; teed-era old-side docs untouched
    const res = await ch.query({
      query: `SELECT countIf(_id LIKE 'pre\_%') AS pre, countIf(_id LIKE 'old\_%') AS old FROM ${DB}.drill_events`,
      format: 'JSONEachRow',
    });
    const [r] = await res.json<{ pre: string; old: string }>();
    expect(Number(r.pre)).toBe(90); // 18 pre-tee minutes x 5 docs
    expect(Number(r.old)).toBe(0);  // nothing at/after the bound
    const chunks = await mc.db(DB).collection('mig_ranges').find({ run_id: SR, lower_cd: { $gte: 0 } } as never).toArray();
    for (const c of chunks) expect(c.upper_cd).toBeLessThanOrEqual(B);

    // a pod started with a CONFLICTING env bound must fail loudly, not guess
    process.env.LEDGER_CD_UPPER_BOUND = String(B + 60_000);
    const config3 = loadConfig();
    const orch2 = new ChunkOrchestrator({
      config: config3, logger, mongoReader, ledger, dlq: dlq2, staging,
      retryPolicy: new RetryPolicy({ maxRetries: 2, baseDelayMs: 50, maxDelayMs: 200 }), hashResolver: resolver2,
    });
    await orch2.run();
    const st = orch2.getStats();
    expect(st.status).toBe('failed');
    expect(String(st.fatalError)).toContain('bound conflict');
    delete process.env.LEDGER_CD_UPPER_BOUND;
  }, 120_000);

  it('refuses detection once the run has mapped chunks — sync parity still reports', async () => {
    await ledger.initChunks(RUN, COLL, [{ lowerCd: 0, upperCd: 1000 }], 'v2', null);
    const report = (await run())!;
    expect(report.detection.status).toBe('refused');
    expect(report.detection.reason).toContain('chunks');
    expect(report.sync.status).toBe('ok'); // parity is migration-agnostic
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: RUN } as never);
  }, 60_000);
});

describe('set-boundary auto-apply decision', () => {
  const report = (detection: Record<string, unknown>) => ({ detection, sync: { status: 'ok' } }) as never;
  const M = 60_000;
  const gapMinutes = (mongoPerMin: number, chPerMin: number, anchorMs = 22 * M) => {
    const gap = { fromMs: 20 * M, toMs: 22 * M };
    const minutes: Array<{ minuteMs: number; mongo: number; ch: number }> = [];
    for (let m = 5; m < 20; m++) minutes.push({ minuteMs: m * M, mongo: mongoPerMin, ch: 0 });
    for (let m = 22; m < 40; m++) minutes.push({ minuteMs: m * M, mongo: 0, ch: chPerMin });
    return { gap, minutes, suggestedBoundMs: 21 * M, anchorMs };
  };

  it('a corroborated gap applies unattended', () => {
    const g = gapMinutes(5, 4);
    expect(decideAutoApply(report({ status: 'ok', method: 'gap', ...g }), false))
      .toEqual({ apply: true, boundMs: 21 * M });
  });

  it('a quiet minute on a sparse install is NOT taken as the seam', () => {
    const g = gapMinutes(1, 1); // 10 docs per flank — any lull looks like this
    const d = decideAutoApply(report({ status: 'ok', method: 'gap', ...g }), false);
    expect(d.apply).toBe(false);
    expect(d.reason).toContain('sparse');
    // …unless the operator explicitly accepts imperfect evidence
    expect(decideAutoApply(report({ status: 'ok', method: 'gap', ...g }), true))
      .toEqual({ apply: true, boundMs: 21 * M });
  });

  it('an anchor needs the explicit acceptAnchor', () => {
    const d = decideAutoApply(report({ status: 'ok', method: 'anchor', suggestedBoundMs: 123, ambiguousMongoDocs: 42 }), false);
    expect(d.apply).toBe(false);
    expect(d.reason).toContain('acceptAnchor');
    expect(d.reason).toContain('42');
    expect(decideAutoApply(report({ status: 'ok', method: 'anchor', suggestedBoundMs: 123 }), true))
      .toEqual({ apply: true, boundMs: 123 });
  });

  it('old-side traffic resuming between the gap and the anchor disqualifies the gap', () => {
    // quiet 20–22, mongo resumes at 22, first new-side data at 24: within
    // the 2-min allowance, but those minute-22/23 docs would be orphaned
    const g = gapMinutes(5, 4, 24 * M);
    g.minutes.push({ minuteMs: 22 * M, mongo: 3, ch: 0 }, { minuteMs: 23 * M, mongo: 3, ch: 0 });
    const d = decideAutoApply(report({ status: 'ok', method: 'gap', ...g }), false);
    expect(d.apply).toBe(false);
    expect(d.reason).toContain('resumed');
  });

  it('a lull that does not abut the ClickHouse anchor is never auto-applied', () => {
    // gap at minutes 20–22 but the first new-side data lands at minute 30:
    // a quiet spell BEFORE the real tee start — applying it would exclude
    // the old-side docs between the false gap and the anchor
    const g = gapMinutes(5, 4, 30 * M);
    const d = decideAutoApply(report({ status: 'ok', method: 'gap', ...g }), false);
    expect(d.apply).toBe(false);
    expect(d.reason).toContain('abut');
    expect(decideAutoApply(report({ status: 'ok', method: 'gap', ...g }), true))
      .toEqual({ apply: true, boundMs: 21 * M });
  });

  it('refused or empty detections never apply', () => {
    expect(decideAutoApply(report({ status: 'refused', reason: 'run already mapped' }), true).apply).toBe(false);
    expect(decideAutoApply(null, true).apply).toBe(false);
  });
});
