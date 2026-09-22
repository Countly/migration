/**
 * Tee-overlap dedupe: a run without the cd bound migrated the mirror's
 * re-ingested docs on top of natively ingested rows. Pinned here:
 *
 *  - dry run counts the duplicates exactly and deletes NOTHING
 *  - execute deletes precisely the id-matched rows inside the window:
 *    native rows and pre-window (legitimately migrated) rows survive
 *  - a bucket whose migrated rows lack count-evidence of native
 *    counterparts (tee outage: the migrated row is the ONLY copy) is
 *    skipped, reported, and NEVER deleted — even under execute
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pino from 'pino';
import { createHash } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { createClient, type ClickHouseClient } from '@clickhouse/client';

import { runDedupeOverlap, newDedupeOverlapState } from '../../src/runtime/dedupe-overlap.ts';
import { StagingManager } from '../../src/target/staging-manager.ts';
import { HashResolver } from '../../src/transform/hash-resolver.ts';
import { loadConfig } from '../../src/config/loader.ts';
import type { Config } from '../../src/config/schema.ts';

const MONGO_URI = 'mongodb://localhost:27017/?directConnection=true';
const CH_URL = process.env.TEST_CLICKHOUSE_URL ?? 'http://localhost:8123';
const CH_PASSWORD = process.env.TEST_CLICKHOUSE_PASSWORD ?? '';
const DB = 'test_mig_dedupe';
const logger = pino({ level: 'silent' });

const APP = 'app_dd';
const COLL = `drill_events${createHash('sha1').update('views' + APP).digest('hex')}`;
// second app: its mirrored docs were migrated but the native side is GONE
// (tee outage during the overlap) — the safety check must protect them
const APP2 = 'app_dd_outage';
const COLL2 = `drill_events${createHash('sha1').update('views' + APP2).digest('hex')}`;
const OUTAGE = 40;
// third app: outage bucket where migrated null-cd SWEEP rows outnumber the
// matched rows — without the sweep correction they'd read as native evidence
const APP3 = 'app_dd_sweep';
const COLL3 = `drill_events${createHash('sha1').update('views' + APP3).digest('hex')}`;
const SWEEPM = 30;
const SWEEPN = 35;
// fourth app: a native retry reused a migrated doc's _id at a DIFFERENT cd
// in the same hour — pair-exact deletion must spare it
const APP4 = 'app_dd_retry';
const COLL4 = `drill_events${createHash('sha1').update('views' + APP4).digest('hex')}`;
// fifth app: DUPLICATE sweep copies must all subtract from native evidence
const APP5 = 'app_dd_dupsweep';
const COLL5 = `drill_events${createHash('sha1').update('views' + APP5).digest('hex')}`;
// base collection: no per-collection (a,e,n) scope resolvable — its matches
// must never be deleted, even though sibling native traffic fills the table
const BASE = 20;

const FLIP = Math.floor(Date.now() / 60_000) * 60_000 - 2 * 3_600_000; // tee flip 2h ago
const DONE = FLIP + 3_600_000;                                          // migration completed 1h later

const chRow = (id: string, cdMs: number): Record<string, unknown> => ({
  a: APP, e: '[CLY]_custom', n: 'views', uid: 'u', did: 'd', _id: id,
  ts: new Date(cdMs).toISOString().replace('T', ' ').replace('Z', ''),
  cd: new Date(cdMs).toISOString().replace('T', ' ').replace('Z', ''),
  up: {}, sg: {}, c: 1, s: 0, dur: 0,
});

describe('tee-overlap dedupe', () => {
  let ch: ClickHouseClient;
  let mc: MongoClient;
  let config: Config;
  let hashResolver: HashResolver;

  const chCount = async (where = '1'): Promise<number> => {
    const res = await ch.query({ query: `SELECT count() AS n FROM ${DB}.drill_events WHERE ${where}`, format: 'JSONEachRow' });
    return Number((await res.json<{ n: string }>())[0].n);
  };

  beforeAll(async () => {
    mc = new MongoClient(MONGO_URI);
    await mc.connect();
    await mc.db(DB).dropDatabase();
    await mc.db(`${DB}_countly`).dropDatabase();
    await mc.db(`${DB}_countly`).collection('apps').insertMany([{ _id: APP }, { _id: APP2 }, { _id: APP3 }, { _id: APP4 }, { _id: APP5 }] as never[]);
    await mc.db(`${DB}_countly`).collection('events').insertMany([
      { _id: APP, list: ['views'] }, { _id: APP2, list: ['views'] }, { _id: APP3, list: ['views'] }, { _id: APP4, list: ['views'] }, { _id: APP5, list: ['views'] },
    ] as never[]);

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

    const mongoDocs: Record<string, unknown>[] = [];
    const chRows: Record<string, unknown>[] = [];
    // pre-flip history: migrated once, identical ids both sides — must survive
    for (let i = 0; i < 200; i++) {
      const cd = FLIP - 3_600_000 + i * 10_000;
      mongoDocs.push({ _id: `hist_${i}`, uid: 'u', did: 'd', ts: cd, cd: new Date(cd), sg: {}, c: 1 });
      chRows.push(chRow(`hist_${i}`, cd));
    }
    // overlap window: each event exists in CH twice — natively (new id) and
    // as the migrated copy of the mirror's re-ingested doc (old-Mongo id)
    for (let i = 0; i < 150; i++) {
      const cd = FLIP + i * 20_000;
      mongoDocs.push({ _id: `mirror_${i}`, uid: 'u', did: 'd', ts: cd, cd: new Date(cd), sg: {}, c: 1 });
      chRows.push(chRow(`mirror_${i}`, cd));            // migrated duplicate
      chRows.push(chRow(`native_${i}`, cd + 300));      // native original
    }
    // extra native rows with no mirror copy (mirror dropped them) — survive
    for (let i = 0; i < 10; i++) chRows.push(chRow(`native_only_${i}`, FLIP + 500_000 + i * 1_000));
    // a SIBLING collection's row sharing an _id with a mirrored doc, in the
    // window — scoped deletes must never touch it
    chRows.push({ ...chRow('mirror_10', FLIP + 10 * 20_000 + 50), a: 'sibling_app' });
    await mc.db(DB).collection(COLL).insertMany(mongoDocs as never[]);
    await mc.db(DB).collection(COLL).createIndex({ cd: 1, _id: 1 });
    await ch.insert({ table: `${DB}.drill_events`, values: chRows, format: 'JSONEachRow' });

    // outage app: mirrored docs migrated, native side never landed —
    // deleting these would remove the only copy
    const outageDocs: Record<string, unknown>[] = [];
    const outageRows: Record<string, unknown>[] = [];
    for (let i = 0; i < OUTAGE; i++) {
      const cd = FLIP + i * 10_000;
      outageDocs.push({ _id: `only_${i}`, uid: 'u', did: 'd', ts: cd, cd: new Date(cd), sg: {}, c: 1 });
      const r = chRow(`only_${i}`, cd);
      (r as Record<string, unknown>).a = APP2;
      outageRows.push(r);
    }
    await mc.db(DB).collection(COLL2).insertMany(outageDocs as never[]);
    await mc.db(DB).collection(COLL2).createIndex({ cd: 1, _id: 1 });
    await ch.insert({ table: `${DB}.drill_events`, values: outageRows, format: 'JSONEachRow' });

    // unscoped base collection: migrated copies whose only "native cover" is
    // SIBLING collections' traffic — no usable evidence, never deletable
    const baseDocs: Record<string, unknown>[] = [];
    const baseRows: Record<string, unknown>[] = [];
    for (let i = 0; i < BASE; i++) {
      const cd = FLIP + i * 15_000;
      baseDocs.push({ _id: `base_${i}`, a: APP, e: '[CLY]_custom', n: 'views', uid: 'u', did: 'd', ts: cd, cd: new Date(cd), sg: {}, c: 1 });
      baseRows.push(chRow(`base_${i}`, cd));
    }
    await mc.db(DB).collection('drill_events').insertMany(baseDocs as never[]);
    await mc.db(DB).collection('drill_events').createIndex({ cd: 1, _id: 1 });
    await ch.insert({ table: `${DB}.drill_events`, values: baseRows, format: 'JSONEachRow' });

    Object.assign(process.env, {
      SERVICE_NAME: 'dedupe-test',
      MONGO_URI, MONGO_DB: DB, MONGO_COUNTLY_DB: `${DB}_countly`, MANIFEST_DB: DB,
      CLICKHOUSE_URL: CH_URL, CLICKHOUSE_PASSWORD: CH_PASSWORD, CLICKHOUSE_DB: DB,
      LEDGER_RUN_ID: 'dedupe-1', BACKPRESSURE_ENABLED: 'false', MULTI_POD_ENABLED: 'false',
    });
    config = loadConfig();
    hashResolver = new HashResolver({ uri: MONGO_URI, countlyDb: `${DB}_countly` }, logger);
    await hashResolver.build();
  }, 120_000);

  afterAll(async () => {
    await hashResolver?.close().catch(() => {});
    await ch.command({ query: `DROP DATABASE IF EXISTS ${DB}` }).catch(() => {});
    await ch.close();
    await mc.db(DB).dropDatabase().catch(() => {});
    await mc.db(`${DB}_countly`).dropDatabase().catch(() => {});
    await mc.close();
  });

  it('dry run counts the duplicates exactly, flags the outage buckets, and deletes nothing', async () => {
    const state = newDedupeOverlapState();
    await runDedupeOverlap({ config, logger, hashResolver }, state, { fromMs: FLIP, toMs: DONE, execute: false });
    expect(state.status).toBe('completed');
    expect(state.totals).toEqual({ mongoDocsInWindow: 150 + OUTAGE + BASE, chMatched: 150 + OUTAGE + BASE, deleted: 0, unsafeMatched: OUTAGE + BASE });
    expect(state.lastDryRun).toMatchObject({ fromMs: FLIP, toMs: DONE, slackPct: 0, chMatched: 150 + OUTAGE + BASE });
    const outageRow = state.collections.find((c) => c.collection === COLL2);
    expect(outageRow?.unsafe.length).toBeGreaterThan(0);
    expect(outageRow?.unsafe.reduce((a, u) => a + u.matched, 0)).toBe(OUTAGE);
    expect(outageRow?.unsafe.every((u) => u.reason === 'no-native-evidence')).toBe(true);
    const baseRow = state.collections.find((c) => c.collection === 'drill_events');
    expect(baseRow?.scoped).toBe(false);
    expect(baseRow?.unsafe.every((u) => u.reason === 'no-scope')).toBe(true);
    expect(baseRow?.unsafe.reduce((a, u) => a + u.matched, 0)).toBe(BASE);
    expect(await chCount()).toBe(200 + 150 + 150 + 10 + OUTAGE + BASE + 1);
  });

  it('execute deletes exactly the evidenced duplicates; unsafe buckets, native and pre-flip rows survive', async () => {
    const state = newDedupeOverlapState();
    await runDedupeOverlap({ config, logger, hashResolver }, state, { fromMs: FLIP, toMs: DONE, execute: true });
    expect(state.status).toBe('completed');
    expect(state.totals).toEqual({ mongoDocsInWindow: 150 + OUTAGE + BASE, chMatched: 150 + OUTAGE + BASE, deleted: 150, unsafeMatched: OUTAGE + BASE });
    expect(await chCount("_id LIKE 'mirror_%'")).toBe(1); // only the sibling collection's same-_id row remains
    expect(await chCount("_id LIKE 'native_%'")).toBe(160);
    expect(await chCount("_id LIKE 'hist_%'")).toBe(200);
    // the only-copy rows are untouched — the safety check protected them
    expect(await chCount("_id LIKE 'only_%'")).toBe(OUTAGE);
    // unscoped base-collection rows: sibling traffic is not evidence
    expect(await chCount("_id LIKE 'base_%'")).toBe(BASE);
    // the sibling collection's same-_id row survives the scoped delete
    expect(await chCount("_id = 'mirror_10' AND a = 'sibling_app'")).toBe(1);
    expect(await chCount("_id = 'mirror_10'")).toBe(1);
  });

  it('execute refuses when the run fingerprint no longer matches the licensed dry run', async () => {
    const state = newDedupeOverlapState();
    const before = await chCount();
    const stubLedger = { runFingerprint: async () => '7:7:1700000000000' } as unknown as import('../../src/state/ledger-store.ts').LedgerStore;
    await runDedupeOverlap({ config, logger, hashResolver, ledger: stubLedger }, state, {
      fromMs: FLIP, toMs: DONE, execute: true, expectedFingerprint: '5:5:1600000000000',
    });
    expect(state.status).toBe('failed');
    expect(state.error).toContain('changed since the reviewed dry run');
    expect(await chCount()).toBe(before); // refused before scanning — nothing deleted
  });

  it('migrated null-cd sweep rows never count as native evidence', async () => {
    // outage bucket: SWEEPM mirrored docs migrated, native side never landed —
    // but SWEEPN migrated sweep rows (null cd in Mongo, ts-derived cd in CH)
    // sit in the same bucket. Uncorrected, native = 65 - 30 = 35 >= matched
    // and execute would delete the only copies.
    const sweepDocs: Record<string, unknown>[] = [];
    const sweepRows: Record<string, unknown>[] = [];
    for (let i = 0; i < SWEEPM; i++) {
      const cd = FLIP + i * 1_000;
      sweepDocs.push({ _id: `sw_mirror_${i}`, uid: 'u', did: 'd', ts: cd, cd: new Date(cd), sg: {}, c: 1 });
      sweepRows.push({ ...chRow(`sw_mirror_${i}`, cd), a: APP3 });
    }
    for (let i = 0; i < SWEEPN; i++) {
      const cd = FLIP + 30_000 + i * 100; // ts-derived cd, same hour bucket
      sweepDocs.push({ _id: `sw_null_${i}`, uid: 'u', did: 'd', ts: cd, cd: null, sg: {}, c: 1 });
      sweepRows.push({ ...chRow(`sw_null_${i}`, cd), a: APP3 });
    }
    await mc.db(DB).collection(COLL3).insertMany(sweepDocs as never[]);
    await mc.db(DB).collection(COLL3).createIndex({ cd: 1, _id: 1 });
    await ch.insert({ table: `${DB}.drill_events`, values: sweepRows, format: 'JSONEachRow' });

    const state = newDedupeOverlapState();
    await runDedupeOverlap({ config, logger, hashResolver }, state, { fromMs: FLIP, toMs: DONE, execute: true });
    expect(state.status).toBe('completed');
    const sweepRow = state.collections.find((c) => c.collection === COLL3);
    expect(sweepRow?.deleted).toBe(0);
    expect(sweepRow?.unsafe.every((u) => u.reason === 'no-native-evidence')).toBe(true);
    expect(sweepRow?.unsafe.reduce((a, u) => a + u.matched, 0)).toBe(SWEEPM);
    // every row survived — mirrors AND sweep rows
    expect(await chCount("_id LIKE 'sw_mirror_%'")).toBe(SWEEPM);
    expect(await chCount("_id LIKE 'sw_null_%'")).toBe(SWEEPN);
  });

  it('pair-exact delete spares a native retry that reused the _id at a different cd in the same hour', async () => {
    // 10 mirrored docs, each with a native counterpart (different ids) —
    // plus rt_0's native RETRY at the SAME _id, 5s later. An id-in-window
    // delete would kill both copies of rt_0's event; the pair delete must
    // remove only the migrated (rt_0, cd) row.
    const docs: Record<string, unknown>[] = [];
    const rows: Record<string, unknown>[] = [];
    for (let i = 0; i < 10; i++) {
      const cd = FLIP + i * 1_000;
      docs.push({ _id: `rt_${i}`, uid: 'u', did: 'd', ts: cd, cd: new Date(cd), sg: {}, c: 1 });
      rows.push({ ...chRow(`rt_${i}`, cd), a: APP4 });               // migrated copy
      rows.push({ ...chRow(`rt_native_${i}`, cd + 300), a: APP4 }); // native original
    }
    rows.push({ ...chRow('rt_0', FLIP + 5_000), a: APP4 });          // native RETRY, same _id, different cd
    await mc.db(DB).collection(COLL4).insertMany(docs as never[]);
    await mc.db(DB).collection(COLL4).createIndex({ cd: 1, _id: 1 });
    await ch.insert({ table: `${DB}.drill_events`, values: rows, format: 'JSONEachRow' });

    const state = newDedupeOverlapState();
    await runDedupeOverlap({ config, logger, hashResolver }, state, { fromMs: FLIP, toMs: DONE, execute: true });
    expect(state.status).toBe('completed');
    const r = state.collections.find((c) => c.collection === COLL4);
    expect(r?.chMatched).toBe(10); // pair-exact: the retry row is NOT a match
    expect(r?.deleted).toBe(10);
    // the retry survived — and it is the ONLY remaining rt_0 row
    expect(await chCount("_id = 'rt_0'")).toBe(1);
    expect(await chCount(`_id = 'rt_0' AND toUnixTimestamp64Milli(cd) = ${FLIP + 5_000}`)).toBe(1);
    expect(await chCount("_id LIKE 'rt_native_%'")).toBe(10);
  });

  it('duplicate sweep copies all subtract from native evidence — extras never masquerade as natives', async () => {
    // 5 migrated only-copies, 3 sweep rows, one of which has 5 duplicate
    // copies (ambiguous insert retries): liveTotal = 13, matched = 5.
    // Distinct-id subtraction would leave native = 13-5-3 = 5 >= 5 and
    // DELETE the only copies; row-count subtraction gives 13-5-8 = 0.
    const docs: Record<string, unknown>[] = [];
    const rows: Record<string, unknown>[] = [];
    for (let i = 0; i < 5; i++) {
      const cd = FLIP + i * 1_000;
      docs.push({ _id: `dsw_m_${i}`, uid: 'u', did: 'd', ts: cd, cd: new Date(cd), sg: {}, c: 1 });
      rows.push({ ...chRow(`dsw_m_${i}`, cd), a: APP5 });
    }
    for (let i = 0; i < 3; i++) {
      const cd = FLIP + 20_000 + i * 100;
      docs.push({ _id: `dsw_n_${i}`, uid: 'u', did: 'd', ts: cd, cd: null, sg: {}, c: 1 });
      rows.push({ ...chRow(`dsw_n_${i}`, cd), a: APP5 });
    }
    for (let k = 0; k < 5; k++) rows.push({ ...chRow('dsw_n_0', FLIP + 20_000), a: APP5 }); // duplicate sweep copies
    await mc.db(DB).collection(COLL5).insertMany(docs as never[]);
    await mc.db(DB).collection(COLL5).createIndex({ cd: 1, _id: 1 });
    await ch.insert({ table: `${DB}.drill_events`, values: rows, format: 'JSONEachRow' });

    const state = newDedupeOverlapState();
    await runDedupeOverlap({ config, logger, hashResolver }, state, { fromMs: FLIP, toMs: DONE, execute: true });
    expect(state.status).toBe('completed');
    const r = state.collections.find((c) => c.collection === COLL5);
    expect(r?.deleted).toBe(0);
    expect(r?.unsafe.every((u) => u.reason === 'no-native-evidence')).toBe(true);
    expect(await chCount("_id LIKE 'dsw_m_%'")).toBe(5); // only-copies survived

    // collapse the duplicate copies again — the duplicateStats test below
    // counts duplicate groups exactly
    await ch.command({ query: `DELETE FROM ${DB}.drill_events WHERE _id = 'dsw_n_0'` });
    await ch.insert({ table: `${DB}.drill_events`, values: [{ ...chRow('dsw_n_0', FLIP + 20_000), a: APP5 }], format: 'JSONEachRow' });
  });

  it('a run that lost the cluster-wide maintenance lease fails and grants no license', async () => {
    const state = newDedupeOverlapState();
    await runDedupeOverlap({ config, logger, hashResolver }, state, {
      fromMs: FLIP, toMs: DONE, execute: false, leaseLost: () => true,
    });
    expect(state.status).toBe('failed');
    expect(state.error).toContain('maintenance reservation was LOST');
    expect(state.lastDryRun).toBeNull();
  });

  it('duplicateStats counts migration-duplicate groups exactly, beyond the display-sample cap', async () => {
    // 25 duplicated ids below the boundary — more than the 20-group sample
    const rows: Record<string, unknown>[] = [];
    // two SIBLING-scope rows sharing an _id below the boundary: a legitimate
    // cross-collection id reuse, never a migration duplicate
    rows.push(chRow('xdup_scope', FLIP - 3_600_000), { ...chRow('xdup_scope', FLIP - 3_500_000), a: 'other_scope_app' });
    for (let i = 0; i < 25; i++) {
      const cd = FLIP - 7_200_000 + i * 1_000;
      rows.push(chRow(`dupg_${i}`, cd), chRow(`dupg_${i}`, cd + 1));
    }
    await ch.insert({ table: `${DB}.drill_events`, values: rows, format: 'JSONEachRow' });
    const staging = new StagingManager(
      { url: CH_URL, database: DB, table: 'drill_events', username: 'default', password: CH_PASSWORD, queryTimeoutMs: 30_000 },
      logger,
    );
    await staging.connect();
    try {
      const stats = await staging.duplicateStats(Date.now());
      expect(stats.migrationDuplicateGroups).toBe(25);
      expect(stats.sample.length).toBeLessThanOrEqual(20);
    } finally {
      await staging.close();
    }
  });
});
