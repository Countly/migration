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
    await mc.db(`${DB}_countly`).collection('apps').insertMany([{ _id: APP }, { _id: APP2 }] as never[]);
    await mc.db(`${DB}_countly`).collection('events').insertMany([
      { _id: APP, list: ['views'] }, { _id: APP2, list: ['views'] },
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

  it('duplicateStats counts migration-duplicate groups exactly, beyond the display-sample cap', async () => {
    // 25 duplicated ids below the boundary — more than the 20-group sample
    const rows: Record<string, unknown>[] = [];
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
