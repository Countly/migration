/**
 * Tee-overlap dedupe: a run without the cd bound migrated the mirror's
 * re-ingested docs on top of natively ingested rows. Pinned here:
 *
 *  - dry run counts the duplicates exactly and deletes NOTHING
 *  - execute deletes precisely the id-matched rows inside the window:
 *    native rows and pre-window (legitimately migrated) rows survive
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pino from 'pino';
import { createHash } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { createClient, type ClickHouseClient } from '@clickhouse/client';

import { runDedupeOverlap, newDedupeOverlapState } from '../../src/runtime/dedupe-overlap.ts';
import { loadConfig } from '../../src/config/loader.ts';
import type { Config } from '../../src/config/schema.ts';

const MONGO_URI = 'mongodb://localhost:27017/?directConnection=true';
const CH_URL = process.env.TEST_CLICKHOUSE_URL ?? 'http://localhost:8123';
const CH_PASSWORD = process.env.TEST_CLICKHOUSE_PASSWORD ?? '';
const DB = 'test_mig_dedupe';
const logger = pino({ level: 'silent' });

const APP = 'app_dd';
const COLL = `drill_events${createHash('sha1').update('views' + APP).digest('hex')}`;

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

  const chCount = async (where = '1'): Promise<number> => {
    const res = await ch.query({ query: `SELECT count() AS n FROM ${DB}.drill_events WHERE ${where}`, format: 'JSONEachRow' });
    return Number((await res.json<{ n: string }>())[0].n);
  };

  beforeAll(async () => {
    mc = new MongoClient(MONGO_URI);
    await mc.connect();
    await mc.db(DB).dropDatabase();

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
    await mc.db(DB).collection(COLL).insertMany(mongoDocs as never[]);
    await mc.db(DB).collection(COLL).createIndex({ cd: 1, _id: 1 });
    await ch.insert({ table: `${DB}.drill_events`, values: chRows, format: 'JSONEachRow' });

    Object.assign(process.env, {
      SERVICE_NAME: 'dedupe-test',
      MONGO_URI, MONGO_DB: DB, MONGO_COUNTLY_DB: `${DB}_countly`, MANIFEST_DB: DB,
      CLICKHOUSE_URL: CH_URL, CLICKHOUSE_PASSWORD: CH_PASSWORD, CLICKHOUSE_DB: DB,
      LEDGER_RUN_ID: 'dedupe-1', BACKPRESSURE_ENABLED: 'false', MULTI_POD_ENABLED: 'false',
    });
    config = loadConfig();
  }, 120_000);

  afterAll(async () => {
    await ch.command({ query: `DROP DATABASE IF EXISTS ${DB}` }).catch(() => {});
    await ch.close();
    await mc.db(DB).dropDatabase().catch(() => {});
    await mc.close();
  });

  it('dry run counts the duplicates exactly and deletes nothing', async () => {
    const state = newDedupeOverlapState();
    await runDedupeOverlap({ config, logger }, state, { fromMs: FLIP, toMs: DONE, execute: false });
    expect(state.status).toBe('completed');
    expect(state.totals).toEqual({ mongoDocsInWindow: 150, chMatched: 150, deleted: 0 });
    expect(state.lastDryRun).toMatchObject({ fromMs: FLIP, toMs: DONE, chMatched: 150 });
    expect(await chCount()).toBe(200 + 150 + 150 + 10);
  });

  it('execute deletes exactly the migrated copies; native and pre-flip rows survive', async () => {
    const state = newDedupeOverlapState();
    await runDedupeOverlap({ config, logger }, state, { fromMs: FLIP, toMs: DONE, execute: true });
    expect(state.status).toBe('completed');
    expect(state.totals).toEqual({ mongoDocsInWindow: 150, chMatched: 150, deleted: 150 });
    expect(await chCount("_id LIKE 'mirror_%'")).toBe(0);
    expect(await chCount("_id LIKE 'native_%'")).toBe(160);
    expect(await chCount("_id LIKE 'hist_%'")).toBe(200);
  });
});
