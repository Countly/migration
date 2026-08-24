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

import { detectBoundary, newBoundaryProgress } from '../../src/runtime/boundary-detector.ts';
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

  it('refuses detection once the run has mapped chunks — sync parity still reports', async () => {
    await ledger.initChunks(RUN, COLL, [{ lowerCd: 0, upperCd: 1000 }], 'v2', null);
    const report = (await run())!;
    expect(report.detection.status).toBe('refused');
    expect(report.detection.reason).toContain('chunks');
    expect(report.sync.status).toBe('ok'); // parity is migration-agnostic
    await mc.db(DB).collection('mig_ranges').deleteMany({ run_id: RUN } as never);
  }, 60_000);
});
