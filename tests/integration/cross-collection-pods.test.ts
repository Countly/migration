/**
 * Cross-collection pod scheduling: many small collections, two pods.
 *
 * Chunks are mapped for ALL collections upfront and claimed globally, so
 * pods spill into the next collection the moment nothing is claimable in
 * the current one — instead of convoying through the collection list
 * together (the many-small-collections shape from the field: ~2,400
 * mostly-single-chunk collections at 12k docs/s on one pod).
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
const DB = 'test_mig_xcoll';
const RUN = 'xcoll-1';
const logger = pino({ level: 'silent' });

const APP = 'app_xcoll';
const COLLS = 8;
const DOCS_EACH = 400;
const BASE = Date.UTC(2026, 0, 1);
const events = Array.from({ length: COLLS }, (_, i) => `ev_${i}`);
const collOf = (ev: string) => `drill_events${createHash('sha1').update(ev + APP).digest('hex')}`;

describe('cross-collection scheduling with two pods', () => {
  let ch: ClickHouseClient;
  let mc: MongoClient;
  const pods: ChunkOrchestrator[] = [];
  const closers: Array<() => Promise<void>> = [];

  beforeAll(async () => {
    mc = new MongoClient(MONGO_URI);
    await mc.connect();
    await mc.db(DB).dropDatabase();
    await mc.db(`${DB}_countly`).dropDatabase();
    await mc.db(`${DB}_countly`).collection('apps').insertOne({ _id: APP } as never);
    await mc.db(`${DB}_countly`).collection('events').insertOne({ _id: APP, list: events } as never);

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

    for (const ev of events) {
      const coll = mc.db(DB).collection(collOf(ev));
      const docs = Array.from({ length: DOCS_EACH }, (_, j) => ({
        _id: `${ev}_${j}`, uid: String(j % 50), did: `d${j}`,
        ts: BASE + j * 60_000, cd: new Date(BASE + j * 60_000), sg: { v: j }, c: 1,
      }));
      await coll.insertMany(docs as never[]);
      await coll.createIndex({ cd: 1, _id: 1 });
    }

    process.env.SERVICE_NAME = 'xcoll';
    process.env.MONGO_URI = MONGO_URI;
    process.env.MONGO_DB = DB;
    process.env.MONGO_COUNTLY_DB = `${DB}_countly`;
    process.env.MANIFEST_DB = DB;
    process.env.CLICKHOUSE_URL = CH_URL;
    process.env.CLICKHOUSE_PASSWORD = CH_PASSWORD;
    process.env.CLICKHOUSE_DB = DB;
    process.env.LEDGER_RUN_ID = RUN;
    process.env.LEDGER_CHUNK_DOCS_TARGET = '100000'; // 1 chunk per collection
    process.env.MONGO_PAGE_SIZE = '100';             // slow pods down enough to overlap
    process.env.LEDGER_MONITOR_INTERVAL_MS = '0';
    process.env.BACKPRESSURE_ENABLED = 'false';
    process.env.MULTI_POD_ENABLED = 'true';
    const config = loadConfig();

    const shared = { ledger: new LedgerStore(MONGO_URI, DB, logger), dlq: new DlqStore(MONGO_URI, DB, logger) };
    await shared.ledger.connect();
    await shared.dlq.connect();
    closers.push(() => shared.ledger.close(), () => shared.dlq.close());

    for (const podId of ['pod-a', 'pod-b']) {
      const mongoReader = new MongoReader({
        uri: MONGO_URI, database: DB, readPreference: 'primary', readConcern: 'local',
        retryReads: true, appName: podId, cursorBatchSize: 100, maxTimeMs: 60_000,
      }, logger);
      const staging = new StagingManager({
        url: CH_URL, database: DB, table: 'drill_events', username: 'default', password: CH_PASSWORD, queryTimeoutMs: 60_000,
      }, logger);
      const hashResolver = new HashResolver({ uri: MONGO_URI, countlyDb: `${DB}_countly` }, logger);
      await mongoReader.connect();
      await staging.connect();
      await hashResolver.build();
      closers.push(() => mongoReader.close(), () => staging.close(), () => hashResolver.close());
      const podConfig = { ...config, worker: { ...config.worker, enabled: true, podId } };
      pods.push(new ChunkOrchestrator({
        config: podConfig as never, logger, mongoReader, ledger: shared.ledger, dlq: shared.dlq, staging,
        retryPolicy: new RetryPolicy({ maxRetries: 2, baseDelayMs: 50, maxDelayMs: 200 }), hashResolver,
      }));
    }
  }, 120_000);

  afterAll(async () => {
    for (const close of closers) await close().catch(() => {});
    await ch.command({ query: `DROP DATABASE IF EXISTS ${DB}` }).catch(() => {});
    await ch.close();
    await mc.db(DB).dropDatabase().catch(() => {});
    await mc.db(`${DB}_countly`).dropDatabase().catch(() => {});
    await mc.close();
  });

  it('two pods split single-chunk collections between them, exactly once each', async () => {
    await Promise.all(pods.map((p) => p.run()));

    const res = await ch.query({
      query: `SELECT count() AS t, uniqExact(_id) AS u FROM ${DB}.drill_events`,
      format: 'JSONEachRow',
    });
    const [row] = await res.json<{ t: string; u: string }>();
    expect(Number(row.t)).toBe(COLLS * DOCS_EACH);
    expect(Number(row.u)).toBe(COLLS * DOCS_EACH); // zero duplicates across racing pods

    // Global claiming means BOTH pods worked distinct collections in
    // parallel: with 8 single-chunk collections and interleaved page reads,
    // each pod must have completed several (the convoy scheduler would let
    // one pod monopolize while the other idles behind the collection gate).
    const chunks = await mc.db(DB).collection('mig_ranges').find({ run_id: RUN, status: 'done' }).toArray();
    expect(chunks.length).toBe(COLLS);
    const byPod = new Map<string, number>();
    for (const c of chunks) byPod.set(c.pod_id as string, (byPod.get(c.pod_id as string) ?? 0) + 1);
    expect(byPod.size).toBe(2);
    for (const [, n] of byPod) expect(n).toBeGreaterThanOrEqual(2);
  }, 180_000);

  it('top-up: data that arrived after mapping gets delta chunks and drains on the next run', async () => {
    // Old ingestion kept writing after the run: cd strictly beyond the
    // mapped upper bound (cd is assigned at write time).
    const DELTA = 150;
    const late = Date.UTC(2026, 1, 1); // beyond every mapped window
    for (const ev of [events[0], events[3]]) {
      const coll = mc.db(DB).collection(collOf(ev));
      const docs = Array.from({ length: DELTA }, (_, j) => ({
        _id: `${ev}_late_${j}`, uid: 'lu', did: 'ld',
        ts: late + j * 60_000, cd: new Date(late + j * 60_000), sg: { late: true }, c: 1,
      }));
      await coll.insertMany(docs as never[]);
    }

    // Resume: mapping detects the delta, appends chunks, drains them.
    await pods[0].run();

    const res = await ch.query({
      query: `SELECT count() AS t, uniqExact(_id) AS u FROM ${DB}.drill_events`,
      format: 'JSONEachRow',
    });
    const [row] = await res.json<{ t: string; u: string }>();
    expect(Number(row.t)).toBe(COLLS * DOCS_EACH + 2 * DELTA);
    expect(Number(row.u)).toBe(COLLS * DOCS_EACH + 2 * DELTA); // no dups: old windows untouched

    // Appended chunks continue the idx sequence and are done
    const appended = await mc.db(DB).collection('mig_ranges')
      .find({ run_id: RUN, collection: collOf(events[0]), idx: { $gte: 1 } }).toArray();
    expect(appended.length).toBeGreaterThanOrEqual(1);
    expect(appended.every((c) => c.status === 'done')).toBe(true);
  }, 180_000);
});
