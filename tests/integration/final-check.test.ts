/**
 * Final check — the interpreted sign-off. Pinned here:
 *
 *  - a clean, cutover-clamped tee run passes (PASS WITH NOTES: the excluded
 *    post-cutover tail is a note, never a problem)
 *  - the SAME data audited WITHOUT the clamp fails — proving the clamp is
 *    what turns a tee audit into a yes/no answer
 *  - pending DLQ docs surface as an action note and their windows are not
 *    double-flagged (unresolved accounting)
 *  - missing target rows / failed chunks / content mismatches each produce a
 *    FAIL with an action sentence
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pino from 'pino';
import { createHash } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { createClient, type ClickHouseClient } from '@clickhouse/client';

import { runFinalCheck, newFinalCheckResult } from '../../src/runtime/final-check.ts';
import { LedgerStore, type ChunkDoc } from '../../src/state/ledger-store.ts';
import { DlqStore } from '../../src/state/dlq-store.ts';
import { HashResolver } from '../../src/transform/hash-resolver.ts';
import { loadConfig } from '../../src/config/loader.ts';
import type { Config } from '../../src/config/schema.ts';

const MONGO_URI = 'mongodb://localhost:27017/?directConnection=true';
const CH_URL = process.env.TEST_CLICKHOUSE_URL ?? 'http://localhost:8123';
const CH_PASSWORD = process.env.TEST_CLICKHOUSE_PASSWORD ?? '';
const DB = 'test_mig_finalcheck';
const RUN = 'fc-1';
const logger = pino({ level: 'silent' });

const APP = 'app_fc';
const COLL = `drill_events${createHash('sha1').update('views' + APP).digest('hex')}`;
const MIN = 60_000;

// Timeline: 600 docs over 2 hours, then the cutover, then a teed tail —
// 100 mirrored copies on the Mongo side, 80 native rows on the CH side
// (different identities, different counts: exactly what a tee looks like).
const CUTOVER = Math.floor((Date.now() - 3_600_000) / MIN) * MIN;
const START = CUTOVER - 120 * MIN;

const chRow = (id: string, cdMs: number): Record<string, unknown> => ({
  a: APP, e: '[CLY]_custom', n: 'views', uid: 'u', did: 'd', _id: id,
  ts: new Date(cdMs).toISOString().replace('T', ' ').replace('Z', ''),
  cd: new Date(cdMs).toISOString().replace('T', ' ').replace('Z', ''),
  up: {}, sg: {}, c: 1, s: 0, dur: 0,
});

const contentClean = {
  contentAudit: async (samples = 500) => ({ sampled: samples, matched: samples, missing: 0, different: 0, mismatches: [] }),
};

describe('final check: the interpreted sign-off', () => {
  let ch: ClickHouseClient;
  let mc: MongoClient;
  let ledger: LedgerStore;
  let dlq: DlqStore;
  let hashResolver: HashResolver;
  let config: Config;

  const check = async (opts?: { cutoverMs?: number | null; orchestrator?: typeof contentClean }) => {
    const out = newFinalCheckResult();
    await runFinalCheck(
      { config, logger, ledger, dlq, hashResolver, orchestrator: opts?.orchestrator ?? contentClean },
      out,
      { cutoverMs: opts?.cutoverMs ?? null, samples: 100 },
    );
    expect(out.status).toBe('completed');
    return out;
  };

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

    const mongoDocs: Record<string, unknown>[] = [];
    const chRows: Record<string, unknown>[] = [];
    // migrated body: identical (_id, cd) on both sides
    for (let i = 0; i < 600; i++) {
      const cd = START + i * 12_000;
      mongoDocs.push({ _id: `m_${i}`, uid: 'u', did: 'd', ts: cd, cd: new Date(cd), sg: {}, c: 1 });
      chRows.push(chRow(`m_${i}`, cd));
    }
    // teed tail: mirrored copies in Mongo, different native rows in CH
    for (let i = 0; i < 100; i++) {
      const cd = CUTOVER + i * 500;
      mongoDocs.push({ _id: `mirror_${i}`, uid: 'u', did: 'd', ts: cd, cd: new Date(cd), sg: {}, c: 1 });
    }
    for (let i = 0; i < 80; i++) chRows.push(chRow(`native_${i}`, CUTOVER + 200 + i * 500));
    await mc.db(DB).collection(COLL).insertMany(mongoDocs as never[]);
    await mc.db(DB).collection(COLL).createIndex({ cd: 1, _id: 1 });
    await ch.insert({ table: `${DB}.drill_events`, values: chRows, format: 'JSONEachRow' });

    Object.assign(process.env, {
      SERVICE_NAME: 'finalcheck-test',
      MONGO_URI, MONGO_DB: DB, MONGO_COUNTLY_DB: `${DB}_countly`, MANIFEST_DB: DB,
      CLICKHOUSE_URL: CH_URL, CLICKHOUSE_PASSWORD: CH_PASSWORD, CLICKHOUSE_DB: DB,
      LEDGER_RUN_ID: RUN, BACKPRESSURE_ENABLED: 'false', MULTI_POD_ENABLED: 'false',
    });
    delete process.env.LEDGER_CD_UPPER_BOUND;
    config = loadConfig();

    ledger = new LedgerStore(MONGO_URI, DB, logger);
    dlq = new DlqStore(MONGO_URI, DB, logger);
    hashResolver = new HashResolver({ uri: MONGO_URI, countlyDb: `${DB}_countly` }, logger);
    await ledger.connect();
    await dlq.connect();
    await hashResolver.build();

    // the run's own record: one done chunk covering the migrated body
    const chunk: ChunkDoc = {
      _id: `${RUN}:${COLL}:0`, run_id: RUN, collection: COLL,
      scope_a: APP, scope_e: '[CLY]_custom', scope_n: 'views',
      idx: 0, lower_cd: START, upper_cd: CUTOVER, status: 'done',
      pod_id: null, lease_until: null, staging_table: null,
      docs_read: 600, docs_skipped: 0, rows_expected: 600,
      partitions: [], attached: [], attach_method: null, attempts: 1,
      last_error: null, transform_version: config.transform.version, updated_at: new Date(),
    };
    await ledger.replaceAllForRun(RUN, [chunk]);
  }, 120_000);

  afterAll(async () => {
    await ledger?.close().catch(() => {});
    await dlq?.close().catch(() => {});
    await hashResolver?.close?.().catch?.(() => {});
    await ch.command({ query: `DROP DATABASE IF EXISTS ${DB}` }).catch(() => {});
    await ch.close();
    await mc.db(DB).dropDatabase().catch(() => {});
    await mc.db(`${DB}_countly`).dropDatabase().catch(() => {});
    await mc.close();
  });

  it('clean tee run with cutover clamp → PASS WITH NOTES (tail excluded is a note, not a problem)', async () => {
    const out = await check({ cutoverMs: CUTOVER });
    expect(out.problems).toEqual([]);
    expect(out.verdict).toBe('PASS_WITH_NOTES');
    expect(out.notes.join(' ')).toContain('excluded');
    expect(out.audit?.excludedBeyondCutover).toBe(100);
    expect(out.audit?.mismatchedWindows).toEqual([]);
    expect(out.headline).toContain('Safe to decommission');
  });

  it('same data WITHOUT the clamp → FAIL: post-cutover divergence reads as data problems', async () => {
    const out = await check({ cutoverMs: null });
    expect(out.verdict).toBe('FAIL');
    expect(out.problems.length).toBeGreaterThan(0);
  });

  it('pending DLQ docs → FAIL (undecided = no sign-off); waiving turns it into a note', async () => {
    // one doc the run skipped: present in Mongo, absent in CH, recorded in DLQ
    await ch.command({ query: `DELETE FROM ${DB}.drill_events WHERE _id = 'm_10'` });
    await dlq.add([{
      run_id: RUN, collection: COLL, chunk_id: `${RUN}:${COLL}:0`, source_id: 'm_10',
      raw_doc: { _id: 'm_10' }, reason: 'skipped', error: 'skip:missing_a',
      transform_version: config.transform.version, cd_ms: START + 10 * 12_000,
    }]);
    const out = await check({ cutoverMs: CUTOVER });
    expect(out.verdict).toBe('FAIL');
    expect(out.problems.join(' ')).toContain('UNRESOLVED');
    // …but the DLQ'd doc's window is NOT double-flagged (unresolved accounting)
    expect(out.audit?.mismatchedWindows).toEqual([]);
    // waive = the decision was made → note, sign-off possible
    await dlq.waive(RUN);
    const out2 = await check({ cutoverMs: CUTOVER });
    expect(out2.verdict).toBe('PASS_WITH_NOTES');
    expect(out2.problems).toEqual([]);
    expect(out2.notes.join(' ')).toContain('waived');
  });

  it('missing target rows → FAIL with a do-not-decommission problem', async () => {
    await ch.command({ query: `DELETE FROM ${DB}.drill_events WHERE _id IN ('m_20','m_21','m_22')` });
    const out = await check({ cutoverMs: CUTOVER });
    expect(out.verdict).toBe('FAIL');
    expect(out.problems.join(' ')).toContain('FEWER');
    // restore for the next cases
    await ch.insert({
      table: `${DB}.drill_events`, format: 'JSONEachRow',
      values: [20, 21, 22].map((i) => chRow(`m_${i}`, START + i * 12_000)),
    });
  });

  it('summarize reports cluster-truth docsSkipped from the ledger', async () => {
    await mc.db(DB).collection('mig_ranges').updateOne({ _id: `${RUN}:${COLL}:0` } as never, { $set: { docs_skipped: 7 } });
    expect((await ledger.summarize(RUN)).docsSkipped).toBe(7);
  });

  it('content mismatch and failed chunks each FAIL with their own action line', async () => {
    const badContent = {
      contentAudit: async (samples = 500) => ({ sampled: samples, matched: samples - 2, missing: 1, different: 1, mismatches: [] }),
    };
    const out = await check({ cutoverMs: CUTOVER, orchestrator: badContent });
    expect(out.verdict).toBe('FAIL');
    expect(out.problems.join(' ')).toContain('Content sampling');

    await mc.db(DB).collection('mig_ranges').updateOne({ _id: `${RUN}:${COLL}:0` } as never, { $set: { status: 'failed' } });
    const out2 = await check({ cutoverMs: CUTOVER });
    expect(out2.problems.join(' ')).toContain('Retry failed chunks');
    await mc.db(DB).collection('mig_ranges').updateOne({ _id: `${RUN}:${COLL}:0` } as never, { $set: { status: 'done' } });
  });

  it('retention drift with masked missing docs → FAIL from the id spot-check', async () => {
    // retention deleted 8 source docs (live > source = drift) while one
    // MIGRATED row also vanished — the surplus count hides it from counting
    await mc.db(DB).collection(COLL).deleteMany({ _id: { $in: ['m_50', 'm_51', 'm_52', 'm_53', 'm_54', 'm_55', 'm_56', 'm_57'] } } as never);
    await ch.command({ query: `DELETE FROM ${DB}.drill_events WHERE _id = 'm_60'` });
    const out = await check({ cutoverMs: CUTOVER });
    expect(out.verdict).toBe('FAIL');
    expect(out.problems.join(' ')).toContain('masking');
    // clean drift (no masked gaps) stays a note
    await ch.insert({ table: `${DB}.drill_events`, format: 'JSONEachRow', values: [chRow('m_60', START + 60 * 12_000)] });
    const out2 = await check({ cutoverMs: CUTOVER });
    expect(out2.verdict).toBe('PASS_WITH_NOTES');
    expect(out2.notes.join(' ')).toContain('retained history');
  });

  it('a WHOLE window missing from the target → FAIL (the audit calls it pending, the check must not)', async () => {
    // stale ledger says done, but every row of the window is gone from CH
    await ch.command({ query: `DELETE FROM ${DB}.drill_events WHERE _id LIKE 'm\\_%'` });
    const out = await check({ cutoverMs: CUTOVER });
    expect(out.verdict).toBe('FAIL');
    expect(out.problems.join(' ')).toContain('ZERO rows');
  });
});
