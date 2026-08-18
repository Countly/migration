/**
 * Ledger engine tests: classifier (pure), coercions (pure), LedgerStore
 * claim/lease/transition semantics, and an end-to-end chunk pipeline run
 * against real MongoDB + ClickHouse — asserting exact counts, DLQ capture
 * with raw docs, and coercion accounting. No Redis anywhere.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import pino from 'pino';
import { MongoClient } from 'mongodb';
import { createClient, type ClickHouseClient } from '@clickhouse/client';

import { classifyError } from '../../src/runtime/error-classifier.ts';
import { CoercionCounter } from '../../src/transform/coercions.ts';
import { sanitizeJsonValue } from '../../src/transform/validators.ts';
import { transformDocument } from '../../src/transform/normalize.ts';
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
const DB = 'test_mig_ledger';
const logger = pino({ level: 'silent' });

// ---------------------------------------------------------------------------
// Pure units
// ---------------------------------------------------------------------------

describe('error-classifier', () => {
  it('classifies ClickHouse data-error codes as permanent', () => {
    for (const code of ['41', '53', '72', '117', '6']) {
      expect(classifyError({ code, message: 'x' })).toBe('permanent');
    }
  });
  it('classifies network errors as transient', () => {
    expect(classifyError({ code: 'ECONNRESET', message: 'socket hang up' })).toBe('transient');
    expect(classifyError({ code: 'ETIMEDOUT', message: '' })).toBe('transient');
  });
  it('classifies BigInt serialization as permanent', () => {
    expect(classifyError(new TypeError('Do not know how to serialize a BigInt'))).toBe('permanent');
  });
  it('defaults unknown errors to transient', () => {
    expect(classifyError(new Error('some novel failure'))).toBe('transient');
    expect(classifyError({ code: '999', message: 'unknown CH code' })).toBe('transient');
  });
});

describe('coercions (shared spec: only non-JSON-carriable values change)', () => {
  it('stringifies NaN/Infinity/bigint losslessly; finite large doubles STAY numbers (matches live ingestion)', () => {
    const out = sanitizeJsonValue({ ok: 42, big: 9.2e25, nan: NaN, inf: Infinity, huge: 10n ** 30n, str: 'hello' }) as Record<string, unknown>;
    expect(out.nan).toBe('NaN');
    expect(out.inf).toBe('Infinity');
    expect(out.huge).toBe('1000000000000000000000000000000');
    expect(out.big).toBe(9.2e25);   // finite double: live keeps it numeric — so do we
    expect(out.ok).toBe(42);
  });
  it('counts coercions per key through the transform', () => {
    const counter = new CoercionCounter();
    const { row } = transformDocument(
      { _id: 'x', a: 'app', e: 'ev', uid: 'u1', ts: 1750000000000, sg: { weird: NaN } },
      undefined,
      counter,
    );
    expect((row?.sg as Record<string, unknown>).weird).toBe('NaN');
    expect(counter.getTotal()).toBe(1);
    expect(counter.getReport()[0].rule_key).toBe('stringify_nonfinite:sg');
  });
  it('caps distinct coercion keys; the overflow bucket keeps totals exact', () => {
    const counter = new CoercionCounter();
    for (let i = 0; i < 10_050; i++) counter.record('stringify_nonfinite', `field_${i}`, NaN, 'NaN');
    expect(counter.getTotal()).toBe(10_050);
    const report = counter.getReport();
    expect(report.length).toBe(10_001); // 10k distinct + 1 overflow bucket
    expect(report.some((r) => r.rule_key === 'other:distinct-key-cap-reached' && r.count === 50)).toBe(true);
  });

  it('clamps the Countly-owned counter c to UInt32', () => {
    const counter = new CoercionCounter();
    const { row } = transformDocument(
      { _id: 'x', a: 'app', e: 'ev', uid: 'u1', ts: 1750000000000, c: 99_999_999_999 },
      undefined,
      counter,
    );
    expect(row?.c).toBe(4_294_967_295);
    expect(counter.getTotal()).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// LedgerStore semantics (real MongoDB)
// ---------------------------------------------------------------------------

describe('LedgerStore', () => {
  let ledger: LedgerStore;

  beforeAll(async () => {
    ledger = new LedgerStore(MONGO_URI, DB, logger);
    await ledger.connect();
  });
  afterAll(async () => {
    const mc = new MongoClient(MONGO_URI);
    await mc.connect();
    await mc.db(DB).dropDatabase();
    await mc.close();
    await ledger.close();
  });

  it('initChunks is idempotent and claims are newest-first with a lease', async () => {
    const bounds = [0, 1, 2].map((i) => ({ lowerCd: i * 1000, upperCd: (i + 1) * 1000 }));
    expect(await ledger.initChunks('r1', 'coll', bounds, 'v1')).toBe(3);
    expect(await ledger.initChunks('r1', 'coll', bounds, 'v1')).toBe(0); // no-op

    const first = await ledger.claimNext('r1', 'coll', 'podA', 60);
    expect(first?.idx).toBe(2); // newest first
    expect(first?.status).toBe('in_progress');
    expect(first?.lease_until!.getTime()).toBeGreaterThan(Date.now());

    const second = await ledger.claimNext('r1', 'coll', 'podB', 60);
    expect(second?.idx).toBe(1); // podA's claim is not re-claimable
  });

  it('guarded transitions reject wrong from-state', async () => {
    const moved = await ledger.transition('r1:coll:2', 'pending', 'done');
    expect(moved).toBeNull(); // it's in_progress, not pending
    const ok = await ledger.transition('r1:coll:2', 'in_progress', 'written', { rows_expected: 10 });
    expect(ok?.status).toBe('written');
  });

  it('findRecoverable honors lease expiry for multi-pod reclaim', async () => {
    // Nothing expired yet
    expect((await ledger.findRecoverable('r1', 'coll', false)).length).toBe(0);
    // includeAll (single-pod startup) sees all non-terminal chunks
    const all = await ledger.findRecoverable('r1', 'coll', true);
    expect(all.length).toBe(2); // idx2 written + idx1 in_progress
  });
});

// ---------------------------------------------------------------------------
// End-to-end chunk pipeline (real MongoDB + ClickHouse, no Redis)
// ---------------------------------------------------------------------------

describe('ledger engine end-to-end', () => {
  const CLEAN_DOCS = 2_000;
  let ch: ClickHouseClient;
  let mc: MongoClient;
  let orchestrator: ChunkOrchestrator;
  let dlq: DlqStore;
  const closers: Array<() => Promise<void>> = [];

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

    // Seed: clean docs + 3 transform-poisoned docs (bad ts) + 1 coercion doc
    const coll = mc.db(DB).collection('drill_events');
    const base = Date.UTC(2026, 0, 1);
    const docs: Record<string, unknown>[] = [];
    for (let i = 0; i < CLEAN_DOCS; i++) {
      const ts = base + i * 60_000;
      docs.push({
        _id: `doc_${i}`, a: 'app1', e: i % 3 === 0 ? '[CLY]_view' : 'my_event',
        uid: String(i % 50), did: `d${i}`, ts, cd: new Date(ts),
        up: { p: 'iOS' }, sg: i % 3 === 0 ? { name: '/home' } : { price: 9.99 }, c: 1,
      });
    }
    for (let i = 0; i < 3; i++) {
      docs.push({ _id: `poison_${i}`, a: 'app1', e: 'bad', uid: 'u', ts: 'not-a-ts', cd: new Date(base + i) });
    }
    docs.push({
      _id: 'coerce_me', a: 'app1', e: 'big_int_event', uid: 'u9', did: 'd9',
      ts: base + 1, cd: new Date(base + 1), sg: { order_id: 9.2e25, weird: Number.POSITIVE_INFINITY }, c: 1,
    });
    // Docs with no cd value — must be picked up by the null-cd sweep chunk
    docs.push({ _id: 'nocd_1', a: 'app1', e: 'legacy_event', uid: 'u1', did: 'd', ts: base - 86_400_000 });
    docs.push({ _id: 'nocd_2', a: 'app1', e: 'legacy_event', uid: 'u2', did: 'd', ts: base - 86_400_000, cd: null });
    // REGRESSION: null-cd doc whose derived cd (from ts) lands INSIDE the
    // regular chunks' windows. If the sweep ran before regular chunks, its
    // row would poison their verify-then-attach check → silently missing
    // data. The sweep must be ordered strictly last.
    docs.push({ _id: 'nocd_overlap', a: 'app1', e: 'legacy_event', uid: 'u3', did: 'd', ts: base + 120_000 });
    await coll.insertMany(docs as never[]);
    await coll.createIndex({ cd: 1, _id: 1 });

    // Engine wiring (mirrors ledger-engine.ts, minus HTTP)
    process.env.SERVICE_NAME = 'ledger-e2e';
    process.env.MONGO_URI = MONGO_URI;
    process.env.MONGO_DB = DB;
    process.env.MANIFEST_DB = DB;
    process.env.CLICKHOUSE_URL = CH_URL;
    process.env.CLICKHOUSE_PASSWORD = CH_PASSWORD;
    process.env.CLICKHOUSE_DB = DB;
    process.env.LEDGER_RUN_ID = 'e2e-1';
    process.env.LEDGER_CHUNK_DOCS_TARGET = '500';
    process.env.LEDGER_MONITOR_INTERVAL_MS = '0';
    process.env.BACKPRESSURE_ENABLED = 'false';
    const config = loadConfig();

    const mongoReader = new MongoReader({
      uri: MONGO_URI, database: DB, readPreference: 'primary', readConcern: 'local',
      retryReads: true, appName: 'e2e', cursorBatchSize: 500, maxTimeMs: 60_000,
    }, logger);
    const ledger = new LedgerStore(MONGO_URI, DB, logger);
    dlq = new DlqStore(MONGO_URI, DB, logger);
    const staging = new StagingManager({
      url: CH_URL, database: DB, table: 'drill_events', username: 'default', password: CH_PASSWORD, queryTimeoutMs: 60_000,
    }, logger);
    const retryPolicy = new RetryPolicy({ maxRetries: 2, baseDelayMs: 50, maxDelayMs: 200 });
    const hashResolver = new HashResolver({ uri: MONGO_URI, countlyDb: `${DB}_countly` }, logger);

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
    await ch.command({ query: `DROP DATABASE IF EXISTS ${DB}` }).catch(() => {});
    await ch.close();
    await mc.db(DB).dropDatabase().catch(() => {});
    await mc.close();
  });

  it('migrates exactly: clean docs land once, poisoned docs go to DLQ with raw docs, coercions counted', async () => {
    await orchestrator.run();

    const res = await ch.query({
      query: `SELECT count() AS t, uniqExact(_id) AS u FROM ${DB}.drill_events`,
      format: 'JSONEachRow',
    });
    const [row] = await res.json<{ t: string; u: string }>();
    // clean docs + coercion doc + 3 null-cd docs land; the 3 poisoned do not
    expect(Number(row.t)).toBe(CLEAN_DOCS + 4);
    expect(Number(row.u)).toBe(CLEAN_DOCS + 4); // zero duplicates

    // Null-cd docs arrived via the sweep chunk
    const nocd = await ch.query({
      query: `SELECT count() AS c FROM ${DB}.drill_events WHERE _id LIKE 'nocd_%'`,
      format: 'JSONEachRow',
    });
    expect(Number((await nocd.json<{ c: string }>())[0].c)).toBe(3);

    // DLQ carries the poisoned docs WITH their raw source docs
    const pending = await dlq.listPending('e2e-1');
    expect(pending.length).toBe(3);
    expect(pending.every((p) => p.reason === 'skipped' && p.error === 'skip:invalid_ts')).toBe(true);
    expect(pending.every((p) => typeof p.raw_doc === 'object' && p.raw_doc.ts === 'not-a-ts')).toBe(true);

    // Spec behavior: finite large double stays numeric; Infinity stringified
    const coerced = await ch.query({
      query: `SELECT sg.order_id AS v, sg.weird AS w FROM ${DB}.drill_events WHERE _id = 'coerce_me'`,
      format: 'JSONEachRow',
    });
    const [c] = await coerced.json<{ v: unknown; w: unknown }>();
    expect(Number(c.v)).toBe(9.2e25);
    expect(String(c.w)).toBe('Infinity');

    const stats = orchestrator.getStats();
    expect(stats.totalCoercions).toBeGreaterThanOrEqual(1);
    expect(stats.chunksFailed).toBe(0);
    expect(stats.status).toBe('completed');

    const report = await orchestrator.getReport();
    expect((report.dlq as { byStatus: Record<string, number> }).byStatus.pending).toBe(3);
  }, 120_000);

  it('replayDlq keeps still-broken docs pending with an updated error', async () => {
    const { replayed, stillFailing } = await orchestrator.replayDlq();
    expect(replayed).toBe(0);       // bad ts still fails transform under same version
    expect(stillFailing).toBe(3);
    const pending = await dlq.listPending('e2e-1');
    expect(pending.length).toBe(3);
    expect(pending[0].error).toContain('still fails transform');
  }, 60_000);

  it('waive is the terminal operator decision: pending drains, raw docs retained', async () => {
    const waived = await dlq.waive('e2e-1');
    expect(waived).toBe(3);
    expect((await dlq.listPending('e2e-1')).length).toBe(0);
    const byStatus = await dlq.countByStatus('e2e-1');
    expect(byStatus.waived).toBe(3); // still in the DLQ as the record of what was excluded
  }, 30_000);
});
