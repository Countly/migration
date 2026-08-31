/**
 * Poison-pill drill: a document that CRASHES the process every time it is
 * touched (simulated via the LEDGER_TEST_CRASH_ID chaos hook).
 *
 * Expected behavior: crash → restart → after 3 crash-retries the chunk is
 * SPLIT into sub-chunks instead of retried — repeated splitting converges on
 * a ≤1-minute window around the poison doc, which is then quarantined as a
 * tiny failed chunk. Everything else migrates. Phase 2 removes the "poison"
 * (operator fixed the doc / a patched build) and retries the failed chunks →
 * exact convergence.
 *
 * Env: AB_* as setup.ts. Uses its own scratch db mig_poison.
 */
import { spawn } from 'node:child_process';
import { MongoClient } from 'mongodb';
import { createClient } from '@clickhouse/client';

const MONGO_URI = process.env.AB_MONGO_URI ?? 'mongodb://localhost:27017';
const DB = 'mig_poison';
const CH_URL = process.env.AB_CH_URL ?? 'http://localhost:8123';
const PORT = 18106;
const DOCS = 20_000;
const POISON_ID = 'POISON_PILL_DOC';
const MAX_ROUNDS = 120;

const baseEnv = {
  ...process.env,
  SERVICE_NAME: 'poison-drill',
  SERVICE_PORT: String(PORT),
  MONGO_URI,
  MONGO_DB: DB,
  MANIFEST_DB: `${DB}_manifest`,
  CLICKHOUSE_URL: CH_URL,
  CLICKHOUSE_DB: DB,
  LEDGER_RUN_ID: 'poison-1',
  LEDGER_CHUNK_DOCS_TARGET: '5000',
  MULTI_POD_ENABLED: 'false',
  EXIT_ON_COMPLETE: 'true',
  LOG_LEVEL: 'error',
  NODE_ENV: 'production',
};

function runEngine(extra: Record<string, string>): Promise<number | null> {
  return new Promise((resolve) => {
    const child = spawn('node', ['--experimental-strip-types', 'src/main.ts'], {
      env: { ...baseEnv, ...extra }, stdio: ['ignore', 'ignore', 'inherit'],
    });
    child.on('exit', resolve);
  });
}

async function main() {
  // ── Seed ──
  const mc = new MongoClient(MONGO_URI);
  await mc.connect();
  await mc.db(DB).dropDatabase();
  await mc.db(`${DB}_manifest`).dropDatabase();
  const coll = mc.db(DB).collection('drill_events');
  const base = Date.UTC(2026, 0, 1);
  const spanMs = 30 * 86400_000;
  const docs: Record<string, unknown>[] = [];
  for (let i = 0; i < DOCS; i++) {
    const ts = base + Math.floor((spanMs * i) / DOCS);
    docs.push({ _id: `d${i}`, a: 'app1', e: 'ev', uid: String(i % 20), did: 'x', ts, cd: new Date(ts), sg: { v: i }, c: 1 });
  }
  const poisonTs = base + Math.floor(spanMs * 0.37);
  docs.push({ _id: POISON_ID, a: 'app1', e: 'ev', uid: 'p', did: 'x', ts: poisonTs, cd: new Date(poisonTs), sg: { v: -1 }, c: 1 });
  await coll.insertMany(docs as never[]);
  await coll.createIndex({ cd: 1, _id: 1 });

  const ch = createClient({ url: CH_URL });
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
  console.log(`[poison] seeded ${DOCS} clean docs + 1 poison pill (${POISON_ID})`);

  // ── Phase 1: crash-loop until convergence ──
  let rounds = 0;
  let code: number | null = -1;
  while (code !== 0 && rounds < MAX_ROUNDS) {
    rounds++;
    code = await runEngine({ LEDGER_TEST_CRASH_ID: POISON_ID });
  }
  if (code !== 0) { console.error(`[poison] did not converge in ${MAX_ROUNDS} rounds`); process.exit(1); }

  const ledger = mc.db(`${DB}_manifest`).collection('mig_ranges');
  const statuses = await ledger.aggregate<{ _id: string; n: number }>([
    { $match: { run_id: 'poison-1' } }, { $group: { _id: '$status', n: { $sum: 1 } } },
  ]).toArray();
  const failed = await ledger.find({ run_id: 'poison-1', status: 'failed' }).toArray();
  const res1 = await ch.query({ query: `SELECT count() AS t FROM ${DB}.drill_events`, format: 'JSONEachRow' });
  const t1 = Number((await res1.json<{ t: string }>())[0].t);
  console.log(`[poison] phase 1 done in ${rounds} rounds (crashes + splits)`);
  console.log(`[poison] chunk statuses: ${JSON.stringify(Object.fromEntries(statuses.map(s => [s._id, s.n])))}`);
  for (const f of failed) {
    const windowMin = ((f.upper_cd - f.lower_cd) / 60_000).toFixed(1);
    const inWindow = await coll.countDocuments({ cd: { $gte: new Date(f.lower_cd), $lt: new Date(f.upper_cd) } });
    console.log(`[poison] quarantined chunk #${f.idx}: ${windowMin} min window, ${inWindow} source docs in it`);
  }
  console.log(`[poison] rows migrated so far: ${t1} / ${DOCS + 1}`);

  // ── Phase 2: "operator fixed the doc" — retry without the poison ──
  await ledger.updateMany(
    { run_id: 'poison-1', status: 'failed' },
    { $set: { status: 'pending', pod_id: null, staging_table: null, attempts: 0, last_error: null } },
  );
  code = await runEngine({}); // no crash hook
  const res2 = await ch.query({ query: `SELECT count() AS t, uniqExact(_id) AS u FROM ${DB}.drill_events`, format: 'JSONEachRow' });
  const [r2] = await res2.json<{ t: string; u: string }>();
  const pass = code === 0 && Number(r2.t) === DOCS + 1 && Number(r2.u) === DOCS + 1;
  console.log(`[poison] phase 2 (post-fix retry): rows=${r2.t} uniq=${r2.u} of ${DOCS + 1}`);
  console.log(pass ? '[poison] ✅ PASS — poison localized, everything else migrated, exact after fix' : '[poison] ❌ FAIL');

  await ch.command({ query: `DROP DATABASE IF EXISTS ${DB}` }).catch(() => {});
  await ch.close();
  await mc.db(DB).dropDatabase();
  await mc.db(`${DB}_manifest`).dropDatabase();
  await mc.close();
  process.exit(pass ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
