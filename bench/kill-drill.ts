/**
 * Kill drill for the ledger engine: repeatedly SIGKILL the service at random
 * points mid-migration and restart it, until the run completes. Then verify:
 *   1. every source doc is accounted for (rows in CH == mongo docs - skipped)
 *   2. zero duplicates (count() == uniqExact(_id))
 *
 * This is the crash-safety half of the A/B: run it, then try the same thing
 * against the classic engine.
 *
 * Env: same AB_* vars as setup.ts, plus KILL_MIN_MS / KILL_MAX_MS (5000/20000).
 */
import { spawn } from 'node:child_process';
import { MongoClient } from 'mongodb';
import { createClient } from '@clickhouse/client';

const MONGO_URI = process.env.AB_MONGO_URI ?? 'mongodb://localhost:27017';
const MONGO_DB = process.env.AB_MONGO_DB ?? 'mig_ab';
const CH_URL = process.env.AB_CH_URL ?? 'http://localhost:8123';
const CH_DB = process.env.AB_CH_DB ?? 'mig_ab';
const PORT = Number(process.env.AB_PORT ?? 18081);
const KILL_MIN = Number(process.env.KILL_MIN_MS ?? 5_000);
const KILL_MAX = Number(process.env.KILL_MAX_MS ?? 20_000);
const MAX_ROUNDS = Number(process.env.KILL_MAX_ROUNDS ?? 60);

const ENGINE = (process.env.DRILL_ENGINE ?? 'ledger') as 'ledger' | 'classic';

const env: NodeJS.ProcessEnv = {
  ...process.env,
  MIGRATION_ENGINE: ENGINE,
  SERVICE_NAME: `kill-drill-${ENGINE}`,
  SERVICE_PORT: String(PORT),
  MONGO_URI,
  MONGO_DB,
  MANIFEST_DB: `${MONGO_DB}_manifest`,
  CLICKHOUSE_URL: CH_URL,
  CLICKHOUSE_DB: CH_DB,
  EXIT_ON_COMPLETE: 'true',
  LOG_LEVEL: 'warn',
  NODE_ENV: 'production',
};
if (ENGINE === 'ledger') {
  env.LEDGER_RUN_ID = process.env.LEDGER_RUN_ID ?? 'kill-drill-1';
  env.LEDGER_CHUNK_DOCS_TARGET = process.env.LEDGER_CHUNK_DOCS_TARGET ?? '25000';
  env.MULTI_POD_ENABLED = 'false';
} else {
  // classic needs Redis and resumes via manifest/Redis recovery
  if (!process.env.REDIS_URL) {
    console.error('DRILL_ENGINE=classic requires REDIS_URL');
    process.exit(1);
  }
  env.RERUN_MODE = 'resume';
}

function sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }

async function isComplete(): Promise<boolean> {
  try {
    const res = await fetch(`http://localhost:${PORT}/stats`);
    const stats = (await res.json()) as { status?: string };
    return stats.status === 'completed';
  } catch { return false; }
}

async function main() {
  let round = 0;
  let completed = false;

  while (!completed && round < MAX_ROUNDS) {
    round++;
    const child = spawn('node', ['--experimental-strip-types', 'src/main.ts'], {
      env, stdio: ['ignore', 'inherit', 'inherit'],
    });
    const exited = new Promise<number | null>((resolve) => child.on('exit', resolve));

    const killAfter = KILL_MIN + Math.random() * (KILL_MAX - KILL_MIN);
    const timer = setTimeout(() => {
      console.log(`\n[drill] round ${round}: SIGKILL after ${(killAfter / 1000).toFixed(1)}s`);
      child.kill('SIGKILL');
    }, killAfter);

    const code = await exited;
    clearTimeout(timer);

    if (code === 0) {
      completed = true;
      console.log(`[drill] round ${round}: service completed and exited cleanly`);
    } else {
      // Killed (or crashed) — brief pause, then restart-resume.
      await sleep(500);
    }
    void isComplete;
  }

  if (!completed) {
    console.error(`[drill] did not complete within ${MAX_ROUNDS} rounds`);
    process.exit(1);
  }

  // ── Verification ──
  const mc = new MongoClient(MONGO_URI);
  await mc.connect();
  const mongoDocs = await mc.db(MONGO_DB).collection('drill_events').countDocuments();
  await mc.close();

  const ch = createClient({ url: CH_URL, database: CH_DB });
  const res = await ch.query({
    query: `SELECT count() AS total, uniqExact(_id) AS distinct_ids FROM ${CH_DB}.drill_events`,
    format: 'JSONEachRow',
  });
  const [row] = await res.json<{ total: string; distinct_ids: string }>();
  await ch.close();

  const total = Number(row.total);
  const distinct = Number(row.distinct_ids);
  const dups = total - distinct;
  const missing = mongoDocs - total; // generator produces no skippable docs

  console.log(`\n[drill] RESULT after ${round} rounds (${round - 1} kills):`);
  console.log(`  mongo source docs:   ${mongoDocs}`);
  console.log(`  clickhouse rows:     ${total}`);
  console.log(`  distinct _ids:       ${distinct}`);
  console.log(`  duplicates:          ${dups}`);
  console.log(`  missing:             ${missing}`);
  const pass = dups === 0 && missing === 0;
  console.log(pass ? '  ✅ PASS — zero loss, zero duplicates' : '  ❌ FAIL');
  process.exit(pass ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
