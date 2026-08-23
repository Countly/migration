/**
 * Chaos-test worker: a REAL migration pod as an OS process, wired exactly
 * like the integration fixtures (no HTTP server — many pods share one host).
 * The harness SIGKILLs it at random moments; consistency is asserted on the
 * end state, never on any individual worker's fate.
 *
 * CHAOS_CRASH_AFTER_ATTACH=1 arms the torn-commit kill: SIGKILL immediately
 * after the first successful ATTACH PARTITION and BEFORE recordAttached —
 * the narrowest recovery window in the pipeline (rows are live, the ledger
 * doesn't know it; recovery must pair-check instead of re-attaching).
 */
import pino from 'pino';
import { loadConfig } from '../../src/config/loader.ts';
import { LedgerStore } from '../../src/state/ledger-store.ts';
import { DlqStore } from '../../src/state/dlq-store.ts';
import { StagingManager } from '../../src/target/staging-manager.ts';
import { MongoReader } from '../../src/source/mongo-reader.ts';
import { RetryPolicy } from '../../src/runtime/retry-policy.ts';
import { HashResolver } from '../../src/transform/hash-resolver.ts';
import { ChunkOrchestrator } from '../../src/runtime/chunk-orchestrator.ts';

const config = loadConfig();
const logger = pino({ level: process.env.CHAOS_LOG_LEVEL ?? 'silent' });

if (process.env.CHAOS_CRASH_AFTER_ATTACH === '1') {
  const orig = StagingManager.prototype.attachPartition;
  StagingManager.prototype.attachPartition = async function (stagingTable: string, partitionId: string): Promise<void> {
    await orig.call(this, stagingTable, partitionId);
    process.kill(process.pid, 'SIGKILL'); // dead before recordAttached
    await new Promise<void>(() => {}); // unreachable; SIGKILL is immediate
  };
}

const uri = process.env.MONGO_URI!;
const db = process.env.MONGO_DB!;

const mongoReader = new MongoReader({
  uri, database: db, readPreference: 'primary', readConcern: 'local',
  retryReads: true, appName: config.worker.podId || 'chaos-pod', cursorBatchSize: 500, maxTimeMs: 60_000,
}, logger);
const ledger = new LedgerStore(uri, process.env.MANIFEST_DB!, logger);
const dlq = new DlqStore(uri, process.env.MANIFEST_DB!, logger);
const staging = new StagingManager({
  url: process.env.CLICKHOUSE_URL!, database: process.env.CLICKHOUSE_DB!, table: 'drill_events',
  username: 'default', password: process.env.CLICKHOUSE_PASSWORD ?? '', queryTimeoutMs: config.target.queryTimeoutMs,
}, logger);
const hashResolver = new HashResolver({ uri, countlyDb: process.env.MONGO_COUNTLY_DB! }, logger);

await mongoReader.connect();
await ledger.connect();
await dlq.connect();
await staging.connect();
await hashResolver.build();

const orchestrator = new ChunkOrchestrator({
  config, logger, mongoReader, ledger, dlq, staging,
  // Real config-driven policy (CLICKHOUSE_MAX_RETRIES etc.) — the outage
  // chaos test relies on prod-like backoff absorbing multi-second blips;
  // pod-kill tests override via env for speed.
  retryPolicy: new RetryPolicy({
    maxRetries: config.target.maxRetries,
    baseDelayMs: config.target.retryBaseDelayMs,
    maxDelayMs: config.target.retryMaxDelayMs,
  }), hashResolver,
});
await orchestrator.run();
const stats = orchestrator.getStats();
process.stdout.write(JSON.stringify({
  status: stats.status, chunksDone: stats.chunksDone, chunksFailed: stats.chunksFailed,
}) + '\n');
process.exit(stats.status === 'completed' ? 0 : 2);
