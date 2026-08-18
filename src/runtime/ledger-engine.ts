/**
 * Bootstrap for the `ledger` engine (MIGRATION_ENGINE=ledger).
 *
 * Dependencies: MongoDB + ClickHouse. Deliberately NO Redis, no async batch
 * writer, no per-batch manifest machinery — the chunk ledger (LedgerStore)
 * is the only persistent state, and it is verified, not trusted (see
 * ChunkOrchestrator).
 */

import Fastify from 'fastify';
import type { Logger } from 'pino';
import { createClient as createClickHouseClient } from '@clickhouse/client';
import type { Config } from '../config/schema.ts';
import { MongoReader } from '../source/mongo-reader.ts';
import { HashResolver } from '../transform/hash-resolver.ts';
import { RetryPolicy } from './retry-policy.ts';
import { LedgerStore } from '../state/ledger-store.ts';
import { DlqStore } from '../state/dlq-store.ts';
import { StagingManager } from '../target/staging-manager.ts';
import { ClickHousePressure } from '../target/clickhouse-pressure.ts';
import { ChunkOrchestrator } from './chunk-orchestrator.ts';
import { wireExitOnComplete } from './exit-on-complete.ts';
import { rebuildLedger, newRebuildProgress } from './ledger-rebuild.ts';

export async function runLedgerEngine(config: Config, logger: Logger): Promise<void> {
  logger.info({ engine: 'ledger', runId: config.ledger.runId }, 'Starting ledger engine (no Redis)');

  // Read preference 'auto' (the default): pick secondaryPreferred on replica
  // sets — the source is frozen after cutover, so secondary reads are exact
  // and the days-long scan stays off the primary. Explicit env wins.
  if (config.source.readPreference === 'auto') {
    const { MongoClient } = await import('mongodb');
    const probe = new MongoClient(config.source.uri);
    try {
      await probe.connect();
      const hello = await probe.db('admin').command({ hello: 1 });
      config.source.readPreference = hello.setName ? 'secondaryPreferred' : 'primary';
      config.source.readPreferenceAuto = true;
      logger.info(
        { readPreference: config.source.readPreference, replicaSet: hello.setName ?? null },
        'Read preference auto-selected',
      );
    } catch {
      config.source.readPreference = 'primary';
    } finally {
      await probe.close().catch(() => {});
    }
  }

  const mongoReader = new MongoReader(
    {
      uri: config.source.uri,
      database: config.source.db,
      readPreference: config.source.readPreference,
      readConcern: config.source.readConcern,
      retryReads: config.source.retryReads,
      appName: config.source.appName ?? config.service.name,
      cursorBatchSize: config.source.cursorBatchSize,
      maxTimeMs: config.source.maxTimeMs,
    },
    logger,
  );

  const ledger = new LedgerStore(config.source.uri, config.state.manifestDb, logger);
  const dlq = new DlqStore(config.source.uri, config.state.manifestDb, logger);

  const staging = new StagingManager(
    {
      url: config.target.url,
      database: config.target.db,
      table: config.target.table,
      username: config.target.username,
      password: config.target.password,
      queryTimeoutMs: config.target.queryTimeoutMs,
    },
    logger,
  );

  const retryPolicy = new RetryPolicy({
    maxRetries: config.target.maxRetries,
    baseDelayMs: config.target.retryBaseDelayMs,
    maxDelayMs: config.target.retryMaxDelayMs,
  });

  const hashResolver = new HashResolver(
    { uri: config.source.uri, countlyDb: config.source.countlyDb },
    logger,
  );

  await mongoReader.connect();
  await ledger.connect();
  await dlq.connect();
  await staging.connect();
  await hashResolver.build();
  logger.info('Ledger engine: all services connected (MongoDB + ClickHouse only)');

  // Backpressure sampler (TTL-cached inside the orchestrator — never per-batch)
  const pressureClient = createClickHouseClient({
    url: config.target.url,
    database: config.target.db,
    username: config.target.username,
    password: config.target.password,
    request_timeout: config.target.queryTimeoutMs,
  });
  const serverLimits = await ClickHousePressure.fetchServerLimits(pressureClient, logger);
  config.backpressure.partsToThrowInsert = serverLimits.partsToThrowInsert;
  config.backpressure.maxPartsInTotal = serverLimits.maxPartsInTotal;
  const chPressure = new ClickHousePressure(pressureClient, config.backpressure, logger);

  const orchestrator = new ChunkOrchestrator({
    config,
    logger,
    mongoReader,
    ledger,
    dlq,
    staging,
    retryPolicy,
    hashResolver,
    chPressure,
  });

  // ── In-process dry-run runner (UI action) ─────────────────────────────
  // Uses its OWN MongoReader/StagingManager so it can never disturb the main
  // orchestrator's collection binding. Allowed only while the main run is
  // not actively copying.
  const dryState: { status: string; stats: Record<string, unknown> | null; error: string | null } =
    { status: 'not_run', stats: null, error: null };

  async function startDryRun(): Promise<{ started: boolean; reason?: string }> {
    if (dryState.status === 'running') return { started: false, reason: 'dry run already running' };
    if (orchestrator.getStatus() === 'running') return { started: false, reason: 'main migration is running — pause or wait for completion first' };
    dryState.status = 'running'; dryState.error = null;

    const dryConfig: Config = { ...config, ledger: { ...config.ledger, dryRun: true } };
    const dryReader = new MongoReader(
      {
        uri: config.source.uri, database: config.source.db,
        readPreference: config.source.readPreference, readConcern: config.source.readConcern,
        retryReads: config.source.retryReads, appName: `${config.service.name}-dry`,
        cursorBatchSize: config.source.cursorBatchSize, maxTimeMs: config.source.maxTimeMs,
      },
      logger,
    );
    const dryStaging = new StagingManager(
      {
        url: config.target.url, database: config.target.db, table: config.target.table,
        username: config.target.username, password: config.target.password,
        queryTimeoutMs: config.target.queryTimeoutMs,
      },
      logger,
    );
    void (async () => {
      try {
        await dryReader.connect();
        await dryStaging.connect();
        const dryOrch = new ChunkOrchestrator({
          config: dryConfig, logger, mongoReader: dryReader, ledger, dlq,
          staging: dryStaging, retryPolicy, hashResolver,
        });
        await dryOrch.run();
        dryState.stats = dryOrch.getStats() as unknown as Record<string, unknown>;
        dryState.status = 'completed';
      } catch (err) {
        dryState.status = 'failed';
        dryState.error = (err as Error).message;
      } finally {
        await dryReader.close().catch(() => {});
        await dryStaging.close().catch(() => {});
      }
    })();
    return { started: true };
  }

  // ── Ledger rebuild (disaster recovery, UI action) ─────────────────────
  // Regenerates mig_ranges from Mongo + ClickHouse counts when the ledger is
  // lost. Guarded hard: single active pod, engine not copying, and an
  // existing ledger is only replaced with force=true.
  const rebuildState = newRebuildProgress();

  async function startRebuild(force: boolean): Promise<{ started: boolean; reason?: string; existingChunks?: number }> {
    if (rebuildState.status === 'running') return { started: false, reason: 'rebuild already running' };
    if (orchestrator.getStatus() === 'running') return { started: false, reason: 'main migration is running — a rebuild only makes sense when progress state is lost; stop/pause first' };
    if (dryState.status === 'running') return { started: false, reason: 'dry run in progress — wait for it to finish' };
    const pods = await ledger.podActivity(config.ledger.runId);
    const others = pods.filter((row) => row.pod !== config.worker.podId && row.active > 0);
    if (others.length > 0) return { started: false, reason: `other pods hold active chunks (${others.map((row) => row.pod).join(', ')}) — stop them first` };
    const existing = await ledger.countForRun(config.ledger.runId);
    if (existing > 0 && !force) {
      return { started: false, reason: `ledger already has ${existing} chunks for run "${config.ledger.runId}" — rebuilding replaces them; confirm with force`, existingChunks: existing };
    }
    Object.assign(rebuildState, newRebuildProgress(), { status: 'running', startedAt: Date.now() });
    void rebuildLedger({ config, logger, ledger, hashResolver, progress: rebuildState })
      .then(() => { rebuildState.status = 'completed'; rebuildState.finishedAt = Date.now(); })
      .catch((err) => {
        rebuildState.status = 'failed';
        rebuildState.error = (err as Error).message;
        rebuildState.finishedAt = Date.now();
        logger.error({ err }, 'Ledger rebuild failed');
      });
    return { started: true };
  }

  // HTTP surface: health + stats + report + controls + branded dashboard (/viz)
  const app = Fastify({ logger: false });
  app.get('/healthz', async () => {
    const stats = orchestrator.getStats();
    return stats.fatalError
      ? { status: 'error', engine: 'ledger', error: stats.fatalError }
      : { status: 'ok', engine: 'ledger' };
  });
  app.get('/stats', async () => orchestrator.getStats());
  app.get('/report', async () => orchestrator.getReport());
  app.post('/control/pause', async () => { orchestrator.pause(); return { status: orchestrator.getStatus() }; });
  app.post('/control/resume', async () => { orchestrator.resume(); return { status: orchestrator.getStatus() }; });
  app.post('/control/replay-dlq', async () => orchestrator.replayDlq());
  app.post('/control/retry-failed', async () => orchestrator.retryFailed());
  app.post<{ Body: { ids?: string[] } }>('/control/waive-dlq', async (req) => ({
    waived: await dlq.waive(config.ledger.dryRun ? `${config.ledger.runId}-dry` : config.ledger.runId, req.body?.ids),
  }));
  app.post('/control/build-indexes', async () => orchestrator.startIndexBuilds());
  app.get('/api/index-progress', async () => orchestrator.indexBuildProgress());
  app.post('/control/dry-run', async () => startDryRun());
  app.post<{ Body: { force?: boolean } }>('/control/rebuild-ledger', async (req) => startRebuild(req.body?.force === true));
  app.get('/api/rebuild', async () => rebuildState);
  app.get('/api/dryrun', async () => dryState);
  app.get('/api/config', async () => ({
    knobs: [
      { env: 'LEDGER_CHUNK_DOCS_TARGET', value: config.ledger.chunkDocsTarget, def: 2_000_000,
        hint: 'Docs per chunk — the unit of crash-redo and pod parallelism. Lower on unstable infra (cheaper redo); raise to shave per-chunk overhead.' },
      { env: 'LEDGER_MAX_CHUNK_DAYS', value: config.ledger.maxChunkDays, def: 7,
        hint: 'Max time span per chunk — guards sizing against bad doc estimates.' },
      { env: 'MONGO_PAGE_SIZE', value: config.source.mongoPageSize, def: 10_000,
        hint: 'Docs per read page / insert batch (held in memory whole). Lower to ≤1,000 for very large documents.' },
      { env: 'LEDGER_INSERT_INFLIGHT', value: config.ledger.insertInflight, def: 3,
        hint: 'Concurrent inserts per chunk. Raise for high-latency ClickHouse; set 1 for a memory-tight one.' },
      { env: 'LEDGER_LEASE_SEC', value: config.ledger.leaseSec, def: 600,
        hint: 'Chunk claim lease — how long before other pods reclaim a dead pod\u2019s chunk.' },
      { env: 'LEDGER_BREAKER_PCT', value: config.ledger.breakerPct, def: 5,
        hint: 'Circuit breaker: pause when more than this % of a chunk\u2019s docs fail.' },
      { env: 'MONGO_READ_PREFERENCE', value: config.source.readPreference + (config.source.readPreferenceAuto ? ' (auto)' : ''), def: 'auto',
        hint: 'Auto-selected: secondaryPreferred on replica sets (offloads the primary; exact since the source is frozen), primary otherwise. Set explicitly only to override.' },
    ],
    stateLocation: {
      ledger: `${config.state.manifestDb}.mig_ranges (MongoDB)`,
      dlq: `${config.state.manifestDb}.mig_dlq_docs (MongoDB)`,
      note: 'Progress state is ~50-100 tiny documents with your MongoDB\u2019s durability. Recovery never trusts it blindly \u2014 chunks are count-verified. Changing a knob requires an engine restart (env vars).',
    },
  }));
  app.get('/api/pods', async () => ({
    pods: await ledger.podActivity(config.ledger.dryRun ? `${config.ledger.runId}-dry` : config.ledger.runId),
    leaseSec: config.ledger.leaseSec,
  }));
  const { registerLedgerVizRoutes } = await import('../http/ledger-viz-route.ts');
  registerLedgerVizRoutes(app, { orchestrator, ledger, dlq, config });
  await app.listen({ port: config.service.port, host: config.service.host });
  logger.info({ port: config.service.port }, 'Ledger engine HTTP listening');

  const runPromise = orchestrator.run();
  runPromise.catch((err) => {
    // Keep the HTTP console alive: an operator with a typo'd MONGO_DB or a
    // missing target table needs to SEE the error, not a dead process.
    logger.fatal({ err }, 'ChunkOrchestrator crashed — console stays up so the error is visible at /');
    orchestrator.markFatal((err as Error).message);
  });
  wireExitOnComplete(runPromise, config.service.exitOnComplete, logger);

  let shuttingDown = false;
  async function shutdown(signal: string): Promise<void> {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'Ledger engine shutting down');
    orchestrator.stopAfterChunk();
    await app.close().catch(() => {});
    await mongoReader.close().catch(() => {});
    await staging.close().catch(() => {});
    await pressureClient.close().catch(() => {});
    await ledger.close().catch(() => {});
    await dlq.close().catch(() => {});
    await hashResolver.close().catch(() => {});
    process.exit(0);
  }
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}
