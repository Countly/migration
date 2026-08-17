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
import type { Config } from '../config/schema.ts';
import { MongoReader } from '../source/mongo-reader.ts';
import { HashResolver } from '../transform/hash-resolver.ts';
import { RetryPolicy } from './retry-policy.ts';
import { LedgerStore } from '../state/ledger-store.ts';
import { StagingManager } from '../target/staging-manager.ts';
import { ChunkOrchestrator } from './chunk-orchestrator.ts';
import { wireExitOnComplete } from './exit-on-complete.ts';

export async function runLedgerEngine(config: Config, logger: Logger): Promise<void> {
  logger.info({ engine: 'ledger', runId: config.ledger.runId }, 'Starting ledger engine (no Redis)');

  const mongoReader = new MongoReader(
    {
      uri: config.source.uri,
      database: config.source.db,
      readPreference: config.source.readPreference,
      readConcern: config.source.readConcern,
      retryReads: config.source.retryReads,
      appName: config.source.appName ?? config.service.name,
      batchRowsTarget: config.source.batchRowsTarget,
      cursorBatchSize: config.source.cursorBatchSize,
      maxTimeMs: config.source.maxTimeMs,
    },
    logger,
  );

  const ledger = new LedgerStore(config.source.uri, config.state.manifestDb, logger);

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
  await staging.connect();
  await hashResolver.build();
  logger.info('Ledger engine: all services connected (MongoDB + ClickHouse only)');

  const orchestrator = new ChunkOrchestrator({
    config,
    logger,
    mongoReader,
    ledger,
    staging,
    retryPolicy,
    hashResolver,
  });

  // Minimal HTTP surface: health + stats
  const app = Fastify({ logger: false });
  app.get('/healthz', async () => ({ status: 'ok', engine: 'ledger' }));
  app.get('/stats', async () => orchestrator.getStats());
  await app.listen({ port: config.service.port, host: config.service.host });
  logger.info({ port: config.service.port }, 'Ledger engine HTTP listening');

  const runPromise = orchestrator.run();
  runPromise.catch((err) => {
    logger.fatal({ err }, 'ChunkOrchestrator crashed unexpectedly');
    process.exit(1);
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
    await ledger.close().catch(() => {});
    await hashResolver.close().catch(() => {});
    process.exit(0);
  }
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}
