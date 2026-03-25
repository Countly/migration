// src/main.ts
// MongoDB -> ClickHouse Migration Service Entry Point

import { loadConfig, createLogger } from './config/loader.ts';
import { ManifestStore } from './state/manifest-store.ts';
import { RedisHotState } from './state/redis-hot-state.ts';
import { MongoReader } from './source/mongo-reader.ts';
import { ClickHouseWriter } from './target/clickhouse-writer.ts';
import { ClickHousePressure } from './target/clickhouse-pressure.ts';
import { CollectionOrchestrator } from './runtime/collection-orchestrator.ts';
import { RetryPolicy } from './runtime/retry-policy.ts';
import { GcController, type GcConfig } from './runtime/gc-controller.ts';
import { ProcessMetricsCollector } from './runtime/process-metrics.ts';
import { registerHealthRoutes } from './http/health-route.ts';
import { registerStatsRoute } from './http/stats-route.ts';
import { registerControlRoutes } from './http/control-route.ts';
import { registerRunRoutes } from './http/run-route.ts';
import Fastify from 'fastify';
import { createClient as createClickHouseClient } from '@clickhouse/client';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERVICE_VERSION = '1.0.0';

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  // ── 1. Load and validate config ─────────────────────────────────────
  const config = loadConfig();

  // ── 2. Create logger ────────────────────────────────────────────────
  const logger = createLogger(config);
  logger.info({ service: config.service.name }, 'Starting migration service');

  // ── 3. Initialize components ────────────────────────────────────────

  // State stores
  const manifestStore = new ManifestStore(config.source.uri, config.state.manifestDb);
  await manifestStore.connect();
  logger.info({ db: config.state.manifestDb }, 'ManifestStore initialized');

  const redisState = new RedisHotState(config.state.redisUrl, config.state.redisKeyPrefix);
  logger.info('RedisHotState initialized');

  // Source (no collection binding — orchestrator handles switchCollection per collection)
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

  // Target
  const chWriter = new ClickHouseWriter(
    {
      url: config.target.url,
      database: config.target.db,
      table: config.target.table,
      username: config.target.username,
      password: config.target.password,
      queryTimeoutMs: config.target.queryTimeoutMs,
      useDedupToken: config.target.useDedupToken,
    },
    logger,
  );

  // Create a separate ClickHouse client for pressure monitoring queries
  const pressureClient = createClickHouseClient({
    url: config.target.url,
    database: config.target.db,
    username: config.target.username,
    password: config.target.password,
    request_timeout: config.target.queryTimeoutMs,
  });

  const chPressure = new ClickHousePressure(pressureClient, config.backpressure, logger);

  // Map config.memory to GcConfig shape
  const gcConfig: GcConfig = {
    enabled: config.memory.gcEnabled,
    rssSoftLimitBytes: config.memory.gcRssSoftLimitMb * 1024 * 1024,
    rssHardLimitBytes: config.memory.gcRssHardLimitMb * 1024 * 1024,
    heapUsedRatio: config.memory.gcHeapUsedRatio,
    everyNBatches: config.memory.gcEveryNBatches,
  };
  const gcController = new GcController(gcConfig, logger);

  const processMetrics = new ProcessMetricsCollector();

  const retryPolicy = new RetryPolicy({
    maxRetries: config.target.maxRetries,
    baseDelayMs: config.target.retryBaseDelayMs,
    maxDelayMs: config.target.retryMaxDelayMs,
  });

  // ── 4. Connect to external services ─────────────────────────────────
  logger.info('Connecting to external services...');

  await mongoReader.connect();
  await chWriter.connect();
  await redisState.connect();

  // Verify Redis is reachable
  const redisHealthy = await redisState.isHealthy();
  if (!redisHealthy) {
    throw new Error('Redis is not reachable. Cannot start migration service.');
  }
  logger.info('All external services connected');

  // ── 5. Create CollectionOrchestrator ────────────────────────────────
  const orchestrator = new CollectionOrchestrator({
    manifestStore,
    redisState,
    mongoReader,
    chWriter,
    chPressure,
    gcController,
    retryPolicy,
    logger,
    config,
  });

  // ── 6. Create Fastify HTTP server and register routes ───────────────
  const app = Fastify({ logger: false });
  const startedAt = new Date();

  registerHealthRoutes(app, {
    mongoReader,
    chWriter,
    redisState,
    manifestStore,
    orchestrator,
  });

  registerStatsRoute(app, {
    orchestrator,
    redisState,
    gcController,
    processMetrics,
    manifestStore,
    config,
    startedAt,
    version: SERVICE_VERSION,
  });

  registerControlRoutes(app, {
    orchestrator,
    gcController,
  });

  registerRunRoutes(app, {
    manifestStore,
    redisState,
  });

  // ── 7. Start HTTP server ────────────────────────────────────────────
  await app.listen({ port: config.service.port, host: config.service.host });
  logger.info(
    { port: config.service.port, host: config.service.host },
    'HTTP server listening',
  );

  // ── 8. Start orchestrator (background) ──────────────────────────────
  processMetrics.start();
  gcController.start();

  orchestrator.run().catch((err) => {
    logger.fatal({ err }, 'CollectionOrchestrator crashed unexpectedly');
    process.exit(1);
  });

  // ── 9. Log startup complete ─────────────────────────────────────────
  logger.info(
    { service: config.service.name, version: SERVICE_VERSION },
    'Migration service started successfully',
  );

  // ── Graceful shutdown ───────────────────────────────────────────────
  let shuttingDown = false;

  async function shutdown(signal: string): Promise<void> {
    if (shuttingDown) return;
    shuttingDown = true;

    logger.info({ signal }, 'Shutdown signal received, starting graceful shutdown');

    // 1. Signal orchestrator to stop after current batch
    orchestrator.stopAfterBatch();

    // 2. Wait for orchestrator to stop (with timeout)
    const timeoutMs = config.service.gracefulShutdownTimeoutMs;
    try {
      await Promise.race([
        orchestrator.waitForStop(),
        new Promise<void>((_, reject) =>
          setTimeout(() => reject(new Error('timeout')), timeoutMs),
        ),
      ]);
      logger.info('Orchestrator stopped gracefully');
    } catch {
      logger.warn('Orchestrator did not stop within timeout, forcing shutdown');
    }

    // 3. Close all resources
    async function closeResource(name: string, fn: () => Promise<void>): Promise<void> {
      try { await fn(); logger.info(`${name} closed`); }
      catch (err) { logger.warn({ err }, `Error closing ${name}`); }
    }

    await closeResource('HTTP server', () => app.close());
    await closeResource('MongoDB', () => mongoReader.close());
    await closeResource('ClickHouse', async () => { await chWriter.close(); await pressureClient.close(); });
    await closeResource('Redis', () => redisState.close());
    await closeResource('ManifestStore', () => manifestStore.close());
    processMetrics.stop();
    gcController.dispose();

    logger.info('Graceful shutdown complete');
    process.exit(0);
  }

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

// ---------------------------------------------------------------------------
// Unhandled rejection handler
// ---------------------------------------------------------------------------

process.on('unhandledRejection', (reason) => {
  console.error('Unhandled rejection:', reason);
  process.exit(1);
});

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

main().catch((err) => {
  console.error('Fatal error during startup:', err);
  process.exit(1);
});
