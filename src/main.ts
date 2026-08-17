// src/main.ts
// MongoDB -> ClickHouse Migration Service Entry Point
//
// Architecture: chunk-checklist engine — work is cut into cd-bounded chunks
// tracked in a MongoDB ledger; each chunk is stream-copied into its own
// staging table, verified (read tally vs exact ClickHouse count), then
// promoted into the live table via verify-then-attach. Dependencies:
// MongoDB + ClickHouse. No Redis.

import { loadConfig, createLogger } from './config/loader.ts';
import { runLedgerEngine } from './runtime/ledger-engine.ts';

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config);
  logger.info({ service: config.service.name }, 'Starting migration service');
  await runLedgerEngine(config, logger);
}

process.on('unhandledRejection', (reason) => {
  console.error('Unhandled rejection:', reason);
  process.exit(1);
});

main().catch((err) => {
  console.error('Fatal error during startup:', err);
  process.exit(1);
});
