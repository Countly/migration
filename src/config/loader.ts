import pino from "pino";
import { hostname } from "node:os";
import { configSchema, type Config } from "./schema.ts";

// ---------------------------------------------------------------------------
// Env-var mapping
// ---------------------------------------------------------------------------

/**
 * Maps flat `process.env` variables into the nested shape expected by the
 * Zod config schema. Every key listed in the requirements is covered.
 */
function envToRawConfig(env: NodeJS.ProcessEnv) {
    return {
        engine: env.MIGRATION_ENGINE,

        ledger: {
            runId: env.LEDGER_RUN_ID,
            chunkDocsTarget: env.LEDGER_CHUNK_DOCS_TARGET,
            insertInflight: env.LEDGER_INSERT_INFLIGHT,
            leaseSec: env.LEDGER_LEASE_SEC,
        },

        service: {
            name: env.SERVICE_NAME,
            port: env.SERVICE_PORT,
            host: env.SERVICE_HOST,
            gracefulShutdownTimeoutMs: env.GRACEFUL_SHUTDOWN_TIMEOUT_MS,
            rerunMode: env.RERUN_MODE,
            exitOnComplete: env.EXIT_ON_COMPLETE,
        },

        source: {
            uri: env.MONGO_URI,
            db: env.MONGO_DB,
            countlyDb: env.MONGO_COUNTLY_DB,
            collectionPrefix: env.MONGO_COLLECTION_PREFIX,
            readPreference: env.MONGO_READ_PREFERENCE,
            readConcern: env.MONGO_READ_CONCERN,
            retryReads: env.MONGO_RETRY_READS,
            appName: env.MONGO_APP_NAME,
            batchRowsTarget: env.MONGO_BATCH_ROWS_TARGET,
            mongoPageSize: env.MONGO_PAGE_SIZE,
            cursorBatchSize: env.MONGO_CURSOR_BATCH_SIZE,
            maxTimeMs: env.MONGO_MAX_TIME_MS,
            rangeParallelThreshold: env.RANGE_PARALLEL_THRESHOLD,
            rangeCount: env.RANGE_COUNT,
            rangeLeaseTtlSec: env.RANGE_LEASE_TTL_SEC,
        },

        transform: {
            version: env.TRANSFORM_VERSION,
        },

        target: {
            url: env.CLICKHOUSE_URL,
            db: env.CLICKHOUSE_DB,
            table: env.CLICKHOUSE_TABLE,
            username: env.CLICKHOUSE_USERNAME,
            password: env.CLICKHOUSE_PASSWORD,
            queryTimeoutMs: env.CLICKHOUSE_QUERY_TIMEOUT_MS,
            maxRetries: env.CLICKHOUSE_MAX_RETRIES,
            retryBaseDelayMs: env.CLICKHOUSE_RETRY_BASE_DELAY_MS,
            retryMaxDelayMs: env.CLICKHOUSE_RETRY_MAX_DELAY_MS,
            useDedupToken: env.CLICKHOUSE_USE_DEDUP_TOKEN,
        },

        backpressure: {
            enabled: env.BACKPRESSURE_ENABLED,
            partsToThrowInsert: env.BACKPRESSURE_PARTS_TO_THROW_INSERT,
            maxPartsInTotal: env.BACKPRESSURE_MAX_PARTS_IN_TOTAL,
            partitionPctHigh: env.BACKPRESSURE_PARTITION_PCT_HIGH,
            partitionPctLow: env.BACKPRESSURE_PARTITION_PCT_LOW,
            totalPctHigh: env.BACKPRESSURE_TOTAL_PCT_HIGH,
            totalPctLow: env.BACKPRESSURE_TOTAL_PCT_LOW,
            pollIntervalMs: env.BACKPRESSURE_POLL_INTERVAL_MS,
            maxPauseEpisodeMs: env.BACKPRESSURE_MAX_PAUSE_EPISODE_MS,
        },

        state: {
            manifestDb: env.MANIFEST_DB,
            redisUrl: env.REDIS_URL,
            redisKeyPrefix: env.REDIS_KEY_PREFIX,
            timelineSnapshotInterval: env.TIMELINE_SNAPSHOT_INTERVAL,
        },

        memory: {
            gcEnabled: env.GC_ENABLED,
            gcRssSoftLimitMb: env.GC_RSS_SOFT_LIMIT_MB,
            gcRssHardLimitMb: env.GC_RSS_HARD_LIMIT_MB,
            gcHeapUsedRatio: env.GC_HEAP_USED_RATIO,
            gcEveryNBatches: env.GC_EVERY_N_BATCHES,
        },

        asyncWrite: {
            flushIntervalMs: env.ASYNC_WRITE_FLUSH_INTERVAL_MS,
            flushBatchSize: env.ASYNC_WRITE_FLUSH_BATCH_SIZE,
        },

        worker: {
            podId: env.POD_ID,
            enabled: env.MULTI_POD_ENABLED,
            lockTtlSec: env.LOCK_TTL_SECONDS,
            lockRenewMs: env.LOCK_RENEW_MS,
            progressUpdateMs: env.PROGRESS_UPDATE_MS,
            podHeartbeatMs: env.POD_HEARTBEAT_MS,
            podDeadAfterSec: env.POD_DEAD_AFTER_SEC,
        },

        log: {
            level: env.LOG_LEVEL,
        },
    };
}

/**
 * Strip `undefined` values so Zod `.default()` kicks in for omitted vars.
 */
function stripUndefined(obj: Record<string, unknown>): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj)) {
        if (value === undefined) continue;
        if (value !== null && typeof value === "object" && !Array.isArray(value)) {
            result[key] = stripUndefined(value as Record<string, unknown>);
        } else {
            result[key] = value;
        }
    }
    return result;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Load and validate configuration from environment variables.
 *
 * 1. Reads environment variables from `process.env`.
 * 2. Maps flat env vars into the nested config shape.
 * 3. Parses and validates through the Zod schema, applying defaults.
 *
 * Throws a `ZodError` if required variables are missing or values are invalid.
 */
export function loadConfig(): Config {
    const raw = envToRawConfig(process.env);
    const cleaned = stripUndefined(raw);

    const config = configSchema.parse(cleaned);

    // Default podId to hostname if not set
    if (!config.worker.podId) {
        config.worker.podId = hostname();
    }

    // Semantic validation
    const { memory, target } = config;

    if (config.engine === "classic" && !config.state.redisUrl) {
        throw new Error("REDIS_URL is required for MIGRATION_ENGINE=classic (the ledger engine needs no Redis)");
    }

    const rssSoft = memory.gcRssSoftLimitMb * 1024 * 1024;
    const rssHard = memory.gcRssHardLimitMb * 1024 * 1024;
    if (rssSoft > rssHard) {
        throw new Error(
            `GC_RSS_SOFT_LIMIT_MB (${memory.gcRssSoftLimitMb}) must be <= GC_RSS_HARD_LIMIT_MB (${memory.gcRssHardLimitMb})`,
        );
    }

    if (target.retryBaseDelayMs > target.retryMaxDelayMs) {
        throw new Error(
            `CLICKHOUSE_RETRY_BASE_DELAY_MS (${target.retryBaseDelayMs}) must be <= CLICKHOUSE_RETRY_MAX_DELAY_MS (${target.retryMaxDelayMs})`,
        );
    }

    const { backpressure } = config;
    if (backpressure.enabled) {
        if (backpressure.partitionPctLow >= backpressure.partitionPctHigh) {
            throw new Error(
                `BACKPRESSURE_PARTITION_PCT_LOW (${backpressure.partitionPctLow}) must be < BACKPRESSURE_PARTITION_PCT_HIGH (${backpressure.partitionPctHigh})`,
            );
        }
        if (backpressure.totalPctLow >= backpressure.totalPctHigh) {
            throw new Error(
                `BACKPRESSURE_TOTAL_PCT_LOW (${backpressure.totalPctLow}) must be < BACKPRESSURE_TOTAL_PCT_HIGH (${backpressure.totalPctHigh})`,
            );
        }
    }

    return config;
}

/**
 * Create a pino logger using the log level from the supplied config.
 */
export function createLogger(config: Config): pino.Logger {
    return pino({
        level: config.log.level,
        transport:
            process.env.NODE_ENV !== "production"
                ? { target: "pino-pretty", options: { colorize: true } }
                : undefined,
    });
}
