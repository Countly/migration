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
        ledger: {
            runId: env.LEDGER_RUN_ID,
            chunkDocsTarget: env.LEDGER_CHUNK_DOCS_TARGET,
            insertInflight: env.LEDGER_INSERT_INFLIGHT,
            leaseSec: env.LEDGER_LEASE_SEC,
            breakerPct: env.LEDGER_BREAKER_PCT,
            breakerConsecutive: env.LEDGER_BREAKER_CONSECUTIVE,
            monitorIntervalMs: env.LEDGER_MONITOR_INTERVAL_MS,
            maxChunkDays: env.LEDGER_MAX_CHUNK_DAYS,
            captureTransformErrors: env.LEDGER_CAPTURE_TRANSFORM_ERRORS,
            dryRun: env.DRY_RUN,
            dryRunSamplePct: env.DRY_RUN_SAMPLE_PCT,
        },

        service: {
            name: env.SERVICE_NAME,
            port: env.SERVICE_PORT,
            host: env.SERVICE_HOST,
            gracefulShutdownTimeoutMs: env.GRACEFUL_SHUTDOWN_TIMEOUT_MS,
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
            mongoPageSize: env.MONGO_PAGE_SIZE,
            cursorBatchSize: env.MONGO_CURSOR_BATCH_SIZE,
            maxTimeMs: env.MONGO_MAX_TIME_MS,
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
        },

        worker: {
            podId: env.POD_ID,
            enabled: env.MULTI_POD_ENABLED,
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
    const { target } = config;

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
