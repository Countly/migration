import { z } from "zod";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Coerce an env-var string to a boolean (accepts "true"/"1"/"yes"). */
const booleanFromEnv = z
    .union([z.boolean(), z.string()])
    .transform((v) => {
        if (typeof v === "boolean") return v;
        const lower = v.trim().toLowerCase();
        return lower === "true" || lower === "1" || lower === "yes";
    });

/** Coerce an env-var string to a finite number. */
const numberFromEnv = z
    .union([z.number(), z.string()])
    .transform((v) => (typeof v === "number" ? v : Number(v)))
    .pipe(z.number().finite());

/** Same as numberFromEnv but restricted to non-negative integers. */
const intFromEnv = z
    .union([z.number(), z.string()])
    .transform((v) => (typeof v === "number" ? v : Number(v)))
    .pipe(z.number().int().nonnegative());

/** Env integer that must be >= 1. */
const positiveIntFromEnv = z
    .union([z.number(), z.string()])
    .transform((v) => (typeof v === "number" ? v : Number(v)))
    .pipe(z.number().int().positive());

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

export const configSchema = z.object({
    // ── Engine selection ─────────────────────────────────────────────────
    // 'classic'  = current architecture (per-batch checkpoints, Redis hot state)
    // 'ledger'   = chunk checklist in MongoDB, per-chunk staging tables, no Redis
    engine: z.enum(["classic", "ledger"]).default("classic"),

    // ── Ledger engine ────────────────────────────────────────────────────
    ledger: z
        .object({
            runId: z.string().default("ledger-v1"),
            chunkDocsTarget: positiveIntFromEnv.default(2_000_000),
            insertInflight: positiveIntFromEnv.default(3),
            leaseSec: positiveIntFromEnv.default(600),
        })
        .default({}),

    // ── Service ──────────────────────────────────────────────────────────
    service: z.object({
        name: z.string().min(1),
        port: positiveIntFromEnv.default(8080),
        host: z.string().default("0.0.0.0"),
        gracefulShutdownTimeoutMs: intFromEnv.default(60_000),
        rerunMode: z.enum(['resume', 'clone-run', 'new-run']).default('resume'),
        exitOnComplete: booleanFromEnv.default(false),
    }),

    // ── MongoDB Source ───────────────────────────────────────────────────
    source: z.object({
        uri: z.string().min(1),
        db: z.string().default("countly_drill"),
        countlyDb: z.string().default("countly"),
        collectionPrefix: z.string().default("drill_events"),
        readPreference: z.string().default("primary"),
        readConcern: z.string().default("majority"),
        retryReads: booleanFromEnv.default(true),
        appName: z.string().optional(),
        batchRowsTarget: positiveIntFromEnv.default(10_000),
        mongoPageSize: positiveIntFromEnv.default(10_000),
        cursorBatchSize: positiveIntFromEnv.default(10_000),
        maxTimeMs: positiveIntFromEnv.default(600_000),
        rangeParallelThreshold: intFromEnv.default(500_000),
        rangeCount: positiveIntFromEnv.default(100),
        rangeLeaseTtlSec: positiveIntFromEnv.default(300),
    }),

    // ── Transform ────────────────────────────────────────────────────────
    transform: z.object({
        version: z.string().default("v1"),
    }),

    // ── ClickHouse Target ────────────────────────────────────────────────
    target: z.object({
        url: z.string().min(1),
        db: z.string().default("countly_drill"),
        table: z.string().default("drill_events"),
        username: z.string().default("default"),
        password: z.string().default(""),
        queryTimeoutMs: positiveIntFromEnv.default(120_000),
        maxRetries: intFromEnv.default(8),
        retryBaseDelayMs: positiveIntFromEnv.default(1_000),
        retryMaxDelayMs: positiveIntFromEnv.default(30_000),
        useDedupToken: booleanFromEnv.default(true),
    }),

    // ── Backpressure ─────────────────────────────────────────────────────
    backpressure: z.object({
        enabled: booleanFromEnv.default(true),
        partsToThrowInsert: intFromEnv.default(300),
        maxPartsInTotal: intFromEnv.default(500),
        partitionPctHigh: numberFromEnv.default(0.70).pipe(z.number().min(0).max(1)),
        partitionPctLow: numberFromEnv.default(0.55).pipe(z.number().min(0).max(1)),
        totalPctHigh: numberFromEnv.default(0.70).pipe(z.number().min(0).max(1)),
        totalPctLow: numberFromEnv.default(0.55).pipe(z.number().min(0).max(1)),
        pollIntervalMs: intFromEnv.default(5_000),
        maxPauseEpisodeMs: intFromEnv.default(180_000),
    }),

    // ── State ────────────────────────────────────────────────────────────
    state: z.object({
        manifestDb: z.string().default("countly_drill"),
        // Required for the classic engine only — the ledger engine has no Redis.
        redisUrl: z.string().min(1).optional(),
        redisKeyPrefix: z.string().default("mig"),
        timelineSnapshotInterval: positiveIntFromEnv.default(10),
    }),

    // ── Memory / GC ─────────────────────────────────────────────────────
    memory: z.object({
        gcEnabled: booleanFromEnv.default(true),
        gcRssSoftLimitMb: intFromEnv.default(3_072),
        gcRssHardLimitMb: intFromEnv.default(6_144),
        gcHeapUsedRatio: numberFromEnv.default(0.70),
        gcEveryNBatches: intFromEnv.default(50),
    }),

    // ── Worker / Multi-Pod ──────────────────────────────────────────────
    worker: z.object({
        podId: z.string().default(""),
        enabled: booleanFromEnv.default(true),
        lockTtlSec: positiveIntFromEnv.default(300),
        lockRenewMs: positiveIntFromEnv.default(60_000),
        progressUpdateMs: positiveIntFromEnv.default(5_000),
        podHeartbeatMs: positiveIntFromEnv.default(30_000),
        podDeadAfterSec: positiveIntFromEnv.default(180),
    }),

    // ── Async Write ──────────────────────────────────────────────────────
    asyncWrite: z.object({
        flushIntervalMs: positiveIntFromEnv.default(5_000),
        flushBatchSize: positiveIntFromEnv.default(10),
    }),

    // ── Logging ──────────────────────────────────────────────────────────
    log: z.object({
        level: z
            .enum(["fatal", "error", "warn", "info", "debug", "trace"])
            .default("info"),
    }),
});

// ---------------------------------------------------------------------------
// Inferred type
// ---------------------------------------------------------------------------

export type Config = z.infer<typeof configSchema>;
