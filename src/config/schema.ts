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
    // ── Ledger engine ────────────────────────────────────────────────────
    ledger: z
        .object({
            runId: z.string().default("ledger-v1"),
            chunkDocsTarget: positiveIntFromEnv.default(2_000_000),
            insertInflight: positiveIntFromEnv.default(3),
            leaseSec: positiveIntFromEnv.default(600),
            // Circuit breaker: pause when >pct% of a chunk's docs fail, or
            // after N consecutive failed chunks (systematic-bug detection).
            breakerPct: numberFromEnv.default(5).pipe(z.number().min(0).max(100)),
            // Per-chunk breakers miss EVENLY-SPREAD failure (1% of every
            // chunk never trips 5%-of-one-chunk) — this is the global guard.
            dlqPauseThreshold: numberFromEnv.default(1_000_000).pipe(z.number().min(0)),
            // Tally-independent per-commit guard: after each chunk promotes,
            // ask the SOURCE how many docs its window holds. ~1% overhead.
            sourceCountCheck: booleanFromEnv.default(true),
            breakerConsecutive: positiveIntFromEnv.default(3),
            // Background invariant spot checks (0 disables).
            monitorIntervalMs: intFromEnv.default(900_000),
            // Capture full raw docs of transform failures into the DLQ.
            captureTransformErrors: booleanFromEnv.default(true),
            // Upper bound on a chunk's time span. Guards chunk sizing against
            // bad estimatedDocumentCount (e.g. metadata fastcount reset after
            // an unclean mongod shutdown) — a wrong estimate can never produce
            // a whole-collection mega-chunk.
            maxChunkDays: numberFromEnv.default(7).pipe(z.number().positive()),
            // Mirror-first mode: migrate ONLY cd < this bound (the mirror's
            // checkpoint T0); the mapper clamps to it and top-up never
            // crosses it — everything at/after belongs to the live mirror.
            cdUpperBoundMs: z
                .union([z.string(), z.number(), z.null(), z.undefined()])
                .transform((v, ctx) => {
                    if (v === null || v === undefined || v === "") return null;
                    const ms = typeof v === "number" ? v : /^\d+$/.test(v) ? Number(v) : Date.parse(v);
                    if (!Number.isFinite(ms) || ms <= 0) {
                        ctx.addIssue({ code: z.ZodIssueCode.custom, message: `LEDGER_CD_UPPER_BOUND must be ISO date or epoch ms, got: ${String(v)}` });
                        return z.NEVER;
                    }
                    return ms;
                }),
            // Deploy now, start later: hold every pod BEFORE mapping until
            // an operator opens the run's start gate (dashboard Start button,
            // or POST /control/resume). The gate lives in the ledger, so one
            // click starts the whole fleet, pods that join later start
            // immediately, and a pod that restarts after Start stays started.
            startPaused: booleanFromEnv.default(false),
            // Dry run: sampled rehearsal against a Null-engine clone.
            dryRun: booleanFromEnv.default(false),
            dryRunSamplePct: numberFromEnv.default(2).pipe(z.number().min(0.1).max(5)),
        })
        .default({}),

    // ── Service ──────────────────────────────────────────────────────────
    service: z.object({
        name: z.string().min(1).default("drill-migrator"),
        port: positiveIntFromEnv.default(8080),
        host: z.string().default("0.0.0.0"),
        gracefulShutdownTimeoutMs: intFromEnv.default(60_000),
        exitOnComplete: booleanFromEnv.default(false),
    }),

    // ── MongoDB Source ───────────────────────────────────────────────────
    source: z.object({
        uri: z.string().min(1),
        db: z.string().default("countly_drill"),
        countlyDb: z.string().default("countly"),
        collectionPrefix: z.string().default("drill_events"),
        readPreference: z.string().default("auto"),
        readPreferenceAuto: z.boolean().default(false),
        readConcern: z.string().default("majority"),
        retryReads: booleanFromEnv.default(true),
        appName: z.string().optional(),
        mongoPageSize: positiveIntFromEnv.default(10_000),
        cursorBatchSize: positiveIntFromEnv.default(10_000),
        maxTimeMs: positiveIntFromEnv.default(600_000),
    }),

    // ── Transform ────────────────────────────────────────────────────────
    transform: z.object({
        version: z.string().default("v2"),
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

    // ── State (chunk ledger + DLQ live here) ─────────────────────────────
    state: z.object({
        manifestDb: z.string().default("countly_drill"),
    }),

    // ── Worker / Multi-Pod ──────────────────────────────────────────────
    worker: z.object({
        podId: z.string().default(""),
        enabled: booleanFromEnv.default(true),
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
