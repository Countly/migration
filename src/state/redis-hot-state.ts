import { Redis } from "ioredis";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface RunState {
    runId: string;
    status: string;
    sourceNs: string;
    targetTable: string;
    upperBoundCursor: string;        // serialized Cursor JSON
    lastCommittedCursor: string | null; // serialized Cursor JSON
    transformVersion: string;
    totalBatches: number;
    completedBatches: number;
    startedAt: string;
}

export interface RunStats {
    docsRead: number;
    docsSkipped: number;
    rowsInserted: number;
    batchesDone: number;
    batchesFailed: number;
    batchesInflight: number;
    elapsedMs: number;
    docsPerSecond: number;
    lastBatchSeq: number;
    lastBatchFinishedAt: string | null;
}

export interface CommandFlags {
    pause?: boolean;
    abort?: boolean;
    resume?: boolean;
    gc?: boolean;
    stopAfterBatch?: boolean;
    skipBatch?: number | null;
}

export interface RecentError {
    batchSeq: number;
    error: string;
    timestamp: string;
    retryCount: number;
}

export interface TimelineSnapshot {
    timestamp: string;
    batch_seq: number;
    docs_read: number;
    rows_inserted: number;
    docs_skipped: number;
    docs_per_second: number;
    rows_per_second: number;
    skip_reasons: Record<string, number>;
    digest_mismatches: number;
    estimated_duplicate_rows: number;
    batches_failed: number;
    heap_used_mb: number;
    rss_mb: number;
}

export interface VerboseError {
    attempt: number;
    error: string;
    stack: string | null;
    timestamp: string;
    context: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Redis key helpers
// ---------------------------------------------------------------------------

function k(prefix: string, ...parts: string[]): string {
    return [prefix, ...parts].join(":");
}

// ---------------------------------------------------------------------------
// RedisHotState
// ---------------------------------------------------------------------------

const RECENT_ERRORS_CAP = 100;

export class RedisHotState {
    private readonly redis: Redis;
    private readonly prefix: string;
    private readonly ownsConnection: boolean;

    private _lastStateWriteMs: number = 0;
    private _lastError: string | null = null;

    constructor(redisUrl: string, prefix: string) {
        this.redis = new Redis(redisUrl, {
            maxRetriesPerRequest: 3,
            enableReadyCheck: true,
            lazyConnect: true,
            connectTimeout: 10_000,
            commandTimeout: 5_000,
            retryStrategy(times: number) {
                return Math.min(times * 200, 3_000);
            },
        });
        this.prefix = prefix;
        this.ownsConnection = true;
    }

    /**
     * Create a RedisHotState that reuses an existing Redis connection
     * with a different key prefix. Useful for per-collection state isolation.
     * The returned instance does NOT own the connection and will not close it.
     */
    static fromExistingConnection(redis: Redis, prefix: string): RedisHotState {
        const instance = Object.create(RedisHotState.prototype) as RedisHotState;
        Object.defineProperty(instance, 'redis', { value: redis, writable: false });
        Object.defineProperty(instance, 'prefix', { value: prefix, writable: false });
        Object.defineProperty(instance, 'ownsConnection', { value: false, writable: false });
        Object.defineProperty(instance, '_lastStateWriteMs', { value: 0, writable: true });
        Object.defineProperty(instance, '_lastError', { value: null, writable: true });
        return instance;
    }

    /** Expose the underlying Redis client for creating derived instances. */
    getRedisClient(): Redis {
        return this.redis;
    }

    async connect(): Promise<void> {
        await this.redis.connect();
    }

    // -----------------------------------------------------------------------
    // Active run
    // -----------------------------------------------------------------------

    async setActiveRun(runId: string): Promise<void> {
        await this.redis.set(k(this.prefix, "active_run"), runId);
    }

    async getActiveRun(): Promise<string | null> {
        return this.redis.get(k(this.prefix, "active_run"));
    }

    // -----------------------------------------------------------------------
    // Run state (JSON blob)
    // -----------------------------------------------------------------------

    async setState(runId: string, state: RunState): Promise<void> {
        try {
            const start = performance.now();
            const key = k(this.prefix, "run", runId, "state");
            await this.redis.set(key, JSON.stringify(state));
            this._lastStateWriteMs = Math.round(performance.now() - start);
        } catch (err) {
            this._lastError = err instanceof Error ? err.message : String(err);
            throw err;
        }
    }

    async getState(runId: string): Promise<RunState | null> {
        const raw = await this.redis.get(
            k(this.prefix, "run", runId, "state"),
        );
        if (!raw) return null;
        return JSON.parse(raw) as RunState;
    }

    // -----------------------------------------------------------------------
    // Batch completion bitmap (SETBIT / GETBIT)
    // -----------------------------------------------------------------------

    async markBatchDone(runId: string, batchSeq: number): Promise<void> {
        await this.redis.setbit(
            k(this.prefix, "run", runId, "done_bitmap"),
            batchSeq,
            1,
        );
    }

    async getBitmapCount(runId: string): Promise<number> {
        return this.redis.bitcount(
            k(this.prefix, "run", runId, "done_bitmap"),
        );
    }

    // -----------------------------------------------------------------------
    // Stats (JSON)
    // -----------------------------------------------------------------------

    async updateStats(runId: string, stats: RunStats): Promise<void> {
        const key = k(this.prefix, "run", runId, "stats", "latest");
        await this.redis.set(key, JSON.stringify(stats));
    }

    async getStats(runId: string): Promise<RunStats | null> {
        const raw = await this.redis.get(
            k(this.prefix, "run", runId, "stats", "latest"),
        );
        if (!raw) return null;
        return JSON.parse(raw) as RunStats;
    }

    // -----------------------------------------------------------------------
    // Command flags (JSON)
    // -----------------------------------------------------------------------

    async setCommand(
        runId: string,
        command: keyof CommandFlags,
        value: boolean | number | null,
    ): Promise<void> {
        const key = k(this.prefix, "run", runId, "commands");
        await this.redis.hset(key, command, JSON.stringify(value));
    }

    async getCommands(runId: string): Promise<CommandFlags> {
        const raw = await this.redis.hgetall(
            k(this.prefix, "run", runId, "commands"),
        );
        if (!raw || Object.keys(raw).length === 0) return {};
        const result: CommandFlags = {};
        const KNOWN_COMMANDS = new Set(['pause', 'abort', 'resume', 'gc', 'stopAfterBatch', 'skipBatch']);
        for (const [key, val] of Object.entries(raw)) {
            if (KNOWN_COMMANDS.has(key)) {
                (result as Record<string, unknown>)[key] = JSON.parse(val as string);
            }
        }
        return result;
    }

    // -----------------------------------------------------------------------
    // Recent errors (capped list)
    // -----------------------------------------------------------------------

    async pushError(runId: string, error: RecentError): Promise<void> {
        const key = k(this.prefix, "run", runId, "recent_errors");
        const pipeline = this.redis.multi();
        pipeline.lpush(key, JSON.stringify(error));
        pipeline.ltrim(key, 0, RECENT_ERRORS_CAP - 1);
        await pipeline.exec();
    }

    async getRecentErrors(runId: string): Promise<RecentError[]> {
        const raw = await this.redis.lrange(
            k(this.prefix, "run", runId, "recent_errors"),
            0,
            -1,
        );
        return raw.map((entry: string) => JSON.parse(entry) as RecentError);
    }

    // -----------------------------------------------------------------------
    // Timeline
    // -----------------------------------------------------------------------

    async pushTimelineSnapshot(runId: string, snapshot: TimelineSnapshot): Promise<void> {
        const key = k(this.prefix, "run", runId, "timeline");
        await this.redis.rpush(key, JSON.stringify(snapshot));
    }

    async getTimeline(runId: string): Promise<TimelineSnapshot[]> {
        const raw = await this.redis.lrange(
            k(this.prefix, "run", runId, "timeline"),
            0,
            -1,
        );
        return raw.map((entry: string) => JSON.parse(entry) as TimelineSnapshot);
    }

    // -----------------------------------------------------------------------
    // Verbose errors
    // -----------------------------------------------------------------------

    async pushVerboseError(
        runId: string,
        batchSeq: number,
        error: VerboseError,
    ): Promise<void> {
        const key = k(this.prefix, "run", runId, "batch", String(batchSeq), "errors");
        await this.redis.rpush(key, JSON.stringify(error));
    }

    async getVerboseErrors(
        runId: string,
        batchSeq: number,
    ): Promise<VerboseError[]> {
        const raw = await this.redis.lrange(
            k(this.prefix, "run", runId, "batch", String(batchSeq), "errors"),
            0,
            -1,
        );
        return raw.map((entry: string) => JSON.parse(entry) as VerboseError);
    }

    // -----------------------------------------------------------------------
    // Key management
    // -----------------------------------------------------------------------

    async scanKeys(pattern: string): Promise<string[]> {
        const keys: string[] = [];
        const stream = this.redis.scanStream({ match: pattern, count: 100 });
        for await (const batch of stream) {
            keys.push(...(batch as string[]));
        }
        return keys;
    }

    async cleanupRun(runId: string): Promise<number> {
        const runKeys = [
            k(this.prefix, "run", runId, "state"),
            k(this.prefix, "run", runId, "done_bitmap"),
            k(this.prefix, "run", runId, "stats", "latest"),
            k(this.prefix, "run", runId, "commands"),
            k(this.prefix, "run", runId, "recent_errors"),
            k(this.prefix, "run", runId, "timeline"),
        ];

        const batchErrorKeys = await this.scanKeys(
            k(this.prefix, "run", runId, "batch", "*", "errors"),
        );
        const allKeys = [...runKeys, ...batchErrorKeys];

        const activeRunKey = k(this.prefix, "active_run");
        const luaScript = `
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                redis.call('UNLINK', KEYS[1])
                return 1
            end
            return 0
        `;
        await this.redis.call("EVAL", luaScript, "1", activeRunKey, runId);

        if (allKeys.length === 0) return 0;
        return this.redis.unlink(...allKeys);
    }

    // -----------------------------------------------------------------------
    // Health
    // -----------------------------------------------------------------------

    async isHealthy(): Promise<boolean> {
        try {
            const pong = await this.redis.ping();
            return pong === "PONG";
        } catch {
            return false;
        }
    }

    // -----------------------------------------------------------------------
    // Metrics
    // -----------------------------------------------------------------------

    getMetrics(): { lastStateWriteMs: number; lastError: string | null } {
        return {
            lastStateWriteMs: this._lastStateWriteMs,
            lastError: this._lastError,
        };
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    async close(): Promise<void> {
        if (this.ownsConnection) {
            await this.redis.quit();
        }
    }
}
