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

export type BatchPhase = "READING" | "TRANSFORMING" | "WRITING" | "COMMITTING";

export interface LiveBatchData {
    collection: string;
    podId: string;
    batchSeq: number;
    phase: BatchPhase;
    docsRead: number;
    rowsToInsert: number;
    startedAt: number;
    rangeIdx?: number;
}

export interface RangeLiveStats {
    idx: number;
    status: string;
    podId: string;
    docsRead: number;
    rowsInserted: number;
    batchesDone: number;
    docsPerSecond: number;
    startedAt: number;
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

    /**
     * Atomically commit a batch: set cursor + mark bitmap in a single MULTI/EXEC.
     * Eliminates the crash window between separate SET and SETBIT calls.
     */
    async commitBatch(runId: string, cursor: string, batchSeq: number): Promise<void> {
        const pipeline = this.redis.multi();
        pipeline.set(k(this.prefix, "run", runId, "cursor"), cursor);
        pipeline.setbit(k(this.prefix, "run", runId, "done_bitmap"), batchSeq, 1);
        await pipeline.exec();
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
        const pipeline = this.redis.multi();
        pipeline.rpush(key, JSON.stringify(snapshot));
        pipeline.ltrim(key, -1000, -1);
        await pipeline.exec();
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
    // Throughput sliding window (5-min window for accurate real-time throughput)
    // -----------------------------------------------------------------------

    async pushThroughputSample(runId: string, sample: { ts: number; docsRead: number }): Promise<void> {
        const key = k(this.prefix, "run", runId, "throughput_window");
        const pipeline = this.redis.multi();
        pipeline.lpush(key, JSON.stringify(sample));
        pipeline.ltrim(key, 0, 59);
        pipeline.expire(key, 600);
        await pipeline.exec();
    }

    async getThroughputWindow(runId: string): Promise<Array<{ ts: number; docsRead: number }>> {
        const key = k(this.prefix, "run", runId, "throughput_window");
        const raw = await this.redis.lrange(key, 0, -1);
        return raw.map(r => JSON.parse(r) as { ts: number; docsRead: number });
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
        const pipeline = this.redis.multi();
        pipeline.rpush(key, JSON.stringify(error));
        pipeline.ltrim(key, -20, -1);
        await pipeline.exec();
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
    // Last committed cursor (hot-path authority for async writes)
    // -----------------------------------------------------------------------

    async setLastCommittedCursor(runId: string, cursor: string): Promise<void> {
        await this.redis.set(k(this.prefix, "run", runId, "cursor"), cursor);
    }

    async getLastCommittedCursor(runId: string): Promise<string | null> {
        return this.redis.get(k(this.prefix, "run", runId, "cursor"));
    }

    // -----------------------------------------------------------------------
    // Live batch tracking (phase visibility for dashboard)
    // -----------------------------------------------------------------------

    async setLiveBatch(collection: string, data: LiveBatchData): Promise<void> {
        const key = k(this.prefix, "liveBatch", collection);
        await this.redis.set(key, JSON.stringify(data), "EX", 30);
    }

    async getLiveBatch(collection: string): Promise<LiveBatchData | null> {
        const raw = await this.redis.get(k(this.prefix, "liveBatch", collection));
        if (!raw) return null;
        return JSON.parse(raw) as LiveBatchData;
    }

    async clearLiveBatch(collection: string): Promise<void> {
        await this.redis.del(k(this.prefix, "liveBatch", collection));
    }

    async getAllLiveBatches(): Promise<LiveBatchData[]> {
        const keys = await this.scanKeys(k(this.prefix, "liveBatch", "*"));
        if (keys.length === 0) return [];
        const pipeline = this.redis.pipeline();
        for (const key of keys) pipeline.get(key);
        const results = await pipeline.exec();
        const batches: LiveBatchData[] = [];
        if (results) {
            for (const [err, val] of results) {
                if (!err && val && typeof val === "string") {
                    batches.push(JSON.parse(val) as LiveBatchData);
                }
            }
        }
        return batches;
    }

    // -----------------------------------------------------------------------
    // Per-range live stats (range-parallel dashboard visibility)
    // -----------------------------------------------------------------------

    async setRangeLiveStats(collection: string, rangeIdx: number, stats: RangeLiveStats): Promise<void> {
        const key = k(this.prefix, "rangeLive", collection, String(rangeIdx));
        await this.redis.set(key, JSON.stringify(stats), "EX", 60);
    }

    async getRangeLiveStats(collection: string): Promise<RangeLiveStats[]> {
        const keys = await this.scanKeys(k(this.prefix, "rangeLive", collection, "*"));
        if (keys.length === 0) return [];
        const pipeline = this.redis.pipeline();
        for (const key of keys) pipeline.get(key);
        const results = await pipeline.exec();
        const stats: RangeLiveStats[] = [];
        if (results) {
            for (const [err, val] of results) {
                if (!err && val && typeof val === "string") {
                    stats.push(JSON.parse(val) as RangeLiveStats);
                }
            }
        }
        return stats;
    }

    async clearRangeLiveStats(collection: string, rangeIdx: number): Promise<void> {
        await this.redis.del(k(this.prefix, "rangeLive", collection, String(rangeIdx)));
    }

    // -----------------------------------------------------------------------
    // Persistent collection estimates (no TTL)
    // -----------------------------------------------------------------------

    /** Bulk-write estimated doc counts for all collections (MSET, single round-trip). */
    async setCollectionEstimates(estimates: Map<string, number>): Promise<void> {
        if (estimates.size === 0) return;
        const args: string[] = [];
        for (const [collection, count] of estimates) {
            args.push(k(this.prefix, "est", collection), String(count));
        }
        await this.redis.mset(...args);
    }

    /** Read all persisted collection estimates (SCAN + MGET). */
    async getAllCollectionEstimates(): Promise<Map<string, number>> {
        const pattern = k(this.prefix, "est", "*");
        const keys = await this.scanKeys(pattern);
        if (keys.length === 0) return new Map();

        const values = await this.redis.mget(...keys);
        const prefix = k(this.prefix, "est") + ":";
        const result = new Map<string, number>();
        for (let i = 0; i < keys.length; i++) {
            const val = values[i];
            if (val !== null) {
                const collection = keys[i].slice(prefix.length);
                result.set(collection, Number(val));
            }
        }
        return result;
    }

    // -----------------------------------------------------------------------
    // Persistent collection completion aggregates (no TTL)
    // -----------------------------------------------------------------------

    /** Write completion aggregate for one collection per pod (SET, no TTL). */
    async setCollectionCompleted(
        collection: string,
        podId: string,
        data: { docsRead: number; rowsInserted: number; runId: string; completedAt: string },
    ): Promise<void> {
        const key = k(this.prefix, "completed", collection, podId);
        await this.redis.set(key, JSON.stringify(data));
    }

    /** Read all completion aggregates (SCAN + MGET), summing per-pod entries per collection. */
    async getAllCollectionCompleted(): Promise<Map<string, { docsRead: number; rowsInserted: number; runId: string; completedAt: string }>> {
        const pattern = k(this.prefix, "completed", "*");
        const keys = await this.scanKeys(pattern);
        if (keys.length === 0) return new Map();

        const values = await this.redis.mget(...keys);
        const prefix = k(this.prefix, "completed") + ":";
        const result = new Map<string, { docsRead: number; rowsInserted: number; runId: string; completedAt: string }>();
        for (let i = 0; i < keys.length; i++) {
            const val = values[i];
            if (val !== null) {
                try {
                    const parsed = JSON.parse(val) as { docsRead: number; rowsInserted: number; runId: string; completedAt: string };
                    // Key format: {prefix}:completed:{collection}:{podId}
                    // Extract collection name by stripping prefix and last :podId segment
                    const suffix = keys[i].slice(prefix.length);
                    const lastColon = suffix.lastIndexOf(":");
                    const collection = lastColon > 0 ? suffix.slice(0, lastColon) : suffix;

                    const existing = result.get(collection);
                    if (existing) {
                        existing.docsRead += parsed.docsRead;
                        existing.rowsInserted += parsed.rowsInserted;
                        if (parsed.completedAt > existing.completedAt) {
                            existing.completedAt = parsed.completedAt;
                        }
                    } else {
                        result.set(collection, { ...parsed });
                    }
                } catch {
                    // skip malformed
                }
            }
        }
        return result;
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
            k(this.prefix, "run", runId, "cursor"),
        ];

        const batchErrorKeys = await this.scanKeys(
            k(this.prefix, "run", runId, "batch", "*", "errors"),
        );
        // liveBatch:* and rangeLive:* keys have TTLs (30s/60s) and self-expire
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
