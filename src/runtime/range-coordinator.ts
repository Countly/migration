import type { Redis } from "ioredis";
import type { Logger } from "pino";
import type { MongoReader } from "../source/mongo-reader.ts";
import type { ManifestStore } from "../state/manifest-store.ts";
import { RedisHotState } from "../state/redis-hot-state.ts";
import type { ClickHouseWriter } from "../target/clickhouse-writer.ts";
import type { ClickHousePressure, BackpressureConfig } from "../target/clickhouse-pressure.ts";
import type { GcController } from "./gc-controller.ts";
import type { RetryPolicy } from "./retry-policy.ts";
import { BatchRunner } from "./batch-runner.ts";
import type { CollectionDefaults } from "../transform/hash-resolver.ts";
import type { AsyncBatchWriter } from "../state/async-batch-writer.ts";
import { serializeCursor } from "../types/cursor.ts";
import { randomUUID } from "node:crypto";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface RangeEntry {
    idx: number;
    startCd: number;  // epoch ms, inclusive
    endCd: number;    // epoch ms, inclusive
    status: "pending" | "processing" | "done" | "failed";
    podId: string | null;
    claimedAt: number | null; // epoch seconds
}

export interface RangeCoordinatorConfig {
    collectionName: string;
    sourceNs: string;
    targetTable: string;
    transformVersion: string;
    rangeCount: number;
    rangeLeaseTtlSec: number;
    batchRowsTarget: number;
    mongoPageSize: number;
    backpressure: BackpressureConfig;
    useDedupToken: boolean;
    database: string;
    table: string;
    snapshotInterval: number;
    collectionDefaults?: CollectionDefaults;
    podId: string;
    redisKeyPrefix: string;
}

export interface RangeCoordinatorDeps {
    redis: Redis;
    manifestStore: ManifestStore;
    redisState: RedisHotState;
    asyncBatchWriter?: AsyncBatchWriter;
    mongoReader: MongoReader;
    chWriter: ClickHouseWriter;
    chPressure: ClickHousePressure;
    gcController: GcController;
    retryPolicy: RetryPolicy;
    logger: Logger;
    config: RangeCoordinatorConfig;
}

export interface RangeResult {
    totalRanges: number;
    completedRanges: number;
    failedRanges: number;
    totalDocsRead: number;
    totalRowsInserted: number;
    runId: string;
}

// ---------------------------------------------------------------------------
// Lua: Claim next pending range (also reclaims stale ranges from dead pods)
//
// This runs atomically on the Redis server via ioredis .eval().
// It is NOT child_process.exec — it is a Redis server-side Lua script.
// ---------------------------------------------------------------------------

const CLAIM_RANGE_LUA = `
local hash = KEYS[1]
local podId = ARGV[1]
local nowSec = tonumber(ARGV[2])
local leaseTtlSec = tonumber(ARGV[3])
local podKeyPrefix = ARGV[4]

local fields = redis.call('HGETALL', hash)

-- First pass: reclaim stale ranges from dead pods
for i = 1, #fields, 2 do
    local data = cjson.decode(fields[i+1])
    if data.status == 'processing' and data.claimedAt then
        local elapsed = nowSec - tonumber(data.claimedAt)
        if elapsed > leaseTtlSec then
            local otherPodKey = podKeyPrefix .. tostring(data.podId)
            local alive = redis.call('EXISTS', otherPodKey)
            if alive == 0 then
                data.status = 'pending'
                data.podId = cjson.null
                data.claimedAt = cjson.null
                redis.call('HSET', hash, fields[i], cjson.encode(data))
            end
        end
    end
end

-- Second pass: claim first pending range
fields = redis.call('HGETALL', hash)
for i = 1, #fields, 2 do
    local data = cjson.decode(fields[i+1])
    if data.status == 'pending' then
        data.status = 'processing'
        data.podId = podId
        data.claimedAt = nowSec
        redis.call('HSET', hash, fields[i], cjson.encode(data))
        return cjson.encode(data)
    end
end
return nil
`;

// ---------------------------------------------------------------------------
// RangeCoordinator
// ---------------------------------------------------------------------------

const BATCH_SEQ_SLOTS_PER_RANGE = 10_000;

export class RangeCoordinator {
    private readonly deps: RangeCoordinatorDeps;
    private readonly logger: Logger;
    private readonly rangesKey: string;
    private readonly initKey: string;
    private readonly runIdKey: string;
    private readonly metaKey: string;
    private stopping = false;

    /** Accumulated docs read across all completed ranges (updated live). */
    totalDocsRead = 0;
    /** Accumulated rows inserted across all completed ranges (updated live). */
    totalRowsInserted = 0;

    constructor(deps: RangeCoordinatorDeps) {
        this.deps = deps;
        this.logger = deps.logger.child({ component: "RangeCoordinator", collection: deps.config.collectionName });
        const prefix = deps.config.redisKeyPrefix;
        const coll = deps.config.collectionName;
        this.rangesKey = `${prefix}:ranges:${coll}`;
        this.initKey = `${prefix}:ranges:${coll}:init`;
        this.runIdKey = `${prefix}:ranges:${coll}:runId`;
        this.metaKey = `${prefix}:ranges:${coll}:meta`;
    }

    async run(): Promise<RangeResult> {
        const { manifestStore, config } = this.deps;

        // 1. Initialize ranges (first pod only, via SETNX)
        const runId = await this.initRanges();

        this.logger.info({ runId, rangeCount: config.rangeCount }, "Starting range-parallel processing");

        this.totalDocsRead = 0;
        this.totalRowsInserted = 0;
        let completedRanges = 0;
        let failedRanges = 0;

        // 2. Claim and process ranges
        while (!this.stopping) {
            const range = await this.claimNextRange();

            if (range) {
                this.logger.info(
                    { rangeIdx: range.idx, startCd: new Date(range.startCd).toISOString(), endCd: new Date(range.endCd).toISOString() },
                    "Claimed range",
                );

                try {
                    const result = await this.processRange(range, runId);
                    this.totalDocsRead += result.docsRead;
                    this.totalRowsInserted += result.rowsInserted;
                    await this.markRangeDone(range.idx);
                    completedRanges++;
                    this.logger.info({ rangeIdx: range.idx, docsRead: result.docsRead, rowsInserted: result.rowsInserted }, "Range completed");
                } catch (err) {
                    const error = err instanceof Error ? err.message : String(err);
                    await this.markRangeFailed(range.idx);
                    failedRanges++;
                    this.logger.error({ rangeIdx: range.idx, error }, "Range failed");
                }
                continue;
            }

            // No pending range — check if any are still processing by other pods
            const status = await this.getRangeStatus();
            if (status.processing > 0) {
                this.logger.info({ processing: status.processing, done: status.done }, "Waiting for other pods to finish ranges");
                await new Promise(r => setTimeout(r, 10_000));
                continue;
            }

            // All done
            break;
        }

        // 3. Mark run complete if all ranges are terminal (only one pod finalizes via SETNX)
        const finalStatus = await this.getRangeStatus();
        if (finalStatus.pending === 0 && finalStatus.processing === 0) {
            const finalizeKey = `${this.rangesKey}:finalized`;
            const acquired = await this.deps.redis.set(finalizeKey, config.podId, "EX", 60, "NX");
            if (acquired) {
                const runStatus = finalStatus.failed > 0 ? "failed" as const : "completed" as const;
                await manifestStore.updateRunStatus(runId, runStatus);
                this.logger.info({ runId, runStatus }, "Run finalized by this pod");
            }
        }

        return {
            totalRanges: config.rangeCount,
            completedRanges,
            failedRanges,
            totalDocsRead: this.totalDocsRead,
            totalRowsInserted: this.totalRowsInserted,
            runId,
        };
    }

    stop(): void {
        this.stopping = true;
    }

    async getRangeStatus(): Promise<{ pending: number; processing: number; done: number; failed: number }> {
        const all = await this.deps.redis.hgetall(this.rangesKey);
        let pending = 0, processing = 0, done = 0, failed = 0;
        for (const val of Object.values(all)) {
            const entry = JSON.parse(val) as RangeEntry;
            if (entry.status === "pending") pending++;
            else if (entry.status === "processing") processing++;
            else if (entry.status === "done") done++;
            else if (entry.status === "failed") failed++;
        }
        return { pending, processing, done, failed };
    }

    // -----------------------------------------------------------------------
    // Private
    // -----------------------------------------------------------------------

    private async initRanges(): Promise<string> {
        const { redis, mongoReader, manifestStore, config } = this.deps;

        // Check if ranges already initialized by another pod
        const existingRunId = await redis.get(this.runIdKey);
        if (existingRunId) {
            this.logger.info({ runId: existingRunId }, "Ranges already initialized by another pod");
            return existingRunId;
        }

        // Try to become the coordinator via SETNX
        const acquired = await redis.set(this.initKey, config.podId, "EX", 60, "NX");
        if (!acquired) {
            this.logger.info("Another pod is initializing ranges, waiting...");
            for (let i = 0; i < 30; i++) {
                await new Promise(r => setTimeout(r, 2_000));
                const rid = await redis.get(this.runIdKey);
                if (rid) return rid;
            }
            throw new Error("Timed out waiting for range initialization");
        }

        // We are the coordinator — query min/max cd
        const lowerBound = await mongoReader.getLowerBound();
        const upperBound = await mongoReader.getUpperBound();
        if (!lowerBound || !upperBound) {
            throw new Error("Collection is empty, cannot initialize ranges");
        }

        const minCd = lowerBound.cd;
        const maxCd = upperBound.cd;
        const rangeCount = config.rangeCount;
        const spanMs = maxCd - minCd;
        const stepMs = Math.max(1, Math.ceil(spanMs / rangeCount));

        // Create shared run in ManifestStore
        const runId = randomUUID();
        const now = new Date().toISOString();
        await manifestStore.createRun({
            run_id: runId,
            status: "active",
            source_ns: config.sourceNs,
            target_table: config.targetTable,
            upper_bound_cursor: serializeCursor(upperBound),
            transform_version: config.transformVersion,
            created_at: now,
        });

        // Create all ranges in Redis (atomic pipeline)
        const pipeline = redis.multi();
        for (let i = 0; i < rangeCount; i++) {
            const startCd = minCd + (i * stepMs);
            const endCd = i === rangeCount - 1 ? maxCd : minCd + ((i + 1) * stepMs);
            const entry: RangeEntry = {
                idx: i,
                startCd,
                endCd,
                status: "pending",
                podId: null,
                claimedAt: null,
            };
            pipeline.hset(this.rangesKey, String(i), JSON.stringify(entry));
        }
        pipeline.set(this.runIdKey, runId);
        pipeline.set(this.metaKey, JSON.stringify({ minCd, maxCd, rangeCount, createdAt: now }));
        await pipeline.exec();

        this.logger.info(
            { runId, minCd: new Date(minCd).toISOString(), maxCd: new Date(maxCd).toISOString(), rangeCount, stepMs },
            "Ranges initialized",
        );
        return runId;
    }

    private async claimNextRange(): Promise<RangeEntry | null> {
        const { redis, config } = this.deps;
        const nowSec = String(Math.floor(Date.now() / 1000));
        const podKeyPrefix = `${config.redisKeyPrefix}:pod:`;

        // ioredis .eval() runs a Lua script atomically on the Redis server
        const result = await redis.eval(
            CLAIM_RANGE_LUA,
            1,
            this.rangesKey,
            config.podId,
            nowSec,
            String(config.rangeLeaseTtlSec),
            podKeyPrefix,
        ) as string | null;

        if (!result) return null;
        return JSON.parse(result) as RangeEntry;
    }

    private async markRangeDone(idx: number): Promise<void> {
        const raw = await this.deps.redis.hget(this.rangesKey, String(idx));
        if (!raw) return;
        const entry = JSON.parse(raw) as RangeEntry;
        entry.status = "done";
        await this.deps.redis.hset(this.rangesKey, String(idx), JSON.stringify(entry));
    }

    private async markRangeFailed(idx: number): Promise<void> {
        const raw = await this.deps.redis.hget(this.rangesKey, String(idx));
        if (!raw) return;
        const entry = JSON.parse(raw) as RangeEntry;
        entry.status = "failed";
        await this.deps.redis.hset(this.rangesKey, String(idx), JSON.stringify(entry));
    }

    private async processRange(range: RangeEntry, runId: string): Promise<{ docsRead: number; rowsInserted: number }> {
        const { manifestStore, asyncBatchWriter, mongoReader, chWriter, chPressure, gcController, retryPolicy, config } = this.deps;

        // Cursor bounds: [startCd, endCd) for non-final ranges, [startCd, maxCd] for final
        const startCursorStr = serializeCursor({ cd: range.startCd, id: "" });
        const isFinalRange = range.idx === config.rangeCount - 1;
        const upperBoundStr = isFinalRange
            ? serializeCursor({ cd: range.endCd, id: "\uffff".repeat(24) })
            : serializeCursor({ cd: range.endCd, id: "" });

        const batchSeqOffset = range.idx * BATCH_SEQ_SLOTS_PER_RANGE;
        const rangeStartedAt = Date.now();

        // Per-range RedisHotState (shares connection, isolated key prefix)
        const rangeRedisState = RedisHotState.fromExistingConnection(
            this.deps.redis,
            `${config.redisKeyPrefix}:${config.collectionName}:range${range.idx}`,
        );

        // Write initial range live stats
        await this.deps.redisState.setRangeLiveStats(config.collectionName, range.idx, {
            idx: range.idx,
            status: "processing",
            podId: config.podId,
            docsRead: 0,
            rowsInserted: 0,
            batchesDone: 0,
            docsPerSecond: 0,
            startedAt: rangeStartedAt,
        }).catch(() => {});

        const batchRunner = new BatchRunner({
            manifestStore,
            redisState: rangeRedisState,
            globalRedisState: this.deps.redisState,
            asyncBatchWriter,
            mongoReader,
            chWriter,
            chPressure,
            gcController,
            retryPolicy,
            logger: this.deps.logger,
            config: {
                runId,
                transformVersion: config.transformVersion,
                sourceNs: config.sourceNs,
                targetTable: config.targetTable,
                upperBoundId: upperBoundStr,
                batchRowsTarget: config.batchRowsTarget,
                mongoPageSize: config.mongoPageSize,
                backpressure: config.backpressure,
                useDedupToken: config.useDedupToken,
                database: config.database,
                table: config.table,
                snapshotInterval: config.snapshotInterval,
                collectionDefaults: config.collectionDefaults,
                batchSeqOffset,
                collectionName: config.collectionName,
                podId: config.podId,
                rangeIdx: range.idx,
            },
        });

        await batchRunner.run(startCursorStr);

        const stats = batchRunner.getStats();

        // Update range live stats on completion
        const elapsedSec = (Date.now() - rangeStartedAt) / 1000;
        await this.deps.redisState.setRangeLiveStats(config.collectionName, range.idx, {
            idx: range.idx,
            status: "done",
            podId: config.podId,
            docsRead: stats.totalDocsRead,
            rowsInserted: stats.totalRowsInserted,
            batchesDone: (stats.batchSeq - batchSeqOffset) - stats.batchesFailed,
            docsPerSecond: elapsedSec > 0 ? stats.totalDocsRead / elapsedSec : 0,
            startedAt: rangeStartedAt,
        }).catch(() => {});

        return { docsRead: stats.totalDocsRead, rowsInserted: stats.totalRowsInserted };
    }
}
