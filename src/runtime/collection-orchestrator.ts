import type { Logger } from "pino";
import type { ManifestStore } from "../state/manifest-store.ts";
import { RedisHotState } from "../state/redis-hot-state.ts";
import type { MongoReader } from "../source/mongo-reader.ts";
import type { ClickHouseWriter } from "../target/clickhouse-writer.ts";
import type { ClickHousePressure, BackpressureConfig } from "../target/clickhouse-pressure.ts";
import type { GcController } from "./gc-controller.ts";
import type { RetryPolicy } from "./retry-policy.ts";
import { BatchRunner, type RunnerStatus, type BatchRunnerStats } from "./batch-runner.ts";
import { resolveRun } from "./resolve-run.ts";
import { discoverCollections } from "../source/discover-collections.ts";
import type { HashResolver } from "../transform/hash-resolver.ts";
import type { Config } from "../config/schema.ts";
import type { CollectionLock } from "../state/collection-lock.ts";
import type { GlobalProgress, CollectionProgress } from "../state/global-progress.ts";
import type { AsyncBatchWriter } from "../state/async-batch-writer.ts";
import { RangeCoordinator } from "./range-coordinator.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type IndexBuildStatus = "checking" | "building" | "ready" | "failed";

export interface IndexStatusSummary {
    ready: number;
    building: number;
    checking: number;
    failed: number;
    details: Array<{ collection: string; status: IndexBuildStatus; elapsedSec?: number }>;
}

export interface CollectionResult {
    collection: string;
    sourceNs: string;
    runId: string;
    status: "completed" | "failed" | "skipped";
    error?: string;
    docsRead?: number;
    rowsInserted?: number;
}

export interface OrchestratorResult {
    collections: CollectionResult[];
    totalCompleted: number;
    totalFailed: number;
    totalSkipped: number;
}

export interface OrchestratorProgress {
    totalCollections: number;
    completedCollections: number;
    failedCollections: number;
    skippedCollections: number;
    currentCollection: string | null;
    collections: string[];
    results: CollectionResult[];
}

export interface OrchestratorDeps {
    manifestStore: ManifestStore;
    redisState: RedisHotState;
    mongoReader: MongoReader;
    chWriter: ClickHouseWriter;
    chPressure: ClickHousePressure;
    gcController: GcController;
    retryPolicy: RetryPolicy;
    hashResolver: HashResolver;
    logger: Logger;
    config: Config;
    collectionLock?: CollectionLock;
    globalProgress?: GlobalProgress;
    asyncBatchWriter?: AsyncBatchWriter;
}

// ---------------------------------------------------------------------------
// CollectionOrchestrator
// ---------------------------------------------------------------------------

export class CollectionOrchestrator {
    private readonly deps: OrchestratorDeps;
    private readonly logger: Logger;

    private discoveredCollections: string[] = [];
    private skippedApmCollections: Set<string> = new Set();
    private results: CollectionResult[] = [];
    private estimatedCounts: Map<string, number> = new Map();
    private indexStatus: Map<string, IndexBuildStatus> = new Map();
    private indexBuildStarted: Map<string, number> = new Map();
    private currentCollection: string | null = null;
    private currentRunId: string | null = null;
    private currentBatchRunner: BatchRunner | null = null;
    private lastBatchStats: BatchRunnerStats | null = null;
    private currentRangeCoordinator: RangeCoordinator | null = null;
    private currentRedisState: RedisHotState | null = null;
    private orchestratorStatus: "idle" | "running" | "waiting_for_index" | "completed" = "idle";
    private stopping = false;

    constructor(deps: OrchestratorDeps) {
        this.deps = deps;
        this.logger = deps.logger.child({ component: "CollectionOrchestrator" });
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    async run(): Promise<OrchestratorResult> {
        const { mongoReader, manifestStore, config, collectionLock, globalProgress } = this.deps;

        // 1. Discover collections
        const db = mongoReader.getDatabase();
        this.discoveredCollections = await discoverCollections(db, config.source.collectionPrefix, this.logger);

        // Filter out APM collections by resolved event name
        const skipEventNames = new Set(['[CLY]_apm_device', '[CLY]_apm_network']);
        this.discoveredCollections = this.discoveredCollections.filter(name => {
            const defaults = this.deps.hashResolver.resolveCollectionName(
                name, config.source.collectionPrefix,
            );
            if (defaults && skipEventNames.has(defaults.e)) {
                this.skippedApmCollections.add(name);
                this.results.push({
                    collection: name,
                    sourceNs: `${config.source.db}.${name}`,
                    runId: '',
                    status: 'skipped',
                    error: 'apm_collections',
                });
                this.logger.info({ collection: name, event: defaults.e }, 'Skipping APM collection');
                return false;
            }
            return true;
        });
        if (this.skippedApmCollections.size > 0) {
            this.logger.info(
                { skipped: this.skippedApmCollections.size, remaining: this.discoveredCollections.length },
                'Filtered APM collections from discovery',
            );
        }

        this.logger.info(
            { totalCollections: this.discoveredCollections.length, multiPod: !!collectionLock },
            "Starting migration with index-aware scheduling",
        );

        // 2a. Upfront: estimate ALL collections in parallel and persist to Redis
        await this.populateAllEstimates();

        // 2b. Recover completed collection aggregates from Redis (+ manifest fallback)
        await this.recoverCompletedCollections();

        // 3. Start lock heartbeat (multi-pod mode)
        collectionLock?.startHeartbeat();

        // 3. Start all index checks in background (non-blocking)
        this.startBackgroundIndexInit();

        this.orchestratorStatus = "running";

        // 4. Process loop: pick ready collections, wait for building ones
        while (!this.stopping) {
            // Check global commands (multi-pod mode)
            if (globalProgress) {
                try {
                    const globalCmds = await globalProgress.getGlobalCommands();
                    if (globalCmds.stop) {
                        this.logger.info("Global stop command received");
                        this.stopping = true;
                        break;
                    }
                    if (globalCmds.pause) {
                        this.currentBatchRunner?.pause();
                    }
                } catch {
                    // best-effort
                }
            }

            const completed = new Set(this.results.map(r => r.collection));

            // Find next collection with index ready, not yet processed, and not locked by another pod
            const next = await this.findNextReadyCollection(completed);

            if (next) {
                this.orchestratorStatus = "running";
                const sourceNs = `${config.source.db}.${next}`;
                const targetTable = `${config.target.db}.${config.target.table}`;

                // Check if range-parallel (uses cached estimate from isRangeParallelCandidate or fresh query)
                const isRangeParallel = await this.isRangeParallelCandidate(next);

                // Already recovered from Redis in recoverCompletedCollections? Skip.
                if (this.results.some(r => r.collection === next)) continue;

                // For range-parallel: check if ranges are all done (not existsCompletedRun)
                // For standard: check if already completed
                if (!isRangeParallel) {
                    const alreadyCompleted = await manifestStore.existsCompletedRun(sourceNs, targetTable);
                    if (alreadyCompleted) {
                        // Manifest says completed but Redis had no data (flushed) — recover from manifest
                        const completedRun = await manifestStore.getCompletedRun(sourceNs, targetTable);
                        const docsRead = completedRun?.summary?.total_docs_read ?? 0;
                        const rowsInserted = completedRun?.summary?.total_rows_inserted ?? 0;
                        const completedRunId = completedRun?.run_id ?? "";
                        this.logger.info({ collection: next, sourceNs, docsRead, rowsInserted }, "Collection already migrated, skipping (manifest fallback)");
                        this.results.push({ collection: next, sourceNs, runId: completedRunId, status: "skipped", docsRead, rowsInserted });

                        // Backfill Redis for next restart
                        await this.deps.redisState.setCollectionCompleted(next, {
                            docsRead, rowsInserted, runId: completedRunId,
                            completedAt: new Date().toISOString(),
                        }).catch(() => {});
                        continue;
                    }
                }

                // Process this collection
                try {
                    const result = await this.processCollection(next, sourceNs, targetTable);
                    this.results.push(result);

                    if (result.status === "completed") {
                        this.logger.info({ collection: next, runId: result.runId }, "Collection migration completed");
                        // Persist completion aggregate to Redis (no TTL)
                        await this.deps.redisState.setCollectionCompleted(next, {
                            docsRead: result.docsRead ?? 0,
                            rowsInserted: result.rowsInserted ?? 0,
                            runId: result.runId,
                            completedAt: new Date().toISOString(),
                        }).catch(() => {});
                    } else if (result.status === "failed") {
                        this.logger.error(
                            { collection: next, runId: result.runId, error: result.error },
                            "Collection migration failed, continuing to next",
                        );
                    }
                } catch (err) {
                    const error = err instanceof Error ? err.message : String(err);
                    this.logger.error({ collection: next, error }, "Unexpected error migrating collection");
                    this.results.push({ collection: next, sourceNs, runId: "", status: "failed", error });
                } finally {
                    // Explicitly release collection lock (normal path, only for non-range-parallel)
                    if (!isRangeParallel) {
                        await collectionLock?.release(next).catch(() => {});
                    }
                }
                continue;
            }

            // No ready collection — are some still checking or building?
            const counts = this.getIndexStatusCounts();
            const pendingIndexWork = counts.checking + counts.building;

            if (pendingIndexWork > 0) {
                this.orchestratorStatus = "waiting_for_index";
                this.logger.info(
                    { checking: counts.checking, building: counts.building, ready: counts.ready },
                    "Waiting for index creation — rechecking in 10s",
                );
                await this.interruptibleSleep(10_000);
                await this.recheckBuildingIndexes();
                continue;
            }

            // In multi-pod mode, remaining collections may be locked by other pods — wait and retry
            if (collectionLock) {
                const remaining = this.discoveredCollections.filter(
                    name => !completed.has(name) && this.indexStatus.get(name) === "ready",
                );
                if (remaining.length > 0) {
                    this.logger.info(
                        { lockedByOthers: remaining.length },
                        "Remaining collections locked by other pods, waiting 10s",
                    );
                    await this.interruptibleSleep(10_000);
                    continue;
                }
            }

            // All done (or all failed/skipped)
            break;
        }

        this.currentCollection = null;
        this.currentRunId = null;
        this.currentBatchRunner = null;
        this.currentRangeCoordinator = null;
        this.orchestratorStatus = "completed";

        // Cleanup multi-pod resources
        collectionLock?.stopHeartbeat();
        await collectionLock?.releaseAll().catch(() => {});

        // Summary
        const summary: OrchestratorResult = {
            collections: this.results,
            totalCompleted: this.results.filter((r) => r.status === "completed").length,
            totalFailed: this.results.filter((r) => r.status === "failed").length,
            totalSkipped: this.results.filter((r) => r.status === "skipped").length,
        };

        this.logger.info(
            {
                totalCollections: this.discoveredCollections.length + this.skippedApmCollections.size,
                completed: summary.totalCompleted,
                failed: summary.totalFailed,
                skipped: summary.totalSkipped,
                skippedApm: this.skippedApmCollections.size,
            },
            "Migration orchestration complete",
        );

        return summary;
    }

    getProgress(): OrchestratorProgress {
        return {
            totalCollections: this.discoveredCollections.length + this.skippedApmCollections.size,
            completedCollections: this.results.filter((r) => r.status === "completed").length,
            failedCollections: this.results.filter((r) => r.status === "failed").length,
            skippedCollections: this.results.filter((r) => r.status === "skipped").length,
            currentCollection: this.currentCollection,
            collections: this.discoveredCollections,
            results: this.results,
        };
    }

    getCurrentBatchRunner(): BatchRunner | null {
        return this.currentBatchRunner;
    }

    getCurrentRunId(): string | null {
        return this.currentRunId;
    }

    pause(): void {
        this.currentBatchRunner?.pause();
    }

    resume(): void {
        this.currentBatchRunner?.resume();
    }

    stopAfterBatch(): void {
        this.stopping = true;
        this.currentBatchRunner?.stopAfterBatch();
    }

    async waitForStop(): Promise<void> {
        if (this.currentBatchRunner) {
            await this.currentBatchRunner.waitForStop();
        }
    }

    getStatus(): RunnerStatus {
        if (this.orchestratorStatus === "waiting_for_index") {
            return "waiting_for_index";
        }
        if (this.currentBatchRunner) {
            return this.currentBatchRunner.getStatus();
        }
        if (this.currentRangeCoordinator) {
            return "running";
        }
        if (this.orchestratorStatus === "completed") {
            return "completed";
        }
        if (this.discoveredCollections.length > 0 && this.results.length >= this.discoveredCollections.length) {
            return "completed";
        }
        return "idle";
    }

    getStats(): BatchRunnerStats | null {
        if (this.currentBatchRunner) {
            const stats = this.currentBatchRunner.getStats();
            this.lastBatchStats = stats;
            return stats;
        }
        if (this.currentRangeCoordinator) {
            return {
                status: "running",
                batchSeq: 0,
                lastCommittedId: null,
                totalDocsRead: this.currentRangeCoordinator.totalDocsRead,
                totalRowsInserted: this.currentRangeCoordinator.totalRowsInserted,
                totalDocsSkipped: 0,
                skipsByReason: {} as Record<string, number>,
                elapsedMs: 0,
                docsPerSecond: 0,
                rowsPerSecond: 0,
                batchesFailed: 0,
                digestMismatches: 0,
                estimatedDuplicateRows: 0,
            } as BatchRunnerStats;
        }
        // Return cached stats from the last active runner (preserves skip reasons etc.)
        return this.lastBatchStats;
    }

    getCurrentBatchSeq(): number {
        return this.currentBatchRunner?.getCurrentBatchSeq() ?? 0;
    }

    getEstimatedCounts(): Map<string, number> {
        return new Map(this.estimatedCounts);
    }

    getIndexStatus(): IndexStatusSummary {
        let ready = 0, building = 0, failed = 0, checking = 0;
        const details: Array<{ collection: string; status: IndexBuildStatus; elapsedSec?: number }> = [];

        for (const [name, status] of this.indexStatus) {
            if (status === "ready") {
                ready++;
            } else if (status === "building") {
                building++;
                const elapsed = Math.round((Date.now() - (this.indexBuildStarted.get(name) ?? Date.now())) / 1000);
                details.push({ collection: name, status, elapsedSec: elapsed });
            } else if (status === "failed") {
                failed++;
                details.push({ collection: name, status });
            } else if (status === "checking") {
                checking++;
                details.push({ collection: name, status });
            }
        }

        return { ready, building, checking, failed, details };
    }

    triggerReindex(collectionName: string): void {
        this.indexStatus.set(collectionName, "building");
        this.indexBuildStarted.set(collectionName, Date.now());
        this.logger.info({ collection: collectionName }, "Manual reindex triggered");
        this.deps.mongoReader.startIndexCreation(collectionName)
            .then(() => {
                this.indexStatus.set(collectionName, "ready");
                const elapsed = Math.round((Date.now() - (this.indexBuildStarted.get(collectionName) ?? Date.now())) / 1000);
                this.logger.info({ collection: collectionName, durationSec: elapsed }, "Manual reindex completed");
            })
            .catch((err) => {
                this.indexStatus.set(collectionName, "failed");
                this.logger.error({ collection: collectionName, err: err instanceof Error ? err.message : String(err) }, "Manual reindex failed");
            });
    }

    retryCollection(collectionName: string): void {
        if (this.skippedApmCollections.has(collectionName)) {
            this.logger.warn({ collection: collectionName }, "Cannot retry APM collection — permanently excluded");
            return;
        }
        this.results = this.results.filter(r => r.collection !== collectionName);
        this.logger.info({ collection: collectionName }, "Collection queued for retry");
    }

    // -----------------------------------------------------------------------
    // Private: Upfront Estimates & Recovery
    // -----------------------------------------------------------------------

    /**
     * Query estimatedDocumentCount() for ALL discovered collections in parallel
     * and persist to Redis. This ensures the overall progress denominator is
     * correct from the first stats request.
     */
    private async populateAllEstimates(): Promise<void> {
        const db = this.deps.mongoReader.getDatabase();
        const CHUNK_SIZE = 10;

        for (let i = 0; i < this.discoveredCollections.length; i += CHUNK_SIZE) {
            const chunk = this.discoveredCollections.slice(i, i + CHUNK_SIZE);
            const results = await Promise.allSettled(
                chunk.map(async (name) => {
                    const est = await db.collection(name).estimatedDocumentCount();
                    return { name, est };
                }),
            );
            for (const r of results) {
                if (r.status === "fulfilled") {
                    this.estimatedCounts.set(r.value.name, r.value.est);
                }
            }
        }

        // Persist to Redis for other pods and resume
        await this.deps.redisState.setCollectionEstimates(this.estimatedCounts).catch((err) => {
            this.logger.warn({ error: err instanceof Error ? err.message : String(err) }, "Failed to persist estimates to Redis");
        });

        this.logger.info(
            { collections: this.estimatedCounts.size, totalEstimated: Array.from(this.estimatedCounts.values()).reduce((a, b) => a + b, 0) },
            "Estimated counts for all collections stored in Redis",
        );
    }

    /**
     * On resume: recover completed collection aggregates from Redis.
     * Populates this.results so completed collections aren't re-processed
     * and their docsRead/rowsInserted are available for progress calculations.
     */
    private async recoverCompletedCollections(): Promise<void> {
        const completedFromRedis = await this.deps.redisState.getAllCollectionCompleted().catch(() => new Map());
        if (completedFromRedis.size === 0) return;

        const { config } = this.deps;
        let recovered = 0;

        for (const [collection, data] of completedFromRedis) {
            // Only recover if this collection is in our discovered list
            if (!this.discoveredCollections.includes(collection)) continue;
            // Don't double-add (e.g. APM skip results already in this.results)
            if (this.results.some(r => r.collection === collection)) continue;

            this.results.push({
                collection,
                sourceNs: `${config.source.db}.${collection}`,
                runId: data.runId,
                status: "skipped",
                docsRead: data.docsRead,
                rowsInserted: data.rowsInserted,
            });
            recovered++;
        }

        if (recovered > 0) {
            this.logger.info(
                { recovered, total: completedFromRedis.size },
                "Recovered completed collection aggregates from Redis",
            );
        }
    }

    // -----------------------------------------------------------------------
    // Private: Index Management
    // -----------------------------------------------------------------------

    /**
     * Start all index checks in the background. Does NOT block.
     * drill_events is first in the list so it gets checked first,
     * but we don't wait for it — migrate whatever is ready first.
     */
    private startBackgroundIndexInit(): void {
        for (const name of this.discoveredCollections) {
            this.indexStatus.set(name, "checking");
        }

        this.runIndexChecks().catch((err) => {
            this.logger.error({ err: err instanceof Error ? err.message : String(err) }, "Background index init crashed");
        });
    }

    private async runIndexChecks(): Promise<void> {
        const CONCURRENCY = 10;
        for (let i = 0; i < this.discoveredCollections.length; i += CONCURRENCY) {
            const chunk = this.discoveredCollections.slice(i, i + CONCURRENCY);
            await Promise.allSettled(chunk.map(name => this.checkAndBuildIndex(name)));
        }
        const counts = this.getIndexStatusCounts();
        this.logger.info(counts, "Background index initialization complete");
    }

    private async checkAndBuildIndex(name: string): Promise<void> {
        const { mongoReader } = this.deps;
        try {
            const hasIndex = await mongoReader.hasRequiredIndex(name);
            if (hasIndex) {
                this.indexStatus.set(name, "ready");
                return;
            }
            this.indexStatus.set(name, "building");
            this.indexBuildStarted.set(name, Date.now());
            mongoReader.startIndexCreation(name)
                .then(() => {
                    this.indexStatus.set(name, "ready");
                    const elapsed = Math.round((Date.now() - (this.indexBuildStarted.get(name) ?? Date.now())) / 1000);
                    this.logger.info({ collection: name, durationSec: elapsed }, "Background index build completed");
                })
                .catch((err) => {
                    this.indexStatus.set(name, "failed");
                    this.logger.error({ collection: name, err: err instanceof Error ? err.message : String(err) }, "Index creation failed");
                });
        } catch (err) {
            this.indexStatus.set(name, "failed");
            this.logger.error({ collection: name, err: err instanceof Error ? err.message : String(err) }, "Failed to check index");
        }
    }

    private getIndexStatusCounts(): { ready: number; building: number; failed: number; checking: number; total: number } {
        let ready = 0, building = 0, failed = 0, checking = 0;
        for (const s of this.indexStatus.values()) {
            if (s === "ready") ready++;
            else if (s === "building") building++;
            else if (s === "failed") failed++;
            else checking++;
        }
        return { ready, building, failed, checking, total: this.discoveredCollections.length };
    }

    private async findNextReadyCollection(completed: Set<string>): Promise<string | null> {
        const { collectionLock } = this.deps;

        // Priority 1: Any unlocked, ready collection (spread pods across different collections)
        for (const name of this.discoveredCollections) {
            if (completed.has(name)) continue;
            if (this.indexStatus.get(name) !== "ready") continue;
            if (!collectionLock || (await collectionLock.tryAcquire(name)) !== "locked") {
                return name;
            }
        }

        // Priority 2: All remaining are locked by other pods — join range-parallel
        // on any large collection that has pending ranges (no lock needed)
        for (const name of this.discoveredCollections) {
            if (completed.has(name)) continue;
            if (this.indexStatus.get(name) !== "ready") continue;
            if (await this.isRangeParallelCandidate(name)) {
                return name;
            }
        }

        return null;
    }

    /** Check if a collection qualifies for range-parallel (cached or fresh query).
     *  Uses getDatabase() directly to avoid mutating the shared MongoReader cursor. */
    private async isRangeParallelCandidate(collectionName: string): Promise<boolean> {
        const rpThreshold = this.deps.config.source.rangeParallelThreshold;
        let est = this.estimatedCounts.get(collectionName);
        if (est === undefined) {
            try {
                const db = this.deps.mongoReader.getDatabase();
                est = await db.collection(collectionName).estimatedDocumentCount();
                this.estimatedCounts.set(collectionName, est);
            } catch {
                return false;
            }
        }
        return est >= rpThreshold;
    }

    private getCollectionsWithStatus(status: IndexBuildStatus): string[] {
        return [...this.indexStatus.entries()]
            .filter(([, s]) => s === status)
            .map(([name]) => name);
    }

    private async recheckBuildingIndexes(): Promise<void> {
        const { mongoReader } = this.deps;
        const building = [...this.indexStatus.entries()].filter(([, s]) => s === "building");
        if (building.length === 0) return;

        await Promise.allSettled(building.map(async ([name]) => {
            try {
                const ready = await mongoReader.hasRequiredIndex(name);
                if (ready) {
                    const elapsed = Math.round((Date.now() - (this.indexBuildStarted.get(name) ?? Date.now())) / 1000);
                    this.indexStatus.set(name, "ready");
                    this.logger.info({ collection: name, durationSec: elapsed }, "Index build detected as complete");
                }
            } catch (err) {
                this.logger.warn({ collection: name, err: err instanceof Error ? err.message : String(err) }, "Failed to recheck index");
            }
        }));
    }

    private async interruptibleSleep(ms: number): Promise<void> {
        return new Promise<void>((resolve) => {
            let resolved = false;
            const done = () => { if (resolved) return; resolved = true; clearTimeout(timer); clearInterval(check); resolve(); };
            const timer = setTimeout(done, ms);
            const check = setInterval(() => { if (this.stopping) done(); }, 1000);
        });
    }

    // -----------------------------------------------------------------------
    // Private: Collection Processing
    // -----------------------------------------------------------------------

    private async processCollection(
        collectionName: string,
        sourceNs: string,
        targetTable: string,
    ): Promise<CollectionResult> {
        const { mongoReader, manifestStore, chWriter, chPressure, gcController, retryPolicy, config } = this.deps;

        this.currentCollection = collectionName;

        // Switch MongoReader to this collection (index already confirmed ready)
        await mongoReader.switchCollection(collectionName);

        // Use cached estimated count (populated by main loop or isRangeParallelCandidate)
        const estimatedCount = this.estimatedCounts.get(collectionName) ?? await mongoReader.getEstimatedCount();
        this.estimatedCounts.set(collectionName, estimatedCount);
        this.logger.info({ collection: collectionName, estimatedCount }, "Estimated document count");

        // Resolve collection defaults from hash (once per collection)
        const collectionDefaults = this.deps.hashResolver.resolveCollectionName(
            collectionName,
            config.source.collectionPrefix,
        );

        if (collectionDefaults) {
            this.logger.info(
                { collection: collectionName, appId: collectionDefaults.a, event: collectionDefaults.e },
                "Resolved collection hash defaults",
            );
        } else if (collectionName !== config.source.collectionPrefix) {
            this.logger.warn(
                { collection: collectionName },
                "No hash match found for collection — documents missing a/e will be skipped",
            );
        }

        // ── Range-parallel mode for large collections ─────────────────
        if (estimatedCount >= config.source.rangeParallelThreshold) {
            this.logger.info(
                { collection: collectionName, estimatedCount, threshold: config.source.rangeParallelThreshold },
                "Collection exceeds range-parallel threshold — using range splitting",
            );

            const redisClient = this.deps.redisState.getRedisClient();
            const coordinator = new RangeCoordinator({
                redis: redisClient,
                manifestStore,
                redisState: this.deps.redisState,
                asyncBatchWriter: this.deps.asyncBatchWriter,
                mongoReader,
                chWriter,
                chPressure,
                gcController,
                retryPolicy,
                logger: this.deps.logger,
                config: {
                    collectionName,
                    sourceNs,
                    targetTable,
                    transformVersion: config.transform.version,
                    rangeCount: config.source.rangeCount,
                    rangeLeaseTtlSec: config.source.rangeLeaseTtlSec,
                    batchRowsTarget: config.source.batchRowsTarget,
                    mongoPageSize: config.source.mongoPageSize,
                    backpressure: config.backpressure,
                    useDedupToken: config.target.useDedupToken,
                    database: config.target.db,
                    table: config.target.table,
                    snapshotInterval: config.state.timelineSnapshotInterval,
                    collectionDefaults: collectionDefaults ?? undefined,
                    podId: config.worker.podId,
                    redisKeyPrefix: config.state.redisKeyPrefix,
                },
            });

            this.currentRangeCoordinator = coordinator;

            // Progress updates for range-parallel mode
            const rpStartedAt = new Date().toISOString();
            const rpGlobalProgress = this.deps.globalProgress;
            const rpProgressInterval = rpGlobalProgress
                ? setInterval(async () => {
                    const status = await coordinator.getRangeStatus().catch(() => ({ pending: 0, processing: 0, done: 0, failed: 0 }));
                    rpGlobalProgress.updateCollectionProgress({
                        collectionName,
                        podId: config.worker.podId,
                        status: "processing",
                        runId: "",
                        docsRead: coordinator.totalDocsRead,
                        rowsInserted: coordinator.totalRowsInserted,
                        estimatedTotal: estimatedCount,
                        batchSeq: status.done,
                        startedAt: rpStartedAt,
                        updatedAt: new Date().toISOString(),
                        isRangeParallel: true,
                        rangeCount: config.source.rangeCount,
                    }).catch(() => {});
                }, config.worker.progressUpdateMs)
                : null;

            try {
                const rangeResult = await coordinator.run();

                if (rpProgressInterval) clearInterval(rpProgressInterval);
                const rpStatus = rangeResult.failedRanges > 0 ? "failed" as const : "completed" as const;
                await rpGlobalProgress?.updateCollectionProgress({
                    collectionName,
                    podId: config.worker.podId,
                    status: rpStatus,
                    runId: rangeResult.runId,
                    docsRead: rangeResult.totalDocsRead,
                    rowsInserted: rangeResult.totalRowsInserted,
                    estimatedTotal: estimatedCount,
                    batchSeq: rangeResult.completedRanges,
                    startedAt: rpStartedAt,
                    updatedAt: new Date().toISOString(),
                    isRangeParallel: true,
                    rangeCount: config.source.rangeCount,
                }).catch(() => {});

                return {
                    collection: collectionName,
                    sourceNs,
                    runId: rangeResult.runId,
                    status: rangeResult.failedRanges > 0 ? "failed" : "completed",
                    docsRead: rangeResult.totalDocsRead,
                    rowsInserted: rangeResult.totalRowsInserted,
                    error: rangeResult.failedRanges > 0 ? `${rangeResult.failedRanges} ranges failed` : undefined,
                };
            } finally {
                if (rpProgressInterval) clearInterval(rpProgressInterval);
                this.currentRangeCoordinator = null;
            }
        }

        // ── Standard single-pod mode ─────────────────────────────────

        // Create per-collection RedisHotState with isolated key prefix
        const redisClient = this.deps.redisState.getRedisClient();
        const collectionPrefix = `${config.state.redisKeyPrefix}:${collectionName}`;
        const redisState = RedisHotState.fromExistingConnection(redisClient, collectionPrefix);
        this.currentRedisState = redisState;

        // Resolve run (resume or new)
        const resolved = await resolveRun({
            rerunMode: config.service.rerunMode,
            manifestStore,
            redisState,
            mongoReader,
            sourceNs,
            targetTable,
            transformVersion: config.transform.version,
            logger: this.logger,
        });

        const { runId, upperBoundId } = resolved;
        this.currentRunId = runId;

        if (resolved.isEmpty) {
            this.currentRunId = null;
            this.logger.info({ collection: collectionName }, "Collection is empty, skipping");
            return { collection: collectionName, sourceNs, runId, status: "completed" };
        }

        this.logger.info(
            { collection: collectionName, runId, upperBoundId },
            "Starting migration for collection",
        );

        // Create BatchRunner for this collection
        const batchRunner = new BatchRunner({
            manifestStore,
            redisState,
            globalRedisState: this.deps.redisState,
            asyncBatchWriter: this.deps.asyncBatchWriter,
            mongoReader,
            chWriter,
            chPressure,
            gcController,
            retryPolicy,
            logger: this.deps.logger,
            config: {
                runId,
                transformVersion: config.transform.version,
                sourceNs,
                targetTable,
                upperBoundId,
                batchRowsTarget: config.source.batchRowsTarget,
                mongoPageSize: config.source.mongoPageSize,
                backpressure: config.backpressure,
                useDedupToken: config.target.useDedupToken,
                database: config.target.db,
                table: config.target.table,
                snapshotInterval: config.state.timelineSnapshotInterval,
                collectionDefaults: collectionDefaults ?? undefined,
                collectionName,
                podId: config.worker.podId,
            },
        });

        this.currentBatchRunner = batchRunner;

        // Start periodic progress updates (multi-pod mode)
        const { globalProgress } = this.deps;
        const progressStartedAt = new Date().toISOString();
        const progressInterval = globalProgress
            ? setInterval(() => {
                const stats = batchRunner.getStats();
                globalProgress.updateCollectionProgress({
                    collectionName,
                    podId: config.worker.podId,
                    status: "processing",
                    runId,
                    docsRead: stats.totalDocsRead,
                    rowsInserted: stats.totalRowsInserted,
                    estimatedTotal: estimatedCount,
                    batchSeq: stats.batchSeq,
                    startedAt: progressStartedAt,
                    updatedAt: new Date().toISOString(),
                }).catch(() => {}); // best-effort
            }, config.worker.progressUpdateMs)
            : null;

        // Run the batch processing
        try {
            await batchRunner.run();

            const finalStatus = batchRunner.getStatus();
            const finalStats = batchRunner.getStats();

            // Write final progress to Redis
            const terminalStatus = finalStatus === "completed" ? "completed" as const : "failed" as const;
            await globalProgress?.updateCollectionProgress({
                collectionName,
                podId: config.worker.podId,
                status: terminalStatus,
                runId,
                docsRead: finalStats.totalDocsRead,
                rowsInserted: finalStats.totalRowsInserted,
                estimatedTotal: estimatedCount,
                batchSeq: finalStats.batchSeq,
                startedAt: progressStartedAt,
                updatedAt: new Date().toISOString(),
            }).catch(() => {});

            if (finalStatus === "completed") {
                return {
                    collection: collectionName, sourceNs, runId, status: "completed",
                    docsRead: finalStats.totalDocsRead,
                    rowsInserted: finalStats.totalRowsInserted,
                };
            }
            if (finalStatus === "stopped" || finalStatus === "stopping") {
                return {
                    collection: collectionName, sourceNs, runId, status: "failed", error: "Stopped by operator",
                    docsRead: finalStats.totalDocsRead,
                    rowsInserted: finalStats.totalRowsInserted,
                };
            }
            return {
                collection: collectionName, sourceNs, runId, status: "failed",
                error: `BatchRunner ended with status: ${finalStatus}`,
                docsRead: finalStats.totalDocsRead,
                rowsInserted: finalStats.totalRowsInserted,
            };
        } catch (err) {
            const error = err instanceof Error ? err.message : String(err);
            await globalProgress?.updateCollectionProgress({
                collectionName,
                podId: config.worker.podId,
                status: "failed",
                runId,
                docsRead: 0,
                rowsInserted: 0,
                estimatedTotal: estimatedCount,
                batchSeq: 0,
                startedAt: progressStartedAt,
                updatedAt: new Date().toISOString(),
                error,
            }).catch(() => {});
            await manifestStore.insertEvent({
                run_id: runId,
                event_type: "collection_migration_failed",
                message: `Migration of ${collectionName} failed: ${error}`,
                metadata: { collection: collectionName, error },
                created_at: new Date().toISOString(),
            });
            return { collection: collectionName, sourceNs, runId, status: "failed", error };
        } finally {
            if (progressInterval) clearInterval(progressInterval);
        }
    }
}
