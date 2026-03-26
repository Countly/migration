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
}

// ---------------------------------------------------------------------------
// CollectionOrchestrator
// ---------------------------------------------------------------------------

export class CollectionOrchestrator {
    private readonly deps: OrchestratorDeps;
    private readonly logger: Logger;

    private discoveredCollections: string[] = [];
    private results: CollectionResult[] = [];
    private estimatedCounts: Map<string, number> = new Map();
    private indexStatus: Map<string, IndexBuildStatus> = new Map();
    private indexBuildStarted: Map<string, number> = new Map();
    private currentCollection: string | null = null;
    private currentRunId: string | null = null;
    private currentBatchRunner: BatchRunner | null = null;
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
        const { mongoReader, manifestStore, config } = this.deps;

        // 1. Discover collections
        const db = mongoReader.getDatabase();
        this.discoveredCollections = await discoverCollections(db, config.source.collectionPrefix, this.logger);

        this.logger.info(
            { totalCollections: this.discoveredCollections.length },
            "Starting migration with index-aware scheduling",
        );

        // 2. Start all index checks in background (non-blocking)
        this.startBackgroundIndexInit();

        this.orchestratorStatus = "running";

        // 3. Process loop: pick ready collections, wait for building ones
        while (!this.stopping) {
            const completed = new Set(this.results.map(r => r.collection));

            // Find next collection with index ready and not yet processed
            const next = this.findNextReadyCollection(completed);

            if (next) {
                this.orchestratorStatus = "running";
                const sourceNs = `${config.source.db}.${next}`;
                const targetTable = `${config.target.db}.${config.target.table}`;

                // Check if already completed (resume mode)
                const alreadyCompleted = await manifestStore.existsCompletedRun(sourceNs, targetTable);
                if (alreadyCompleted) {
                    this.logger.info({ collection: next, sourceNs }, "Collection already migrated, skipping");
                    this.results.push({ collection: next, sourceNs, runId: "", status: "skipped" });
                    continue;
                }

                // Process this collection
                try {
                    const result = await this.processCollection(next, sourceNs, targetTable);
                    this.results.push(result);

                    if (result.status === "completed") {
                        this.logger.info({ collection: next, runId: result.runId }, "Collection migration completed");
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
                    "Waiting for index creation — rechecking in 60s",
                );
                await this.interruptibleSleep(10_000);
                await this.recheckBuildingIndexes();
                continue;
            }

            // All done (or all failed/skipped)
            break;
        }

        this.currentCollection = null;
        this.currentRunId = null;
        this.currentBatchRunner = null;
        this.orchestratorStatus = "completed";

        // Summary
        const summary: OrchestratorResult = {
            collections: this.results,
            totalCompleted: this.results.filter((r) => r.status === "completed").length,
            totalFailed: this.results.filter((r) => r.status === "failed").length,
            totalSkipped: this.results.filter((r) => r.status === "skipped").length,
        };

        this.logger.info(
            {
                totalCollections: this.discoveredCollections.length,
                completed: summary.totalCompleted,
                failed: summary.totalFailed,
                skipped: summary.totalSkipped,
            },
            "Migration orchestration complete",
        );

        return summary;
    }

    getProgress(): OrchestratorProgress {
        return {
            totalCollections: this.discoveredCollections.length,
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
        if (this.orchestratorStatus === "completed") {
            return "completed";
        }
        if (this.discoveredCollections.length > 0 && this.results.length === this.discoveredCollections.length) {
            return "completed";
        }
        return "idle";
    }

    getStats(): BatchRunnerStats | null {
        return this.currentBatchRunner?.getStats() ?? null;
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
        this.results = this.results.filter(r => r.collection !== collectionName);
        this.logger.info({ collection: collectionName }, "Collection queued for retry");
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

    private findNextReadyCollection(completed: Set<string>): string | null {
        const prefix = this.deps.config.source.collectionPrefix;

        // Always prefer the base collection (drill_events) when it becomes ready
        if (!completed.has(prefix) && this.indexStatus.get(prefix) === "ready") {
            return prefix;
        }

        // Otherwise pick any ready collection in discovery order
        for (const name of this.discoveredCollections) {
            if (completed.has(name)) continue;
            if (this.indexStatus.get(name) === "ready") return name;
        }
        return null;
    }

    private getCollectionsWithStatus(status: IndexBuildStatus): string[] {
        return [...this.indexStatus.entries()]
            .filter(([, s]) => s === status)
            .map(([name]) => name);
    }

    private async recheckBuildingIndexes(): Promise<void> {
        const { mongoReader } = this.deps;

        for (const [name, status] of this.indexStatus) {
            if (status !== "building") continue;
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
        }
    }

    private async interruptibleSleep(ms: number): Promise<void> {
        return new Promise<void>((resolve) => {
            const timer = setTimeout(resolve, ms);
            // Allow stopping to interrupt the sleep
            const check = setInterval(() => {
                if (this.stopping) {
                    clearTimeout(timer);
                    clearInterval(check);
                    resolve();
                }
            }, 1000);
            // Ensure interval is cleaned up when timer fires normally
            setTimeout(() => clearInterval(check), ms + 100);
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

        // Get estimated document count for progress tracking
        const estimatedCount = await mongoReader.getEstimatedCount();
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
            },
        });

        this.currentBatchRunner = batchRunner;

        // Run the batch processing
        try {
            await batchRunner.run();

            const finalStatus = batchRunner.getStatus();
            const finalStats = batchRunner.getStats();
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
            await manifestStore.insertEvent({
                run_id: runId,
                event_type: "collection_migration_failed",
                message: `Migration of ${collectionName} failed: ${error}`,
                metadata: { collection: collectionName, error },
                created_at: new Date().toISOString(),
            });
            return { collection: collectionName, sourceNs, runId, status: "failed", error };
        }
    }
}
