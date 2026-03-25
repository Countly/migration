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
import type { Config } from "../config/schema.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface CollectionResult {
    collection: string;
    sourceNs: string;
    runId: string;
    status: "completed" | "failed" | "skipped";
    error?: string;
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
    private currentCollection: string | null = null;
    private currentRunId: string | null = null;
    private currentBatchRunner: BatchRunner | null = null;
    private currentRedisState: RedisHotState | null = null;
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
            "Starting sequential migration across collections",
        );

        // 2. Process each collection sequentially
        for (const collectionName of this.discoveredCollections) {
            if (this.stopping) {
                this.logger.info({ collection: collectionName }, "Orchestrator stopping, skipping remaining collections");
                break;
            }

            const sourceNs = `${config.source.db}.${collectionName}`;
            const targetTable = `${config.target.db}.${config.target.table}`;

            // Check if a completed run already exists for this collection
            const alreadyCompleted = await manifestStore.existsCompletedRun(sourceNs, targetTable);

            if (alreadyCompleted) {
                this.logger.info({ collection: collectionName, sourceNs }, "Collection already migrated, skipping");
                this.results.push({
                    collection: collectionName,
                    sourceNs,
                    runId: "",
                    status: "skipped",
                });
                continue;
            }

            // Process this collection
            try {
                const result = await this.processCollection(collectionName, sourceNs, targetTable);
                this.results.push(result);

                if (result.status === "completed") {
                    this.logger.info({ collection: collectionName, runId: result.runId }, "Collection migration completed");
                } else if (result.status === "failed") {
                    this.logger.error(
                        { collection: collectionName, runId: result.runId, error: result.error },
                        "Collection migration failed, continuing to next",
                    );
                }
            } catch (err) {
                const error = err instanceof Error ? err.message : String(err);
                this.logger.error({ collection: collectionName, error }, "Unexpected error migrating collection, continuing to next");
                this.results.push({
                    collection: collectionName,
                    sourceNs,
                    runId: "",
                    status: "failed",
                    error,
                });
            }
        }

        this.currentCollection = null;
        this.currentRunId = null;
        this.currentBatchRunner = null;

        // 3. Summary
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
        if (this.currentBatchRunner) {
            return this.currentBatchRunner.getStatus();
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

    // -----------------------------------------------------------------------
    // Private
    // -----------------------------------------------------------------------

    private async processCollection(
        collectionName: string,
        sourceNs: string,
        targetTable: string,
    ): Promise<CollectionResult> {
        const { mongoReader, manifestStore, chWriter, chPressure, gcController, retryPolicy, config } = this.deps;

        this.currentCollection = collectionName;

        // Switch MongoReader to this collection (also ensures index)
        await mongoReader.switchCollection(collectionName);

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
                backpressure: config.backpressure,
                useDedupToken: config.target.useDedupToken,
                database: config.target.db,
                table: config.target.table,
                snapshotInterval: config.state.timelineSnapshotInterval,
            },
        });

        this.currentBatchRunner = batchRunner;

        // Run the batch processing
        try {
            await batchRunner.run();

            const finalStatus = batchRunner.getStatus();
            if (finalStatus === "completed") {
                return { collection: collectionName, sourceNs, runId, status: "completed" };
            }
            if (finalStatus === "stopped" || finalStatus === "stopping") {
                // Stopped by operator — treat as incomplete, not failed
                return { collection: collectionName, sourceNs, runId, status: "failed", error: "Stopped by operator" };
            }
            return {
                collection: collectionName,
                sourceNs,
                runId,
                status: "failed",
                error: `BatchRunner ended with status: ${finalStatus}`,
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
