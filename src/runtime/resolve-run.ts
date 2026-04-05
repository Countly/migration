import type { Logger } from 'pino';
import type { ManifestStore } from '../state/manifest-store.ts';
import type { RedisHotState } from '../state/redis-hot-state.ts';
import type { MongoReader } from '../source/mongo-reader.ts';
import { serializeCursor } from '../types/cursor.ts';
import { randomUUID } from 'node:crypto';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ResolvedRun {
    runId: string;
    upperBoundId: string;
    /** True if the source collection was empty — caller should skip migration. */
    isEmpty?: boolean;
}

export interface ResolveRunOpts {
    rerunMode: 'resume' | 'clone-run' | 'new-run';
    manifestStore: ManifestStore;
    redisState: RedisHotState;
    mongoReader: MongoReader;
    sourceNs: string;
    targetTable: string;
    transformVersion: string;
    logger: Logger;
}

// ---------------------------------------------------------------------------
// resolveRun
// ---------------------------------------------------------------------------

export async function resolveRun(opts: ResolveRunOpts): Promise<ResolvedRun> {
    const { rerunMode, manifestStore, redisState, mongoReader, sourceNs, targetTable, transformVersion, logger } = opts;

    async function createNewRun(rid: string, ubId: string): Promise<void> {
        const now = new Date().toISOString();
        await manifestStore.createRun({
            run_id: rid,
            status: 'active',
            source_ns: sourceNs,
            target_table: targetTable,
            upper_bound_cursor: ubId,
            transform_version: transformVersion,
            created_at: now,
            updated_at: now,
        });
        await redisState.setActiveRun(rid);
        await redisState.setState(rid, {
            runId: rid,
            status: 'active',
            sourceNs,
            targetTable,
            upperBoundCursor: ubId,
            lastCommittedCursor: null,
            transformVersion,
            totalBatches: 0,
            completedBatches: 0,
            startedAt: now,
        });
    }

    if (rerunMode === 'resume') {
        // Priority 1: active run for THIS collection (crash recovery)
        const activeRun = await manifestStore.getActiveRun(sourceNs, targetTable);
        if (activeRun) {
            logger.info({ runId: activeRun.run_id }, 'Resuming active run');
            return { runId: activeRun.run_id, upperBoundId: activeRun.upper_bound_cursor };
        }

        // Priority 2: most recent paused/stopped run (operator restart)
        const resumableRun = await manifestStore.getResumableRun(sourceNs, targetTable, transformVersion);
        if (resumableRun) {
            await manifestStore.updateRunStatus(resumableRun.run_id, 'active');
            await redisState.setActiveRun(resumableRun.run_id);

            await manifestStore.insertEvent({
                run_id: resumableRun.run_id,
                event_type: 'run_resumed',
                message: `Run resumed from ${resumableRun.status} state`,
                metadata: { prior_status: resumableRun.status, last_cursor: resumableRun.last_committed_cursor },
                created_at: new Date().toISOString(),
            });

            logger.info(
                { runId: resumableRun.run_id, priorStatus: resumableRun.status },
                'Resuming stopped/paused run',
            );
            return { runId: resumableRun.run_id, upperBoundId: resumableRun.upper_bound_cursor };
        }

        // Priority 3: no resumable run, create new
        const runId = randomUUID();
        const upperBound = await mongoReader.getUpperBound();
        if (!upperBound) {
            const hasNullCd = await mongoReader.hasNullCdDocuments();
            if (!hasNullCd) {
                return { runId, upperBoundId: '', isEmpty: true };
            }
            const upperBoundId = serializeCursor({ cd: 0, id: "\uffff".repeat(24) });
            await createNewRun(runId, upperBoundId);
            logger.info({ runId, sourceNs, targetTable }, 'All-null collection — created run for null-cd sweep');
            return { runId, upperBoundId };
        }
        const upperBoundId = serializeCursor(upperBound);
        await createNewRun(runId, upperBoundId);
        logger.info({ runId, upperBoundId, sourceNs, targetTable }, 'Created new migration run');
        return { runId, upperBoundId };
    }

    if (rerunMode === 'new-run') {
        const runId = randomUUID();
        const upperBound = await mongoReader.getUpperBound();
        if (!upperBound) {
            const hasNullCd = await mongoReader.hasNullCdDocuments();
            if (!hasNullCd) {
                return { runId, upperBoundId: '', isEmpty: true };
            }
            const upperBoundId = serializeCursor({ cd: 0, id: "\uffff".repeat(24) });
            const activeRun = await manifestStore.getActiveRun(sourceNs, targetTable);
            if (activeRun) {
                const deleted = await manifestStore.deleteRunData(activeRun.run_id);
                logger.info({ oldRunId: activeRun.run_id, deletedRecords: deleted }, 'Cleaned old run data for fresh start');
                await manifestStore.updateRunStatus(activeRun.run_id, 'completed');
            }
            await createNewRun(runId, upperBoundId);
            logger.info({ runId, sourceNs, targetTable }, 'All-null collection — created run for null-cd sweep (new-run mode)');
            return { runId, upperBoundId };
        }
        const upperBoundId = serializeCursor(upperBound);
        const activeRun = await manifestStore.getActiveRun(sourceNs, targetTable);
        if (activeRun) {
            // Clean old run data before starting fresh
            const deleted = await manifestStore.deleteRunData(activeRun.run_id);
            logger.info({ oldRunId: activeRun.run_id, deletedRecords: deleted }, 'Cleaned old run data for fresh start');
            await manifestStore.updateRunStatus(activeRun.run_id, 'completed');
        }
        await createNewRun(runId, upperBoundId);
        logger.info({ runId, upperBoundId, sourceNs, targetTable }, 'Created new migration run (new-run mode)');
        return { runId, upperBoundId };
    }

    if (rerunMode === 'clone-run') {
        const activeRun = await manifestStore.getActiveRun(sourceNs, targetTable);
        if (!activeRun) throw new Error('No existing run to clone from');
        const runId = randomUUID();
        const upperBoundId = activeRun.upper_bound_cursor;
        // Clean old run data before starting fresh
        const deleted = await manifestStore.deleteRunData(activeRun.run_id);
        logger.info({ oldRunId: activeRun.run_id, deletedRecords: deleted }, 'Cleaned old run data for fresh start');
        await manifestStore.updateRunStatus(activeRun.run_id, 'completed');
        await createNewRun(runId, upperBoundId);
        logger.info({ runId, upperBoundId, sourceNs, targetTable }, 'Created new migration run (clone-run mode)');
        return { runId, upperBoundId };
    }

    throw new Error(`Unknown rerun mode: ${rerunMode satisfies never}`);
}
