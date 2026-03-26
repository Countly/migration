import type { Redis } from "ioredis";
import type { Logger } from "pino";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface CollectionProgress {
    collectionName: string;
    podId: string;
    status: "processing" | "completed" | "failed" | "skipped";
    runId: string;
    docsRead: number;
    rowsInserted: number;
    estimatedTotal: number;
    batchSeq: number;
    startedAt: string;
    updatedAt: string;
    error?: string;
    isRangeParallel?: boolean;
    rangeCount?: number;
    throughput?: number;
}

export interface PodInfo {
    podId: string;
    lastHeartbeat: string;
    collectionsActive: string[];
}

export interface GlobalCommands {
    pause: boolean;
    stop: boolean;
}

// ---------------------------------------------------------------------------
// GlobalProgress
// ---------------------------------------------------------------------------

export class GlobalProgress {
    private readonly redis: Redis;
    private readonly podId: string;
    private readonly keyPrefix: string;
    private readonly logger: Logger;

    constructor(redis: Redis, podId: string, keyPrefix: string, logger: Logger) {
        this.redis = redis;
        this.podId = podId;
        this.keyPrefix = keyPrefix;
        this.logger = logger.child({ component: "GlobalProgress", podId });
    }

    // -----------------------------------------------------------------------
    // Per-collection progress
    // -----------------------------------------------------------------------

    async updateCollectionProgress(progress: CollectionProgress): Promise<void> {
        const key = `${this.keyPrefix}:progress:${progress.collectionName}`;
        await this.redis.set(
            key,
            JSON.stringify(progress),
            "EX",
            300, // 5 minute TTL, renewed on each update
        );
    }

    async getCollectionProgress(collectionName: string): Promise<CollectionProgress | null> {
        const key = `${this.keyPrefix}:progress:${collectionName}`;
        const raw = await this.redis.get(key);
        if (!raw) return null;
        return JSON.parse(raw) as CollectionProgress;
    }

    async getAllCollectionProgress(): Promise<CollectionProgress[]> {
        const pattern = `${this.keyPrefix}:progress:*`;
        const keys: string[] = [];
        const stream = this.redis.scanStream({ match: pattern, count: 100 });
        for await (const batch of stream) {
            keys.push(...(batch as string[]));
        }
        if (keys.length === 0) return [];

        const values = await this.redis.mget(...keys);
        const results: CollectionProgress[] = [];

        for (const val of values) {
            if (!val) continue;
            try {
                results.push(JSON.parse(val) as CollectionProgress);
            } catch {
                // skip malformed entries
            }
        }
        return results;
    }

    // -----------------------------------------------------------------------
    // Pod registry
    // -----------------------------------------------------------------------

    async getAllPods(): Promise<PodInfo[]> {
        const pattern = `${this.keyPrefix}:pod:*`;
        const keys: string[] = [];
        const stream = this.redis.scanStream({ match: pattern, count: 100 });
        for await (const batch of stream) {
            keys.push(...(batch as string[]));
        }
        if (keys.length === 0) return [];

        const values = await this.redis.mget(...keys);
        const pods: PodInfo[] = [];

        for (const val of values) {
            if (!val) continue;
            try {
                pods.push(JSON.parse(val) as PodInfo);
            } catch {
                // skip malformed entries
            }
        }
        return pods;
    }

    // -----------------------------------------------------------------------
    // Global commands
    // -----------------------------------------------------------------------

    async setGlobalCommand(command: keyof GlobalCommands, value: boolean): Promise<void> {
        const key = `${this.keyPrefix}:cmd:global`;
        await this.redis.hset(key, command, value ? "true" : "false");
    }

    async getGlobalCommands(): Promise<GlobalCommands> {
        const key = `${this.keyPrefix}:cmd:global`;
        const raw = await this.redis.hgetall(key);
        return {
            pause: raw.pause === "true",
            stop: raw.stop === "true",
        };
    }
}
