import type { Redis } from "ioredis";
import type { Logger } from "pino";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type AcquireResult = "acquired" | "stolen" | "locked";

export interface LockInfo {
    collectionName: string;
    podId: string;
    acquiredAt: string;
    ttlSec: number;
}

export interface CollectionLockConfig {
    lockTtlSec: number;         // default 300 (5 minutes)
    renewIntervalMs: number;    // default 60_000 (1 minute)
    podHeartbeatMs: number;     // default 30_000
    podDeadAfterSec: number;    // default 180 (3 minutes)
    keyPrefix: string;          // default "mig"
}

// ---------------------------------------------------------------------------
// Lua scripts (executed atomically on the Redis server via EVAL)
// ---------------------------------------------------------------------------

/**
 * Atomic lock acquisition with pod-liveness check.
 *
 * KEYS[1] = lock key (mig:lock:{collection})
 * ARGV[1] = podId (caller)
 * ARGV[2] = lockTtlSec
 * ARGV[3] = JSON payload {podId, acquiredAt}
 * ARGV[4] = pod key prefix (mig:pod:)
 *
 * Returns: 1 = acquired, 2 = stolen from dead pod, 0 = locked by alive pod
 */
const ACQUIRE_LUA = `
local current = redis.call('GET', KEYS[1])
if not current then
    redis.call('SET', KEYS[1], ARGV[3], 'EX', tonumber(ARGV[2]))
    return 1
end

local data = cjson.decode(current)
if data.podId == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[3], 'EX', tonumber(ARGV[2]))
    return 1
end

local otherPodKey = ARGV[4] .. data.podId
local otherPod = redis.call('GET', otherPodKey)
if otherPod then
    return 0
end

redis.call('SET', KEYS[1], ARGV[3], 'EX', tonumber(ARGV[2]))
return 2
`;

/**
 * Renew lock TTL if owned by this pod.
 *
 * KEYS[1] = lock key
 * ARGV[1] = podId
 * ARGV[2] = lockTtlSec
 *
 * Returns: 1 = renewed, 0 = lock lost
 */
const RENEW_LUA = `
local current = redis.call('GET', KEYS[1])
if not current then
    return 0
end
local data = cjson.decode(current)
if data.podId ~= ARGV[1] then
    return 0
end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
`;

/**
 * Release lock if owned by this pod.
 *
 * KEYS[1] = lock key
 * ARGV[1] = podId
 *
 * Returns: 1 = released, 0 = not owned
 */
const RELEASE_LUA = `
local current = redis.call('GET', KEYS[1])
if not current then
    return 1
end
local data = cjson.decode(current)
if data.podId ~= ARGV[1] then
    return 0
end
redis.call('DEL', KEYS[1])
return 1
`;

// ---------------------------------------------------------------------------
// CollectionLock
// ---------------------------------------------------------------------------

export class CollectionLock {
    private readonly redis: Redis;
    private readonly podId: string;
    private readonly config: CollectionLockConfig;
    private readonly logger: Logger;

    private readonly heldLocks = new Set<string>();
    private lockRenewTimer: ReturnType<typeof setInterval> | null = null;
    private podHeartbeatTimer: ReturnType<typeof setInterval> | null = null;

    constructor(redis: Redis, podId: string, config: CollectionLockConfig, logger: Logger) {
        this.redis = redis;
        this.podId = podId;
        this.config = config;
        this.logger = logger.child({ component: "CollectionLock", podId });
    }

    // -----------------------------------------------------------------------
    // Lock operations
    // -----------------------------------------------------------------------

    async tryAcquire(collectionName: string): Promise<AcquireResult> {
        const lockKey = this.lockKey(collectionName);
        const payload = JSON.stringify({ podId: this.podId, acquiredAt: new Date().toISOString() });
        const podKeyPrefix = `${this.config.keyPrefix}:pod:`;

        // ioredis .eval() runs a Lua script atomically on the Redis server
        const result = await this.redis.eval(
            ACQUIRE_LUA,
            1,
            lockKey,
            this.podId,
            String(this.config.lockTtlSec),
            payload,
            podKeyPrefix,
        ) as number;

        if (result === 1) {
            this.heldLocks.add(collectionName);
            this.logger.debug({ collection: collectionName }, "Lock acquired");
            return "acquired";
        }
        if (result === 2) {
            this.heldLocks.add(collectionName);
            this.logger.info({ collection: collectionName }, "Lock stolen from dead pod");
            return "stolen";
        }
        return "locked";
    }

    async release(collectionName: string): Promise<void> {
        const lockKey = this.lockKey(collectionName);

        await this.redis.eval(
            RELEASE_LUA,
            1,
            lockKey,
            this.podId,
        );

        this.heldLocks.delete(collectionName);
        this.logger.debug({ collection: collectionName }, "Lock released");
    }

    async releaseAll(): Promise<void> {
        const collections = [...this.heldLocks];
        await Promise.allSettled(
            collections.map(name => this.release(name)),
        );
    }

    // -----------------------------------------------------------------------
    // Heartbeat
    // -----------------------------------------------------------------------

    startHeartbeat(): void {
        // Renew held locks
        this.lockRenewTimer = setInterval(() => {
            this.renewAll().catch(err => {
                this.logger.warn({ error: err instanceof Error ? err.message : String(err) }, "Lock renewal failed");
            });
        }, this.config.renewIntervalMs);

        // Pod liveness heartbeat
        this.podHeartbeatTimer = setInterval(() => {
            this.updatePodLiveness().catch(err => {
                this.logger.warn({ error: err instanceof Error ? err.message : String(err) }, "Pod heartbeat failed");
            });
        }, this.config.podHeartbeatMs);

        // Initial pod liveness write
        this.updatePodLiveness().catch(() => {});
    }

    stopHeartbeat(): void {
        if (this.lockRenewTimer) {
            clearInterval(this.lockRenewTimer);
            this.lockRenewTimer = null;
        }
        if (this.podHeartbeatTimer) {
            clearInterval(this.podHeartbeatTimer);
            this.podHeartbeatTimer = null;
        }
    }

    // -----------------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------------

    async listAllLocks(): Promise<LockInfo[]> {
        const pattern = `${this.config.keyPrefix}:lock:*`;
        const keys: string[] = [];
        const stream = this.redis.scanStream({ match: pattern, count: 100 });
        for await (const batch of stream) {
            keys.push(...(batch as string[]));
        }
        if (keys.length === 0) return [];

        // Fetch values and TTLs in parallel via pipeline
        const pipeline = this.redis.pipeline();
        for (const key of keys) {
            pipeline.get(key);
            pipeline.ttl(key);
        }
        const results = await pipeline.exec();

        const prefix = `${this.config.keyPrefix}:lock:`;
        const locks: LockInfo[] = [];

        for (let i = 0; i < keys.length; i++) {
            const val = results?.[i * 2]?.[1] as string | null;
            const ttl = results?.[i * 2 + 1]?.[1] as number ?? -1;
            if (!val) continue;
            try {
                const data = JSON.parse(val) as { podId: string; acquiredAt: string };
                locks.push({
                    collectionName: keys[i].slice(prefix.length),
                    podId: data.podId,
                    acquiredAt: data.acquiredAt,
                    ttlSec: ttl,
                });
            } catch {
                // skip malformed entries
            }
        }
        return locks;
    }

    getHeldLocks(): string[] {
        return [...this.heldLocks];
    }

    // -----------------------------------------------------------------------
    // Admin operations (lock management)
    // -----------------------------------------------------------------------

    /** Force-release a lock regardless of owner. Admin override. */
    async forceRelease(collectionName: string): Promise<void> {
        const lockKey = this.lockKey(collectionName);
        await this.redis.del(lockKey);
        this.heldLocks.delete(collectionName);
        this.logger.info({ collection: collectionName }, "Lock force-released (admin)");
    }

    /** Delete a pod's heartbeat key, marking it as dead for lock stealing. */
    async deletePodKey(podId: string): Promise<void> {
        const podKey = `${this.config.keyPrefix}:pod:${podId}`;
        await this.redis.del(podKey);
        this.logger.info({ targetPod: podId }, "Pod key deleted (admin)");
    }

    /** List all pod heartbeat keys. */
    async listAllPodKeys(): Promise<Array<{ podId: string; lastHeartbeat: string; collectionsActive: string[] }>> {
        const pattern = `${this.config.keyPrefix}:pod:*`;
        const keys: string[] = [];
        const stream = this.redis.scanStream({ match: pattern, count: 100 });
        for await (const batch of stream) {
            keys.push(...(batch as string[]));
        }
        if (keys.length === 0) return [];

        const values = await this.redis.mget(...keys);
        const results: Array<{ podId: string; lastHeartbeat: string; collectionsActive: string[] }> = [];

        for (const val of values) {
            if (!val) continue;
            try {
                results.push(JSON.parse(val) as { podId: string; lastHeartbeat: string; collectionsActive: string[] });
            } catch {
                // skip
            }
        }
        return results;
    }

    /** Release all locks held by a specific (dead) pod. Returns released collection names. */
    async releaseLocksForPod(podId: string): Promise<string[]> {
        const allLocks = await this.listAllLocks();
        const toRelease = allLocks.filter(l => l.podId === podId);
        const released: string[] = [];

        for (const lock of toRelease) {
            const lockKey = this.lockKey(lock.collectionName);
            await this.redis.del(lockKey);
            released.push(lock.collectionName);
        }

        this.logger.info({ targetPod: podId, releasedCount: released.length, collections: released }, "Released locks for dead pod");
        return released;
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private lockKey(collectionName: string): string {
        return `${this.config.keyPrefix}:lock:${collectionName}`;
    }

    private async renewAll(): Promise<void> {
        const collections = [...this.heldLocks];
        if (collections.length === 0) return;

        const results = await Promise.allSettled(
            collections.map(async name => {
                const lockKey = this.lockKey(name);
                const result = await this.redis.eval(
                    RENEW_LUA,
                    1,
                    lockKey,
                    this.podId,
                    String(this.config.lockTtlSec),
                ) as number;
                if (result === 0) {
                    this.heldLocks.delete(name);
                    this.logger.warn({ collection: name }, "Lock lost during renewal — another pod may have taken it");
                }
            }),
        );

        const failures = results.filter(r => r.status === "rejected");
        if (failures.length > 0) {
            this.logger.warn({ failedRenewals: failures.length }, "Some lock renewals failed");
        }
    }

    private async updatePodLiveness(): Promise<void> {
        const podKey = `${this.config.keyPrefix}:pod:${this.podId}`;
        await this.redis.set(
            podKey,
            JSON.stringify({
                podId: this.podId,
                lastHeartbeat: new Date().toISOString(),
                collectionsActive: [...this.heldLocks],
            }),
            "EX",
            this.config.podDeadAfterSec,
        );
    }
}
