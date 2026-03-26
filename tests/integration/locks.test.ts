/**
 * Integration tests for the CollectionLock mechanism.
 *
 * Verifies Redis-based distributed locking: acquisition, contention,
 * release, and TTL-based expiry reclaim.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import pino from "pino";
import {
  getRedis,
  teardownRedis,
  closeAll,
  TEST_REDIS_PREFIX,
} from "../helpers/setup.ts";
import {
  CollectionLock,
  type CollectionLockConfig,
} from "../../src/state/collection-lock.ts";

// ---------------------------------------------------------------------------
// Shared config
// ---------------------------------------------------------------------------

const logger = pino({ level: "silent" });

const baseLockConfig: CollectionLockConfig = {
  lockTtlSec: 10,
  renewIntervalMs: 60_000,     // won't fire during short tests
  podHeartbeatMs: 60_000,      // won't fire during short tests
  podDeadAfterSec: 2,          // short TTL so we can test dead-pod steal
  keyPrefix: TEST_REDIS_PREFIX,
};

const COLLECTION = "drill_events_lock_test";

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

beforeAll(async () => {
  await teardownRedis();
});

afterAll(async () => {
  await teardownRedis();
  await closeAll();
});

beforeEach(async () => {
  // Flush test-prefix keys between tests for isolation
  await teardownRedis();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("CollectionLock", () => {
  it("lock acquisition succeeds for unclaimed collection", async () => {
    const redis = await getRedis();
    const lock = new CollectionLock(redis, "pod-A", baseLockConfig, logger);

    const result = await lock.tryAcquire(COLLECTION);

    expect(result).toBe("acquired");
    expect(lock.getHeldLocks()).toContain(COLLECTION);

    await lock.releaseAll();
  });

  it("second pod cannot acquire same lock", async () => {
    const redis = await getRedis();
    const lockA = new CollectionLock(redis, "pod-A", baseLockConfig, logger);
    const lockB = new CollectionLock(redis, "pod-B", baseLockConfig, logger);

    // Pod A registers its heartbeat so it appears alive
    await redis.set(
      `${TEST_REDIS_PREFIX}:pod:pod-A`,
      JSON.stringify({ podId: "pod-A", lastHeartbeat: new Date().toISOString(), collectionsActive: [] }),
      "EX",
      baseLockConfig.podDeadAfterSec,
    );

    const resultA = await lockA.tryAcquire(COLLECTION);
    expect(resultA).toBe("acquired");

    const resultB = await lockB.tryAcquire(COLLECTION);
    expect(resultB).toBe("locked");

    expect(lockB.getHeldLocks()).not.toContain(COLLECTION);

    await lockA.releaseAll();
  });

  it("lock release allows reacquisition", async () => {
    const redis = await getRedis();
    const lockA = new CollectionLock(redis, "pod-A", baseLockConfig, logger);
    const lockB = new CollectionLock(redis, "pod-B", baseLockConfig, logger);

    // Acquire with pod-A
    const r1 = await lockA.tryAcquire(COLLECTION);
    expect(r1).toBe("acquired");

    // Release with pod-A
    await lockA.release(COLLECTION);
    expect(lockA.getHeldLocks()).not.toContain(COLLECTION);

    // Now pod-B can acquire
    const r2 = await lockB.tryAcquire(COLLECTION);
    expect(r2).toBe("acquired");
    expect(lockB.getHeldLocks()).toContain(COLLECTION);

    await lockB.releaseAll();
  });

  it("expired lock can be reclaimed", async () => {
    const redis = await getRedis();

    // Use a very short lock TTL (1 second) and short dead-after (1 second)
    const shortConfig: CollectionLockConfig = {
      ...baseLockConfig,
      lockTtlSec: 1,
      podDeadAfterSec: 1,
    };

    const lockA = new CollectionLock(redis, "pod-A", shortConfig, logger);
    const lockB = new CollectionLock(redis, "pod-B", shortConfig, logger);

    // Pod-A acquires but registers a heartbeat with short TTL
    await redis.set(
      `${TEST_REDIS_PREFIX}:pod:pod-A`,
      JSON.stringify({ podId: "pod-A", lastHeartbeat: new Date().toISOString(), collectionsActive: [] }),
      "EX",
      1, // 1 second TTL
    );

    const r1 = await lockA.tryAcquire(COLLECTION);
    expect(r1).toBe("acquired");

    // Wait for both the lock and the pod heartbeat to expire
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Pod-B should be able to steal the lock from the dead pod
    const r2 = await lockB.tryAcquire(COLLECTION);
    // The lock key may have expired (TTL=1s), so result is "acquired",
    // or if the key is still present but pod-A's heartbeat expired, result is "stolen"
    expect(["acquired", "stolen"]).toContain(r2);
    expect(lockB.getHeldLocks()).toContain(COLLECTION);

    await lockB.releaseAll();
  });
});
