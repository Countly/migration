/**
 * Global test setup: connections to MongoDB, ClickHouse, Redis.
 * Shared across all integration tests.
 */
import { MongoClient, type Db } from "mongodb";
import { createClient, type ClickHouseClient } from "@clickhouse/client";
import Redis from "ioredis";

// ---------------------------------------------------------------------------
// Test constants
// ---------------------------------------------------------------------------
export const TEST_MONGO_URI = "mongodb://localhost:27017/?directConnection=true";
export const TEST_MONGO_DB = "test_mig_integration";
export const TEST_MANIFEST_DB = "test_mig_integration";
export const TEST_CH_URL = "http://localhost:8123";
export const TEST_CH_DB = "test_mig_integration";
export const TEST_CH_TABLE = "drill_events";
export const TEST_REDIS_URL = "redis://localhost:6379";
export const TEST_REDIS_PREFIX = "test_mig";
export const TEST_COLLECTION_PREFIX = "drill_events";

// ---------------------------------------------------------------------------
// Singleton connections (reused across tests in a file)
// ---------------------------------------------------------------------------
let mongoClient: MongoClient | null = null;
let chClient: ClickHouseClient | null = null;
let redisClient: Redis | null = null;

export async function getMongoClient(): Promise<MongoClient> {
  if (!mongoClient) {
    mongoClient = new MongoClient(TEST_MONGO_URI);
    await mongoClient.connect();
  }
  return mongoClient;
}

export async function getMongoDb(): Promise<Db> {
  const client = await getMongoClient();
  return client.db(TEST_MONGO_DB);
}

export async function getClickHouseClient(): Promise<ClickHouseClient> {
  if (!chClient) {
    chClient = createClient({
      url: TEST_CH_URL,
      username: "default",
      password: "",
      database: TEST_CH_DB,
      clickhouse_settings: {
        date_time_input_format: "best_effort",
      },
    });
  }
  return chClient;
}

export async function getRedis(): Promise<Redis> {
  if (!redisClient) {
    redisClient = new Redis(TEST_REDIS_URL);
  }
  return redisClient;
}

// ---------------------------------------------------------------------------
// Setup / Teardown
// ---------------------------------------------------------------------------

/** Create the ClickHouse test database and drill_events table. */
export async function setupClickHouse(): Promise<void> {
  // Create DB using a temporary client connected to 'default' (the DB may not exist yet)
  const adminCh = createClient({
    url: TEST_CH_URL,
    username: "default",
    password: "",
    database: "default",
  });
  await adminCh.command({
    query: `CREATE DATABASE IF NOT EXISTS ${TEST_CH_DB}`,
  });
  await adminCh.close();

  const ch = await getClickHouseClient();

  // Create table matching production schema
  await ch.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${TEST_CH_DB}.${TEST_CH_TABLE}
      (
          a      LowCardinality(String),
          e      LowCardinality(String),
          n      String,
          uid    String,
          uid_canon Nullable(String),
          did    String,
          lsid   Nullable(String),
          _id    String,
          ts     DateTime64(3),
          up     String DEFAULT '{}',
          custom Nullable(String),
          cmp    Nullable(String),
          sg     String DEFAULT '{}',
          c      UInt32,
          s      Float64,
          dur    Float64,
          lu     Nullable(DateTime64(3)) CODEC(Delta, LZ4),
          cd     DateTime64(3) DEFAULT now64(3) CODEC(Delta, LZ4)
      )
      ENGINE = MergeTree
      PARTITION BY toYYYYMM(ts, 'UTC')
      ORDER BY (a, e, n, ts)
      SETTINGS index_granularity = 8192
    `,
  });
}

/** Drop the ClickHouse test table (fresh start). */
export async function teardownClickHouse(): Promise<void> {
  try {
    const ch = await getClickHouseClient();
    await ch.command({ query: `DROP TABLE IF EXISTS ${TEST_CH_DB}.${TEST_CH_TABLE}` });
  } catch {
    // DB may not exist yet — ignore
  }
}

/** Drop the MongoDB test database. */
export async function teardownMongo(): Promise<void> {
  const db = await getMongoDb();
  await db.dropDatabase();
}

/** Flush all Redis keys with the test prefix. */
export async function teardownRedis(): Promise<void> {
  const redis = await getRedis();
  const keys = await redis.keys(`${TEST_REDIS_PREFIX}*`);
  if (keys.length > 0) {
    await redis.del(...keys);
  }
}

/** Full cleanup: all three stores. */
export async function cleanAll(): Promise<void> {
  await Promise.all([
    teardownMongo(),
    teardownClickHouse(),
    teardownRedis(),
  ]);
}

/** Close all connections. Call in afterAll(). */
export async function closeAll(): Promise<void> {
  if (mongoClient) {
    await mongoClient.close();
    mongoClient = null;
  }
  if (chClient) {
    await chClient.close();
    chClient = null;
  }
  if (redisClient) {
    await redisClient.quit();
    redisClient = null;
  }
}

/** Flush ClickHouse async insert queue to ensure all data is visible. */
export async function flushClickHouse(): Promise<void> {
  const ch = await getClickHouseClient();
  await ch.command({ query: "SYSTEM FLUSH ASYNC INSERT QUEUE" }).catch(() => {});
}

/** Query ClickHouse row count for the test table. */
export async function chRowCount(where?: string): Promise<number> {
  await flushClickHouse();
  const ch = await getClickHouseClient();
  const q = where
    ? `SELECT count() AS cnt FROM ${TEST_CH_TABLE} WHERE ${where}`
    : `SELECT count() AS cnt FROM ${TEST_CH_TABLE}`;
  const result = await ch.query({ query: q, format: "JSONEachRow" });
  const rows = await result.json<{ cnt: string }[]>();
  return Number(rows[0]?.cnt ?? 0);
}

/** Query ClickHouse for specific rows. */
export async function chQuery<T = Record<string, unknown>>(query: string): Promise<T[]> {
  await flushClickHouse();
  const ch = await getClickHouseClient();
  const result = await ch.query({ query, format: "JSONEachRow" });
  return result.json<T[]>();
}
