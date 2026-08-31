/**
 * Seed a scratch A/B environment: synthetic drill docs in MongoDB + a clone
 * of the production drill_events DDL in ClickHouse.
 *
 * Env: AB_DOCS (250000), AB_MONGO_URI, AB_MONGO_DB (mig_ab), AB_CH_URL, AB_CH_DB (mig_ab)
 */
import { MongoClient } from 'mongodb';
import { createClient } from '@clickhouse/client';
import { generateDoc } from './gen.ts';

const DOCS = Number(process.env.AB_DOCS ?? 250_000);
const MONGO_URI = process.env.AB_MONGO_URI ?? 'mongodb://localhost:27017';
const MONGO_DB = process.env.AB_MONGO_DB ?? 'mig_ab';
const CH_URL = process.env.AB_CH_URL ?? 'http://localhost:8123';
const CH_DB = process.env.AB_CH_DB ?? 'mig_ab';
const COLL = 'drill_events';

const CH_DDL_COLS = `
    \`a\` LowCardinality(String),
    \`e\` LowCardinality(String),
    \`n\` String,
    \`uid\` String,
    \`uid_canon\` Nullable(String),
    \`did\` String,
    \`lsid\` Nullable(String),
    \`_id\` String,
    \`ts\` DateTime64(3),
    \`up\` JSON(max_dynamic_paths = 32),
    \`custom\` Nullable(JSON(max_dynamic_paths = 0)),
    \`cmp\` Nullable(JSON(max_dynamic_paths = 0)),
    \`sg\` JSON(max_dynamic_paths = 0),
    \`c\` UInt32,
    \`s\` Float64,
    \`dur\` Float64,
    \`lu\` Nullable(DateTime64(3)) CODEC(Delta(8), LZ4),
    \`cd\` DateTime64(3) DEFAULT now64(3) CODEC(Delta(8), LZ4)`;

async function main() {
  const ch = createClient({ url: CH_URL });
  await ch.command({ query: `CREATE DATABASE IF NOT EXISTS ${CH_DB}` });
  await ch.command({ query: `DROP TABLE IF EXISTS ${CH_DB}.${COLL}` });
  await ch.command({
    query: `CREATE TABLE ${CH_DB}.${COLL} (${CH_DDL_COLS},
      INDEX uid_bloom uid TYPE bloom_filter(0.01) GRANULARITY 4,
      INDEX cd_minmax cd TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree PARTITION BY toYYYYMM(ts, 'UTC') ORDER BY (a, e, n, ts)
    SETTINGS index_granularity = 8192`,
  });
  await ch.close();
  console.log(`ClickHouse: ${CH_DB}.${COLL} created`);

  const mc = new MongoClient(MONGO_URI);
  await mc.connect();
  const coll = mc.db(MONGO_DB).collection(COLL);
  await coll.drop().catch(() => {});

  const t0 = performance.now();
  const CHUNK = 5000;
  for (let done = 0; done < DOCS; done += CHUNK) {
    const n = Math.min(CHUNK, DOCS - done);
    const docs = Array.from({ length: n }, (_, j) => generateDoc(done + j));
    await coll.insertMany(docs as never[], { ordered: false });
  }
  console.log(`Mongo: inserted ${DOCS} docs in ${((performance.now() - t0) / 1000).toFixed(1)}s`);

  await coll.createIndex({ cd: 1, _id: 1 });
  const stats = await mc.db(MONGO_DB).command({ collStats: COLL });
  console.log(`Mongo: ${(stats.size / 1e6).toFixed(1)} MB BSON, avg doc ${stats.avgObjSize.toFixed(0)} B, index ready`);
  await mc.close();
}

main().catch((e) => { console.error(e); process.exit(1); });
