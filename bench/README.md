# A/B harness: `classic` vs `ledger` engine

Both engines live in this build, selected by `MIGRATION_ENGINE` (`classic` is
the default and byte-identical to `main`'s behavior). This directory seeds a
scratch dataset and runs the comparison.

## 1. Seed

```bash
AB_DOCS=250000 node --experimental-strip-types bench/setup.ts
```

Creates `mig_ab.drill_events` in MongoDB (with the `{cd,_id}` index) and a
clone of the production `drill_events` DDL in ClickHouse db `mig_ab`.

## 2. Throughput A/B

Same dataset, same machine — run each engine once with `EXIT_ON_COMPLETE=true`
and compare wall time / docs-per-second (classic reports via `/stats`, ledger
logs a summary and serves `/stats` too).

```bash
# A: classic (needs Redis)
SERVICE_NAME=ab-classic MONGO_URI=mongodb://localhost:27017 MONGO_DB=mig_ab \
MANIFEST_DB=mig_ab_manifest CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DB=mig_ab \
REDIS_URL=redis://localhost:6379 RERUN_MODE=new-run EXIT_ON_COMPLETE=true \
SERVICE_PORT=18080 npm start

# reset the target between runs
# TRUNCATE TABLE mig_ab.drill_events

# B: ledger (no Redis)
MIGRATION_ENGINE=ledger SERVICE_NAME=ab-ledger MONGO_URI=mongodb://localhost:27017 \
MONGO_DB=mig_ab MANIFEST_DB=mig_ab_manifest CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_DB=mig_ab LEDGER_CHUNK_DOCS_TARGET=50000 EXIT_ON_COMPLETE=true \
SERVICE_PORT=18081 npm start
```

## 3. Crash-safety A/B (the interesting one)

Repeatedly SIGKILLs the ledger engine at random points and restarts it until
the migration completes, then verifies **zero loss and zero duplicates**
(`count() == uniqExact(_id) == mongo count`):

```bash
node --experimental-strip-types bench/kill-drill.ts
```

Run the same kill pattern against the classic engine for the comparison — pay
attention to `digest_mismatches` / `estimatedDuplicateRows` in its stats and
to whether the final table has duplicate `_id`s.

## Verification queries

```sql
-- exact, instant
SELECT count() AS total, uniqExact(_id) AS distinct_ids FROM mig_ab.drill_events;
-- per-chunk breakdown vs the ledger (mig_ab_manifest.mig_ranges)
SELECT toStartOfDay(cd) d, count() FROM mig_ab.drill_events GROUP BY d ORDER BY d;
```
