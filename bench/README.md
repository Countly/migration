# Validation harness

Seed a synthetic dataset and prove the migrator's two core guarantees:
exactness (every doc lands exactly once) and crash safety (SIGKILL at any
moment, restart, converge — zero loss, zero duplicates).

## 1. Seed

```bash
AB_DOCS=250000 node --experimental-strip-types bench/setup.ts
```

Creates `mig_ab.drill_events` in MongoDB (with the `{cd,_id}` index) and a
clone of the production `drill_events` DDL in ClickHouse db `mig_ab`.

## 2. Straight run

```bash
SERVICE_NAME=validate MONGO_URI=mongodb://localhost:27017 MONGO_DB=mig_ab \
MANIFEST_DB=mig_ab_manifest CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_DB=mig_ab \
LEDGER_CHUNK_DOCS_TARGET=50000 EXIT_ON_COMPLETE=true SERVICE_PORT=18081 npm start
```

Then verify (both numbers must equal the seeded doc count):

```sql
SELECT count() AS total, uniqExact(_id) AS distinct_ids FROM mig_ab.drill_events;
```

## 3. Crash drill

Repeatedly SIGKILLs the service at random points and restarts it until the
migration completes, then verifies zero loss and zero duplicates:

```bash
node --experimental-strip-types bench/kill-drill.ts
```

Watch progress on the dashboard: `http://localhost:18081/viz`.
