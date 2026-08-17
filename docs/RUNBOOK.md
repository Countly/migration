# Migration Runbook

Operational procedure for migrating a customer's `drill_events` from MongoDB
to ClickHouse with this service. The guiding property: **after cutover, no
failure anywhere in this flow can touch live data** — every incident response
is *restart or resume*, never clean up or restore. Ingestion pauses exactly
once, for minutes, at cutover — never for the migration.

## The flow

1. **Prepare** (old cluster still live, no customer impact)
   - Deploy the new stack alongside the old.
   - Set Kafka `drill-events` retention to cover the migration window
     (14 days default). Replication factor is the customer's redundancy
     choice — RF≥2 recommended for large instances; if RF=1, record the
     accepted risk (one broker disk loss forfeits the replay guarantee).
   - Bulk pre-copy the stateful set: apps & app keys, `app_users`, event
     definitions, dashboard users, plugin configs, aggregated data.

2. **Index** — start `{cd:1,_id:1}` builds on all `drill_events*` collections
   now (background, throttled, secondaries where possible). ~1–3 days for
   10 TB; this must not sit inside the post-cutover window. The service also
   builds missing indexes itself, but starting early overlaps the wait.
   No collection consolidation is ever needed.

3. **Rehearse** — dry run with `DRY_RUN=1` (≤5% stratified sample against a
   Null-engine clone; full ClickHouse validation, nothing stored). Review
   `GET /report` (skips, coercions per key, DLQ) with the customer, sign off.

4. **Cutover** — stop old ingestion → sync the stateful-set delta since the
   pre-copy (changed users via last-seen; aggregated data must land BEFORE
   new ingestion writes current-period docs) → enable ingestion on the new
   stack. `app_users` must be complete first or new ingestion mints colliding
   uids. SDK offline queues absorb the window (minutes with pre-copy+delta).
   The old MongoDB is now FROZEN — which is what makes everything after this
   safe to redo.

5. **Migrate** — start the service (see README env vars; scale with pods —
   they claim chunks via leases). Newest data first: the last 30 days are
   visible within hours; the full backfill runs for days with zero impact on
   live ingestion. Watch `/viz`; the invariant monitor spot-checks
   continuously.

6. **Finish** — all chunks done → final `GET /report` → customer sign-off →
   revert Kafka retention → decommission old cluster.

## Incident responses

| Incident | What happens | Operator action |
|---|---|---|
| A doc can't be inserted / converted | Isolated automatically (bisection), stored in DLQ with the full raw doc; run continues | Later: fix the transform rule (platform-first, sync goldens) or fix the stored raw doc, then `POST /control/replay-dlq` |
| Systematic failures (>5% of a chunk) | Circuit breaker pauses the engine; DLQ already names the error | Investigate, fix, `POST /control/retry-failed` (purges + redoes failed chunks, resumes) |
| Migrator crashes / pod dies | Nothing else notices. In-flight chunks are redone from their staging tables; a dead pod's lease expires and others reclaim | Restart the pod. No manual cleanup exists in this flow |
| Live-table rows lost/corrupted for a done chunk | Invariant monitor detects the count mismatch, pauses, flags the chunk | `POST /control/retry-failed` — the chunk's cd window is purged and redone |
| A doc CRASHES the process every time (poison pill) | After 3 crash-retries the chunk is auto-split instead of retried; repeated splitting converges on a ≤1-min window quarantined as a tiny failed chunk — everything else migrates (verified: 20k-doc drill localized 1 poison doc to a 2-doc window in 25 restarts) | Inspect the few source docs in the failed chunk's cd window; fix/remove them, then `POST /control/retry-failed` |
| Live ClickHouse itself must be rebuilt | Live events still sit in the Kafka log; history still sits in frozen Mongo | Recreate table → reset ONLY the ClickHouse-sink connector's offsets to earliest (aggregator groups untouched) → re-run the migrator |

## Verification cheat sheet

```sql
-- exactness (instant, exact):
SELECT count() AS total, uniqExact(_id) AS distinct_ids FROM countly_drill.drill_events;
-- full re-verification of the whole migration in minutes:
--   grouped count per chunk window vs the ledger's rows_expected (mig_ranges)
```

The ledger (`mig_ranges`) and DLQ (`mig_dlq_docs`) live in `MANIFEST_DB`.
Recovery never trusts the ledger blindly — every claim it makes is verified
against actual row counts before anything irreversible happens.

## In-place upgrades (same cluster, MongoDB stays)

Phases 1 & 4 collapse to a config flip (no stateful copy, easy rollback while
the old drill collections still exist). Watch instead: resource contention
(throttle the migrator, read from a secondary, build indexes off-peak), peak
disk (Mongo keeps its data while ClickHouse + staging grow beside it — drop
old per-event collections only after their chunks are done and signed off),
and hard memory limits on the new components — an OOM there is a production
incident.

## Validation before a customer run

`bench/README.md`: seed → straight run (counts must be exact) → SIGKILL crash
drill → optionally `bench/seed-failures.ts` for a full failure-scenario drill
(breaker, DLQ, monitor, retry-failed).
