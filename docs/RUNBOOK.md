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
| A doc can't be inserted / converted | Isolated automatically (bisection), stored in DLQ with the full raw doc; run continues | Later: fix the transform rule (platform-first, sync goldens) or fix the stored raw doc, then `POST /control/replay-dlq`. Docs that keep failing stay pending with an updated error — terminal outcomes are fix-and-replay or `POST /control/waive-dlq` (explicitly accept non-migration; raw docs are retained as the record). Sign-off requires pending = 0 |
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

## Choose your scenario first

The one decision that changes the configuration is whether a TEE mirrors
the same requests into both stacks. Everything else is shared machinery.

| # | Topology | LEDGER_CD_UPPER_BOUND | Ingestion switch | New data arriving in old Mongo | Sign-off |
|---|---|---|---|---|---|
| 1 | Two clusters, **no mirroring** (plain switch) | **UNSET** | Before the migration (cutover-first) or after the bulk (bulk-before-cutover + final drain) | **Migrated** — top-up passes chase it until the drain finds nothing | Verify + audits, DLQ = 0 |
| 2 | Two clusters, **mirror old → new** (old primary) | **SET** = tee flip | At customer sign-off | **Never migrated past the bound** — it is the tee's copy (different _id/cd; duplicates would be undetectable) | Verify + audits for pre-bound; dashboard comparison + sync parity for post-bound |
| 3 | Two clusters, **mirror new → old** (new primary, old = rollback net) | **SET** = the moment new became primary | Already happened at the flip | Same as 2 — post-flip old-side docs are mirror copies | Same as 2 |
| 4 | **Single cluster, in-place upgrade** (drill mongo → ClickHouse in background) | **UNSET** | The upgrade itself is the switch; old drill collections freeze | Transition tail drained by top-up; no tee → nothing to duplicate | Verify + audits, DLQ = 0 (live-parallel path; backpressure protects prod CH) |

Scenario is also selectable on the dashboard's **Migration Guide** tab —
it renders the per-scenario checklist and states the bound requirement.
For 2 and 3: use **Detect boundary** + **Apply this bound to the run**
(one click covers all pods), verify the `bounded · cd < …` badge on every
pod, and keep re-running sync parity during the validation window.

## Tee-mirror cutover (customer keeps the old architecture until sign-off)

For customers who require approval before switching: the old arch stays
authoritative, nginx TEES the same SDK requests to the new architecture
(which re-ingests them with its own logic — drill, sessions, aggregations,
profiles all populate natively), and the bulk migration backfills history
up to the moment the tee was enabled.

CRITICAL: the tee re-ingests requests, so the same event exists in both
systems under DIFFERENT identities (new _id, new cd). Nothing downstream
can deduplicate across that seam — the ONLY protection is the time bound.

1. Deploy the new arch cluster; point no direct traffic at it.
2. Flip the nginx tee INSIDE a short old-ingestion pause (~60s): pause the
   old API (SDKs queue and retry — nothing is lost), enable the tee,
   resume. The pause creates a sharp boundary: every old-cluster doc with
   cd before the pause predates the tee; everything after was teed.
   Record any timestamp inside the pause window as THE BOUND.
   - If a pause is not possible: bound = flip time + the old arch's worst
     drill-write latency, and accept that the few seconds of teed traffic
     inside that margin will be double-counted once (pick a quiet hour).
3. Run the bulk migration with `LEDGER_CD_UPPER_BOUND=<bound>` on every
   pod (epoch ms or ISO). The mapper never crosses it, top-up is disabled,
   collections born after it are skipped, and preflight treats the growing
   source as the expected state. The header badge shows `bounded · cd < …`
   on every pod — if it is missing on any pod, STOP that pod.
4. Verify + Audit-vs-source as usual: they cover the migrated (pre-bound)
   region; post-bound windows show as pending/uncovered, never as defects.
   The post-bound region is the tee's responsibility and is validated by
   comparing dashboards between the two systems, not by this tool.
5. Customer validates side-by-side as long as needed; both systems ingest
   the same requests the whole time.
6. On approval: point SDK traffic solely at the new arch, drop the tee,
   decommission old ingestion on its own schedule.

Caveats:
- NEVER run without the bound while the tee is active — every post-flip
  doc migrated from the old cluster is an undetectable duplicate of its
  re-ingested twin.
- GDPR erasures and app-user merges executed on the OLD system during the
  validation window apply only there; re-apply them through the new arch
  before sign-off.
- Retention TTL keeps deleting on the old side throughout — the source
  audit reports that as deletion drift, not as a defect.

### Bound is opt-in — pick the mode deliberately

| Situation | LEDGER_CD_UPPER_BOUND | Behavior |
|---|---|---|
| No tee (classic cutover / bulk-before-cutover) | UNSET | Migrate EVERYTHING, including data that keeps arriving in the old cluster — top-up passes chase it until the final drain. ClickHouse's existing rows play no role in mapping. |
| Tee active (same requests re-ingested on both sides) | SET to the flip boundary | Post-flip old-cluster data is the tee's copy — migrating it would duplicate undetectably. Mapper clamps, top-up disabled. |

The boundary detector only SUGGESTS a value; it is never applied
automatically — in the no-tee mode applying it would orphan new arrivals.
