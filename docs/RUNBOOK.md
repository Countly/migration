# Migration Runbook

Operational procedure for migrating a deployment's `drill_events` data from
MongoDB to ClickHouse with this service. It assumes no prior knowledge of the
tool — terms are defined below, and every action is available both in the
dashboard and as a `curl` command. The guiding property: **after cutover, no
failure anywhere in this flow can touch live data** — every incident response
is *restart or resume*, never clean up or restore. Ingestion pauses exactly
once, for minutes, at cutover — never for the migration.

## Terms used throughout

| Term | Meaning |
|---|---|
| **Old cluster / source** | The MongoDB holding the `drill_events*` collections being migrated (sometimes a frozen clone of it — see the clone-source variant). |
| **New stack / target** | The new Countly architecture whose ClickHouse holds the `drill_events` table this service fills. |
| **cd** | Each document's server-side creation timestamp. The migration chunks, verifies and audits by cd; migrated rows keep their historical cd, live-ingested rows get post-cutover cds. |
| **Chunk** | One cd range of one collection — the unit of work, retry and verification. Chunk state lives in `mig_ranges` (the *ledger*) in `MANIFEST_DB`. |
| **DLQ** | Dead-letter queue (`mig_dlq_docs`): documents that could not or should not be migrated, stored with their full raw source so nothing is silently dropped. |
| **Tee / mirror** | A reverse-proxy (e.g. nginx) duplicating incoming SDK requests to both stacks; each side re-ingests independently, so the same event gets DIFFERENT `_id`/`cd` on each side. |
| **Bound** | `LEDGER_CD_UPPER_BOUND`: a cd ceiling — documents at/after it are never migrated. Required exactly when a tee is active (see the scenario table). |
| **Pod** | One instance of this service. Pods coordinate through chunk leases in MongoDB; any pod's dashboard shows the whole run. |

## The flow

1. **Prepare** (old cluster still live, no user-facing impact)
   - Deploy the new stack alongside the old.
   - Set Kafka `drill-events` retention to cover the migration window
     (14 days default). Replication factor is a redundancy
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
   `GET /report` (skips, coercions per key, DLQ) with whoever owns sign-off.

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

6. **Finish** — all chunks done → Final check green → sign-off →
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

## Final check — the one-click sign-off

Don't interpret audit buckets by hand: the **Final check** runs everything
(chunk states, DLQ, full source recount, cd-checksum fingerprints, sampled
content comparison), applies the tee/cutover rules itself, and answers the
only question that matters — *is it safe to decommission the old cluster?* —
as **PASS / PASS WITH NOTES / FAIL** in plain sentences with the action named
on every red line.

- Dashboard: the **Final check** card → *Run final check*. On tee/mirror runs
  without a stored bound, type the cutover time into the field first.
- SSH-only:

```bash
# start (add {"cutoverMs": <epoch ms of the tee flip>} for mirror runs without a stored bound)
curl -s -X POST localhost:PORT/control/final-check -H 'content-type: application/json' -d '{}'
# read the verdict (re-run until it says PASS/FAIL; shows progress while running)
curl -s localhost:PORT/final-check.txt
```

A stored/env cd bound is picked up automatically as the cutover. Post-cutover
source windows are excluded and explained in a note — divergence there is the
mirror still feeding the old side, not data loss. Run it while the old
cluster is still up: the source is the reference.

## Tee-overlap dedupe — fixing a missing bound after the fact

A mirrored cutover migrated WITHOUT `LEDGER_CD_UPPER_BOUND` copies the
mirror's re-ingested docs on top of natively ingested rows: every event in
the overlap window (tee flip → migration completion) exists twice in
ClickHouse. The copies are separable — the migrated copy's `_id` exists in
the old cluster's Mongo; the native one's doesn't. **Must run before the old
cluster is decommissioned** (old Mongo is the separator).

An id match alone is not proof of duplication: if the tee (or the new
side's ingestion) dropped a request, the migrated row is the ONLY copy of
that event. Every hour bucket therefore needs count-evidence of native
counterparts — `native = live − matched` must roughly cover `matched` —
before anything in it is deleted. Buckets that fall short are skipped and
reported (`unsafe` in the result); review those hours (tee outage? wrong
start time?) instead of forcing them. The check is strict (zero slack) by
default; `slackPct` (≤5) may be passed consciously to absorb ingest-timing
straddle at bucket edges. Known limit: a loss exactly offset by
mirror-dropped natives in the same hour is invisible to count evidence —
an EMPTY dry run means no duplicates (skip the step; never widen the window
to make it match something). Both dedupe (dry run included) and the Final
check refuse while any pod still holds an active chunk claim.

There is a dashboard card for this (Overview → **Tee-overlap dedupe**:
enter the window, *Dry run* first — *Delete duplicates* unlocks only after
it) as well as the endpoints below.

```bash
# 1. DRY RUN (counts only): fromMs = tee flip / IP swap, toMs = migration completion
curl -s -X POST localhost:PORT/control/dedupe-overlap -H 'content-type: application/json' \
  -d '{"fromMs": 1789700000000, "toMs": 1789794970435}'
curl -s localhost:PORT/api/dedupe-overlap        # totals.chMatched = the duplicates
# 2. EXECUTE (refused unless the dry run over the SAME window completed first)
curl -s -X POST localhost:PORT/control/dedupe-overlap -H 'content-type: application/json' \
  -d '{"fromMs": 1789700000000, "toMs": 1789794970435, "execute": true}'
# 3. re-run the Final check with the same cutover to confirm
```

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

## Validation before a production run

`bench/README.md`: seed → straight run (counts must be exact) → SIGKILL crash
drill → optionally `bench/seed-failures.ts` for a full failure-scenario drill
(breaker, DLQ, monitor, retry-failed).

## Choose your scenario first

The one decision that changes the configuration is whether a TEE mirrors
the same requests into both stacks. Everything else is shared machinery.

| # | Topology | LEDGER_CD_UPPER_BOUND | Ingestion switch | New data arriving in old Mongo | Sign-off |
|---|---|---|---|---|---|
| 1 | Two clusters, **no mirroring** (plain switch) | **UNSET** | Before the migration (cutover-first) or after the bulk (bulk-before-cutover + final drain) | **Migrated** — top-up passes chase it until the drain finds nothing | Verify + audits, DLQ = 0 |
| 2 | Two clusters, **mirror old → new** (old primary) | **SET** = tee flip | At sign-off | **Never migrated past the bound** — it is the tee's copy (different _id/cd; duplicates would be undetectable) | Verify + audits for pre-bound; dashboard comparison + sync parity for post-bound |
| 3 | Two clusters, **mirror new → old** (new primary, old = rollback net) | **SET** = the moment new became primary | Already happened at the flip | Same as 2 — post-flip old-side docs are mirror copies | Same as 2 |
| 4 | **Single cluster, in-place upgrade** (drill mongo → ClickHouse in background) | **UNSET** | The upgrade itself is the switch; old drill collections freeze | Transition tail drained by top-up; no tee → nothing to duplicate | Verify + audits, DLQ = 0 (live-parallel path; backpressure protects prod CH) |

Scenario is also selectable on the dashboard's **Migration Guide** tab —
it renders the per-scenario checklist and states the bound requirement.
For 2 and 3: use **Detect boundary** + **Apply this bound to the run**
(one click covers all pods), verify the `bounded · cd < …` badge on every
pod, and keep re-running sync parity during the validation window.

## Clone-source variant (migrate from a frozen copy)

A robust pattern: pause old ingestion, clone the source MongoDB onto the
new machine, then resume ingestion on the NEW stack (optionally mirroring
back to the old one as the rollback net) and migrate from the clone. SDK
offline queues absorb the pause. Properties worth knowing:

- The source is frozen at the clone moment, so no bound is needed and top-up
  finds nothing — the startup guard will still ask (the target ingests live
  while the run starts): **Proceed unbounded is correct** here.
- Parity/audit tables compare against the CLONE: zeros after the clone
  moment mean "clone taken here", not a dead mirror. The live old-side
  MongoDB is invisible to the tool.
- Cloned INSIDE the ingestion pause (the sequence above) → the clone can
  never hold a natively-ingested event's mirror copy: **no duplicates, no
  bound, no dedupe** — the cleanest possible run. Only a clone taken AFTER
  ingestion resumed has a duplicated tail: dedupe with exactly
  [ingestion-resume, clone-moment], never earlier.
- Any doc-count comparison against the live old-arch Mongo will drift by
  everything ingested after T-clone — compare against the clone, or scope
  counts to cd < T-clone.

## Tee-mirror cutover (keep the old architecture until sign-off)

When approval is required before switching: the old stack stays
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
5. Validate side-by-side as long as needed; both systems ingest
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

### One-call boundary setting (SSH / API)

The whole detect-and-apply flow is a single endpoint:

```bash
# detect, and apply automatically when the seam is an exact ingestion-pause gap
curl -s -X POST localhost:PORT/control/set-boundary -H 'content-type: application/json' -d '{}'
# read the outcome — the apply receipt lands in .applied
curl -s localhost:PORT/api/boundary
# no exact gap? review the report, then accept the anchor explicitly…
curl -s -X POST localhost:PORT/control/set-boundary -H 'content-type: application/json' -d '{"acceptAnchor": true}'
# …or set the bound to a known timestamp directly (applies immediately)
curl -s -X POST localhost:PORT/control/set-boundary -H 'content-type: application/json' -d '{"boundMs": 1789966140000}'
```

An exact gap applies unattended; an anchor (quantified ambiguity) is never
auto-applied without `acceptAnchor`. The dashboard flow and the separate
`/control/detect-boundary` + `/control/apply-bound` endpoints keep working.

### The startup guard — the bound mistake, made impossible to miss

A FRESH run that finds its target ClickHouse already receiving live data,
with no cd bound set, **holds before mapping** (`pauseReason:
boundary-unset`). That is exactly the setup where an unset bound either
duplicates the overlap window (mirror active) or is a deliberate choice
(cutover-first / in-place, where new data must still be migrated). The tool
cannot tell those apart from data alone, so it asks — once:

- mirror active → apply the bound (`POST /control/set-boundary`, the
  dashboard card, or `LEDGER_CD_UPPER_BOUND`); the run releases itself, or
- nothing mirrors traffic → click **Proceed unbounded** in the banner, or
  `curl -X POST localhost:PORT/control/allow-unbounded` (cluster-wide,
  releases every held pod), or deploy with `LEDGER_UNBOUNDED_OK=1`.

A plain Resume is deliberately ignored while the question is open. Resumed
runs and runs whose target holds no recent data never trip the guard.

### Bound is opt-in — pick the mode deliberately

| Situation | LEDGER_CD_UPPER_BOUND | Behavior |
|---|---|---|
| No tee (classic cutover / bulk-before-cutover) | UNSET | Migrate EVERYTHING, including data that keeps arriving in the old cluster — top-up passes chase it until the final drain. ClickHouse's existing rows play no role in mapping. |
| Tee active (same requests re-ingested on both sides) | SET to the flip boundary | Post-flip old-cluster data is the tee's copy — migrating it would duplicate undetectably. Mapper clamps, top-up disabled. |

The boundary detector only SUGGESTS a value; it is never applied
automatically — in the no-tee mode applying it would orphan new arrivals.

## DLQ: `skip:missing_uid` — orphan docs of deleted users

Old deployments accumulate drill documents with NO `uid` (often also no
`ts`/`did`, cd around the epoch): these are orphan docs of app users that
were deleted in the old system — the user record and its uid link are
gone, the event document remained. They cannot be attributed to any user
in either the old or the new system.

**Standard call: Waive.** They are captured in full in the DLQ (nothing is
silently dropped), the waive is recorded and counted, and the source audit
attributes each window's shortfall to its waived docs — sign-off stays
exact. Only consider a sentinel-uid replay instead if the affected volume
is large enough to distort historical event totals for an app AND the
docs carry usable ts/did (check a few samples in the DLQ panel first).

Chunks that were 100% such docs complete as done (structured skips do not
trip the fail-rate breaker); the mass-DLQ pause that fires when millions
of them accumulate is the built-in "stop and decide" moment — after
waiving, click Resume.

## SSH-only operation (no browser access to the dashboard port)

Everything the dashboard shows and does is plain HTTP on the pod
(default SERVICE_PORT 8080). Three tools:

**1. Live status in the terminal** (the dashboard as text):
```
watch -n 5 'curl -s localhost:8080/status.txt'
```

**2. Progress heartbeat in the logs** — one structured line per minute
(`migration progress heartbeat`: docs read/total/%, docs/s, chunks,
failed, DLQ, status + pause reason). No network access needed:
```
kubectl logs -f deploy/drill-migrator | grep 'progress heartbeat'
docker logs -f drill-migrator-p1 2>&1 | grep 'progress heartbeat'
```
Because they go to stdout, they flow into whatever log pipeline collects
container output (Loki, ELK, CloudWatch, …) with zero extra plumbing.

**3. Actions via curl** (same endpoints the buttons call; POSTs need the
JSON content type):
```
# state
curl -s localhost:8080/status.txt                 # human snapshot
curl -s localhost:8080/stats                      # full JSON (incl. cluster rate, run times)
curl -s localhost:8080/api/pods                   # pod table
curl -s localhost:8080/report                     # skips, coercions, DLQ summary

# control
curl -s -X POST localhost:8080/control/pause         -H 'content-type: application/json' -d '{}'
curl -s -X POST localhost:8080/control/resume        -H 'content-type: application/json' -d '{}'
curl -s -X POST localhost:8080/control/retry-failed  -H 'content-type: application/json' -d '{}'
curl -s -X POST localhost:8080/control/replay-dlq    -H 'content-type: application/json' -d '{}'   # progress: /api/replay
curl -s -X POST localhost:8080/control/waive-dlq     -H 'content-type: application/json' -d '{}'   # ALL pending; or {"ids":[...]}

# sign-off checks (background tasks: POST to start, GET to poll)
curl -s -X POST localhost:8080/control/verify        -H 'content-type: application/json' -d '{}'; curl -s localhost:8080/api/verify
curl -s -X POST localhost:8080/control/audit-source  -H 'content-type: application/json' -d '{}'; curl -s localhost:8080/api/audit-source
curl -s -X POST localhost:8080/control/audit-content -H 'content-type: application/json' -d '{}'; curl -s localhost:8080/api/audit-content

# tee-mirror cutovers
curl -s -X POST localhost:8080/control/detect-boundary -H 'content-type: application/json' -d '{}'; curl -s localhost:8080/api/boundary
curl -s -X POST localhost:8080/control/apply-bound     -H 'content-type: application/json' -d '{"boundMs": 1787561650562}'
```
Reminder: retry/replay/waive only QUEUE work while the engine is paused —
finish with `/control/resume`. Any pod answers; state is shared.
