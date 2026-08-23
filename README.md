# MongoDB to ClickHouse Migration Service

Migrates Countly `drill_events*` collections from MongoDB into a single ClickHouse table. Supports multi-pod horizontal scaling, range-parallel processing, async writes, pause/resume, crash recovery, backpressure monitoring, and a real-time dashboard.

## Setup & run

Prerequisites: reachable MongoDB (the source) and ClickHouse (the target with
the `drill_events` table — created by the new Countly stack). Nothing else.

```bash
cp .env.example .env    # point MONGO_URI / CLICKHOUSE_URL at your systems
docker compose up --build
# or, with Node 25+:  npm install && npm start
```

Then open **http://localhost:8080** — from here the dashboard takes over:
the **Migration Guide** tab walks the whole procedure (preflight checks,
index building, dry run, cutover checklist, live progress, verification and
sign-off gates), and **Help & Recovery** covers every failure scenario with
the fix one click away.

To scale: start more instances with the same `.env` and a unique `POD_ID`
each, on separate machines (see Scaling with pods below).

Chunks for ALL collections are mapped upfront and claimed globally
(collections in order, newest data first within each), so pods spill into
the next collection the moment the current one has nothing claimable —
many-small-collection datasets parallelize across pods just like one big
collection does.

**Docker (no Kubernetes)**: scaling works the same way — pods are just
processes coordinating through chunk leases in MongoDB, and `POD_ID`
defaults to the container hostname (unique automatically). Run one
container per machine with the same `.env`:

```bash
docker run -d --env-file .env --name drill-migrator \
  -p 8080:8080 europe-docker.pkg.dev/<registry>/drill-migrator:<tag>
```

Add machines by running the same command there — nothing to configure,
each container's dashboard shows the whole run. Scale across MACHINES,
not on one host: a single container saturates ~4 cores on BSON decode,
so `docker compose --scale migration=N` on one box only makes sense for
testing (and requires dropping the fixed published port). Set
`EXIT_ON_COMPLETE=true` for fire-and-forget runs — containers exit 0
when every chunk is done.

**Kubernetes**: ready-to-apply manifests live in `k8s/` —
`k8s/migration.yaml` (Deployment + Service: pods keep serving the dashboard
after completion for verification and sign-off; scale with
`kubectl scale deployment/drill-migrator --replicas=N`) and `k8s/job.yaml`
(fire-and-forget Job using `EXIT_ON_COMPLETE`). Pods coordinate through
chunk leases in MongoDB, `POD_ID` defaults to the pod name, and abrupt
kills/evictions are safe by design (chunk redo). Reach the dashboard with
`kubectl port-forward svc/drill-migrator 8080:8080` — any pod shows the
whole run.

No schema changes are made to the live table. Migrated and live-ingested
rows are distinguished by construction: migrated rows carry their historical
`cd`, live rows are stamped at post-cutover insert time. Where an `_id` alone
would be ambiguous (an SDK retry across the cutover lands the same event in
both stacks, in the same partition), checks match `(_id, cd)` pairs — the
retry copy's cd can never equal the migrated copy's. Preflight verifies the
boundary is trustworthy (source frozen, clocks sane) before anything runs.

This README covers what you need BEFORE the dashboard exists (installing,
env vars, starting the service, automation reference). Everything after —
running, monitoring, troubleshooting, verifying — lives in the dashboard,
with `docs/RUNBOOK.md` as the cross-system procedure (cutover choreography,
Kafka retention, incident tables) for operators.

## Architecture