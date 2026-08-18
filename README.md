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

Then open **http://localhost:8080/viz** — from here the dashboard takes over:
the **Migration Guide** tab walks the whole procedure (preflight checks,
index building, dry run, cutover checklist, live progress, verification and
sign-off gates), and **Help & Recovery** covers every failure scenario with
the fix one click away.

To scale: start more instances with the same `.env` and a unique `POD_ID`
each, on separate machines (see Scaling with pods below).

This README covers what you need BEFORE the dashboard exists (installing,
env vars, starting the service, automation reference). Everything after —
running, monitoring, troubleshooting, verifying — lives in the dashboard,
with `docs/RUNBOOK.md` as the cross-system procedure (cutover choreography,
Kafka retention, incident tables) for operators.

## Architecture