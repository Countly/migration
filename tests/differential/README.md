# Ingestion-matching transformer differential harness (migration side)

Drill migration overhaul item **D4** ([#6](https://github.com/Countly/migration/issues/6)).

`differential.test.ts` asserts that this repo's transform
(`src/transform/normalize.ts`) reproduces, for every document in the shared fixture
corpus, exactly the canonical `countly_drill.drill_events` row of the agreed
normalization spec.

**Status of the platform side:** the spec was co-developed with a rewrite of
countly-platform's `api/utils/eventTransformer.ts` (branch
`claude/jovial-shannon-b3dd29`), and these goldens were generated from that code.
That platform PR is currently UNMERGED, and this tool does not require it: the
migrator writes ClickHouse directly and never runs platform code. The goldens
therefore act as this repo's frozen spec. The platform PR remains desirable for
platform-side replay paths (anything feeding old drill docs through
KafkaEventSink on main today re-stamps `cd` to insert time and skips
sanitization) — but that is hardening for the platform, not a dependency of
the migration. The cd re-stamping specifically is fixed by the surgical
[countly-platform#1105](https://github.com/Countly/countly-platform/pull/1105),
split out of the branch because it also affects LIVE rows: without it,
Kafka offset replay and connector redelivery re-date live events too.

**Vendored files — do not edit here:** `corpus.json`, `goldens.json`, `decode.mjs`,
`canonicalize.mjs` are synced byte-identical from countly-platform
`test/unit/fixtures/drill-transform-differential/`. The goldens are generated from the
platform's live ingestion code (`generate-goldens.mjs` there) — the platform is the
source of truth. The goldens embed sha256 hashes of the corpus and of
decode+canonicalize, so a partial sync fails the first test.

When this suite fails:

- **after a change in this repo** — the migration transform drifted from live
  ingestion; fix `src/transform/`, never the vendored goldens.
- **after syncing fresh fixtures** — countly-platform changed normalization behavior;
  align `src/transform/` to match (the golden diff in the platform PR describes the
  change).

The full shared normalization spec and the pre-alignment divergence report live next
to the generator in the countly-platform fixture directory.

Runs in CI via `.github/workflows/ci.yml` (`npx vitest run tests/differential`) —
pure unit tests, no MongoDB/ClickHouse/Redis needed.
