# Ingestion-matching transformer differential harness (migration side)

Drill migration overhaul item **D4** ([#6](https://github.com/Countly/migration/issues/6)).

`differential.test.ts` asserts that this repo's transform
(`src/transform/normalize.ts`) reproduces, for every document in the shared fixture
corpus, exactly the canonical `countly_drill.drill_events` row that countly-platform's
live ingestion normalization (`api/utils/eventTransformer.ts`) produces. During
cutover the same Mongo document can reach ClickHouse through both pipelines, so the
rows must be byte-identical or dedup breaks.

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
