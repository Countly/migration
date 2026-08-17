/**
 * Seed a failure-scenario dataset on top of bench/setup.ts's clean docs:
 *   - a burst of unmigratable docs (bad ts) concentrated in ONE chunk's cd
 *     window → trips the circuit breaker (>5% of that chunk fails)
 *   - a few scattered unmigratable docs → DLQ capture without a breaker trip
 *   - a few oversized-integer segmentation values → coercion counters
 *
 * Run AFTER bench/setup.ts, same env vars.
 */
import { MongoClient } from 'mongodb';

const MONGO_URI = process.env.AB_MONGO_URI ?? 'mongodb://localhost:27017';
const MONGO_DB = process.env.AB_MONGO_DB ?? 'mig_ab';
const BURST = Number(process.env.FAIL_BURST ?? 800);
const SCATTER = Number(process.env.FAIL_SCATTER ?? 5);

async function main() {
  const mc = new MongoClient(MONGO_URI);
  await mc.connect();
  const coll = mc.db(MONGO_DB).collection('drill_events');

  const bounds = await coll
    .aggregate<{ min: Date; max: Date }>([{ $group: { _id: null, min: { $min: '$cd' }, max: { $max: '$cd' } } }])
    .toArray();
  const min = bounds[0].min.getTime();
  const max = bounds[0].max.getTime();
  const span = max - min;

  const docs: Record<string, unknown>[] = [];
  // Burst: concentrated in the OLDEST 2% of the cd span (the last chunk to be
  // processed, since work goes newest-first — the breaker fires at the end,
  // after the healthy chunks are already done).
  for (let i = 0; i < BURST; i++) {
    docs.push({
      _id: `burst_bad_${i}`, a: 'app1', e: 'corrupted_event', uid: `u${i}`,
      ts: 'not-a-timestamp', cd: new Date(min + Math.floor((span * 0.02 * i) / BURST)),
    });
  }
  // Scatter: a handful of bad docs spread across the middle of the span.
  for (let i = 0; i < SCATTER; i++) {
    docs.push({
      _id: `scatter_bad_${i}`, a: 'app1', e: 'odd_event', uid: `s${i}`,
      ts: 'garbage', cd: new Date(min + Math.floor(span * 0.3) + i * 60_000),
    });
  }
  // Coercion: oversized integers in customer segmentation.
  for (let i = 0; i < 3; i++) {
    docs.push({
      _id: `coerce_${i}`, a: 'app1', e: 'big_numbers', uid: `c${i}`, did: 'd',
      ts: min + Math.floor(span * 0.5) + i * 60_000,
      cd: new Date(min + Math.floor(span * 0.5) + i * 60_000),
      sg: { order_id: 9.2e25, weird: Number.POSITIVE_INFINITY }, c: 1,
    });
  }

  await coll.insertMany(docs as never[], { ordered: false });
  console.log(`Seeded failure scenarios: ${BURST} burst (oldest 2% of span), ${SCATTER} scattered, 3 coercion docs`);
  await mc.close();
}

main().catch((e) => { console.error(e); process.exit(1); });
