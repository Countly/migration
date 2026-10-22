/**
 * $currentOp → index-build progress, against REAL captures only:
 *
 *  fixtures/currentop-index-build.json  MongoDB 8.0.34 replica set, one op per shape,
 *                                       primary + secondary
 *  fixtures/streams/*.json              every $currentOp sample (250ms apart, the
 *                                       service's own projection + readConcern) over
 *                                       one whole build: 8.0 primary, 8.0 secondary,
 *                                       8.0 2-shard cluster via mongos
 *
 * Pinned here:
 *  - the builder thread's progress wins over the client op waiting on it
 *  - the phase travels with the percentage (it restarts at 0% in each phase)
 *  - secondaries (no command.$db) are matched through ns
 *  - builds of other indexes, other dbs and other commands are ignored
 *  - behind mongos the least advanced shard is reported, with the shard count
 *  - replayed over a whole build: one entry per sample, phases never go
 *    backwards, the percentage never drops within a phase
 */
import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';

import { parseIndexBuildOps, INDEX_BUILD_PHASES, type IndexBuildOp } from '../../src/source/index-build-ops.ts';

type Op = Record<string, unknown>;
const load = <T>(rel: string): T => JSON.parse(readFileSync(new URL(rel, import.meta.url), 'utf8')) as T;
const m8 = load<Record<string, Op>>('./fixtures/currentop-index-build.json');
// the primary's ops, one per shape
const p = {
  waiter: m8['primary.waiter'], scan: m8['primary.scan'], scanNoProgress: m8['primary.scanNoProgress'],
  sorter: m8['primary.sorter'], sorterNoProgress: m8['primary.sorterNoProgress'],
};
const STREAMS: Record<string, Op[][]> = {
  'MongoDB 8.0 replica-set primary': load('./fixtures/streams/currentop-m8-primary.json'),
  'MongoDB 8.0 replica-set secondary': load('./fixtures/streams/currentop-m8-secondary.json'),
  'MongoDB 8.0 sharded (2 shards, via mongos)': load('./fixtures/streams/currentop-m8-sharded-mongos.json'),
};
const clone = (op: Op): Op => structuredClone(op);
const COLL = 'drill_events_probe';
const DB = 'probe';

describe('parseIndexBuildOps — MongoDB 8.0 primary captures', () => {
  it('reports the scanning phase from the builder, not the waiting client', () => {
    expect(parseIndexBuildOps([p.waiter, p.scan], DB)).toEqual([{
      collection: COLL, phase: 1, phaseLabel: 'scanning documents',
      done: 101951, total: 1500000, pct: 7, msg: p.scan.msg, shard: null, shards: 0,
    }]);
    // order of the two ops does not matter
    expect(parseIndexBuildOps([p.scan, p.waiter], DB)[0].pct).toBe(7);
  });

  it('reports the key-writing step with its own percentage', () => {
    expect(parseIndexBuildOps([p.waiter, p.sorter], DB)[0])
      .toMatchObject({ phase: 3, phaseLabel: 'writing index keys', done: 38606, total: 1500000, pct: 3 });
  });

  it('"scanning" without counters is the sorting step; key writing without counters stays step 3', () => {
    expect(parseIndexBuildOps([p.scanNoProgress], DB)[0])
      .toMatchObject({ phase: 2, phaseLabel: 'sorting index keys (scan complete)', done: null, total: null, pct: null });
    expect(parseIndexBuildOps([p.sorterNoProgress], DB)[0]).toMatchObject({ phase: 3, pct: null });
  });

  it('falls back to the client op when no builder is visible', () => {
    expect(parseIndexBuildOps([p.waiter], DB)).toEqual([{
      collection: COLL, phase: null, phaseLabel: 'in progress', done: null, total: null, pct: null, msg: '', shard: null, shards: 0,
    }]);
  });

  it('labels the draining phase and keeps unknown phases in the server\'s words', () => {
    const drain = clone(p.scan);
    drain.msg = 'Index Build: draining writes received during build';
    delete drain.progress;
    expect(parseIndexBuildOps([drain], DB)[0])
      .toMatchObject({ phase: 4, phaseLabel: 'applying writes made during the build', pct: null });
    const future = clone(p.scan);
    future.msg = 'Index Build: committing the index: 5/10 50%';
    delete future.progress;
    expect(parseIndexBuildOps([future], DB)[0])
      .toMatchObject({ phase: null, phaseLabel: 'committing the index', pct: null });
  });

  it('ignores other databases, other indexes and non-index ops', () => {
    const otherDb = clone(p.scan);
    (otherDb.command as Op).$db = 'countly_migration';
    const otherIndex = clone(p.scan);
    (otherIndex.command as Op).indexes = [{ v: 2, key: { run_id: 1, status: 1 }, name: 'run_id_1_status_1' }];
    const reversed = clone(p.scan);
    (reversed.command as Op).indexes = [{ v: 2, key: { _id: 1, cd: 1 }, name: '_id_1_cd_1' }];
    const wider = clone(p.scan);
    (wider.command as Op).indexes = [{ v: 2, key: { cd: 1, _id: 1, uid: 1 }, name: 'cd_1__id_1_uid_1' }];
    const find: Op = { ns: `${DB}.${COLL}`, command: { find: COLL, $db: DB } };
    const noCommand: Op = { ns: `${DB}.${COLL}`, msg: 'Index Build: scanning collection' };
    expect(parseIndexBuildOps([otherDb, otherIndex, reversed, wider, find, noCommand], DB)).toEqual([]);
  });

  it('finds the {cd,_id} spec inside a multi-index createIndexes', () => {
    const multi = clone(p.scan);
    (multi.command as Op).indexes = [{ v: 2, key: { uid: 1 }, name: 'uid_1' }, { v: 2, key: { cd: 1, _id: 1 }, name: 'cd_1__id_1' }];
    expect(parseIndexBuildOps([multi], DB)).toHaveLength(1);
  });

  it('caps a percentage the server overshoots and ignores a zero total', () => {
    const over = clone(p.scan);
    over.progress = { done: 1_600_000, total: 1_500_000 };
    expect(parseIndexBuildOps([over], DB)[0].pct).toBe(100);
    const zero = clone(p.scan);
    zero.progress = { done: 0, total: 0 };
    expect(parseIndexBuildOps([zero], DB)[0]).toMatchObject({ pct: null, total: null, phase: 1 });
  });

  it('returns one entry per collection and nothing for an empty op list', () => {
    const second = clone(p.sorter);
    (second.command as Op).createIndexes = 'drill_events_other';
    const out = parseIndexBuildOps([p.waiter, p.scan, second], DB);
    expect(out.map((o) => [o.collection, o.phase])).toEqual([[COLL, 1], ['drill_events_other', 3]]);
    expect(parseIndexBuildOps([], DB)).toEqual([]);
  });
});

describe('parseIndexBuildOps — MongoDB 8.0 replica set', () => {
  it('primary: every shape', () => {
    expect(parseIndexBuildOps([m8['primary.waiter'], m8['primary.scan']], DB)[0])
      .toMatchObject({ collection: COLL, phase: 1, total: 1_500_000, shards: 0 });
    expect(parseIndexBuildOps([m8['primary.scanNoProgress']], DB)[0]).toMatchObject({ phase: 2, pct: null });
    expect(parseIndexBuildOps([m8['primary.waiter'], m8['primary.sorter']], DB)[0])
      .toMatchObject({ phase: 3, total: 1_500_000 });
    expect(parseIndexBuildOps([m8['primary.sorterNoProgress']], DB)[0]).toMatchObject({ phase: 3, pct: null });
  });

  it('secondary: the builder op has no command.$db and is matched through ns', () => {
    const op = m8['secondary.scan'];
    expect((op.command as Op).$db).toBeUndefined();
    expect(parseIndexBuildOps([op], DB)[0]).toMatchObject({ collection: COLL, phase: 1, total: 1_500_000 });
    expect(parseIndexBuildOps([op], 'countly_drill')).toEqual([]);
    expect(parseIndexBuildOps([m8['secondary.scanNoProgress']], DB)[0]).toMatchObject({ phase: 2 });
    expect(parseIndexBuildOps([m8['secondary.sorter']], DB)[0]).toMatchObject({ phase: 3 });
  });
});

describe('parseIndexBuildOps — sharded cluster via mongos (real 2-shard capture)', () => {
  const stream = STREAMS['MongoDB 8.0 sharded (2 shards, via mongos)'];

  it('every op carries its shard and both shards build', () => {
    const shards = new Set(stream.flat().map((o) => o.shard));
    expect(shards).toEqual(new Set(['s1', 's2']));
  });

  it('reports the least advanced shard and the shard count, sample by sample', () => {
    for (const sample of stream) {
      const [entry] = parseIndexBuildOps(sample, DB);
      const builders = sample
        .filter((o) => o.progress !== undefined)
        .map((o) => parseIndexBuildOps([o], DB)[0]);
      expect(entry.shards).toBe(new Set(sample.map((o) => o.shard)).size);
      if (builders.length === 0) continue;
      const minPhase = Math.min(...builders.map((b) => b.phase ?? 0));
      const inPhase = builders.filter((b) => (b.phase ?? 0) === minPhase);
      if (entry.pct !== null) {
        expect(entry.phase).toBe(minPhase);
        expect(entry.pct).toBe(Math.min(...inPhase.map((b) => b.pct ?? 101)));
        expect(entry.total).toBeLessThan(1_000_000); // per-shard counters, not the collection total
      }
    }
  });

  it('a shard momentarily without counters never hides one that has them', () => {
    const withCounters = clone(m8['primary.sorter']);
    withCounters.shard = 's1';
    withCounters.progress = { done: 455_170, total: 499_456 };
    const noCounters = clone(m8['primary.sorterNoProgress']);
    noCounters.shard = 's2';
    for (const ops of [[withCounters, noCounters], [noCounters, withCounters]]) {
      expect(parseIndexBuildOps(ops, DB)[0]).toMatchObject({ shard: 's1', pct: 91, shards: 2 });
    }
  });
});

describe.each(Object.entries(STREAMS))('replaying a whole build — %s', (_name, stream) => {
  const parsed = stream.map((sample) => parseIndexBuildOps(sample, DB));
  const entries: IndexBuildOp[] = parsed.map((out) => out[0]).filter(Boolean);

  it('one entry per sample (one collection building)', () => {
    for (const out of parsed) expect(out.length).toBeLessThanOrEqual(1);
  });

  it('captured a real build', () => {
    expect(stream.length).toBeGreaterThanOrEqual(5);
    expect(entries.length).toBeGreaterThanOrEqual(5);
  });

  it('sees both counted steps with counters', () => {
    for (const phase of [1, 3]) {
      expect(entries.some((e) => e.phase === phase && e.pct !== null)).toBe(true);
    }
  });

  it('passes through the sorting step, only after the scan counters and before key writing', () => {
    const firstSort = entries.findIndex((e) => e.phase === 2);
    expect(firstSort).toBeGreaterThan(0);
    expect(entries.slice(0, firstSort).some((e) => e.phase === 1 && e.pct !== null)).toBe(true);
    expect(entries.slice(0, firstSort).some((e) => (e.phase ?? 0) > 2)).toBe(false);
    // the scan was (near) complete when sorting started
    const lastScan = entries.slice(0, firstSort).filter((e) => e.phase === 1 && e.pct !== null).pop()!;
    expect(lastScan.pct).toBeGreaterThanOrEqual(70);
  });

  it('never goes backwards: phase non-decreasing, percentage non-decreasing within a phase', () => {
    let last: IndexBuildOp | null = null;
    for (const e of entries) {
      if (e.phase === null) continue;
      if (last && last.phase !== null) {
        expect(e.phase).toBeGreaterThanOrEqual(last.phase);
        if (e.phase === last.phase && e.pct !== null && last.pct !== null) {
          expect(e.pct).toBeGreaterThanOrEqual(last.pct);
        }
      }
      if (e.pct !== null || !last || e.phase !== last.phase) last = e;
    }
  });

  it('every entry is the drill collection with a sane percentage', () => {
    for (const e of entries) {
      expect(e.collection).toBe(COLL);
      if (e.pct !== null) {
        expect(e.pct).toBeGreaterThanOrEqual(0);
        expect(e.pct).toBeLessThanOrEqual(100);
        expect(e.done).not.toBeNull();
      }
      if (e.phase !== null) expect(e.phase).toBeLessThanOrEqual(INDEX_BUILD_PHASES);
    }
  });
});
