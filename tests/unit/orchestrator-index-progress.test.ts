/**
 * ChunkOrchestrator index-build bookkeeping against a fake MongoDB (no
 * database needed). Pinned here:
 *
 *  - $currentOp runs with readConcern 'local' and projects `shard`; any
 *    failure is reported as serverOpsError, never swallowed into "no builds"
 *  - startIndexBuilds builds unlisted indexes AND joins builds already in
 *    flight (listed as present, but not ready), reporting how many it joined
 *  - a failing build stops the queue and is reported
 *  - mapping clears the "waiting on index" state when ensureIndex fails, and
 *    names the collection while it waits
 */
import { describe, it, expect, vi } from 'vitest';
import pino from 'pino';
import { readFileSync } from 'node:fs';

import { ChunkOrchestrator, type ChunkOrchestratorDeps } from '../../src/runtime/chunk-orchestrator.ts';

type Op = Record<string, unknown>;
const m8 = JSON.parse(readFileSync(new URL('./fixtures/currentop-index-build.json', import.meta.url), 'utf8')) as Record<string, Op>;
const building = [m8['primary.waiter'], m8['primary.scan']]; // MongoDB 8.0 primary, scan at 7%
const DB = 'probe';
const PREFIX = 'drill_events';
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

interface FakeColl { indexed: boolean; createIndex: (spec: unknown) => Promise<string> }

function fakeMongo(opts: {
  collections: Record<string, FakeColl>;
  currentOp?: () => Promise<Op[]>;
}) {
  const aggregateCalls: Array<{ pipeline: Op[]; options: Op | undefined }> = [];
  const db = {
    databaseName: DB,
    listCollections: () => ({ toArray: async () => Object.keys(opts.collections).map((name) => ({ name })) }),
    collection: (name: string) => ({
      indexes: async () => [{ name: '_id_', key: { _id: 1 } }, ...(opts.collections[name].indexed ? [{ name: 'cd_1__id_1', key: { cd: 1, _id: 1 } }] : [])],
      createIndex: (spec: unknown) => opts.collections[name].createIndex(spec),
    }),
    client: {
      db: (name: string) => {
        expect(name).toBe('admin');
        return {
          aggregate: (pipeline: Op[], options?: Op) => {
            aggregateCalls.push({ pipeline, options });
            return { toArray: () => (opts.currentOp ?? (async () => []))() };
          },
        };
      },
    },
  };
  return { db, aggregateCalls };
}

function mkOrchestrator(db: unknown, reader: Partial<Record<string, unknown>> = {}): ChunkOrchestrator {
  const deps = {
    config: {
      ledger: { dryRun: false, runId: 'unit', cdUpperBoundMs: null },
      worker: { podId: 'unit-pod' },
      source: { collectionPrefix: PREFIX },
    },
    logger: pino({ level: 'silent' }),
    mongoReader: { getDatabase: () => db, ...reader },
    hashResolver: { resolveCollectionName: () => null },
  } as unknown as ChunkOrchestratorDeps;
  return new ChunkOrchestrator(deps);
}

const waitUntil = async (cond: () => Promise<boolean> | boolean, ms = 2_000): Promise<void> => {
  const until = Date.now() + ms;
  while (!(await cond())) {
    if (Date.now() > until) throw new Error('condition not met in time');
    await sleep(5);
  }
};

describe('index-build progress — $currentOp query', () => {
  it('pins readConcern local and projects the shard field', async () => {
    const { db, aggregateCalls } = fakeMongo({ collections: {}, currentOp: async () => building });
    const progress = await mkOrchestrator(db).indexBuildProgress();
    expect(aggregateCalls).toHaveLength(1);
    expect(aggregateCalls[0].options).toEqual({ readConcern: { level: 'local' } });
    expect(aggregateCalls[0].pipeline[0]).toEqual({ $currentOp: { allUsers: true } });
    expect(aggregateCalls[0].pipeline[2]).toEqual({ $project: { ns: 1, command: 1, progress: 1, msg: 1, shard: 1 } });
    expect(progress).toMatchObject({ serverOpsError: null, phases: 4, auto: null, running: false });
    expect(progress.serverOps).toEqual([expect.objectContaining({ collection: 'drill_events_probe', phase: 1, pct: 7 })]);
  });

  it('reports a failing $currentOp instead of pretending nothing is building', async () => {
    const { db } = fakeMongo({
      collections: {},
      currentOp: async () => { throw new Error('not authorized on admin to execute command { aggregate: 1 }'); },
    });
    const progress = await mkOrchestrator(db).indexBuildProgress();
    expect(progress.serverOps).toEqual([]);
    expect(progress.serverOpsError).toBe('not authorized on admin to execute command { aggregate: 1 }');
  });
});

describe('index-build progress — Build missing indexes button', () => {
  it('builds unlisted indexes, joins the in-flight one, skips the ready one', async () => {
    const built: string[] = [];
    const mk = (name: string, indexed: boolean): FakeColl => ({
      indexed,
      createIndex: async (spec) => { expect(spec).toEqual({ cd: 1, _id: 1 }); await sleep(20); built.push(name); return 'cd_1__id_1'; },
    });
    // drill_events_probe: listed (in-flight build, $currentOp shows it) → joined
    // drill_events_ready: listed, nothing in flight → skipped
    // drill_events_new:   not listed → built
    const collections = {
      drill_events_new: mk('drill_events_new', false),
      drill_events_probe: mk('drill_events_probe', true),
      drill_events_ready: mk('drill_events_ready', true),
    };
    const { db } = fakeMongo({ collections, currentOp: async () => building });
    const orch = mkOrchestrator(db);

    expect(await orch.startIndexBuilds()).toEqual({ started: true, missing: 2, inProgress: 1 });
    // a second click while running does not start a second queue
    expect((await orch.startIndexBuilds()).started).toBe(false);
    await waitUntil(async () => (await orch.indexBuildProgress()).running === false);
    const progress = await orch.indexBuildProgress();
    expect(progress).toMatchObject({ running: false, total: 2, error: null, current: null });
    expect(progress.done).toEqual(['drill_events_new', 'drill_events_probe']);
    expect(built).toEqual(['drill_events_new', 'drill_events_probe']);
  });

  it('reports "nothing to do" only when nothing is unlisted and nothing is in flight', async () => {
    const coll = (): FakeColl => ({ indexed: true, createIndex: vi.fn() });
    const { db } = fakeMongo({ collections: { drill_events_a: coll(), drill_events_b: coll() } });
    expect(await mkOrchestrator(db).startIndexBuilds()).toEqual({ started: false, missing: 0, inProgress: 0 });
  });

  it('still builds unlisted indexes when $currentOp is not permitted', async () => {
    const { db } = fakeMongo({
      collections: { drill_events_a: { indexed: false, createIndex: async () => 'cd_1__id_1' } },
      currentOp: async () => { throw new Error('not authorized'); },
    });
    const orch = mkOrchestrator(db);
    expect(await orch.startIndexBuilds()).toEqual({ started: true, missing: 1, inProgress: 0 });
    await waitUntil(async () => (await orch.indexBuildProgress()).running === false);
    expect(await orch.indexBuildProgress()).toMatchObject({ done: ['drill_events_a'], error: null, serverOpsError: 'not authorized' });
  });

  it('stops the queue on a failed build and reports which one', async () => {
    const { db } = fakeMongo({
      collections: {
        drill_events_a: { indexed: false, createIndex: async () => { throw new Error('disk full'); } },
        drill_events_b: { indexed: false, createIndex: vi.fn(async () => 'cd_1__id_1') },
      },
    });
    const orch = mkOrchestrator(db);
    await orch.startIndexBuilds();
    await waitUntil(async () => (await orch.indexBuildProgress()).running === false);
    expect(await orch.indexBuildProgress()).toMatchObject({ running: false, done: [], error: 'drill_events_a: disk full' });
  });
});

describe('index-build progress — mapping-time wait', () => {
  type Mapper = { mapCollection(c: string, at?: { position: number; of: number }): Promise<number> };

  it('names the collection while ensureIndex blocks, then clears', async () => {
    let release: () => void = () => {};
    const { db } = fakeMongo({ collections: {} });
    const orch = mkOrchestrator(db, {
      switchCollection: async () => {},
      hasRequiredIndex: async () => false,
      ensureIndex: () => new Promise<void>((r) => { release = r; }),
      getLowerBound: async () => null,
      getUpperBound: async () => null,
      hasNullCdDocuments: async () => false,
    });
    const t0 = Date.now();
    const mapping = (orch as unknown as Mapper).mapCollection('drill_events_probe', { position: 3, of: 12 });
    await waitUntil(() => orch.autoIndexState() !== null);
    const state = orch.autoIndexState()!;
    expect(state).toMatchObject({ collection: 'drill_events_probe', position: 3, of: 12 });
    expect(state.startedAtMs).toBeGreaterThanOrEqual(t0);
    expect((await orch.indexBuildProgress()).auto).toEqual(state);
    release();
    expect(await mapping).toBe(0); // empty collection: nothing mapped
    expect(orch.autoIndexState()).toBeNull();
  });

  it('clears the state when the index build fails', async () => {
    const { db } = fakeMongo({ collections: {} });
    const orch = mkOrchestrator(db, {
      switchCollection: async () => {},
      hasRequiredIndex: async () => true,
      ensureIndex: async () => { throw new Error('index build aborted'); },
    });
    await expect((orch as unknown as Mapper).mapCollection('drill_events_probe')).rejects.toThrow('index build aborted');
    expect(orch.autoIndexState()).toBeNull();
  });

  it('outside the map pass the position is unknown, not invented', async () => {
    let release: () => void = () => {};
    const { db } = fakeMongo({ collections: {} });
    const orch = mkOrchestrator(db, {
      switchCollection: async () => {},
      hasRequiredIndex: async () => true,
      ensureIndex: () => new Promise<void>((r) => { release = r; }),
      getLowerBound: async () => null,
      getUpperBound: async () => null,
      hasNullCdDocuments: async () => false,
    });
    const mapping = (orch as unknown as Mapper).mapCollection('drill_events_probe');
    await waitUntil(() => orch.autoIndexState() !== null);
    expect(orch.autoIndexState()).toMatchObject({ position: null, of: null });
    release();
    await mapping;
  });
});
