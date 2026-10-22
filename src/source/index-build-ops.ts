/**
 * Live {cd:1,_id:1} index-build progress from `$currentOp` output.
 *
 * A createIndex shows up as TWO ops (verified on MongoDB 8.0): the
 * client connection waiting on the build (no progress fields) and the
 * builder thread `IndexBuildsCoordinatorMongod-N` carrying
 * `progress {done,total}` and a `msg` naming the phase. The percentage
 * restarts per phase — "scanning collection" (docs), then "inserting keys
 * from external sorter into index" (keys), then a short "draining writes
 * received during build" — so a bare percentage jumps 100% → 0% mid-build;
 * the phase travels with it.
 *
 * Between the two counted phases the builder keeps saying "scanning
 * collection" WITHOUT counters while it sorts the keys: 30–45% of the whole
 * build in every capture (replica-set primary and secondary), and
 * never before the scan's counters appear. That state is its own step, so
 * the bar does not sit on "scanning documents" long after the scan ended.
 *
 * Through mongos every shard reports its own builder op (tagged `shard`),
 * with `total` = that shard's documents; the run waits for all of them, so
 * the least advanced shard is reported along with the shard count. On a
 * secondary the builder op carries no `command.$db`, hence the `ns` fallback.
 * Pure; the orchestrator runs the aggregation.
 */

export interface IndexBuildOp {
  collection: string;
  /** 1 = scanning documents, 2 = sorting keys, 3 = writing index keys, 4 = draining writes, null = unknown */
  phase: 1 | 2 | 3 | 4 | null;
  phaseLabel: string;
  done: number | null;
  total: number | null;
  /** percentage of the CURRENT phase */
  pct: number | null;
  /** the raw server message — kept for the unknown-phase case */
  msg: string;
  /** shard of the reported op (mongos only), else null */
  shard: string | null;
  /** distinct shards building this index (0 when not behind mongos) */
  shards: number;
}

export const INDEX_BUILD_PHASES = 4;

type Phase = { phase: 1 | 2 | 3 | 4; label: string };
const PHASES: Array<Phase & { re: RegExp }> = [
  { re: /scanning collection/i, phase: 1, label: 'scanning documents' },
  { re: /external sorter|inserting keys/i, phase: 3, label: 'writing index keys' },
  { re: /draining writes/i, phase: 4, label: 'applying writes made during the build' },
];
const SORTING: Phase = { phase: 2, label: 'sorting index keys (scan complete)' };

function isCdIdIndex(spec: unknown): boolean {
  const key = (spec as { key?: Record<string, unknown> } | null)?.key;
  if (!key) return false;
  const names = Object.keys(key);
  return names.length === 2 && names[0] === 'cd' && names[1] === '_id';
}

function parseOne(op: Record<string, unknown>): IndexBuildOp {
  const command = op.command as Record<string, unknown>;
  const msg = String(op.msg ?? '');
  const progress = op.progress as { done?: number; total?: number } | undefined;
  let hit: Phase | undefined = PHASES.find((p) => p.re.test(msg));
  if (hit?.phase === 1 && progress === undefined) hit = SORTING;
  const done = typeof progress?.done === 'number' ? progress.done : null;
  const total = typeof progress?.total === 'number' && progress.total > 0 ? progress.total : null;
  let phaseLabel = hit?.label ?? '';
  if (!hit) {
    // an unknown phase: show the server's own words; an op with no msg is a
    // client waiting on a build whose builder thread is not visible from here
    phaseLabel = msg.replace(/^Index Build:\s*/i, '').replace(/:\s*\d+\/\d+\s+\d+%$/, '').trim() || 'in progress';
  }
  return {
    collection: String(command.createIndexes),
    phase: hit?.phase ?? null,
    phaseLabel,
    done,
    total,
    pct: done !== null && total !== null ? Math.min(100, Math.round((done / total) * 100)) : null,
    msg,
    shard: typeof op.shard === 'string' ? op.shard : null,
    shards: 0,
  };
}

/**
 * Least-advanced first: the slowest member/shard is what the run waits on.
 * Within one phase an op without counters (seen at phase boundaries) never
 * displaces one that has them.
 */
function lessAdvanced(a: IndexBuildOp, b: IndexBuildOp): boolean {
  const pa = a.phase ?? 0;
  const pb = b.phase ?? 0;
  if (pa !== pb) return pa < pb;
  if (a.pct === null) return false;
  if (b.pct === null) return true;
  return a.pct < b.pct;
}

/**
 * One entry per collection with an in-flight {cd:1,_id:1} build in `dbName`.
 * Builder ops win over waiter ops; among several builders (shards) the least
 * advanced one is reported.
 */
export function parseIndexBuildOps(ops: Array<Record<string, unknown>>, dbName: string): IndexBuildOp[] {
  const byColl = new Map<string, { op: IndexBuildOp; builder: boolean; shards: Set<string> }>();
  for (const op of ops) {
    const command = op.command as Record<string, unknown> | undefined;
    if (!command || typeof command.createIndexes !== 'string') continue;
    const opDb = typeof command.$db === 'string' ? command.$db : String(op.ns ?? '').split('.')[0];
    if (opDb !== dbName) continue;
    if (!Array.isArray(command.indexes) || !command.indexes.some(isCdIdIndex)) continue;
    const parsed = parseOne(op);
    const builder = op.progress !== undefined || /^Index Build/i.test(parsed.msg);
    let entry = byColl.get(parsed.collection);
    if (!entry) {
      entry = { op: parsed, builder, shards: new Set() };
      byColl.set(parsed.collection, entry);
    } else if ((builder && !entry.builder) || (builder === entry.builder && lessAdvanced(parsed, entry.op))) {
      entry.op = parsed;
      entry.builder = builder;
    }
    if (parsed.shard !== null) entry.shards.add(parsed.shard);
  }
  return [...byColl.values()].map((v) => ({ ...v.op, shards: v.shards.size }));
}
