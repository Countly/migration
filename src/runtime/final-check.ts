/**
 * Final check: the whole sign-off, interpreted.
 *
 * Operators kept having to understand four audit buckets, tee semantics and
 * DLQ states to answer the only question they actually have at the end of a
 * migration: "is it safe to decommission the old cluster?" This module runs
 * every validation the tool has — chunk states, DLQ, the full source recount
 * with cd-checksum fingerprints, sampled content comparison — applies the
 * interpretation rules itself (including the tee cutover: windows past the
 * cutover diverge BY DESIGN and must not read as data loss), and emits one
 * verdict in plain sentences:
 *
 *   PASS             — safe to decommission.
 *   PASS WITH NOTES  — safe, but read the amber lines first (waived DLQ,
 *                      source retention drift, excluded post-cutover tail).
 *   FAIL             — do not decommission; each red line names the action.
 */

import type { Logger } from 'pino';
import type { Config } from '../config/schema.ts';
import type { HashResolver } from '../transform/hash-resolver.ts';
import type { LedgerStore } from '../state/ledger-store.ts';
import type { DlqStore } from '../state/dlq-store.ts';
import { rebuildLedger, newRebuildProgress, type RebuildProgress } from './ledger-rebuild.ts';

export interface FinalCheckResult {
  status: 'not_run' | 'running' | 'completed' | 'failed';
  /** quick = ledger-verify + sampled source checks (minutes); deep = full source recount + checksums (the pre-teardown gate). */
  mode: 'quick' | 'deep' | null;
  verdict: 'PASS' | 'PASS_WITH_NOTES' | 'FAIL' | null;
  /** One sentence answering "can I decommission the old cluster?" */
  headline: string | null;
  /** Green lines — what was verified and held. */
  passes: string[];
  /** Amber lines — true, explained, and safe; read before sign-off. */
  notes: string[];
  /** Red lines — each names the problem AND the action. */
  problems: string[];
  /** The cutover used to scope the source recount (null = full range). */
  cutoverMs: number | null;
  phase: string;
  /** Drill-down: the raw source-audit report backing the verdict. */
  audit: RebuildProgress | null;
  content: { sampled: number; matched: number; missing: number; different: number } | null;
  error: string | null;
  startedAt: number | null;
  finishedAt: number | null;
}

export function newFinalCheckResult(): FinalCheckResult {
  return {
    status: 'not_run', mode: null, verdict: null, headline: null,
    passes: [], notes: [], problems: [],
    cutoverMs: null, phase: '', audit: null, content: null,
    error: null, startedAt: null, finishedAt: null,
  };
}

const fmt = (n: number): string => n.toLocaleString('en-US');
const iso = (ms: number): string => new Date(ms).toISOString().slice(0, 16).replace('T', ' ') + ' UTC';

interface ContentAuditRunner {
  contentAudit(samplesPerCollection?: number, upToMs?: number | null, totalBudget?: number | null): Promise<{
    sampled: number; matched: number; missing: number; different: number;
    mismatches: Array<{ _id: string; collection: string; kind: string; fields?: string[] }>;
  }>;
  verifyMigration(upToMs?: number | null): Promise<Record<string, unknown>>;
  snapshotSourceState(): Promise<Array<{ collection: string; maxCd: number; n: number; cdSum: number }>>;
}

/**
 * Collections whose source MUTATED between two snapshots — a collection that
 * appeared OR disappeared, a higher max cd, or ANY change in the exact count
 * or the order-free cd checksum (which catches backdated inserts, deletes,
 * and insert+delete pairs that leave the count unchanged). Both directions
 * are compared: a collection dropped mid-check must void the authorization,
 * or its (possibly empty) recount would stand unchallenged. Exported for
 * tests.
 */
export function sourceAdvanced(
  before: Array<{ collection: string; maxCd: number; n: number; cdSum: number }>,
  after: Array<{ collection: string; maxCd: number; n: number; cdSum: number }>,
): string[] {
  const b = new Map(before.map((s) => [s.collection, s]));
  const seen = new Set(after.map((s) => s.collection));
  const grew: string[] = [];
  for (const a of after) {
    const prev = b.get(a.collection);
    if (!prev || a.maxCd > prev.maxCd || a.n !== prev.n || a.cdSum !== prev.cdSum) grew.push(a.collection);
  }
  for (const prev of before) {
    if (!seen.has(prev.collection)) grew.push(prev.collection);
  }
  return grew;
}

export async function runFinalCheck(
  deps: {
    config: Config;
    logger: Logger;
    ledger: LedgerStore;
    dlq: DlqStore;
    hashResolver: HashResolver;
    orchestrator: ContentAuditRunner;
  },
  out: FinalCheckResult,
  opts: { cutoverMs: number | null; samples: number; deep?: boolean; acceptUnscoped?: boolean; leaseLost?: () => boolean },
): Promise<void> {
  const { config, ledger, dlq, hashResolver } = deps;
  const logger = deps.logger.child({ component: 'FinalCheck' });
  const runId = config.ledger.runId;
  const deep = opts.deep === true;

  Object.assign(out, newFinalCheckResult(), { status: 'running', mode: deep ? 'deep' : 'quick', startedAt: Date.now(), phase: 'starting' });
  try {
    // Bracket the whole check with a durable state marker: a deep run takes
    // hours, and a retry/top-up landing mid-check invalidates everything the
    // earlier layers measured — a stale PASS must be impossible.
    const fpBefore = await ledger.runFingerprint(runId);
    // ── Cutover: explicit param > stored bound > env bound > none ─────────
    // fail CLOSED: if the bound cannot be read, the check errors out rather
    // than silently auditing a different range
    const stored = await ledger.getStoredBound(runId);
    const cutoverMs = opts.cutoverMs ?? stored ?? config.ledger.cdUpperBoundMs ?? null;
    out.cutoverMs = cutoverMs;

    // ── 1. Chunk ledger states ─────────────────────────────────────────────
    out.phase = 'checking chunk states';
    const counts = await ledger.statusCounts(runId);
    const done = counts.done ?? 0;
    const failed = counts.failed ?? 0;
    const superseded = counts.superseded ?? 0;
    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    const notDone = total - done - superseded - failed;
    if (failed > 0) {
      out.problems.push(`${fmt(failed)} chunk(s) FAILED — click "Retry failed chunks" (or POST /control/retry-failed), wait for them to finish, then run this check again.`);
    }
    if (notDone > 0) {
      out.problems.push(`${fmt(notDone)} chunk(s) are not migrated yet — the run is not complete. Let it finish (or press Start/Resume), then run this check again.`);
    }
    if (failed === 0 && notDone === 0 && total > 0) {
      out.passes.push(`All ${fmt(done)} chunks migrated and verified (per-chunk count + id checks passed before every attach).`);
    }
    if (total === 0) {
      out.problems.push('The ledger holds no chunks — nothing has been migrated under this run id.');
    }

    // ── 2. DLQ ─────────────────────────────────────────────────────────────
    out.phase = 'checking dead-letter queue';
    // fail CLOSED: an unreadable DLQ is indistinguishable from an empty one —
    // a thrown error here fails the whole check as a tooling error instead of
    // authorizing teardown without DLQ evidence
    const dlqCounts = await dlq.countByStatus(runId);
    const dlqPending = dlqCounts.pending ?? 0;
    const dlqWaived = dlqCounts.waived ?? 0;
    if (dlqPending > 0) {
      const top = await dlq.topErrors(runId, 3).catch(() => []);
      const reasons = top.map((t) => `${t.error} ×${fmt(t.n)}`).join(', ');
      // unresolved = undecided: these docs are NOT in ClickHouse and nobody
      // has accepted that yet — a sign-off cannot authorize teardown over
      // an open decision, so this is a problem, not a note
      out.problems.push(`${fmt(dlqPending)} skipped docs wait UNRESOLVED in the DLQ (${reasons}) — they are NOT in ClickHouse. Review a few in the DLQ panel, then Waive them (accepted as unmigratable) or Replay after a fix, and run this check again.`);
    }
    if (dlqWaived > 0) {
      out.notes.push(`${fmt(dlqWaived)} docs were waived earlier — deliberately accepted as not migrated (their raw copies stay in the DLQ collection as the record).`);
    }
    if (dlqPending === 0 && dlqWaived === 0) out.passes.push('Dead-letter queue is empty — no document was skipped.');

    // ── 3a. Target vs the run's own ledger — BOTH tiers ────────────────────
    // Catches everything that happened AFTER reading: lost partitions, rows
    // deleted from the live table, and exact duplicate attribution (which
    // the deep recount alone would misread as retention drift). Cutover-
    // aware: post-cutover windows mix native rows and are skipped here.
    {
      out.phase = 'verifying the target against the run ledger';
      const verify = await deps.orchestrator.verifyMigration(cutoverMs);
      const vMism = (verify.mismatches as Array<Record<string, unknown>> | undefined) ?? [];
      const vDup = Number((verify as Record<string, unknown>).migrationDuplicates ?? 0);
      if (verify.ok !== true) {
        if (vMism.length > 0) {
          out.problems.push(`${fmt(vMism.length)} chunk window(s) hold a different live row count than the ledger recorded — rows were lost or duplicated after migration. Run "Retry failed chunks" after a rebuild, or escalate; do NOT decommission the old cluster.`);
        }
        if (vDup > 0) {
          out.problems.push(`${fmt(vDup)} document(s) exist more than once below the migration boundary — a migration-side duplicate class; escalate before decommissioning.`);
        }
        if (vMism.length === 0 && vDup === 0) {
          out.problems.push('Ledger verification reported a failure — inspect GET /api/verify before decommissioning.');
        }
      } else {
        out.passes.push('Target verified against the run ledger: every migrated chunk window holds exactly the recorded row count, with no migration-side duplicates.');
      }
      if (!deep) {
        out.notes.push(`Quick mode: the ledger itself was not re-proven against the source. Per-chunk verification at attach time plus the random content samples below cover that class probabilistically — run the DEEP check ({"deep": true}, or the checkbox in the dashboard) before deleting the source if you want the full recount + checksum fingerprints.`);
      }
    }

    // deep with NO cutover claims a FROZEN source — prove it by bracketing:
    // the recount snapshots each collection's high cd at ITS start and can
    // never see documents accepted afterwards, so any source advance across
    // the deep phase voids the authorization. (With a cutover the recount is
    // clamped and post-cutover writes are excluded by construction.)
    let sourceBefore: Array<{ collection: string; maxCd: number; n: number; cdSum: number }> | null = null;
    if (deep && cutoverMs === null) {
      out.phase = 'snapshotting the source (frozen-source proof)';
      sourceBefore = await deps.orchestrator.snapshotSourceState();
    }

    // ── 3b. DEEP tier: full source recount + cd-checksum fingerprint ──────
    const audit = newRebuildProgress();
    if (deep) {
    out.phase = 'recounting every window against the source';
    out.audit = audit;
    await rebuildLedger({ config, logger, ledger, dlq, hashResolver, progress: audit, checkOnly: true, upToMs: cutoverMs });
    const windows = audit.summary.reduce((a, s) => a + s.chunks, 0);
    // a window with live === 0 is classified 'pending' by the audit, not
    // mismatched — on a run that claims completion it means the WHOLE
    // window is missing from the target (e.g. rows removed after attach)
    const scopedPendingWindows = audit.summary.filter((s) => s.scoped || audit.summary.length === 1)
      .reduce((a, s) => a + s.pending, 0);
    const unscopedWindows = audit.summary.length > 1
      ? audit.summary.filter((s) => !s.scoped).reduce((a, s) => a + s.chunks, 0)
      : 0;
    if (scopedPendingWindows > 0) {
      out.problems.push(`${fmt(scopedPendingWindows)} window(s) hold ZERO rows in ClickHouse for data the source has — whole windows are missing from the target. Rebuild the ledger from data, Retry failed chunks, and run this check again; do NOT decommission the old cluster.`);
    }
    if (unscopedWindows > 0 && opts.acceptUnscoped !== true) {
      // a teardown authorization must not stand on windows that CANNOT be
      // recounted — the operator either keeps the source or accepts the
      // reduced evidence explicitly
      out.problems.push(`${fmt(unscopedWindows)} window(s) belong to collection(s) without their own (a,e,n) scope and CANNOT be recounted against the source. Their evidence is per-chunk attach verification, sampled id coverage and the content samples — if that is acceptable, re-run with {"acceptUnscoped": true}; otherwise keep the source.`);
    } else if (unscopedWindows > 0) {
      out.notes.push(`${fmt(unscopedWindows)} window(s) in unscopable collection(s) were EXPLICITLY ACCEPTED on reduced evidence (attach-time verification + sampled id coverage + content samples) — recorded here for the sign-off trail.`);
    }
    if (audit.mismatchedWindows.length > 0) {
      out.problems.push(`${fmt(audit.mismatchedWindows.length)} window(s) hold FEWER docs in ClickHouse than the source — data is missing from the target. Click "Retry failed chunks" after a rebuild, or escalate; do NOT decommission the old cluster.`);
    }
    if ((audit.idCoverageMissing ?? []).length > 0) {
      const idMissingN = (audit.idCoverageMissing ?? []).reduce((a, w) => a + w.missing, 0);
      out.problems.push(`${fmt((audit.idCoverageMissing ?? []).length)} window(s) hold the right COUNT and checksum but ${fmt(idMissingN)} sampled document identit${idMissingN === 1 ? 'y is' : 'ies are'} MISSING live — documents were swapped for others; escalate; do NOT decommission the old cluster.`);
    }
    if (audit.checksumMismatchWindows.length > 0) {
      out.problems.push(`${fmt(audit.checksumMismatchWindows.length)} window(s) hold the right COUNT of the WRONG documents (checksum fingerprint differs) — escalate; do NOT decommission the old cluster.`);
    }
    if ((audit.driftSubsetMissing ?? []).length > 0) {
      const missingN = (audit.driftSubsetMissing ?? []).reduce((a, w) => a + w.missing, 0);
      out.problems.push(`${fmt((audit.driftSubsetMissing ?? []).length)} retention-drift window(s) are MISSING current source docs behind their surplus counts (${fmt(missingN)} sampled ids not found live) — surplus rows were masking gaps; do NOT decommission the old cluster.`);
    }
    if (audit.deletionDriftWindows.length > 0 && (audit.driftSubsetMissing ?? []).length === 0) {
      const partial = audit.driftWindowsPartial ?? 0;
      out.notes.push(`${fmt(audit.deletionDriftWindows.length)} window(s) now hold MORE docs in ClickHouse than the source — the source shrank after migration (retention TTL / deletions). Every drift window was id-checked (${fmt(audit.driftWindowsChecked ?? 0)} checked${partial > 0 ? `; ${fmt(partial)} larger than the 5,000-id sample were checked on that sample — statistical, not exhaustive, evidence` : ' exhaustively'}) and the sampled source ids were all found live.`);
    }
    if (audit.mismatchedWindows.length === 0 && audit.checksumMismatchWindows.length === 0 && scopedPendingWindows === 0 && (audit.idCoverageMissing ?? []).length === 0) {
      out.passes.push(`Recounted ${fmt(windows)} window(s) directly against the source: every count matches, every checksum fingerprint matches, and sampled identity coverage is complete.`);
    }
    if (cutoverMs !== null) {
      const excluded = audit.excludedBeyondCutover ?? 0;
      out.notes.push(`Source docs after the cutover (${iso(cutoverMs)}) were excluded from the comparison${excluded > 0 ? ` (${fmt(excluded)} docs)` : ''} — after that moment the old side receives mirrored/live traffic that was never meant to be migrated, so divergence there is expected and is NOT data loss.`);
    }
    // Coverage reconciliation: every collection the ledger migrated must
    // have been found and audited in the source — a dropped/renamed source
    // collection would otherwise silently vanish from the recount and the
    // gate could PASS without examining it.
    out.phase = 'reconciling audit coverage against the ledger';
    const audited = new Set(audit.summary.map((s) => s.collection));
    const ledgerColls = (await ledger.summarize(runId)).perCollection
      .filter((c) => (c.byStatus.done ?? 0) > 0)
      .map((c) => c.collection)
      .filter((name) => {
        const defaults = hashResolver.resolveCollectionName(name, config.source.collectionPrefix);
        return !(defaults && (defaults.e === '[CLY]_apm_device' || defaults.e === '[CLY]_apm_network'));
      });
    const unaudited = ledgerColls.filter((name) => !audited.has(name));
    if (unaudited.length > 0) {
      out.problems.push(`${fmt(unaudited.length)} collection(s) hold completed chunks but were NOT found in the source during the recount (dropped or renamed? e.g. ${unaudited.slice(0, 3).join(', ')}) — their data cannot be re-proven against the source; do NOT decommission until this is explained.`);
    }
    }

    // ── 4. Sampled content comparison ──────────────────────────────────────
    out.phase = 'comparing sampled documents field-by-field';
    const content = await deps.orchestrator.contentAudit(opts.samples, cutoverMs, Math.max(2_000, opts.samples));
    out.content = { sampled: content.sampled, matched: content.matched, missing: content.missing, different: content.different };
    if (content.missing > 0 || content.different > 0) {
      out.problems.push(`Content sampling found ${fmt(content.missing)} missing and ${fmt(content.different)} differing doc(s) out of ${fmt(content.sampled)} sampled — the migrated content does not match the source; escalate before decommissioning.`);
    } else if (content.sampled > 0) {
      // say exactly what was compared: scalar columns exactly, JSON columns
      // by key set (ClickHouse's JSON type normalizes value encodings —
      // value-level fidelity is pinned by the transform's differential
      // harness, not by this sampler)
      out.passes.push(`Sampled ${fmt(content.sampled)} random docs against the source — every scalar field exact, every JSON field's key set matched.`);
    }

    // ── Frozen-source proof (unbounded deep): the closing bracket ─────────
    if (sourceBefore !== null) {
      out.phase = 'confirming the source stayed frozen during the check';
      const grew = sourceAdvanced(sourceBefore, await deps.orchestrator.snapshotSourceState());
      if (grew.length > 0) {
        out.problems.push(`The SOURCE MUTATED while the deep check ran (${grew.join(', ')}) — it is not frozen (ingestion, retention or repairs are still writing), so an unbounded recount cannot authorize teardown. Freeze the old cluster (or pass a cutoverMs boundary) and run the deep check again.`);
      } else {
        out.passes.push('Frozen-source bracket held: exact per-collection count and cd checksum unchanged across the whole deep check.');
      }
    }

    // ── Staleness: did the run's chunk state move while we measured? ──────
    out.phase = 'confirming the run state did not change during the check';
    const fpAfter = await ledger.runFingerprint(runId);
    if (fpAfter !== fpBefore) {
      out.problems.push('The run\'s chunk state CHANGED while this check ran (a retry, top-up or remap landed mid-check) — every layer above measured a moving target. Let the run settle, then run this check again.');
    }

    // ── Reservation: did this pod keep the cluster-wide lease throughout? ─
    // A lost lease means another maintenance operation (a dedupe EXECUTE
    // deletes target rows the ledger fingerprint cannot see) may have run
    // under this check's reads — no verdict computed from them may stand.
    if (opts.leaseLost?.()) {
      out.problems.push('This check LOST the cluster-wide maintenance reservation while running (the pod stalled past the lease expiry) — another maintenance operation may have changed the target under its reads. Run the check again.');
    }

    // ── Verdict ────────────────────────────────────────────────────────────
    out.verdict = out.problems.length > 0 ? 'FAIL' : out.notes.length > 0 ? 'PASS_WITH_NOTES' : 'PASS';
    // Only the DEEP check may authorize teardown — quick mode deliberately
    // skips the source recount, so a clean quick result is routine
    // confidence, never a license to delete the source.
    out.headline = out.verdict === 'FAIL'
      ? `DO NOT decommission the old cluster yet — ${out.problems.length} problem(s) below need action first.`
      : !deep
        ? 'No problems found at quick depth — routine confidence only. Decommissioning the source still requires the DEEP check (the pre-teardown gate).'
        : out.verdict === 'PASS_WITH_NOTES'
          ? 'Safe to decommission the old cluster after reading the notes below.'
          : 'ClickHouse verifiably holds everything the source holds — safe to decommission the old cluster.';
    out.status = 'completed';
    out.phase = 'done';
    out.finishedAt = Date.now();
    logger.info({ verdict: out.verdict, problems: out.problems.length, notes: out.notes.length }, 'Final check complete');
  } catch (err) {
    out.status = 'failed';
    out.error = (err as Error).message;
    out.phase = 'failed';
    out.finishedAt = Date.now();
    logger.error({ err }, 'Final check failed to complete');
  }
}

/** Plain-text rendering for SSH-only operation (GET /final-check.txt). */
export function renderFinalCheckText(fc: FinalCheckResult, runId: string): string {
  const lines: string[] = [`FINAL CHECK - run ${runId}`];
  if (fc.status === 'not_run') {
    lines.push('Not run yet. Start it with: curl -X POST localhost:PORT/control/final-check');
  } else if (fc.status === 'running') {
    const a = fc.audit;
    lines.push(`RUNNING - ${fc.phase}${a && a.collectionsTotal > 0 ? ` (${a.collectionsDone}/${a.collectionsTotal} collections)` : ''}`);
  } else if (fc.status === 'failed') {
    lines.push(`CHECK FAILED TO COMPLETE: ${fc.error} - fix and re-run; this is a tooling error, not a data verdict.`);
  } else {
    const badge = fc.verdict === 'PASS' ? 'PASS' : fc.verdict === 'PASS_WITH_NOTES' ? 'PASS WITH NOTES' : 'FAIL';
    lines.push(`Verdict: ${badge} (${fc.mode === 'deep' ? 'deep: full source recount' : 'quick: ledger verify + samples'}) - ${fc.headline}`);
    for (const p of fc.problems) lines.push(`  [X] ${p}`);
    for (const n of fc.notes) lines.push(`  [!] ${n}`);
    for (const g of fc.passes) lines.push(`  [ok] ${g}`);
    if (fc.cutoverMs !== null) lines.push(`  cutover used: ${new Date(fc.cutoverMs).toISOString()}`);
    if (fc.finishedAt) lines.push(`  finished: ${new Date(fc.finishedAt).toISOString()}`);
  }
  return lines.join('\n') + '\n';
}
