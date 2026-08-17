/**
 * Countly-branded operator console for the ledger engine.
 *
 * Three tabs:
 *  - Overview: live state (counters, chunk map, failed chunks, DLQ, coercions)
 *    plus one-click actions with receipts.
 *  - Guide: the migration runbook as a guided checklist — automated phases
 *    report their own status (preflight, index, dry run, progress, sign-off
 *    gates); manual phases are persistent checkboxes (localStorage) so a
 *    self-hosted customer can walk the whole cutover themselves.
 *  - Help: the incident-response scenarios from docs/RUNBOOK.md, each with
 *    the relevant action inline.
 *
 * Data source: the chunk ledger + DLQ (MongoDB) + in-process engine stats.
 * No Redis. Brand tokens sampled from countly.com.
 */

import type { FastifyInstance } from 'fastify';
import type { ChunkOrchestrator } from '../runtime/chunk-orchestrator.ts';
import type { LedgerStore } from '../state/ledger-store.ts';
import type { DlqStore } from '../state/dlq-store.ts';
import type { Config } from '../config/schema.ts';

export interface LedgerVizDeps {
  orchestrator: ChunkOrchestrator;
  ledger: LedgerStore;
  dlq: DlqStore;
  config: Config;
}

export function registerLedgerVizRoutes(app: FastifyInstance, deps: LedgerVizDeps): void {
  const runId = () => deps.config.ledger.dryRun ? `${deps.config.ledger.runId}-dry` : deps.config.ledger.runId;

  app.get('/api/chunks', async () => {
    const chunks = await deps.ledger.listAll(runId());
    return { runId: runId(), chunks };
  });

  app.get('/api/dlq', async () => {
    const pending = await deps.dlq.listPending(runId(), 20);
    return {
      byStatus: await deps.dlq.countByStatus(runId()),
      topErrors: await deps.dlq.topErrors(runId(), 8),
      samples: pending.map((p) => ({
        source_id: p.source_id,
        collection: p.collection,
        reason: p.reason,
        error: p.error,
        raw_doc: JSON.stringify(p.raw_doc).slice(0, 2_000),
      })),
    };
  });

  app.get('/api/preflight', async () => deps.orchestrator.preflight());
  app.get('/api/verify', async () => deps.orchestrator.verifyMigration());

  app.get('/viz', async (_req, reply) => {
    reply.type('text/html').send(PAGE);
  });
}

const PAGE = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Countly Data Migration</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@500;700;800&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root {
    --green: #21B566; --green-soft: #E4F6EC; --ink: #24292E; --ink-2: #5D6C7B;
    --muted: #81868D; --line: #DDDFE5; --bg: #F4F7F8; --card: #FFFFFF;
    --amber: #FAA262; --red: #FD8B89; --blue: #3898EC;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--ink);
    font-family: Inter, -apple-system, Arial, sans-serif; font-size: 14px; line-height: 1.5; }
  h1, h2, .stat b { font-family: 'Plus Jakarta Sans', Inter, Arial, sans-serif; }
  header { background: var(--card); border-bottom: 1px solid var(--line);
    padding: 14px 28px; display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }
  .logo { display: flex; align-items: center; gap: 9px; font-family: 'Plus Jakarta Sans'; font-weight: 800; font-size: 20px; letter-spacing: -0.02em; }
  .logo .mark { width: 22px; height: 22px; border-radius: 6px; background: var(--green); position: relative; }
  .logo .mark::after { content: ''; position: absolute; inset: 6px; border-radius: 3px; background: #fff; }
  .divider { width: 1px; height: 22px; background: var(--line); }
  .subtitle { color: var(--ink-2); font-weight: 500; }
  .badges { margin-left: auto; display: flex; gap: 8px; }
  .badge { font-size: 11px; font-weight: 600; letter-spacing: 0.04em; text-transform: uppercase;
    padding: 4px 10px; border-radius: 999px; background: var(--green-soft); color: #157A45; }
  .badge.warn { background: #FDEEDD; color: #A05A16; }
  .badge.grey { background: var(--bg); color: var(--ink-2); border: 1px solid var(--line); }
  nav.tabs { background: var(--card); border-bottom: 1px solid var(--line); padding: 0 28px; display: flex; gap: 4px; }
  .tab { padding: 11px 18px; font-size: 13.5px; font-weight: 600; color: var(--ink-2); cursor: pointer;
    border: none; background: none; border-bottom: 2px solid transparent; font-family: Inter; }
  .tab.active { color: var(--green); border-bottom-color: var(--green); }
  main { max-width: 1080px; margin: 0 auto; padding: 24px 28px 60px; }
  .pane { display: none; } .pane.active { display: block; }
  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .stat { background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 14px 16px; }
  .stat small { display: block; color: var(--muted); font-size: 11px; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; margin-bottom: 4px; }
  .stat b { font-size: 22px; font-weight: 700; font-variant-numeric: tabular-nums; }
  .stat b.green { color: var(--green); }
  .card { background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 18px 20px; margin-bottom: 16px; }
  .card h2 { margin: 0 0 12px; font-size: 15px; font-weight: 700; }
  .hint { color: var(--muted); font-weight: 400; font-size: 12px; }
  .collection { margin-bottom: 14px; } .collection:last-child { margin-bottom: 0; }
  .coll-head { display: flex; justify-content: space-between; font-size: 13px; margin-bottom: 6px; }
  .coll-head .name { font-weight: 600; font-family: ui-monospace, Menlo, monospace; font-size: 12px; }
  .coll-head .pct { color: var(--ink-2); font-variant-numeric: tabular-nums; }
  .bar { height: 8px; background: var(--bg); border-radius: 999px; overflow: hidden; display: flex; }
  .bar i { display: block; height: 100%; }
  .bar .done { background: var(--green); } .bar .active { background: var(--blue); animation: pulse 1.2s ease-in-out infinite; }
  .bar .failed { background: var(--red); }
  @keyframes pulse { 50% { opacity: 0.55; } }
  .grid { display: flex; flex-wrap: wrap; gap: 4px; }
  .cell { width: 16px; height: 16px; border-radius: 4px; background: var(--line); cursor: default; }
  .cell.done { background: var(--green); }
  .cell.in_progress { background: var(--blue); animation: pulse 1.2s ease-in-out infinite; }
  .cell.written, .cell.attaching { background: var(--amber); }
  .cell.failed { background: var(--red); }
  .cell.superseded { background: repeating-linear-gradient(45deg, var(--line), var(--line) 3px, transparent 3px, transparent 6px); }
  .legend { display: flex; gap: 16px; margin-top: 12px; color: var(--ink-2); font-size: 12px; flex-wrap: wrap; }
  .legend span { display: inline-flex; align-items: center; gap: 6px; }
  .legend i { width: 10px; height: 10px; border-radius: 3px; display: inline-block; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; padding: 6px 12px 6px 0; border-bottom: 1px solid var(--line); }
  td { padding: 8px 12px 8px 0; border-bottom: 1px solid var(--bg); font-variant-numeric: tabular-nums; vertical-align: top; }
  td.err { color: #C0392B; font-family: ui-monospace, Menlo, monospace; font-size: 12px; word-break: break-word; }
  .empty { color: var(--muted); padding: 8px 0; }
  .controls { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 20px; }
  .btn { font-family: Inter; font-size: 13px; font-weight: 600; cursor: pointer;
    padding: 8px 16px; border-radius: 8px; border: 1px solid var(--line);
    background: var(--card); color: var(--ink); transition: all 0.15s; }
  .btn:hover { border-color: var(--green); color: var(--green); }
  .btn.primary { background: var(--green); border-color: var(--green); color: #fff; }
  .btn.primary:hover { opacity: 0.9; color: #fff; }
  .btn.armed { background: #C0392B; border-color: #C0392B; color: #fff; }
  .btn:disabled { opacity: 0.45; cursor: default; }
  .btn-note { color: var(--muted); font-size: 12px; align-self: center; }
  .pill { display: inline-block; font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 999px; margin-right: 6px; }
  .pill.pending { background: #FDEEDD; color: #A05A16; }
  .pill.resolved { background: var(--green-soft); color: #157A45; }
  .pill.waived { background: var(--bg); color: var(--ink-2); border: 1px solid var(--line); }
  details.dlq-sample { margin: 6px 0; font-size: 12px; }
  details.dlq-sample summary { cursor: pointer; font-family: ui-monospace, Menlo, monospace; color: var(--ink-2); }
  details.dlq-sample pre { background: var(--bg); border: 1px solid var(--line); border-radius: 6px; padding: 10px; overflow-x: auto; font-size: 11px; max-height: 200px; }
  #toast { position: fixed; bottom: 20px; right: 20px; background: var(--ink); color: #fff;
    padding: 10px 18px; border-radius: 8px; font-size: 13px; opacity: 0; transition: opacity 0.3s; pointer-events: none; max-width: 480px; z-index: 10; }
  #toast.show { opacity: 1; }
  .check { display: flex; gap: 10px; padding: 8px 0; border-bottom: 1px solid var(--bg); font-size: 13px; align-items: baseline; }
  .check:last-child { border-bottom: none; }
  .check .ic { width: 18px; flex-shrink: 0; text-align: center; }
  .check .lbl { font-weight: 600; min-width: 240px; }
  .check .det { color: var(--ink-2); }
  .phase { border: 1px solid var(--line); border-radius: 10px; margin-bottom: 12px; background: var(--card); }
  .phase summary { cursor: pointer; padding: 14px 18px; font-weight: 700; font-size: 14px;
    font-family: 'Plus Jakarta Sans'; display: flex; align-items: center; gap: 10px; list-style: none; }
  .phase summary::-webkit-details-marker { display: none; }
  .phase .num { width: 24px; height: 24px; border-radius: 50%; background: var(--bg); color: var(--ink-2);
    font-size: 12px; display: inline-flex; align-items: center; justify-content: center; font-family: ui-monospace, Menlo, monospace; flex-shrink: 0; }
  .phase.done-phase .num { background: var(--green); color: #fff; }
  .phase .ph-status { margin-left: auto; font-size: 11px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }
  .phase .body { padding: 0 18px 16px 52px; color: var(--ink-2); font-size: 13px; }
  .phase .body p { margin: 6px 0; }
  .phase label.step { display: flex; gap: 8px; align-items: baseline; padding: 5px 0; cursor: pointer; color: var(--ink); }
  .phase label.step input { accent-color: var(--green); }
  .scenario { border: 1px solid var(--line); border-radius: 10px; margin-bottom: 12px; background: var(--card); }
  .scenario summary { cursor: pointer; padding: 13px 18px; font-weight: 600; font-size: 13.5px; list-style: none; }
  .scenario summary::-webkit-details-marker { display: none; }
  .scenario .body { padding: 0 18px 16px; font-size: 13px; color: var(--ink-2); }
  .scenario .body b { color: var(--ink); }
  .scenario .act { margin-top: 8px; display: inline-flex; gap: 8px; }
  footer { color: var(--muted); font-size: 12px; text-align: center; margin-top: 24px; }
  code { background: var(--bg); border: 1px solid var(--line); border-radius: 4px; padding: 1px 5px; font-size: 12px; }
</style>
</head>
<body>
<header>
  <div class="logo"><span class="mark"></span>countly</div>
  <div class="divider"></div>
  <div class="subtitle">Data Migration</div>
  <div class="badges">
    <span class="badge grey" id="b-engine">ledger engine · no redis</span>
    <span class="badge" id="b-dedup" style="display:none">dedup verified</span>
    <span class="badge grey" id="b-status">starting…</span>
  </div>
</header>
<nav class="tabs">
  <button class="tab active" data-pane="overview" onclick="showTab('overview')">Overview</button>
  <button class="tab" data-pane="guide" onclick="showTab('guide')">Migration Guide</button>
  <button class="tab" data-pane="help" onclick="showTab('help')">Help &amp; Recovery</button>
</nav>
<main>

<!-- ══════════════════ OVERVIEW ══════════════════ -->
<div class="pane active" id="pane-overview">
  <div class="stats">
    <div class="stat"><small>Docs migrated</small><b class="green" id="s-rows">–</b></div>
    <div class="stat"><small>Docs / second</small><b id="s-dps">–</b></div>
    <div class="stat"><small>Chunks done</small><b id="s-chunks">–</b></div>
    <div class="stat"><small>Skipped</small><b id="s-skipped">–</b></div>
    <div class="stat"><small>Failed chunks</small><b id="s-failed">–</b></div>
    <div class="stat"><small>ETA</small><b id="s-eta">–</b></div>
  </div>

  <div class="controls">
    <button class="btn" id="btn-pause" onclick="control('pause', 'Paused', this)">Pause</button>
    <button class="btn primary" id="btn-resume" onclick="control('resume', 'Resumed', this)">Resume</button>
    <button class="btn" id="btn-retry" onclick="control('retry-failed', 'Failed chunks queued for redo', this, true)">Retry failed chunks</button>
    <button class="btn" id="btn-replay" onclick="control('replay-dlq', 'DLQ replay finished', this)">Replay DLQ</button>
    <button class="btn" id="btn-waive" onclick="control('waive-dlq', 'Pending DLQ entries waived', this, true)">Waive pending DLQ</button>
    <span class="btn-note">Destructive actions ask for a second click. Every action shows a receipt.</span>
  </div>

  <div class="card">
    <h2>Collections</h2>
    <div id="collections"><div class="empty">Waiting for first chunk…</div></div>
  </div>

  <div class="card">
    <h2>Chunk map <span class="hint">(newest data first — chunks are processed right to left)</span></h2>
    <div class="grid" id="grid"></div>
    <div class="legend">
      <span><i style="background:var(--line)"></i> pending</span>
      <span><i style="background:var(--blue)"></i> copying</span>
      <span><i style="background:var(--amber)"></i> verifying / merging</span>
      <span><i style="background:var(--green)"></i> done</span>
      <span><i style="background:var(--red)"></i> failed</span>
      <span><i style="background:repeating-linear-gradient(45deg,var(--line),var(--line) 3px,transparent 3px,transparent 6px)"></i> split (poison hunt)</span>
    </div>
  </div>

  <div class="card">
    <h2>Failed chunks</h2>
    <div id="failed"><div class="empty">None 🎉</div></div>
  </div>

  <div class="card">
    <h2>Dead-letter queue <span class="hint">(unmigratable docs, stored with their full raw source — replay after a fix, or waive)</span></h2>
    <div id="dlq-status" style="margin-bottom:10px"></div>
    <div id="dlq-errors"></div>
    <div id="dlq-samples"></div>
  </div>

  <div class="card">
    <h2>Coercions <span class="hint">(values the transform had to alter — the data-quality report)</span></h2>
    <div id="coercions"><div class="empty">None</div></div>
  </div>

  <div class="card">
    <h2>Pods <span class="hint">(scale by starting more instances with the same env + unique POD_ID — they claim chunks via leases; add machines, not processes, once one machine's CPU saturates)</span></h2>
    <div id="pods"><div class="empty">–</div></div>
  </div>
</div>

<!-- ══════════════════ GUIDE ══════════════════ -->
<div class="pane" id="pane-guide">
  <div class="card">
    <h2>Preflight checks <span class="hint">— run anytime; read-only</span></h2>
    <button class="btn primary" id="btn-preflight" onclick="runPreflight(this)">Run preflight checks</button>
    <div id="preflight-result" style="margin-top:12px"></div>
  </div>

  <details class="phase" id="phase-1"><summary><span class="num">1</span> Prepare <span class="ph-status" id="ph1-s">manual</span></summary>
    <div class="body">
      <p>Old cluster stays live — nothing changes for users yet.</p>
      <label class="step"><input type="checkbox" data-step="p1-stack"> New stack deployed next to the old one (this service can reach both MongoDB and ClickHouse — see Preflight above)</label>
      <label class="step"><input type="checkbox" data-step="p1-kafka"> Kafka <code>drill-events</code> retention set to cover the migration window (14 days default); replication factor decided and recorded</label>
      <label class="step"><input type="checkbox" data-step="p1-copy"> Stateful data pre-copied: apps &amp; app keys, app_users, event definitions, dashboard users, plugin configs, aggregated data</label>
    </div>
  </details>

  <details class="phase" id="phase-2"><summary><span class="num">2</span> Index <span class="ph-status" id="ph2-s">auto</span></summary>
    <div class="body">
      <p>Every <code>drill_events*</code> collection needs the <code>{cd:1,_id:1}</code> index. The migrator builds missing ones itself, but pre-building avoids a long pause at start (~1–3 days for 10&nbsp;TB). No collection consolidation is ever needed.</p>
      <p><button class="btn primary" id="btn-buildidx" onclick="buildIndexes(this)">Build missing indexes</button>
      <span class="hint">— server-side, background; safe to leave running.</span></p>
      <div id="idx-progress"></div>
    </div>
  </details>

  <details class="phase" id="phase-3"><summary><span class="num">3</span> Rehearse (dry run) <span class="ph-status" id="ph3-s">auto</span></summary>
    <div class="body">
      <p>A ≤5% sample goes through the full pipeline with real ClickHouse validation and nothing stored. Review the Overview tab's DLQ and Coercions panels afterwards — that is the data-quality report to sign off before the real run.</p>
      <p><button class="btn primary" id="btn-dryrun" onclick="startDry(this)">Run dry run (sampled)</button>
      <span class="hint" id="dry-status">— available while the main migration is not running.</span></p>
    </div>
  </details>

  <details class="phase" id="phase-4"><summary><span class="num">4</span> Cutover <span class="ph-status" id="ph4-s">manual</span></summary>
    <div class="body">
      <p>The only ingestion pause in the whole flow — minutes, absorbed by SDK offline queues.</p>
      <label class="step"><input type="checkbox" data-step="p4-stop"> Old ingestion stopped</label>
      <label class="step"><input type="checkbox" data-step="p4-delta"> Stateful delta synced (changed app_users via last-seen; aggregated data BEFORE new ingestion writes current-period docs)</label>
      <label class="step"><input type="checkbox" data-step="p4-start"> New ingestion enabled — old MongoDB is now frozen (this is what makes every later step safe to redo)</label>
    </div>
  </details>

  <details class="phase" id="phase-5"><summary><span class="num">5</span> Migrate <span class="ph-status" id="ph5-s">–</span></summary>
    <div class="body">
      <p>Start the service (scale with pods — they share work via leases). Newest data first: recent dashboards fill within hours. Watch the Overview tab; the invariant monitor spot-checks continuously. Live progress: <b id="ph5-progress">–</b></p>
    </div>
  </details>

  <details class="phase" id="phase-6"><summary><span class="num">6</span> Verify &amp; sign off <span class="ph-status" id="ph6-s">gated</span></summary>
    <div class="body">
      <p>Three gates, all must be green:</p>
      <div class="check"><span class="ic" id="g1-ic">…</span><span class="lbl">All chunks done</span><span class="det" id="g1-det">–</span></div>
      <div class="check"><span class="ic" id="g2-ic">…</span><span class="lbl">DLQ pending = 0</span><span class="det" id="g2-det">every unmigrated doc explicitly fixed or waived</span></div>
      <div class="check"><span class="ic" id="g3-ic">…</span><span class="lbl">Full verification passed</span><span class="det" id="g3-det">run it below</span></div>
      <p style="margin-top:10px">
        <button class="btn primary" id="btn-verify" onclick="runVerify(this)">Verify migration</button>
        <span class="hint">— recounts every completed chunk against the live table + checks for duplicates. Exact.</span>
      </p>
      <div id="verify-result" style="margin-top:10px"></div>
      <p>Then: final report (<a href="/report" target="_blank">/report</a>), customer sign-off, revert Kafka retention, decommission the old cluster.</p>
    </div>
  </details>
</div>

<!-- ══════════════════ HELP ══════════════════ -->
<div class="pane" id="pane-help">
  <p style="color:var(--ink-2);margin:0 0 16px">Every situation below ends in <b>restart or resume</b> — never restore, never wipe. Live data cannot be touched by a migration failure: history sits in the frozen source, in-flight work sits in disposable staging tables, and the live table only ever receives whole verified chunks.</p>

  <details class="scenario"><summary>🔌 The migrator crashed / a pod died</summary>
    <div class="body"><p><b>What happened:</b> nothing, to your data. In-flight chunks will be redone; a dead pod's lease expires and others reclaim its work.</p>
    <p><b>Do:</b> restart the process (or let your orchestrator do it). There is no manual cleanup step in this flow.</p></div>
  </details>

  <details class="scenario"><summary>📄 Some documents won't migrate (DLQ pending &gt; 0)</summary>
    <div class="body"><p><b>What happened:</b> documents ClickHouse or the transform rejected were isolated automatically and stored in the dead-letter queue <b>with their full raw source</b> — inspect them in the Overview tab.</p>
    <p><b>Do:</b> after a transform fix (or after editing the stored raw docs):</p>
    <span class="act"><button class="btn" onclick="control('replay-dlq','DLQ replay finished',this)">Replay DLQ</button>
    <button class="btn" onclick="control('waive-dlq','Pending DLQ entries waived',this,true)">Waive pending DLQ</button></span>
    <p>Waiving is the explicit decision that they will not migrate — raw docs are kept as the record.</p></div>
  </details>

  <details class="scenario"><summary>⛔ The engine paused itself (circuit breaker)</summary>
    <div class="body"><p><b>What happened:</b> too many documents in one chunk failed — that pattern means a systematic problem, not dirty data. The DLQ already names the error.</p>
    <p><b>Do:</b> read the top error in the Overview tab, fix the cause, then:</p>
    <span class="act"><button class="btn" onclick="control('retry-failed','Failed chunks queued for redo',this,true)">Retry failed chunks</button>
    <button class="btn" onclick="control('resume','Resumed',this)">Resume</button></span></div>
  </details>

  <details class="scenario"><summary>🧨 A chunk keeps failing / keeps crashing the process</summary>
    <div class="body"><p><b>What happened:</b> a poison-pill document. After repeated crashes the chunk is automatically split into smaller pieces — the quarantine converges on a window of minutes around the offending doc(s) while everything else migrates. Split chunks show as hatched cells in the chunk map.</p>
    <p><b>Do:</b> when a tiny chunk ends up failed, inspect the few source documents in its time range (shown in Failed chunks), fix or remove them, then:</p>
    <span class="act"><button class="btn" onclick="control('retry-failed','Failed chunks queued for redo',this,true)">Retry failed chunks</button></span></div>
  </details>

  <details class="scenario"><summary>🔍 Counts look wrong / trust is in question</summary>
    <div class="body"><p><b>What happened:</b> maybe nothing — but never guess. The invariant monitor spot-checks continuously; a violation pauses the engine and flags the chunk.</p>
    <p><b>Do:</b> run the full check — every completed chunk recounted against the live table:</p>
    <span class="act"><button class="btn primary" onclick="showTab('guide'); document.getElementById('phase-6').open = true; runVerify(document.getElementById('btn-verify'))">Verify migration</button></span>
    <p>A flagged chunk is healed with <b>Retry failed chunks</b> (its live window is purged and redone cleanly).</p></div>
  </details>

  <details class="scenario"><summary>🔥 Live ClickHouse itself must be rebuilt (worst case)</summary>
    <div class="body"><p><b>What happened:</b> catastrophic loss of the target. Your data still exists twice: live events since cutover sit in the Kafka log; history sits in the frozen source MongoDB.</p>
    <p><b>Do:</b> recreate the table → reset ONLY the ClickHouse-sink connector's offsets to earliest (Kafka replays the live window; aggregator groups untouched) → re-run this migrator for history. Zero data loss. See <code>docs/RUNBOOK.md</code>.</p></div>
  </details>
</div>

<footer>State source: chunk ledger (MongoDB) + live engine counters — refreshed every 2s. No Redis involved.</footer>
<div id="toast"></div>

<script>
const fmt = (n) => n == null ? '–' : Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 });
const esc = (x) => String(x).replace(/[&<>"]/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

function showTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.pane === name));
  document.querySelectorAll('.pane').forEach(p => p.classList.toggle('active', p.id === 'pane-' + name));
}

function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 4000);
}

// Two-step confirmation: first click arms the button, second click fires.
const armed = new WeakMap();
async function control(action, okMsg, btn, needsConfirm) {
  if (needsConfirm && btn && !armed.get(btn)) {
    armed.set(btn, true);
    btn.dataset.label = btn.textContent;
    btn.textContent = 'Click again to confirm';
    btn.classList.add('armed');
    setTimeout(() => { armed.delete(btn); btn.textContent = btn.dataset.label; btn.classList.remove('armed'); }, 4000);
    return;
  }
  if (btn) { armed.delete(btn); if (btn.dataset.label) { btn.textContent = btn.dataset.label; btn.classList.remove('armed'); } btn.disabled = true; }
  try {
    const res = await fetch('/control/' + action, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) toast('\\u274c ' + action + ' failed (' + res.status + '): ' + (body.message || JSON.stringify(body)));
    else toast('\\u2705 ' + okMsg + ' \\u2014 ' + JSON.stringify(body));
    tick(); slowTick();
  } catch (e) { toast('\\u274c ' + action + ' failed: ' + e.message); }
  if (btn) btn.disabled = false;
}

async function runPreflight(btn) {
  btn.disabled = true; btn.textContent = 'Running…';
  try {
    const pf = await fetch('/api/preflight').then(r => r.json());
    const ic = { pass: '\\u2705', warn: '\\u26a0\\ufe0f', fail: '\\u274c' };
    document.getElementById('preflight-result').innerHTML = (pf.checks || []).map(c =>
      '<div class="check"><span class="ic">' + ic[c.status] + '</span><span class="lbl">' + esc(c.label) + '</span><span class="det">' + esc(c.detail) + '</span></div>'
    ).join('');
  } catch (e) { toast('\\u274c preflight failed: ' + e.message); }
  btn.disabled = false; btn.textContent = 'Run preflight checks';
}

async function runVerify(btn) {
  btn.disabled = true; btn.textContent = 'Verifying…';
  try {
    const v = await fetch('/api/verify').then(r => r.json());
    const ok = v.ok;
    document.getElementById('verify-result').innerHTML =
      '<div class="check"><span class="ic">' + (ok ? '\\u2705' : '\\u274c') + '</span><span class="lbl">' +
      (ok ? 'Verification passed' : 'Verification FAILED') + '</span><span class="det">' +
      fmt(v.checkedChunks) + ' chunks checked \\u00b7 table: ' + fmt(v.table.rows) + ' rows, ' +
      fmt(v.table.distinctIds) + ' distinct ids, ' + fmt(v.table.duplicates) + ' duplicates' +
      (v.mismatches.length ? ' \\u00b7 mismatches: ' + esc(JSON.stringify(v.mismatches.slice(0, 3))) : '') +
      '</span></div>';
    document.getElementById('g3-ic').textContent = ok ? '\\u2705' : '\\u274c';
    document.getElementById('g3-det').textContent = ok ? 'passed' : 'FAILED — see result below';
  } catch (e) { toast('\\u274c verify failed: ' + e.message); }
  btn.disabled = false; btn.textContent = 'Verify migration';
}

async function buildIndexes(btn) {
  btn.disabled = true;
  try {
    const r = await fetch('/control/build-indexes', { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' }).then(x => x.json());
    toast(r.started ? '\u2705 Index builds started (' + r.missing + ' collection(s))' : (r.missing === 0 ? '\u2705 All collections already indexed' : '\u26a0\ufe0f Build already running'));
    pollIndexProgress();
  } catch (e) { toast('\u274c ' + e.message); }
  btn.disabled = false;
}
let idxTimer = null;
async function pollIndexProgress() {
  const p = await fetch('/api/index-progress').then(r => r.json()).catch(() => null);
  if (!p) return;
  const el = document.getElementById('idx-progress');
  const rows = [];
  if (p.total > 0) rows.push('<div class="check"><span class="ic">' + (p.running ? '\u23f3' : (p.error ? '\u274c' : '\u2705')) + '</span><span class="lbl">' +
    p.done.length + '/' + p.total + ' built</span><span class="det">' + esc(p.current ? 'building: ' + p.current : (p.error || 'idle')) + '</span></div>');
  for (const op of p.serverOps || []) {
    rows.push('<div class="check"><span class="ic">\u23f3</span><span class="lbl">' + esc(op.collection) + '</span><span class="det">' + (op.pct != null ? op.pct + '%' : esc(op.msg || 'in progress')) + '</span></div>');
  }
  el.innerHTML = rows.join('');
  if (p.running && !idxTimer) { idxTimer = setInterval(pollIndexProgress, 3000); }
  if (!p.running && idxTimer) { clearInterval(idxTimer); idxTimer = null; }
}
async function startDry(btn) {
  btn.disabled = true;
  try {
    const r = await fetch('/control/dry-run', { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' }).then(x => x.json());
    toast(r.started ? '\u2705 Dry run started' : '\u26a0\ufe0f ' + (r.reason || 'not started'));
  } catch (e) { toast('\u274c ' + e.message); }
  btn.disabled = false;
}

// Manual guide checkboxes persist locally per browser.
document.querySelectorAll('label.step input').forEach((cb) => {
  const key = 'mig-step-' + cb.dataset.step;
  cb.checked = localStorage.getItem(key) === '1';
  cb.addEventListener('change', () => { localStorage.setItem(key, cb.checked ? '1' : '0'); updatePhaseBadges(); });
});
function updatePhaseBadges() {
  [['phase-1', 'ph1-s'], ['phase-4', 'ph4-s']].forEach(([ph, badge]) => {
    const boxes = [...document.querySelectorAll('#' + ph + ' input')];
    const done = boxes.every(b => b.checked);
    document.getElementById(badge).textContent = done ? 'done \\u2713' : boxes.filter(b => b.checked).length + '/' + boxes.length + ' steps';
    document.getElementById(ph).classList.toggle('done-phase', done);
  });
}
updatePhaseBadges();

async function tick() {
  try {
    const [stats, chunkResp] = await Promise.all([
      fetch('/stats').then(r => r.json()),
      fetch('/api/chunks').then(r => r.json()),
    ]);
    const chunks = chunkResp.chunks || [];

    document.getElementById('s-rows').textContent = fmt(stats.totalRowsInserted);
    document.getElementById('s-dps').textContent = fmt(stats.docsPerSecond);
    document.getElementById('s-skipped').textContent = fmt(stats.totalDocsSkipped);
    document.getElementById('s-failed').textContent = fmt(stats.chunksFailed);

    const done = chunks.filter(c => c.status === 'done').length;
    const active = chunks.filter(c => ['in_progress', 'written', 'attaching'].includes(c.status)).length;
    const failedN = chunks.filter(c => c.status === 'failed').length;
    const countable = chunks.filter(c => c.status !== 'superseded').length;
    document.getElementById('s-chunks').textContent = done + ' / ' + countable;

    const remainingDocs = chunks.filter(c => c.status !== 'done' && c.status !== 'superseded')
      .reduce((s, c) => s + (c.rows_expected || 0), 0);
    const knownRemaining = chunks.filter(c => c.status === 'pending').length;
    const avgDone = done > 0 ? chunks.filter(c => c.status === 'done').reduce((s, c) => s + c.docs_read, 0) / done : 0;
    const etaDocs = remainingDocs + knownRemaining * avgDone;
    document.getElementById('s-eta').textContent =
      stats.status === 'completed' ? 'done' :
      (stats.docsPerSecond > 0 && etaDocs > 0 ? Math.max(1, Math.round(etaDocs / stats.docsPerSecond / 60)) + ' min' : '–');

    const st = document.getElementById('b-status');
    st.textContent = stats.status;
    st.className = 'badge ' + (stats.status === 'completed' ? '' : stats.status === 'running' ? 'grey' : 'warn');

    if (stats.dedupWorks !== null) {
      const b = document.getElementById('b-dedup');
      b.style.display = '';
      b.textContent = stats.dedupWorks ? 'dedup verified' : 'dedup inert';
      b.className = 'badge' + (stats.dedupWorks ? '' : ' warn');
    }

    // Guide live bits
    document.getElementById('ph5-s').textContent = stats.status;
    document.getElementById('ph5-progress').textContent = done + '/' + countable + ' chunks \\u00b7 ' + fmt(stats.totalRowsInserted) + ' docs \\u00b7 ' + (stats.status || '');
    document.getElementById('g1-ic').textContent = (countable > 0 && done === countable) ? '\\u2705' : (failedN > 0 ? '\\u274c' : '\\u23f3');
    document.getElementById('g1-det').textContent = done + '/' + countable + ' done' + (failedN ? ', ' + failedN + ' failed' : '') + (active ? ', ' + active + ' active' : '');

    // Per-collection bars
    const byColl = {};
    for (const c of chunks) { if (c.status !== 'superseded') (byColl[c.collection] ||= []).push(c); }
    const collDiv = document.getElementById('collections');
    collDiv.innerHTML = Object.entries(byColl).map(([name, list]) => {
      const total = list.length;
      const d = list.filter(c => c.status === 'done').length;
      const a = list.filter(c => ['in_progress','written','attaching'].includes(c.status)).length;
      const f = list.filter(c => c.status === 'failed').length;
      return '<div class="collection">' +
        '<div class="coll-head"><span class="name">' + esc(name) + '</span>' +
        '<span class="pct">' + d + '/' + total + ' chunks \\u00b7 ' + Math.round(d / total * 100) + '%</span></div>' +
        '<div class="bar"><i class="done" style="width:' + (d / total * 100) + '%"></i>' +
        '<i class="active" style="width:' + (a / total * 100) + '%"></i>' +
        '<i class="failed" style="width:' + (f / total * 100) + '%"></i></div></div>';
    }).join('') || '<div class="empty">Waiting for first chunk…</div>';

    document.getElementById('grid').innerHTML = chunks.map(c =>
      '<span class="cell ' + c.status + '" title="' + esc(c.collection) + ' #' + c.idx +
      ' [' + new Date(c.lower_cd).toISOString().slice(0, 10) + ' \\u2192 ' + new Date(c.upper_cd).toISOString().slice(0, 10) + '] ' +
      c.status + (c.docs_read ? ' \\u00b7 ' + fmt(c.docs_read) + ' docs' : '') + '"></span>'
    ).join('');

    const failed = chunks.filter(c => c.status === 'failed');
    document.getElementById('failed').innerHTML = failed.length === 0
      ? '<div class="empty">None 🎉</div>'
      : '<table><tr><th>Chunk</th><th>Range</th><th>Attempts</th><th>Error</th></tr>' +
        failed.map(c => '<tr><td>' + esc(c.collection) + ' #' + c.idx + '</td><td>' +
          new Date(c.lower_cd).toISOString().slice(0, 10) + ' \\u2192 ' + new Date(c.upper_cd).toISOString().slice(0, 10) +
          '</td><td>' + c.attempts + '</td><td class="err">' + esc(c.last_error || '') + '</td></tr>').join('') + '</table>';
  } catch { /* engine restarting — keep polling */ }
}

async function slowTick() {
  try {
    const [dlq, report] = await Promise.all([
      fetch('/api/dlq').then(r => r.json()),
      fetch('/report').then(r => r.json()),
    ]);
    const bs = dlq.byStatus || {};
    const pending = bs.pending || 0;
    document.getElementById('dlq-status').innerHTML =
      ['pending', 'resolved', 'waived'].map(k =>
        '<span class="pill ' + k + '">' + k + ': ' + fmt(bs[k] || 0) + '</span>').join('') +
      (pending === 0 ? ' <span style="color:var(--green);font-size:12px;font-weight:600">ready for sign-off</span>'
                     : ' <span style="color:#A05A16;font-size:12px">sign-off requires pending = 0 (fix &amp; replay, or waive)</span>');
    document.getElementById('g2-ic').textContent = pending === 0 ? '\\u2705' : '\\u274c';
    document.getElementById('g2-det').textContent = pending === 0 ? 'clean' : fmt(pending) + ' pending \\u2014 fix & replay, or waive (Overview tab)';

    const errs = dlq.topErrors || [];
    document.getElementById('dlq-errors').innerHTML = errs.length === 0 ? '' :
      '<table><tr><th>Error</th><th>Docs</th></tr>' +
      errs.map(e => '<tr><td class="err">' + esc(e.error) + '</td><td>' + fmt(e.n) + '</td></tr>').join('') + '</table>';
    document.getElementById('dlq-samples').innerHTML = (dlq.samples || []).slice(0, 8).map(sm =>
      '<details class="dlq-sample"><summary>' + esc(sm.source_id) + ' \\u00b7 ' + esc(sm.reason) + ' \\u00b7 ' + esc(sm.error) + '</summary>' +
      '<pre>' + esc(sm.raw_doc) + '</pre></details>').join('');

    const co = (report.coercions || []).slice(0, 12);
    document.getElementById('coercions').innerHTML = co.length === 0
      ? '<div class="empty">None</div>'
      : '<table><tr><th>Rule \\u00b7 field</th><th>Count</th><th>Sample</th></tr>' +
        co.map(c => '<tr><td class="err">' + esc(c.rule_key) + '</td><td>' + fmt(c.count) + '</td><td style="font-size:12px;color:var(--ink-2)">' +
          (c.sample ? esc(c.sample.original) + ' \\u2192 ' + esc(c.sample.coerced) : '') + '</td></tr>').join('') + '</table>';

    document.getElementById('ph3-s').textContent = report.dryRun ? 'this is a dry run' : 'run with DRY_RUN=1';

    const [pods, dry] = await Promise.all([
      fetch('/api/pods').then(r => r.json()).catch(() => null),
      fetch('/api/dryrun').then(r => r.json()).catch(() => null),
    ]);
    if (pods && pods.pods) {
      const now = Date.now();
      document.getElementById('pods').innerHTML = pods.pods.length === 0
        ? '<div class="empty">No pods have claimed work yet.</div>'
        : '<table><tr><th>Pod</th><th>Chunks done</th><th>Active</th><th>Last seen</th></tr>' +
          pods.pods.map(p => {
            const ago = p.lastSeen ? Math.round((now - new Date(p.lastSeen).getTime()) / 1000) : null;
            const alive = ago !== null && ago < pods.leaseSec;
            return '<tr><td>' + esc(p.pod) + (alive ? ' <span class="pill resolved">alive</span>' : ' <span class="pill waived">idle/gone</span>') +
              '</td><td>' + fmt(p.done) + '</td><td>' + fmt(p.active) + '</td><td>' + (ago === null ? '–' : ago + 's ago') + '</td></tr>';
          }).join('') + '</table>';
    }
    if (dry) {
      const el = document.getElementById('dry-status');
      if (dry.status === 'running') el.textContent = '— dry run in progress…';
      else if (dry.status === 'completed') el.textContent = '— dry run completed: review DLQ & Coercions, then Preflight shows the result';
      else if (dry.status === 'failed') el.textContent = '— dry run FAILED: ' + (dry.error || '');
    }
  } catch { /* engine restarting */ }
}

tick(); slowTick();
setInterval(tick, 2000);
setInterval(slowTick, 5000);
</script>
</body>
</html>`;
