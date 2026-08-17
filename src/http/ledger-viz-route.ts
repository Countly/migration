/**
 * Countly-branded live dashboard for the ledger engine.
 *
 * Data source: the chunk ledger (MongoDB) + in-process engine stats — no
 * Redis. Served at /viz; /api/chunks feeds it. Brand tokens sampled from
 * countly.com (green #21B566, ink #24292E, Plus Jakarta Sans / Inter).
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
    --green: #21B566;
    --green-soft: #E4F6EC;
    --ink: #24292E;
    --ink-2: #5D6C7B;
    --muted: #81868D;
    --line: #DDDFE5;
    --bg: #F4F7F8;
    --card: #FFFFFF;
    --amber: #FAA262;
    --red: #FD8B89;
    --blue: #3898EC;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--ink);
    font-family: Inter, -apple-system, Arial, sans-serif; font-size: 14px; line-height: 1.5;
  }
  h1, h2, .stat b { font-family: 'Plus Jakarta Sans', Inter, Arial, sans-serif; }
  header {
    background: var(--card); border-bottom: 1px solid var(--line);
    padding: 14px 28px; display: flex; align-items: center; gap: 14px;
  }
  .logo { display: flex; align-items: center; gap: 9px; font-family: 'Plus Jakarta Sans'; font-weight: 800; font-size: 20px; letter-spacing: -0.02em; }
  .logo .mark { width: 22px; height: 22px; border-radius: 6px; background: var(--green); position: relative; }
  .logo .mark::after { content: ''; position: absolute; inset: 6px; border-radius: 3px; background: #fff; }
  .divider { width: 1px; height: 22px; background: var(--line); }
  .subtitle { color: var(--ink-2); font-weight: 500; }
  .badges { margin-left: auto; display: flex; gap: 8px; }
  .badge {
    font-size: 11px; font-weight: 600; letter-spacing: 0.04em; text-transform: uppercase;
    padding: 4px 10px; border-radius: 999px; background: var(--green-soft); color: #157A45;
  }
  .badge.warn { background: #FDEEDD; color: #A05A16; }
  .badge.grey { background: var(--bg); color: var(--ink-2); border: 1px solid var(--line); }
  main { max-width: 1080px; margin: 0 auto; padding: 24px 28px 60px; }
  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .stat {
    background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 14px 16px;
  }
  .stat small { display: block; color: var(--muted); font-size: 11px; font-weight: 600; letter-spacing: 0.05em; text-transform: uppercase; margin-bottom: 4px; }
  .stat b { font-size: 22px; font-weight: 700; font-variant-numeric: tabular-nums; }
  .stat b.green { color: var(--green); }
  .card { background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 18px 20px; margin-bottom: 16px; }
  .card h2 { margin: 0 0 12px; font-size: 15px; font-weight: 700; }
  .collection { margin-bottom: 14px; }
  .collection:last-child { margin-bottom: 0; }
  .coll-head { display: flex; justify-content: space-between; font-size: 13px; margin-bottom: 6px; }
  .coll-head .name { font-weight: 600; font-family: ui-monospace, Menlo, monospace; font-size: 12px; }
  .coll-head .pct { color: var(--ink-2); font-variant-numeric: tabular-nums; }
  .bar { height: 8px; background: var(--bg); border-radius: 999px; overflow: hidden; display: flex; }
  .bar i { display: block; height: 100%; }
  .bar .done { background: var(--green); }
  .bar .active { background: var(--blue); animation: pulse 1.2s ease-in-out infinite; }
  .bar .failed { background: var(--red); }
  @keyframes pulse { 50% { opacity: 0.55; } }
  .grid { display: flex; flex-wrap: wrap; gap: 4px; }
  .cell {
    width: 16px; height: 16px; border-radius: 4px; background: var(--line); cursor: default;
  }
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
  .btn {
    font-family: Inter; font-size: 13px; font-weight: 600; cursor: pointer;
    padding: 8px 16px; border-radius: 8px; border: 1px solid var(--line);
    background: var(--card); color: var(--ink); transition: all 0.15s;
  }
  .btn:hover { border-color: var(--green); color: var(--green); }
  .btn.primary { background: var(--green); border-color: var(--green); color: #fff; }
  .btn.primary:hover { opacity: 0.9; color: #fff; }
  .btn.danger:hover { border-color: var(--red); color: #C0392B; }
  .btn:disabled { opacity: 0.45; cursor: default; }
  .btn-note { color: var(--muted); font-size: 12px; align-self: center; }
  .pill { display: inline-block; font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 999px; margin-right: 6px; }
  .pill.pending { background: #FDEEDD; color: #A05A16; }
  .pill.resolved { background: var(--green-soft); color: #157A45; }
  .pill.waived { background: var(--bg); color: var(--ink-2); border: 1px solid var(--line); }
  details.dlq-sample { margin: 6px 0; font-size: 12px; }
  details.dlq-sample summary { cursor: pointer; font-family: ui-monospace, Menlo, monospace; color: var(--ink-2); }
  details.dlq-sample pre {
    background: var(--bg); border: 1px solid var(--line); border-radius: 6px;
    padding: 10px; overflow-x: auto; font-size: 11px; max-height: 200px;
  }
  #toast {
    position: fixed; bottom: 20px; right: 20px; background: var(--ink); color: #fff;
    padding: 10px 18px; border-radius: 8px; font-size: 13px; opacity: 0; transition: opacity 0.3s; pointer-events: none;
  }
  #toast.show { opacity: 1; }
  footer { color: var(--muted); font-size: 12px; text-align: center; margin-top: 24px; }
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
<main>
  <div class="stats">
    <div class="stat"><small>Docs migrated</small><b class="green" id="s-rows">–</b></div>
    <div class="stat"><small>Docs / second</small><b id="s-dps">–</b></div>
    <div class="stat"><small>Chunks done</small><b id="s-chunks">–</b></div>
    <div class="stat"><small>Skipped</small><b id="s-skipped">–</b></div>
    <div class="stat"><small>Failed chunks</small><b id="s-failed">–</b></div>
    <div class="stat"><small>ETA</small><b id="s-eta">–</b></div>
  </div>

  <div class="controls">
    <button class="btn" onclick="control('pause', 'Paused')">Pause</button>
    <button class="btn primary" onclick="control('resume', 'Resumed')">Resume</button>
    <button class="btn" onclick="confirm('Reset all failed chunks to pending (purges their live windows) and resume?') && control('retry-failed', 'Failed chunks queued for redo')">Retry failed chunks</button>
    <button class="btn" onclick="control('replay-dlq', 'DLQ replay finished')">Replay DLQ</button>
    <button class="btn danger" onclick="confirm('Waive ALL pending DLQ docs? This is the explicit decision that they will NOT migrate (raw docs are retained).') && control('waive-dlq', 'Pending DLQ entries waived')">Waive pending DLQ</button>
    <span class="btn-note">Actions call the /control endpoints — same as curl, with receipts.</span>
  </div>

  <div class="card">
    <h2>Collections</h2>
    <div id="collections"><div class="empty">Waiting for first chunk…</div></div>
  </div>

  <div class="card">
    <h2>Chunk map <span style="color:var(--muted);font-weight:400;font-size:12px">(newest data first — chunks are processed right to left)</span></h2>
    <div class="grid" id="grid"></div>
    <div class="legend">
      <span><i style="background:var(--line)"></i> pending</span>
      <span><i style="background:var(--blue)"></i> copying</span>
      <span><i style="background:var(--amber)"></i> verifying / merging</span>
      <span><i style="background:var(--green)"></i> done</span>
      <span><i style="background:var(--red)"></i> failed</span>
    </div>
  </div>

  <div class="card">
    <h2>Failed chunks</h2>
    <div id="failed"><div class="empty">None 🎉</div></div>
  </div>

  <div class="card">
    <h2>Dead-letter queue <span style="color:var(--muted);font-weight:400;font-size:12px">(unmigratable docs, stored with their full raw source — replay after a fix, or waive)</span></h2>
    <div id="dlq-status" style="margin-bottom:10px"></div>
    <div id="dlq-errors"></div>
    <div id="dlq-samples"></div>
  </div>

  <div class="card">
    <h2>Coercions <span style="color:var(--muted);font-weight:400;font-size:12px">(values the transform had to alter — the data-quality report)</span></h2>
    <div id="coercions"><div class="empty">None</div></div>
  </div>

  <div id="toast"></div>

  <footer>State source: chunk ledger (MongoDB) + live engine counters — refreshed every 2s. No Redis involved.</footer>
</main>
<script>
const fmt = (n) => n == null ? '–' : Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 });
const esc = (x) => String(x).replace(/[&<>"]/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 3500);
}

async function control(action, okMsg) {
  try {
    const res = await fetch('/control/' + action, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      toast('\u274c ' + action + ' failed (' + res.status + '): ' + (body.message || JSON.stringify(body)));
    } else {
      toast('\u2705 ' + okMsg + ' — ' + JSON.stringify(body));
    }
    tick(); slowTick();
  } catch (e) { toast('\u274c ' + action + ' failed: ' + e.message); }
  return true;
}

async function slowTick() {
  try {
    const [dlq, report] = await Promise.all([
      fetch('/api/dlq').then(r => r.json()),
      fetch('/report').then(r => r.json()),
    ]);
    const bs = dlq.byStatus || {};
    document.getElementById('dlq-status').innerHTML =
      ['pending', 'resolved', 'waived'].map(k =>
        '<span class="pill ' + k + '">' + k + ': ' + fmt(bs[k] || 0) + '</span>').join('') +
      ((bs.pending || 0) === 0 ? ' <span style="color:var(--green);font-size:12px;font-weight:600">ready for sign-off</span>'
                               : ' <span style="color:#A05A16;font-size:12px">sign-off requires pending = 0 (fix &amp; replay, or waive)</span>');
    const errs = dlq.topErrors || [];
    document.getElementById('dlq-errors').innerHTML = errs.length === 0 ? '' :
      '<table><tr><th>Error</th><th>Docs</th></tr>' +
      errs.map(e => '<tr><td class="err">' + esc(e.error) + '</td><td>' + fmt(e.n) + '</td></tr>').join('') + '</table>';
    document.getElementById('dlq-samples').innerHTML = (dlq.samples || []).slice(0, 8).map(sm =>
      '<details class="dlq-sample"><summary>' + esc(sm.source_id) + ' · ' + esc(sm.reason) + ' · ' + esc(sm.error) + '</summary>' +
      '<pre>' + esc(sm.raw_doc) + '</pre></details>').join('');

    const co = (report.coercions || []).slice(0, 12);
    document.getElementById('coercions').innerHTML = co.length === 0
      ? '<div class="empty">None</div>'
      : '<table><tr><th>Rule · field</th><th>Count</th><th>Sample</th></tr>' +
        co.map(c => '<tr><td class="err">' + esc(c.rule_key) + '</td><td>' + fmt(c.count) + '</td><td style="font-size:12px;color:var(--ink-2)">' +
          (c.sample ? esc(c.sample.original) + ' → ' + esc(c.sample.coerced) : '') + '</td></tr>').join('') + '</table>';
  } catch { /* engine restarting */ }
}

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
    document.getElementById('s-chunks').textContent = done + ' / ' + chunks.length;

    const remainingDocs = chunks.filter(c => c.status !== 'done')
      .reduce((s, c) => s + (c.rows_expected || 0), 0);
    const knownRemaining = chunks.filter(c => c.status === 'pending').length;
    const avgDone = done > 0
      ? chunks.filter(c => c.status === 'done').reduce((s, c) => s + c.docs_read, 0) / done : 0;
    const etaDocs = remainingDocs + knownRemaining * avgDone;
    document.getElementById('s-eta').textContent =
      stats.status === 'completed' ? 'done' :
      (stats.docsPerSecond > 0 && etaDocs > 0
        ? Math.max(1, Math.round(etaDocs / stats.docsPerSecond / 60)) + ' min'
        : '–');

    const st = document.getElementById('b-status');
    st.textContent = stats.status;
    st.className = 'badge ' + (stats.status === 'completed' ? '' : stats.status === 'running' ? 'grey' : 'warn');

    if (stats.dedupWorks !== null) {
      const b = document.getElementById('b-dedup');
      b.style.display = '';
      b.textContent = stats.dedupWorks ? 'dedup verified' : 'dedup inert';
      b.className = 'badge' + (stats.dedupWorks ? '' : ' warn');
    }

    // Per-collection progress bars
    const byColl = {};
    for (const c of chunks) (byColl[c.collection] ||= []).push(c);
    const collDiv = document.getElementById('collections');
    collDiv.innerHTML = Object.entries(byColl).map(([name, list]) => {
      const total = list.length;
      const d = list.filter(c => c.status === 'done').length;
      const a = list.filter(c => ['in_progress','written','attaching'].includes(c.status)).length;
      const f = list.filter(c => c.status === 'failed').length;
      return '<div class="collection">' +
        '<div class="coll-head"><span class="name">' + name + '</span>' +
        '<span class="pct">' + d + '/' + total + ' chunks · ' + Math.round(d / total * 100) + '%</span></div>' +
        '<div class="bar">' +
          '<i class="done" style="width:' + (d / total * 100) + '%"></i>' +
          '<i class="active" style="width:' + (a / total * 100) + '%"></i>' +
          '<i class="failed" style="width:' + (f / total * 100) + '%"></i>' +
        '</div></div>';
    }).join('') || '<div class="empty">Waiting for first chunk…</div>';

    // Chunk map
    document.getElementById('grid').innerHTML = chunks.map(c =>
      '<span class="cell ' + c.status + '" title="' + c.collection + ' #' + c.idx +
      ' [' + new Date(c.lower_cd).toISOString().slice(0, 10) + ' → ' + new Date(c.upper_cd).toISOString().slice(0, 10) + '] ' +
      c.status + (c.docs_read ? ' · ' + fmt(c.docs_read) + ' docs' : '') + '"></span>'
    ).join('');

    // Failed list
    const failed = chunks.filter(c => c.status === 'failed');
    document.getElementById('failed').innerHTML = failed.length === 0
      ? '<div class="empty">None 🎉</div>'
      : '<table><tr><th>Chunk</th><th>Range</th><th>Attempts</th><th>Error</th></tr>' +
        failed.map(c => '<tr><td>' + c.collection + ' #' + c.idx + '</td><td>' +
          new Date(c.lower_cd).toISOString().slice(0, 10) + ' → ' + new Date(c.upper_cd).toISOString().slice(0, 10) +
          '</td><td>' + c.attempts + '</td><td class="err">' + (c.last_error || '') + '</td></tr>').join('') + '</table>';
  } catch { /* engine restarting — keep polling */ }
}
tick();
slowTick();
setInterval(tick, 2000);
setInterval(slowTick, 5000);
</script>
</body>
</html>`;
