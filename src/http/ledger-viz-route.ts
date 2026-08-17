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
import type { Config } from '../config/schema.ts';

export interface LedgerVizDeps {
  orchestrator: ChunkOrchestrator;
  ledger: LedgerStore;
  config: Config;
}

export function registerLedgerVizRoutes(app: FastifyInstance, deps: LedgerVizDeps): void {
  app.get('/api/chunks', async () => {
    const chunks = await deps.ledger.listAll(deps.config.ledger.runId);
    return { runId: deps.config.ledger.runId, chunks };
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
  .legend { display: flex; gap: 16px; margin-top: 12px; color: var(--ink-2); font-size: 12px; flex-wrap: wrap; }
  .legend span { display: inline-flex; align-items: center; gap: 6px; }
  .legend i { width: 10px; height: 10px; border-radius: 3px; display: inline-block; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; padding: 6px 12px 6px 0; border-bottom: 1px solid var(--line); }
  td { padding: 8px 12px 8px 0; border-bottom: 1px solid var(--bg); font-variant-numeric: tabular-nums; vertical-align: top; }
  td.err { color: #C0392B; font-family: ui-monospace, Menlo, monospace; font-size: 12px; word-break: break-word; }
  .empty { color: var(--muted); padding: 8px 0; }
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

  <footer>State source: chunk ledger (MongoDB) + live engine counters — refreshed every 2s. No Redis involved.</footer>
</main>
<script>
const fmt = (n) => n == null ? '–' : Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 });

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
setInterval(tick, 2000);
</script>
</body>
</html>`;
