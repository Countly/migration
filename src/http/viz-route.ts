import type { FastifyInstance } from 'fastify';

export function registerVizRoute(app: FastifyInstance): void {
  app.get('/viz', async (_request, reply) => {
    return reply.type('text/html').send(DASHBOARD_HTML);
  });
}

// ---------------------------------------------------------------------------
// Self-contained HTML dashboard
//
// Security note: This dashboard renders only data from its own trusted API
// endpoints (/stats, /readyz) which return structured JSON from the migration
// service. All user-visible text is set via textContent (safe). The only
// innerHTML usage is for rendering collection table rows and skip reason grids
// from the service's own structured data (collection names, numeric values,
// status enums) — these are not user-supplied and do not contain executable
// content. The dashboard has no user input fields or URL-sourced data.
// ---------------------------------------------------------------------------

const DASHBOARD_HTML = /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Migration Dashboard</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg: #0f1117; --bg2: #1a1d27; --bg3: #242736; --border: #2e3144;
    --text: #e1e4ed; --text2: #8b8fa3; --text3: #5c6074;
    --green: #22c55e; --yellow: #eab308; --red: #ef4444; --blue: #3b82f6;
    --orange: #f97316; --purple: #a855f7; --cyan: #06b6d4;
    --bar-bg: #1e2235; --bar-fill: #3b82f6;
  }
  body { background: var(--bg); color: var(--text); font-family: 'SF Mono', 'Cascadia Code', 'Fira Code', monospace; font-size: 13px; line-height: 1.5; padding: 16px; }
  a { color: var(--blue); }

  /* Layout */
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 12px; }
  .full { grid-column: 1 / -1; }

  /* Card */
  .card { background: var(--bg2); border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
  .card h3 { font-size: 11px; text-transform: uppercase; letter-spacing: 1px; color: var(--text2); margin-bottom: 10px; }

  /* Header */
  .header { display: flex; align-items: center; justify-content: space-between; padding: 12px 16px; background: var(--bg2); border: 1px solid var(--border); border-radius: 8px; }
  .header-left { display: flex; align-items: center; gap: 12px; }
  .header-left h1 { font-size: 16px; font-weight: 600; }
  .header-right { display: flex; align-items: center; gap: 16px; font-size: 12px; color: var(--text2); }
  .status-badge { padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
  .status-running { background: rgba(59,130,246,0.15); color: var(--blue); }
  .status-paused { background: rgba(234,179,8,0.15); color: var(--yellow); }
  .status-completed { background: rgba(34,197,94,0.15); color: var(--green); }
  .status-failed { background: rgba(239,68,68,0.15); color: var(--red); }
  .status-stopped { background: rgba(234,179,8,0.15); color: var(--yellow); }
  .status-idle { background: rgba(92,96,116,0.15); color: var(--text3); }
  .status-waiting_for_index { background: rgba(249,115,22,0.15); color: var(--orange); }

  /* Light theme */
  body.light {
    --bg: #f5f5f7; --bg2: #ffffff; --bg3: #e8e8ed; --border: #d1d1d6;
    --text: #1d1d1f; --text2: #6e6e73; --text3: #aeaeb2;
    --bar-bg: #e8e8ed;
  }
  body.light .btn { background: #f0f0f5; }
  body.light .btn:hover:not(:disabled) { background: #e0e0e5; }

  /* Progress bars */
  .progress-outer { background: var(--bar-bg); border-radius: 6px; height: 24px; overflow: hidden; position: relative; }
  .progress-fill { height: 100%; border-radius: 6px; transition: width 0.8s ease; background: linear-gradient(90deg, var(--blue) 0%, var(--cyan) 100%); position: relative; }
  .progress-fill.green { background: linear-gradient(90deg, var(--green) 0%, #16a34a 100%); }
  .progress-text { position: absolute; right: 8px; top: 50%; transform: translateY(-50%); font-size: 11px; font-weight: 600; color: #fff; text-shadow: 0 1px 2px rgba(0,0,0,0.5); }
  .progress-sm { height: 6px; border-radius: 3px; }
  .progress-sm .progress-fill { border-radius: 3px; }

  /* Stats row */
  .stat-row { display: flex; justify-content: space-between; gap: 8px; flex-wrap: wrap; }
  .stat { text-align: center; flex: 1; min-width: 80px; }
  .stat-val { font-size: 20px; font-weight: 700; color: var(--text); }
  .stat-val.sm { font-size: 15px; }
  .stat-label { font-size: 10px; color: var(--text2); text-transform: uppercase; letter-spacing: 0.5px; margin-top: 2px; }

  /* Health dots */
  .health-row { display: flex; gap: 16px; flex-wrap: wrap; }
  .health-item { display: flex; align-items: center; gap: 6px; }
  .health-dot { width: 10px; height: 10px; border-radius: 50%; }
  .dot-green { background: var(--green); box-shadow: 0 0 6px var(--green); }
  .dot-red { background: var(--red); box-shadow: 0 0 6px var(--red); }
  .dot-gray { background: var(--text3); }

  /* Controls */
  .controls { display: flex; gap: 8px; flex-wrap: wrap; }
  .btn { padding: 8px 16px; border: 1px solid var(--border); border-radius: 6px; background: var(--bg3); color: var(--text); cursor: pointer; font-family: inherit; font-size: 12px; font-weight: 500; transition: all 0.15s; }
  .btn:hover:not(:disabled) { background: var(--border); }
  .btn:disabled { opacity: 0.3; cursor: not-allowed; }
  .btn-pause { border-color: var(--yellow); color: var(--yellow); }
  .btn-resume { border-color: var(--green); color: var(--green); }
  .btn-stop { border-color: var(--red); color: var(--red); }
  .btn-gc { border-color: var(--purple); color: var(--purple); }

  /* Collection table */
  .coll-table { width: 100%; border-collapse: collapse; font-size: 12px; }
  .coll-table th { text-align: left; padding: 6px 8px; color: var(--text2); font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 1px solid var(--border); }
  .coll-table td { padding: 5px 8px; border-bottom: 1px solid var(--border); vertical-align: middle; }
  .coll-table tr:last-child td { border-bottom: none; }
  .coll-scroll { max-height: 260px; overflow-y: auto; }
  .coll-scroll::-webkit-scrollbar { width: 6px; }
  .coll-scroll::-webkit-scrollbar-track { background: var(--bg2); }
  .coll-scroll::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
  .tag { display: inline-block; padding: 1px 6px; border-radius: 3px; font-size: 10px; font-weight: 600; }
  .tag-completed { background: rgba(34,197,94,0.15); color: var(--green); }
  .tag-failed { background: rgba(239,68,68,0.15); color: var(--red); }
  .tag-skipped { background: rgba(92,96,116,0.15); color: var(--text3); }
  .tag-running { background: rgba(59,130,246,0.15); color: var(--blue); }

  /* Skip reasons */
  .skip-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 4px 16px; font-size: 12px; }

  /* Heartbeat */
  .heartbeat { width: 8px; height: 8px; border-radius: 50%; background: var(--green); animation: pulse 2s infinite; }
  @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
  .heartbeat.error { background: var(--red); animation: none; }

  /* Memory bar */
  .mem-bar { display: flex; align-items: center; gap: 8px; margin: 4px 0; }
  .mem-label { width: 40px; font-size: 11px; color: var(--text2); }
  .mem-val { font-size: 11px; color: var(--text); min-width: 60px; text-align: right; }

  /* Tabs */
  .tab-bar { display: flex; gap: 2px; margin-bottom: 10px; }
  .tab { padding: 4px 12px; border-radius: 4px; font-size: 11px; cursor: pointer; color: var(--text2); }
  .tab.active { background: var(--bg3); color: var(--text); }

  .muted { color: var(--text3); }

  /* Key-value grid */
  .kv-grid { font-size: 12px; }
  .kv-row { display: flex; justify-content: space-between; padding: 2px 0; border-bottom: 1px solid rgba(46,49,68,0.5); }
  .kv-row:last-child { border-bottom: none; }
  .kv-key { color: var(--text2); }
  .kv-val { color: var(--text); font-weight: 500; text-align: right; max-width: 60%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <div class="header-left">
    <h1 id="svc-name">Migration Dashboard</h1>
    <span id="svc-status" class="status-badge status-idle">idle</span>
  </div>
  <div class="header-right">
    <span id="svc-host" class="muted"></span>
    <span id="svc-uptime"></span>
    <span>v<span id="svc-version">-</span></span>
    <button class="btn" id="btn-theme" style="padding:4px 8px;font-size:11px">Theme</button>
    <div class="heartbeat" id="heartbeat"></div>
  </div>
</div>

<!-- Overall Progress -->
<div class="card full" style="margin-top:12px">
  <h3>Overall Progress</h3>
  <div class="progress-outer" style="height:32px;margin-bottom:10px">
    <div class="progress-fill" id="overall-bar" style="width:0%">
      <span class="progress-text" id="overall-pct">0%</span>
    </div>
  </div>
  <div class="stat-row">
    <div class="stat"><div class="stat-val" id="s-docs">-</div><div class="stat-label">Docs Progress</div></div>
    <div class="stat"><div class="stat-val" id="s-throughput">-</div><div class="stat-label">Throughput</div></div>
    <div class="stat"><div class="stat-val" id="s-elapsed">-</div><div class="stat-label">Elapsed</div></div>
    <div class="stat"><div class="stat-val" id="s-eta">-</div><div class="stat-label">ETA</div></div>
    <div class="stat"><div class="stat-val" id="s-colls">-</div><div class="stat-label">Collections</div></div>
  </div>
</div>

<div class="grid">
  <!-- Current Collection -->
  <div class="card">
    <h3>Current Collection</h3>
    <div id="cur-coll-name" style="font-size:12px;margin-bottom:8px;word-break:break-all">-</div>
    <div class="progress-outer" style="margin-bottom:8px">
      <div class="progress-fill" id="cur-bar" style="width:0%">
        <span class="progress-text" id="cur-pct">0%</span>
      </div>
    </div>
    <div class="stat-row">
      <div class="stat"><div class="stat-val sm" id="c-read">0</div><div class="stat-label">Read</div></div>
      <div class="stat"><div class="stat-val sm" id="c-inserted">0</div><div class="stat-label">Inserted</div></div>
      <div class="stat"><div class="stat-val sm" id="c-batch">0</div><div class="stat-label">Batch</div></div>
      <div class="stat"><div class="stat-val sm" id="c-skip">0%</div><div class="stat-label">Skip Rate</div></div>
    </div>
  </div>

  <!-- Controls & Health -->
  <div class="card">
    <h3>Controls</h3>
    <div class="controls" style="margin-bottom:14px">
      <button class="btn btn-pause" id="btn-pause">Pause</button>
      <button class="btn btn-resume" id="btn-resume">Resume</button>
      <button class="btn btn-stop" id="btn-stop">Stop After Batch</button>
      <button class="btn btn-gc" id="btn-gc">GC</button>
    </div>
    <div id="ctrl-msg" style="font-size:11px;margin-bottom:12px;min-height:16px"></div>
    <h3>Health</h3>
    <div class="health-row" id="health-row">
      <div class="health-item"><div class="health-dot dot-gray" id="h-mongo"></div><span>MongoDB</span></div>
      <div class="health-item"><div class="health-dot dot-gray" id="h-ch"></div><span>ClickHouse</span></div>
      <div class="health-item"><div class="health-dot dot-gray" id="h-redis"></div><span>Redis</span></div>
    </div>
  </div>
</div>

<div class="grid">
  <!-- Throughput & Integrity -->
  <div class="card">
    <h3>Throughput & Integrity</h3>
    <div class="stat-row" style="margin-bottom:12px">
      <div class="stat"><div class="stat-val sm" id="t-read">0</div><div class="stat-label">Docs Read</div></div>
      <div class="stat"><div class="stat-val sm" id="t-inserted">0</div><div class="stat-label">Rows Inserted</div></div>
      <div class="stat"><div class="stat-val sm" id="t-skipped">0</div><div class="stat-label">Skipped</div></div>
    </div>
    <div class="stat-row">
      <div class="stat"><div class="stat-val sm" id="t-digest" style="color:var(--text)">0</div><div class="stat-label">Digest Mismatch</div></div>
      <div class="stat"><div class="stat-val sm" id="t-dups" style="color:var(--text)">0</div><div class="stat-label">Est. Duplicates</div></div>
      <div class="stat"><div class="stat-val sm" id="t-bfail" style="color:var(--text)">0</div><div class="stat-label">Batches Failed</div></div>
    </div>
  </div>

  <!-- Skip Reasons & System -->
  <div class="card">
    <h3>Skip Reasons</h3>
    <div id="skip-grid" class="skip-grid"></div>
    <h3 style="margin-top:12px">System</h3>
    <div class="mem-bar">
      <span class="mem-label">RSS</span>
      <div class="progress-outer progress-sm" style="flex:1"><div class="progress-fill" id="mem-rss" style="width:0%;background:var(--orange)"></div></div>
      <span class="mem-val" id="mem-rss-val">-</span>
    </div>
    <div class="mem-bar">
      <span class="mem-label">Heap</span>
      <div class="progress-outer progress-sm" style="flex:1"><div class="progress-fill" id="mem-heap" style="width:0%;background:var(--purple)"></div></div>
      <span class="mem-val" id="mem-heap-val">-</span>
    </div>
    <div id="gc-line" style="font-size:11px;color:var(--text2);margin-top:6px"></div>
    <div id="gc-detail" style="font-size:11px;color:var(--text2);margin-top:2px"></div>
  </div>
</div>

<!-- Infrastructure -->
<div class="grid" style="margin-top:12px">
  <div class="card">
    <h3>Run Details</h3>
    <div class="kv-grid" id="run-details"></div>
  </div>
  <div class="card">
    <h3>Infrastructure</h3>
    <div class="kv-grid" id="infra-details"></div>
  </div>
</div>

<!-- Cluster (multi-pod, hidden when single-pod) -->
<div class="card full" style="margin-top:12px;display:none" id="cluster-card">
  <h3>Cluster <span id="cluster-summary" style="font-weight:400;color:var(--text2)"></span></h3>
  <div class="controls" style="margin-bottom:10px">
    <button class="btn btn-pause" id="btn-global-pause">Global Pause</button>
    <button class="btn btn-resume" id="btn-global-resume">Global Resume</button>
    <button class="btn btn-stop" id="btn-global-stop">Global Stop</button>
  </div>
  <!-- Cluster progress bar -->
  <div style="margin-bottom:12px">
    <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:4px">
      <span id="cluster-progress-label">0 / 0 collections</span>
      <span id="cluster-progress-pct" style="font-weight:600">0%</span>
    </div>
    <div class="progress-outer" style="height:20px">
      <div class="progress-fill" id="cluster-bar" style="width:0%">
        <span class="progress-text" style="font-size:10px" id="cluster-bar-text">0%</span>
      </div>
    </div>
    <div class="stat-row" style="margin-top:8px">
      <div class="stat"><div class="stat-val sm" id="cl-docs">-</div><div class="stat-label">Cluster Docs Read</div></div>
      <div class="stat"><div class="stat-val sm" id="cl-rows">-</div><div class="stat-label">Cluster Rows</div></div>
      <div class="stat"><div class="stat-val sm" id="cl-processing">-</div><div class="stat-label">Processing</div></div>
      <div class="stat"><div class="stat-val sm" id="cl-pending">-</div><div class="stat-label">Pending</div></div>
    </div>
  </div>
  <!-- Pods -->
  <h3 style="margin-top:4px">Pods</h3>
  <div id="cluster-pods" style="font-size:12px"></div>
  <!-- Active Locks -->
  <h3 style="margin-top:12px">Active Locks <span id="lock-count" style="font-weight:400;color:var(--text2)"></span></h3>
  <div class="coll-scroll" style="max-height:160px">
    <table class="coll-table">
      <thead><tr><th>Collection</th><th>Pod</th><th>Acquired</th><th>Action</th></tr></thead>
      <tbody id="lock-body"></tbody>
    </table>
  </div>
  <!-- Stale Pods -->
  <div id="stale-pods-section" style="display:none;margin-top:12px">
    <h3>Stale Pods <span style="font-weight:400;color:var(--red)">(locks held by dead pods)</span></h3>
    <div id="stale-pods-list" style="font-size:12px"></div>
  </div>
</div>

<!-- Live Batches (always available, single or multi-pod) -->
<div class="card full" style="margin-top:12px;display:none" id="batches-panel">
  <h3>Live Batches <span id="batches-summary" style="font-weight:400;color:var(--text2)"></span></h3>
  <div class="coll-scroll" style="max-height:200px">
    <table class="coll-table">
      <thead><tr><th>Collection</th><th>Pod</th><th>Batch#</th><th>Docs</th><th>Rows</th><th>Elapsed</th><th>Phase</th></tr></thead>
      <tbody id="batches-body"></tbody>
    </table>
  </div>
</div>

<!-- Active Ranges (shown when range-parallel is active) -->
<div class="card full" style="margin-top:12px;display:none" id="ranges-panel">
  <h3>Active Ranges <span id="ranges-summary" style="font-weight:400;color:var(--text2)"></span></h3>
  <div id="ranges-content"></div>
</div>

<!-- Index Status -->
<div class="card full" style="margin-top:12px">
  <h3>Index Status <span id="idx-summary" style="font-weight:400;color:var(--text2)"></span></h3>
  <div id="idx-details" style="font-size:12px"></div>
</div>

<!-- Collections Table -->
<div class="card full" style="margin-top:12px">
  <h3>Collections <span id="coll-summary" style="font-weight:400;color:var(--text2)"></span></h3>
  <div class="tab-bar" id="tab-bar">
    <div class="tab active" data-tab="active">Active & Done</div>
    <div class="tab" data-tab="failed">Failed</div>
    <div class="tab" data-tab="skipped">Skipped</div>
    <div class="tab" data-tab="all">All</div>
  </div>
  <div class="coll-scroll">
    <table class="coll-table">
      <thead><tr><th>Collection</th><th>Status</th><th>Pod</th><th>Estimated</th><th>Read</th><th>Inserted</th><th>Action</th></tr></thead>
      <tbody id="coll-body"></tbody>
    </table>
  </div>
</div>

<script>
(function() {
  'use strict';

  var currentTab = 'active';
  var lastData = null;

  function $(id) { return document.getElementById(id); }
  function fmt(n) { return n == null ? '-' : Number(n).toLocaleString('en-US'); }
  function mb(b) { return b ? (b / 1024 / 1024).toFixed(0) + ' MB' : '-'; }

  // Theme toggle
  var theme = localStorage.getItem('viz-theme') || 'dark';
  if (theme === 'light') document.body.classList.add('light');
  $('btn-theme').addEventListener('click', function() {
    document.body.classList.toggle('light');
    theme = document.body.classList.contains('light') ? 'light' : 'dark';
    localStorage.setItem('viz-theme', theme);
  });

  // Tab switching
  $('tab-bar').addEventListener('click', function(e) {
    var tab = e.target.closest('.tab');
    if (!tab) return;
    var tabs = document.querySelectorAll('.tab');
    for (var i = 0; i < tabs.length; i++) tabs[i].classList.remove('active');
    tab.classList.add('active');
    currentTab = tab.getAttribute('data-tab');
    if (lastData) renderCollections(lastData);
  });

  // Control buttons
  $('btn-pause').addEventListener('click', function() { doControl('pause'); });
  $('btn-resume').addEventListener('click', function() { doControl('resume'); });
  $('btn-stop').addEventListener('click', function() { doControl('stop-after-batch'); });
  $('btn-gc').addEventListener('click', function() { doControl('gc'); });
  $('btn-global-pause').addEventListener('click', function() { doControl('global/pause'); });
  $('btn-global-resume').addEventListener('click', function() { doControl('global/resume'); });
  $('btn-global-stop').addEventListener('click', function() { doControl('global/stop'); });

  function statusClass(s) {
    if (s === 'running' || s === 'stopping') return 'status-running';
    if (s === 'waiting_for_index') return 'status-waiting_for_index';
    if (s === 'paused') return 'status-paused';
    if (s === 'completed') return 'status-completed';
    if (s === 'failed') return 'status-failed';
    if (s === 'stopped') return 'status-stopped';
    return 'status-idle';
  }

  function tagFor(status) {
    var cls = 'tag-' + (status === 'completed' ? 'completed' : status === 'failed' ? 'failed' : status === 'skipped' ? 'skipped' : 'running');
    return cls;
  }

  function truncHash(name) {
    if (name && name.length > 50) return name.slice(0, 20) + '...' + name.slice(-8);
    return name || '-';
  }

  function renderCollections(d) {
    var cp = (d.orchestrator && d.orchestrator.collectionProgress) || [];
    var cur = d.orchestrator && d.orchestrator.currentCollection;
    var filtered;

    if (currentTab === 'active') {
      filtered = cp.filter(function(c) { return c.status === 'completed' || c.status === 'processing' || c.collection === cur; });
    } else if (currentTab === 'failed') {
      filtered = cp.filter(function(c) { return c.status === 'failed'; });
    } else if (currentTab === 'skipped') {
      filtered = cp.filter(function(c) { return c.status === 'skipped'; });
    } else {
      filtered = cp;
    }

    if (filtered.length > 200) filtered = filtered.slice(0, 200);

    var tbody = $('coll-body');
    // Clear existing rows
    while (tbody.firstChild) tbody.removeChild(tbody.firstChild);

    if (filtered.length === 0) {
      var emptyRow = document.createElement('tr');
      var emptyCell = document.createElement('td');
      emptyCell.setAttribute('colspan', '7');
      emptyCell.className = 'muted';
      emptyCell.style.textAlign = 'center';
      emptyCell.style.padding = '20px';
      emptyCell.textContent = 'No entries';
      emptyRow.appendChild(emptyCell);
      tbody.appendChild(emptyRow);
      return;
    }

    for (var i = 0; i < filtered.length; i++) {
      var c = filtered[i];
      var isCurrent = c.collection === cur;
      var tr = document.createElement('tr');

      // Collection name
      var td1 = document.createElement('td');
      td1.style.maxWidth = '200px';
      td1.style.overflow = 'hidden';
      td1.style.textOverflow = 'ellipsis';
      td1.style.whiteSpace = 'nowrap';
      td1.style.fontSize = '11px';
      td1.setAttribute('title', c.collection || '');
      td1.textContent = truncHash(c.collection);
      tr.appendChild(td1);

      // Status tag
      var td2 = document.createElement('td');
      var span = document.createElement('span');
      span.className = 'tag ' + (isCurrent ? 'tag-running' : tagFor(c.status));
      span.textContent = isCurrent ? 'running' : c.status;
      td2.appendChild(span);
      tr.appendChild(td2);

      // Pod
      var tdPod = document.createElement('td');
      tdPod.style.cssText = 'font-size:10px;color:var(--text2);max-width:80px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap';
      tdPod.textContent = c.podId || '-';
      tr.appendChild(tdPod);

      // Estimated
      var td3 = document.createElement('td');
      td3.textContent = fmt(c.estimated);
      tr.appendChild(td3);

      // Read
      var td4 = document.createElement('td');
      td4.textContent = isCurrent ? fmt(d.currentCollectionProgress && d.currentCollectionProgress.docsRead) : fmt(c.docsRead);
      tr.appendChild(td4);

      // Inserted
      var td5 = document.createElement('td');
      td5.textContent = isCurrent ? fmt(d.currentCollectionProgress && d.currentCollectionProgress.rowsInserted) : fmt(c.rowsInserted);
      tr.appendChild(td5);

      // Action
      var td6 = document.createElement('td');
      td6.style.cssText = 'display:flex;gap:4px';
      if (!isCurrent && (c.status === 'failed' || c.status === 'skipped')) {
        var retryBtn = document.createElement('button');
        retryBtn.className = 'btn';
        retryBtn.style.cssText = 'padding:2px 8px;font-size:10px;border-color:var(--blue);color:var(--blue)';
        retryBtn.textContent = 'Retry';
        retryBtn.setAttribute('data-collection', c.collection);
        retryBtn.addEventListener('click', function() {
          var coll = this.getAttribute('data-collection');
          fetch('/control/retry-collection/' + encodeURIComponent(coll), { method: 'POST' });
        });
        td6.appendChild(retryBtn);
      }
      if (!isCurrent && c.runId && (c.status === 'completed' || c.status === 'failed')) {
        var retrySkipBtn = document.createElement('button');
        retrySkipBtn.className = 'btn';
        retrySkipBtn.style.cssText = 'padding:2px 8px;font-size:10px;border-color:var(--yellow);color:var(--yellow)';
        retrySkipBtn.textContent = 'Retry Skipped';
        retrySkipBtn.setAttribute('data-runid', c.runId);
        retrySkipBtn.addEventListener('click', function() {
          var rid = this.getAttribute('data-runid');
          fetch('/control/retry-skipped-batches/' + encodeURIComponent(rid), { method: 'POST' });
        });
        td6.appendChild(retrySkipBtn);
      }
      tr.appendChild(td6);

      tbody.appendChild(tr);
    }
  }

  function renderSkipReasons(sr) {
    var container = $('skip-grid');
    while (container.firstChild) container.removeChild(container.firstChild);

    var keys = Object.keys(sr || {});
    for (var i = 0; i < keys.length; i++) {
      var k = keys[i];
      var v = sr[k];
      var row = document.createElement('div');
      row.style.display = 'flex';
      row.style.justifyContent = 'space-between';

      var keySpan = document.createElement('span');
      keySpan.style.color = 'var(--text2)';
      keySpan.textContent = k.replace(/_/g, ' ');
      row.appendChild(keySpan);

      var valSpan = document.createElement('span');
      valSpan.style.fontWeight = '600';
      valSpan.style.color = v > 0 ? 'var(--yellow)' : 'var(--text3)';
      valSpan.textContent = fmt(v);
      row.appendChild(valSpan);

      container.appendChild(row);
    }
  }

  function renderKV(containerId, entries) {
    var container = $(containerId);
    while (container.firstChild) container.removeChild(container.firstChild);
    for (var i = 0; i < entries.length; i++) {
      var row = document.createElement('div');
      row.className = 'kv-row';
      var keyEl = document.createElement('span');
      keyEl.className = 'kv-key';
      keyEl.textContent = entries[i][0];
      row.appendChild(keyEl);
      var valEl = document.createElement('span');
      valEl.className = 'kv-val';
      valEl.textContent = entries[i][1];
      if (entries[i][2]) valEl.style.color = entries[i][2];
      row.appendChild(valEl);
      container.appendChild(row);
    }
  }

  function doControl(action) {
    var msg = $('ctrl-msg');
    fetch('/control/' + action, { method: 'POST' })
      .then(function(res) { return res.json(); })
      .then(function(data) {
        msg.textContent = 'OK: ' + action + ' (status: ' + (data.status || data.mode || 'done') + ')';
        msg.style.color = 'var(--green)';
      })
      .catch(function(e) {
        msg.textContent = 'Error: ' + e.message;
        msg.style.color = 'var(--red)';
      });
    setTimeout(function() { msg.textContent = ''; }, 4000);
  }

  function refresh() {
    Promise.all([
      fetch('/stats').then(function(r) { return r.json(); }),
      fetch('/readyz').then(function(r) { return r.json(); })
    ]).then(function(results) {
      var d = results[0];
      var h = results[1];
      lastData = d;
      $('heartbeat').className = 'heartbeat';

      // Header
      $('svc-name').textContent = (d.service && d.service.name || 'Migration') + ' Dashboard';
      var st = (d.service && d.service.status) || 'idle';
      $('svc-status').textContent = st;
      $('svc-status').className = 'status-badge ' + statusClass(st);
      $('svc-host').textContent = (d.service && d.service.hostname) || '';
      $('svc-uptime').textContent = (d.summary && d.summary.elapsed) || '-';
      $('svc-version').textContent = (d.service && d.service.version) || '-';

      // Overall progress — prefer cluster-wide data when available
      var clp = d.clusterProgress;
      var op;
      if (clp && clp.total > 0) {
        op = clp.pct || 0;
        $('s-docs').textContent = fmt(clp.docsRead) + ' / ~' + fmt(clp.estimated) + ' docs';
        $('s-colls').textContent = clp.done + '/' + clp.total + ' done'
          + (clp.failed > 0 ? ', ' + clp.failed + ' failed' : '')
          + (clp.processing > 0 ? ', ' + clp.processing + ' processing' : '');
      } else {
        op = (d.summary && d.summary.overallPct) || 0;
        $('s-docs').textContent = (d.summary && d.summary.docsProgress) || '-';
        $('s-colls').textContent = (d.summary && d.summary.collections) || '-';
      }
      $('overall-bar').style.width = op + '%';
      $('overall-pct').textContent = op + '%';
      if (op >= 100) $('overall-bar').classList.add('green');
      else $('overall-bar').classList.remove('green');
      $('s-throughput').textContent = (d.summary && d.summary.throughput) || '-';
      $('s-elapsed').textContent = (d.summary && d.summary.elapsed) || '-';
      $('s-eta').textContent = (d.summary && d.summary.eta) || '-';

      // Current collection
      var cc = d.currentCollectionProgress;
      if (cc) {
        $('cur-coll-name').textContent = cc.collection || '-';
        $('cur-bar').style.width = cc.pct + '%';
        $('cur-pct').textContent = cc.pct + '%';
        if (cc.pct >= 100) $('cur-bar').classList.add('green');
        else $('cur-bar').classList.remove('green');
        $('c-read').textContent = fmt(cc.docsRead);
        $('c-inserted').textContent = fmt(cc.rowsInserted);
        $('c-batch').textContent = cc.batchSeq || 0;
        $('c-skip').textContent = cc.skipRate || '0%';
      } else {
        $('cur-coll-name').textContent = 'idle';
        $('cur-bar').style.width = '0%';
        $('cur-pct').textContent = '-';
      }

      // Controls state
      var cmds = d.commands || {};
      $('btn-pause').disabled = st !== 'running';
      $('btn-resume').disabled = st !== 'paused';
      $('btn-stop').disabled = st === 'completed' || st === 'idle' || st === 'stopped' || cmds.stopAfterBatchRequested;
      $('btn-stop').textContent = cmds.stopAfterBatchRequested ? 'Stopping...' : 'Stop After Batch';

      // Health
      var checks = (h && h.checks) || {};
      $('h-mongo').className = 'health-dot ' + (checks.mongo ? 'dot-green' : 'dot-red');
      $('h-ch').className = 'health-dot ' + (checks.clickhouse ? 'dot-green' : 'dot-red');
      $('h-redis').className = 'health-dot ' + (checks.redis ? 'dot-green' : 'dot-red');

      // Throughput — use cluster totals when available
      var tp = d.throughput || {};
      var clpT = d.clusterProgress;
      $('t-read').textContent = fmt(clpT ? clpT.docsRead : tp.sourceDocsReadTotal);
      $('t-inserted').textContent = fmt(clpT ? clpT.rowsInserted : tp.rowsInsertedTotal);
      $('t-skipped').textContent = fmt(tp.docsSkippedTotal);
      var integ = d.integrity || {};
      $('t-digest').textContent = fmt(integ.digestMismatches);
      $('t-dups').textContent = fmt(integ.estimatedDuplicateRows);
      $('t-bfail').textContent = fmt(integ.batchesFailed);
      $('t-digest').style.color = integ.digestMismatches > 0 ? 'var(--yellow)' : 'var(--text)';
      $('t-bfail').style.color = integ.batchesFailed > 0 ? 'var(--red)' : 'var(--text)';

      // Skip reasons
      renderSkipReasons(d.skipReasons);

      // System
      var proc = d.process || {};
      var rssBytes = proc.rssBytes || 0;
      var heapBytes = proc.heapUsedBytes || 0;
      var heapTotal = proc.heapTotalBytes || 1;
      var maxRss = 2048 * 1024 * 1024;
      $('mem-rss').style.width = Math.min(100, (rssBytes / maxRss) * 100) + '%';
      $('mem-rss-val').textContent = mb(rssBytes);
      $('mem-heap').style.width = Math.min(100, (heapBytes / heapTotal) * 100) + '%';
      $('mem-heap-val').textContent = mb(heapBytes) + ' / ' + mb(heapTotal);
      var gc = d.gc || {};
      $('gc-line').textContent = 'GC: ' + (gc.gcCountTotal || 0) + ' runs, last ' + (gc.lastGcDurationMs || 0).toFixed(1) + 'ms | CPU: ' + (proc.cpuUserSec || 0).toFixed(0) + 's user';
      $('gc-detail').textContent = 'State: ' + (gc.gcState || 'n/a') + ' | Available: ' + (gc.gcAvailable ? 'yes' : 'no') + ' | Observed: ' + (gc.observedGcCount || 0) + ' | EL lag p95: ' + (proc.eventLoopLagMs_p95_1m || 0).toFixed(1) + 'ms';

      // Run details
      var run = d.run || {};
      renderKV('run-details', [
        ['Source', run.sourceNs || 'n/a'],
        ['Target', run.targetTable || 'n/a'],
        ['Run ID', (d.service && d.service.runId) || 'n/a'],
        ['Transform', run.transformVersion || 'n/a'],
        ['Batch Committed', String(run.batchSeqCommitted || 0)],
        ['PID', String((d.service && d.service.pid) || '-')],
        ['Manifest DB', (d.manifest && d.manifest.db) || 'n/a'],
        ['Last Checkpoint', (d.manifest && d.manifest.lastCheckpointTime) || 'n/a'],
      ]);

      // Infrastructure
      var mongo = d.mongo || {};
      var ch = d.clickhouse || {};
      var redis = d.redis || {};
      renderKV('infra-details', [
        ['MongoDB', mongo.connected ? 'Connected' : 'Disconnected', mongo.connected ? 'var(--green)' : 'var(--red)'],
        ['Read Pref / Concern', (mongo.readPreference || '-') + ' / ' + (mongo.readConcern || '-')],
        ['Batch Target', fmt(mongo.batchRowsTarget) + ' rows'],
        ['ClickHouse', ch.connected ? 'Connected' : 'Disconnected', ch.connected ? 'var(--green)' : 'var(--red)'],
        ['CH Target', ch.target || '-'],
        ['CH Parts Limit', (ch.partsToThrowInsert || '-') + ' / ' + (ch.maxPartsInTotal || '-')],
        ['Redis', redis.connected ? 'Connected' : 'Disconnected', redis.connected ? 'var(--green)' : 'var(--red)'],
        ['Redis Write Latency', (redis.lastStateWriteMs || 0) + 'ms'],
        ['Bitmap Bits Set', String(redis.bitmapBitsSet || 0)],
        ['Redis Error', redis.lastError || 'none', redis.lastError ? 'var(--red)' : 'var(--text3)'],
      ]);

      // Index status
      var idx = d.indexStatus || { ready: 0, building: 0, checking: 0, failed: 0, details: [] };
      var idxParts = [idx.ready + ' ready'];
      if (idx.checking > 0) idxParts.push(idx.checking + ' checking');
      if (idx.building > 0) idxParts.push(idx.building + ' building');
      if (idx.failed > 0) idxParts.push(idx.failed + ' failed');
      $('idx-summary').textContent = '(' + idxParts.join(', ') + ')';
      var idxContainer = $('idx-details');
      while (idxContainer.firstChild) idxContainer.removeChild(idxContainer.firstChild);
      if (idx.checking === 0 && idx.building === 0 && idx.failed === 0) {
        var allReady = document.createElement('span');
        allReady.style.color = 'var(--green)';
        allReady.textContent = 'All indexes ready';
        idxContainer.appendChild(allReady);
      } else {
        if (idx.checking > 0) {
          var checkMsg = document.createElement('div');
          checkMsg.style.cssText = 'color:var(--cyan);font-size:11px;margin-bottom:4px';
          checkMsg.textContent = 'Checking for indexed collections... (' + idx.checking + ' remaining)';
          idxContainer.appendChild(checkMsg);
        }
        for (var ii = 0; ii < idx.details.length; ii++) {
          var det = idx.details[ii];
          var detRow = document.createElement('div');
          detRow.style.cssText = 'display:flex;align-items:center;justify-content:space-between;padding:2px 0';
          var detName = document.createElement('span');
          detName.style.cssText = 'max-width:50%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px';
          detName.textContent = det.collection.length > 50 ? det.collection.slice(0, 20) + '...' + det.collection.slice(-8) : det.collection;
          detRow.appendChild(detName);
          var detRight = document.createElement('span');
          detRight.style.cssText = 'display:flex;align-items:center;gap:8px';
          var detStatus = document.createElement('span');
          detStatus.style.fontWeight = '600';
          if (det.status === 'building') {
            detStatus.style.color = 'var(--orange)';
            detStatus.textContent = 'building' + (det.elapsedSec ? ' (' + Math.round(det.elapsedSec / 60) + 'm)' : '');
          } else if (det.status === 'checking') {
            detStatus.style.color = 'var(--cyan)';
            detStatus.textContent = 'checking';
          } else {
            detStatus.style.color = 'var(--red)';
            detStatus.textContent = 'failed';
          }
          detRight.appendChild(detStatus);
          if (det.status === 'failed') {
            var reindexBtn = document.createElement('button');
            reindexBtn.className = 'btn';
            reindexBtn.style.cssText = 'padding:2px 8px;font-size:10px;border-color:var(--orange);color:var(--orange)';
            reindexBtn.textContent = 'Reindex';
            reindexBtn.setAttribute('data-collection', det.collection);
            reindexBtn.addEventListener('click', function() {
              var coll = this.getAttribute('data-collection');
              fetch('/control/reindex/' + encodeURIComponent(coll), { method: 'POST' });
            });
            detRight.appendChild(reindexBtn);
          }
          detRow.appendChild(detRight);
          idxContainer.appendChild(detRow);
        }
      }

      // Cluster (multi-pod)
      var cluster = d.cluster;
      if (cluster) {
        $('cluster-card').style.display = 'block';
        $('cluster-summary').textContent = '(' + cluster.podCount + ' pods, ' + cluster.locks.length + ' locks)';
        var gcmds = cluster.globalCommands || {};
        $('btn-global-pause').disabled = gcmds.pause;
        $('btn-global-resume').disabled = !gcmds.pause;
        $('btn-global-stop').disabled = gcmds.stop;

        // Cluster progress bar
        if (clp) {
          var cpct = clp.pct || 0;
          $('cluster-bar').style.width = cpct + '%';
          $('cluster-bar-text').textContent = cpct + '%';
          if (cpct >= 100) $('cluster-bar').classList.add('green');
          else $('cluster-bar').classList.remove('green');
          $('cluster-progress-label').textContent = clp.done + ' / ' + clp.total + ' collections done';
          $('cluster-progress-pct').textContent = cpct + '%';
          $('cl-docs').textContent = fmt(clp.docsRead);
          $('cl-rows').textContent = fmt(clp.rowsInserted);
          $('cl-processing').textContent = String(clp.processing || 0);
          $('cl-pending').textContent = String(clp.pending || 0);
        }

        // Pods (compact rows with progress bars)
        var podContainer = $('cluster-pods');
        while (podContainer.firstChild) podContainer.removeChild(podContainer.firstChild);
        var pods = cluster.pods || [];
        // Total docs across all pods for proportional bars
        var totalPodDocs = 0;
        for (var pj = 0; pj < pods.length; pj++) totalPodDocs += (pods[pj].stats || {}).docsRead || 0;
        for (var pi = 0; pi < pods.length; pi++) {
          var pod = pods[pi];
          var ps = pod.stats || {};
          var podDocsRead = ps.docsRead || 0;
          // Show pod's share as % of total work done (not total estimated)
          var podPct = totalPodDocs > 0 ? Math.min(100, Math.round((podDocsRead / totalPodDocs) * 100)) : 0;
          var podActive = (pod.collectionsActive || []);
          var podRow = document.createElement('div');
          podRow.style.cssText = 'background:var(--bg2);border:1px solid var(--border);border-radius:4px;padding:8px 10px;margin-bottom:6px';
          podRow.innerHTML = '<div style="display:flex;align-items:center;gap:8px">'
            + '<span class="health-dot dot-green" style="width:6px;height:6px"></span>'
            + '<span style="font-weight:600;font-size:12px;min-width:60px">' + pod.podId + '</span>'
            + '<div class="progress-outer" style="flex:1;height:8px"><div class="progress-fill" style="width:' + podPct + '%"></div></div>'
            + '<span style="font-size:10px;font-weight:600;min-width:28px">' + podPct + '%</span>'
            + '<span style="font-size:10px;color:var(--text2)">'
              + fmt(podDocsRead) + ' docs · '
              + fmt(ps.rowsInserted || 0) + ' rows · '
              + podActive.length + ' active'
            + '</span>'
            + '</div>';
          podContainer.appendChild(podRow);
        }
        if (pods.length === 0) {
          var noPods = document.createElement('span');
          noPods.className = 'muted';
          noPods.textContent = 'No pods reporting';
          podContainer.appendChild(noPods);
        }

        // Active Locks table
        var locks = cluster.locks || [];
        $('lock-count').textContent = '(' + locks.length + ')';
        var lockBody = $('lock-body');
        while (lockBody.firstChild) lockBody.removeChild(lockBody.firstChild);
        if (locks.length === 0) {
          var noLockRow = document.createElement('tr');
          var noLockCell = document.createElement('td');
          noLockCell.setAttribute('colspan', '4');
          noLockCell.className = 'muted';
          noLockCell.style.textAlign = 'center';
          noLockCell.textContent = 'No active locks';
          noLockRow.appendChild(noLockCell);
          lockBody.appendChild(noLockRow);
        } else {
          for (var li = 0; li < locks.length; li++) {
            var lk = locks[li];
            var lkRow = document.createElement('tr');
            var lkTd1 = document.createElement('td');
            lkTd1.style.cssText = 'font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap';
            lkTd1.textContent = truncHash(lk.collectionName);
            lkTd1.setAttribute('title', lk.collectionName);
            lkRow.appendChild(lkTd1);
            var lkTd2 = document.createElement('td');
            lkTd2.style.fontSize = '11px';
            lkTd2.textContent = lk.podId;
            lkRow.appendChild(lkTd2);
            var lkTd3 = document.createElement('td');
            lkTd3.style.cssText = 'font-size:10px;color:var(--text2)';
            lkTd3.textContent = lk.acquiredAt ? new Date(lk.acquiredAt).toLocaleTimeString() : '-';
            lkRow.appendChild(lkTd3);
            var lkTd4 = document.createElement('td');
            var relBtn = document.createElement('button');
            relBtn.className = 'btn';
            relBtn.style.cssText = 'padding:2px 8px;font-size:10px;border-color:var(--red);color:var(--red)';
            relBtn.textContent = 'Release';
            relBtn.setAttribute('data-lock-coll', lk.collectionName);
            relBtn.addEventListener('click', function() {
              var coll = this.getAttribute('data-lock-coll');
              fetch('/control/locks/release/' + encodeURIComponent(coll), { method: 'POST' });
            });
            lkTd4.appendChild(relBtn);
            lkRow.appendChild(lkTd4);
            lockBody.appendChild(lkRow);
          }
        }

        // Stale Pods
        var stalePods = cluster.stalePods || [];
        var staleSection = $('stale-pods-section');
        if (stalePods.length > 0) {
          staleSection.style.display = 'block';
          var staleList = $('stale-pods-list');
          while (staleList.firstChild) staleList.removeChild(staleList.firstChild);
          for (var si = 0; si < stalePods.length; si++) {
            var sp = stalePods[si];
            var staleLocks = locks.filter(function(l) { return l.podId === sp; });
            var staleRow = document.createElement('div');
            staleRow.style.cssText = 'display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid var(--border)';
            var staleLeft = document.createElement('span');
            staleLeft.style.cssText = 'display:flex;align-items:center;gap:6px';
            var staleDot = document.createElement('span');
            staleDot.className = 'health-dot dot-red';
            staleDot.style.cssText = 'width:6px;height:6px';
            staleLeft.appendChild(staleDot);
            var staleName = document.createElement('span');
            staleName.style.cssText = 'font-weight:600;color:var(--red)';
            staleName.textContent = sp;
            staleLeft.appendChild(staleName);
            var staleLockCount = document.createElement('span');
            staleLockCount.style.color = 'var(--text2)';
            staleLockCount.textContent = ' (' + staleLocks.length + ' locks)';
            staleLeft.appendChild(staleLockCount);
            staleRow.appendChild(staleLeft);
            var removeBtn = document.createElement('button');
            removeBtn.className = 'btn';
            removeBtn.style.cssText = 'padding:4px 12px;font-size:11px;border-color:var(--red);color:var(--red)';
            removeBtn.textContent = 'Remove Pod';
            removeBtn.setAttribute('data-stale-pod', sp);
            removeBtn.addEventListener('click', function() {
              var pid = this.getAttribute('data-stale-pod');
              fetch('/control/pods/remove/' + encodeURIComponent(pid), { method: 'POST' });
            });
            staleRow.appendChild(removeBtn);
            staleList.appendChild(staleRow);
          }
        } else {
          staleSection.style.display = 'none';
        }

      } else {
        $('cluster-card').style.display = 'none';
      }

      // Live Batches (outside cluster block — works in single-pod mode too)
      var liveBatches = d.liveBatches || [];
      var batchesPanel = $('batches-panel');
      if (batchesPanel) {
        batchesPanel.style.display = liveBatches.length > 0 ? 'block' : 'none';
        $('batches-summary').textContent = '(' + liveBatches.length + ' inflight)';
        var batchesBody = $('batches-body');
        while (batchesBody.firstChild) batchesBody.removeChild(batchesBody.firstChild);
        if (liveBatches.length === 0) {
          var noBatchRow = document.createElement('tr');
          var noBatchCell = document.createElement('td');
          noBatchCell.setAttribute('colspan', '7');
          noBatchCell.className = 'muted';
          noBatchCell.style.textAlign = 'center';
          noBatchCell.textContent = 'No active batches';
          noBatchRow.appendChild(noBatchCell);
          batchesBody.appendChild(noBatchRow);
        } else {
          var phaseColors = { READING: 'var(--blue)', TRANSFORMING: 'var(--cyan)', WRITING: 'var(--yellow)', COMMITTING: 'var(--green)' };
          for (var bi = 0; bi < liveBatches.length; bi++) {
            var lb = liveBatches[bi];
            var elapsed = Math.round((Date.now() - lb.startedAt) / 1000);
            var elStr = elapsed >= 60 ? Math.floor(elapsed / 60) + 'm ' + (elapsed % 60) + 's' : elapsed + 's';
            var phColor = phaseColors[lb.phase] || 'var(--text2)';
            var batchTr = document.createElement('tr');
            batchTr.style.borderBottom = '1px solid var(--border)';
            batchTr.innerHTML = '<td style="font-size:11px;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + lb.collection + '">' + truncHash(lb.collection) + '</td>'
              + '<td style="font-size:11px">' + (lb.podId || '-') + '</td>'
              + '<td style="font-size:11px">#' + lb.batchSeq + '</td>'
              + '<td style="font-size:11px">' + fmt(lb.docsRead) + '</td>'
              + '<td style="font-size:11px">' + (lb.rowsToInsert ? fmt(lb.rowsToInsert) : '—') + '</td>'
              + '<td style="font-size:11px">' + elStr + '</td>'
              + '<td><span style="color:' + phColor + ';font-size:9px;background:var(--bg3);padding:1px 4px;border-radius:2px">' + lb.phase + '</span></td>';
            batchesBody.appendChild(batchTr);
          }
        }
      }

      // Ranges panel (outside cluster block)
      var hasRanges = liveBatches.some(function(b) { return b.rangeIdx !== undefined && b.rangeIdx !== null; });
      var rangesPanel = $('ranges-panel');
      if (rangesPanel) {
        if (hasRanges) {
          rangesPanel.style.display = 'block';
          var rangeCollections = {};
          for (var ri = 0; ri < liveBatches.length; ri++) {
            if (liveBatches[ri].rangeIdx !== undefined) {
              rangeCollections[liveBatches[ri].collection] = true;
            }
          }
          var rcNames = Object.keys(rangeCollections);
          $('ranges-summary').textContent = '(' + rcNames.length + ' collection' + (rcNames.length > 1 ? 's' : '') + ' in range-parallel mode)';
          var rangesContent = $('ranges-content');
          while (rangesContent.firstChild) rangesContent.removeChild(rangesContent.firstChild);
          for (var rci = 0; rci < rcNames.length; rci++) {
            var rcName = rcNames[rci];
            var rcBatches = liveBatches.filter(function(b) { return b.collection === rcName && b.rangeIdx !== undefined; });
            var rcDiv = document.createElement('div');
            rcDiv.style.cssText = 'background:var(--bg2);border:1px solid var(--border);border-radius:4px;padding:10px;margin-bottom:8px';
            rcDiv.innerHTML = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">'
              + '<span style="font-weight:600;font-size:12px">' + truncHash(rcName) + ' <span style="font-size:9px;color:var(--purple);background:var(--bg3);padding:1px 4px;border-radius:2px">RANGE-PARALLEL</span></span>'
              + '<span style="font-size:10px;color:var(--text2)">' + rcBatches.length + ' ranges processing</span>'
              + '</div>';
            rangesContent.appendChild(rcDiv);
          }
        } else {
          rangesPanel.style.display = 'none';
        }
      }

      // Collections
      var orch = d.orchestrator || {};
      $('coll-summary').textContent = '(' + (orch.totalCollections || 0) + ' total: '
        + (orch.completedCollections || 0) + ' done, '
        + (orch.failedCollections || 0) + ' failed, '
        + (orch.skippedCollections || 0) + ' skipped)';
      renderCollections(d);

    }).catch(function(e) {
      $('heartbeat').className = 'heartbeat error';
      console.error('Refresh error:', e);
    });
  }

  // Initial load + auto-refresh
  refresh();
  setInterval(refresh, 2000);
})();
</script>
</body>
</html>`;
