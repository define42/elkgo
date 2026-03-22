package webui

const ClusterPageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>elkgo cluster</title>
  <style>
    :root {
      --bg: #1d1e24;
      --surface: #25262d;
      --surface-alt: #202126;
      --panel: #1a1b20;
      --panel-strong: #16171c;
      --line: #343741;
      --line-strong: #4a4d57;
      --text: #dfe5ef;
      --muted: #a6adbb;
      --accent: #00bfb3;
      --accent-strong: #3ea8cf;
      --accent-soft: rgba(0, 191, 179, 0.16);
      --danger: #f66f6f;
      --danger-soft: rgba(246, 111, 111, 0.14);
      --good: #5ad18a;
      --good-soft: rgba(90, 209, 138, 0.16);
      --warn: #d8b15c;
      --warn-soft: rgba(216, 177, 92, 0.16);
      --shadow: none;
      --radius: 4px;
    }

    * {
      box-sizing: border-box;
    }

    html, body {
      margin: 0;
      min-height: 100%;
      background: var(--bg);
      color: var(--text);
      font-family: "IBM Plex Sans", "Segoe UI", "Trebuchet MS", sans-serif;
    }

    body {
      padding: 0;
    }

    .shell {
      width: 100%;
      min-height: 100vh;
      display: grid;
      gap: 0;
      animation: rise 420ms ease-out both;
    }

    .menu {
      position: sticky;
      top: 0;
      z-index: 10;
      min-height: 52px;
      padding: 0 16px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      background: var(--surface);
      border-bottom: 1px solid var(--line);
    }

    .menu-brand {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
    }

    .brand-badge {
      width: 28px;
      height: 28px;
      border-radius: 5px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      background: #22c7bd;
      color: #081015;
      font-size: 0.82rem;
      font-weight: 700;
      letter-spacing: 0.02em;
      flex: 0 0 auto;
    }

    .brand-copy {
      display: grid;
      gap: 2px;
      min-width: 0;
    }

    .menu-label {
      color: var(--muted);
      letter-spacing: 0.18em;
      text-transform: uppercase;
      font-size: 0.68rem;
      font-weight: 700;
      white-space: nowrap;
    }

    .menu-title {
      font-size: 1rem;
      font-weight: 700;
      white-space: nowrap;
    }

    .menu-links {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      gap: 20px;
    }

    .menu-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 52px;
      padding: 0;
      border: 0;
      border-radius: 0;
      background: transparent;
      color: var(--muted);
      text-decoration: none;
      font-weight: 600;
      transition: color 160ms ease;
    }

    .menu-link:hover {
      color: var(--text);
    }

    .menu-link.active {
      color: var(--text);
    }

    .dashboard-shell {
      display: grid;
      gap: 16px;
      padding: 16px;
      background: var(--bg);
    }

    .card {
      background: var(--surface-alt);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }

    .hero {
      display: grid;
      grid-template-columns: minmax(0, 1.05fr) minmax(320px, 0.95fr);
      gap: 16px;
      align-items: start;
    }

    .intro, .controls, .summary, .retention, .nodes, .routing {
      padding: 16px;
    }

    .eyebrow, .section-title {
      text-transform: uppercase;
      letter-spacing: 0.16em;
      font-size: 0.72rem;
      font-weight: 700;
    }

    .eyebrow {
      color: var(--muted);
      margin-bottom: 10px;
    }

    .section-title {
      color: var(--muted);
      margin-bottom: 12px;
    }

    h1, h2, h3 {
      margin: 0;
      color: var(--text);
    }

    h1 {
      font-size: clamp(1.65rem, 2.7vw, 2.25rem);
      line-height: 1.1;
      margin-bottom: 10px;
    }

    p, .muted, .empty, .status, .hint, label, th, td {
      color: var(--muted);
    }

    code {
      color: var(--text);
      font-family: "IBM Plex Mono", "SFMono-Regular", Consolas, monospace;
    }

    .intro p {
      margin: 0;
      max-width: 62ch;
      line-height: 1.6;
    }

    .nav {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 16px;
    }

    .nav a, button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      min-height: 40px;
      border: 1px solid var(--line-strong);
      border-radius: var(--radius);
      padding: 0 14px;
      text-decoration: none;
      font: inherit;
      cursor: pointer;
      background: var(--panel);
      color: var(--text);
      transition: border-color 160ms ease, background 160ms ease, color 160ms ease;
    }

    .nav a.primary, button.primary {
      border-color: var(--accent-strong);
      color: #081015;
      font-weight: 700;
      background: var(--accent-strong);
    }

    .nav a.secondary, button.secondary {
      border: 1px solid var(--line-strong);
      color: var(--text);
      background: transparent;
    }

    .nav a:hover, button:hover {
      border-color: var(--accent-strong);
      background: var(--panel-strong);
    }

    .nav a.primary:hover, button.primary:hover {
      background: #53b5da;
    }

    .controls-grid, .summary-grid, .nodes-grid {
      display: grid;
      gap: 12px;
    }

    .controls-grid {
      grid-template-columns: repeat(12, minmax(0, 1fr));
      align-items: end;
    }

    label {
      display: grid;
      gap: 6px;
      font-size: 0.82rem;
      color: var(--text);
    }

    label > span {
      color: var(--muted);
    }

    .span-6 { grid-column: span 6; }
    .span-4 { grid-column: span 4; }
    .span-3 { grid-column: span 3; }

    input, select {
      width: 100%;
      border: 1px solid var(--line-strong);
      border-radius: var(--radius);
      padding: 10px 12px;
      font: inherit;
      color: var(--text);
      background: var(--panel);
      transition: border-color 160ms ease, background 160ms ease;
    }

    input:focus, select:focus {
      outline: none;
      border-color: var(--accent-strong);
      background: var(--panel-strong);
    }

    .toggle {
      display: flex;
      align-items: center;
      gap: 10px;
      padding-bottom: 8px;
      min-height: 40px;
    }

    .toggle input {
      width: 16px;
      height: 16px;
      margin: 0;
      accent-color: var(--accent);
    }

    .toggle span {
      color: var(--text);
    }

    .summary-grid {
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    }

    .retention-layout {
      display: grid;
      gap: 16px;
      grid-template-columns: minmax(280px, 340px) minmax(0, 1fr);
      align-items: start;
    }

    .retention-form {
      display: grid;
      gap: 12px;
      padding: 16px;
      border-radius: var(--radius);
      background: var(--panel);
      border: 1px solid var(--line);
    }

    .retention-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }

    .retention-actions button {
      padding: 10px 14px;
    }

    .retention-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .retention-pill {
      display: inline-flex;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid rgba(0, 191, 179, 0.24);
      background: var(--accent-soft);
      color: var(--text);
      font-size: 0.82rem;
    }

    .retention-pill.warn {
      border-color: rgba(216, 177, 92, 0.34);
      background: var(--warn-soft);
    }

    .small-button {
      min-height: 32px;
      padding: 0 10px;
      font-size: 0.88rem;
    }

    .metric {
      padding: 14px;
      border-radius: var(--radius);
      background: var(--panel);
      border: 1px solid var(--line);
    }

    .metric strong {
      display: block;
      font-size: 1.75rem;
      color: var(--text);
      margin-top: 8px;
    }

    .nodes-grid {
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    }

    .node-card {
      padding: 16px;
      border-radius: var(--radius);
      background: var(--panel);
      border: 1px solid var(--line);
      display: grid;
      gap: 12px;
    }

    .node-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 5px 10px;
      font-size: 0.82rem;
      border: 1px solid rgba(62, 168, 207, 0.28);
      background: rgba(62, 168, 207, 0.12);
      color: var(--text);
    }

    .badge.draining-badge {
      border-color: rgba(216, 177, 92, 0.34);
      background: var(--warn-soft);
    }

    .badge.drained-badge {
      border-color: rgba(90, 209, 138, 0.34);
      background: var(--good-soft);
    }

    .badge.primary-badge {
      border-color: rgba(62, 168, 207, 0.34);
      background: rgba(62, 168, 207, 0.14);
    }

    .node-stats {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }

    .node-actions {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }

    .node-actions button {
      padding: 9px 14px;
    }

    .node-stat {
      flex: 1 1 90px;
      padding: 10px 12px;
      border-radius: var(--radius);
      background: var(--panel-strong);
      border: 1px solid var(--line);
    }

    .node-stat strong {
      display: block;
      font-size: 1.05rem;
      color: var(--text);
      margin-top: 4px;
    }

    .placement-list {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .placement-pill {
      padding: 6px 9px;
      border-radius: 999px;
      font-size: 0.8rem;
      border: 1px solid var(--line);
      background: var(--panel-strong);
      color: var(--text);
    }

    .table-wrap {
      overflow: auto;
      border-radius: var(--radius);
      border: 1px solid var(--line);
      background: var(--panel);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 980px;
    }

    th, td {
      text-align: left;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
      font-size: 0.92rem;
    }

    th {
      position: sticky;
      top: 0;
      background: var(--surface);
      color: var(--text);
      font-size: 0.75rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .replicas {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .replica-pill {
      padding: 5px 9px;
      border-radius: 999px;
      background: var(--accent-soft);
      border: 1px solid rgba(0, 191, 179, 0.24);
      color: var(--text);
      font-size: 0.82rem;
    }

    .replica-pill.primary-badge {
      background: rgba(62, 168, 207, 0.14);
      border-color: rgba(62, 168, 207, 0.28);
    }

    .primary-node {
      color: var(--text);
      font-weight: 700;
    }

    .status {
      min-height: 1.4em;
      margin-top: 10px;
      font-size: 0.88rem;
    }

    .status.error {
      color: var(--danger);
    }

    .empty {
      padding: 24px;
      text-align: center;
      border-radius: var(--radius);
      border: 1px dashed var(--line-strong);
      background: var(--panel);
    }

    @keyframes rise {
      from {
        opacity: 0;
        transform: translateY(8px);
      }
      to {
        opacity: 1;
        transform: translateY(0);
      }
    }

    @media (max-width: 980px) {
      .hero,
      .retention-layout {
        grid-template-columns: 1fr;
      }

      .dashboard-shell {
        padding: 12px;
      }

      .menu,
      .menu-links {
        flex-direction: column;
        align-items: stretch;
      }

      .menu-link {
        min-height: 40px;
        justify-content: flex-start;
      }

      .span-6,
      .span-4,
      .span-3 {
        grid-column: span 12;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <header class="menu">
      <div class="menu-brand">
        <span class="brand-badge">C</span>
        <div class="brand-copy">
          <div class="menu-label">elkgo</div>
          <div class="menu-title">Cluster dashboard</div>
        </div>
      </div>
      <nav class="menu-links" aria-label="Primary">
        <a class="menu-link" href="/">Search</a>
        <a class="menu-link active" href="/cluster">Cluster dashboard</a>
      </nav>
    </header>

    <div class="dashboard-shell">
      <section class="hero">
        <article class="card intro">
          <div class="eyebrow">Operations</div>
          <h1>Cluster operations at a glance.</h1>
          <p>
            Review node membership, shard placement, drain state, and retention policies from the live
            <code>/admin/routing</code> and <code>/admin/indexes</code> APIs.
          </p>
          <div class="nav">
            <a class="secondary" href="/admin/routing" target="_blank" rel="noreferrer">Routing JSON</a>
          </div>
        </article>
        <section class="card controls">
          <div class="section-title">Filters</div>
          <div class="controls-grid">
            <label class="span-6">
              <span>Index</span>
              <select id="index-filter">
                <option value="">All indexes</option>
              </select>
            </label>
            <label class="span-6">
              <span>Day</span>
              <select id="day-filter">
                <option value="">All days</option>
              </select>
            </label>
            <label class="span-3 toggle">
              <input id="auto-refresh" type="checkbox" checked>
              <span>Auto refresh</span>
            </label>
            <div class="span-3">
              <button id="refresh-btn" class="primary" type="button">Refresh now</button>
            </div>
          </div>
          <div id="status" class="status" aria-live="polite"></div>
        </section>
      </section>

      <section class="card summary">
        <div class="section-title">Cluster summary</div>
        <div id="summary" class="summary-grid"></div>
      </section>

      <section class="card retention">
        <div class="section-title">Index retention</div>
        <div class="retention-layout">
          <form id="retention-form" class="retention-form">
            <label>
              <span>Index</span>
              <input id="retention-index" list="retention-index-options" type="text" placeholder="events or test1" autocomplete="off">
              <datalist id="retention-index-options"></datalist>
            </label>
            <label>
              <span>Retention days</span>
              <input id="retention-days" type="number" min="1" step="1" placeholder="30">
            </label>
            <div class="retention-actions">
              <button id="retention-apply" class="primary" type="submit">Apply retention</button>
              <button id="retention-clear" class="secondary" type="button">Clear policy</button>
            </div>
            <div class="hint">
              Apply will create or update the policy for that index. Clear removes the policy entry.
            </div>
            <div id="retention-status" class="status" aria-live="polite"></div>
          </form>
          <div id="retention-list"></div>
        </div>
      </section>

      <section class="card nodes">
        <div class="section-title">Nodes</div>
        <div id="nodes" class="nodes-grid"></div>
      </section>

      <section class="card routing">
        <div class="section-title">Shard routing</div>
        <div id="routing"></div>
      </section>
    </div>
  </main>

  <script>
    const summaryEl = document.getElementById("summary");
    const nodesEl = document.getElementById("nodes");
    const routingEl = document.getElementById("routing");
    const statusEl = document.getElementById("status");
    const refreshBtn = document.getElementById("refresh-btn");
    const autoRefreshEl = document.getElementById("auto-refresh");
    const indexFilterEl = document.getElementById("index-filter");
    const dayFilterEl = document.getElementById("day-filter");
    const retentionListEl = document.getElementById("retention-list");
    const retentionFormEl = document.getElementById("retention-form");
    const retentionStatusEl = document.getElementById("retention-status");
    const retentionIndexEl = document.getElementById("retention-index");
    const retentionDaysEl = document.getElementById("retention-days");
    const retentionClearEl = document.getElementById("retention-clear");
    const retentionIndexOptionsEl = document.getElementById("retention-index-options");

    let rawData = null;
    let refreshTimer = null;

    function setStatus(message, isError) {
      statusEl.textContent = message || "";
      statusEl.className = isError ? "status error" : "status";
    }

    function shardSort(a, b) {
      if (a.index_name !== b.index_name) return a.index_name.localeCompare(b.index_name);
      if (a.day !== b.day) return a.day.localeCompare(b.day);
      return a.shard_id - b.shard_id;
    }

    function routingArray(data) {
      return Object.values((data && data.routing) || {}).sort(shardSort);
    }

    function membersArray(data) {
      return Object.values((data && data.members) || {}).sort(function (a, b) {
        return a.id.localeCompare(b.id);
      });
    }

    function indexEntries(data) {
      return ((data && data.indexes) || []).slice().sort(function (a, b) {
        return a.name.localeCompare(b.name);
      });
    }

    function refreshFilters(data) {
      const routes = routingArray(data);
      const currentIndex = indexFilterEl.value;
      const currentDay = dayFilterEl.value;
      const knownIndexes = indexEntries(data).map(function (entry) { return entry.name; });
      const indexes = Array.from(new Set(knownIndexes.concat(routes.map(function (route) { return route.index_name; })))).sort();
      const days = Array.from(new Set(routes.map(function (route) { return route.day; }))).sort();

      indexFilterEl.innerHTML = '<option value="">All indexes</option>';
      dayFilterEl.innerHTML = '<option value="">All days</option>';
      retentionIndexOptionsEl.innerHTML = "";

      indexes.forEach(function (indexName) {
        const option = document.createElement("option");
        option.value = indexName;
        option.textContent = indexName;
        indexFilterEl.appendChild(option);

        const retentionOption = document.createElement("option");
        retentionOption.value = indexName;
        retentionIndexOptionsEl.appendChild(retentionOption);
      });

      days.forEach(function (day) {
        const option = document.createElement("option");
        option.value = day;
        option.textContent = day;
        dayFilterEl.appendChild(option);
      });

      if (indexes.includes(currentIndex)) indexFilterEl.value = currentIndex;
      if (days.includes(currentDay)) dayFilterEl.value = currentDay;
    }

    function filteredRoutes(data) {
      const indexFilter = indexFilterEl.value;
      const dayFilter = dayFilterEl.value;
      return routingArray(data).filter(function (route) {
        if (indexFilter && route.index_name !== indexFilter) return false;
        if (dayFilter && route.day !== dayFilter) return false;
        return true;
      });
    }

    function buildNodePlacements(routes) {
      const placementsByNode = {};
      routes.forEach(function (route) {
        (route.replicas || []).forEach(function (nodeID, idx) {
          if (!placementsByNode[nodeID]) {
            placementsByNode[nodeID] = { total: 0, primary: 0, replica: 0, events: 0, placements: [] };
          }
          placementsByNode[nodeID].total += 1;
          placementsByNode[nodeID].events += Number(route.event_count || 0);
          if (idx === 0) {
            placementsByNode[nodeID].primary += 1;
          } else {
            placementsByNode[nodeID].replica += 1;
          }
          placementsByNode[nodeID].placements.push(route.index_name + "/" + route.day + "/s" + route.shard_id + " · " + Number(route.event_count || 0));
        });
      });
      return placementsByNode;
    }

    function nodeStatus(member, overallStats) {
      if (member && member.drain_requested) {
        if ((overallStats && overallStats.total) > 0) {
          return {
            label: "draining",
            badgeClass: "badge draining-badge",
            actionLabel: "Resume",
            actionDrain: false,
            note: overallStats.total + " shard copies still assigned"
          };
        }
        return {
          label: "drained",
          badgeClass: "badge drained-badge",
          actionLabel: "Resume",
          actionDrain: false,
          note: "All shard copies moved away"
        };
      }
      return {
        label: "active",
        badgeClass: "badge",
        actionLabel: "Drain",
        actionDrain: true,
        note: "Available for shard allocation"
      };
    }

    function renderSummary(data) {
      const routes = filteredRoutes(data);
      const members = membersArray(data);
      const visibleIndexes = new Set(routes.map(function (route) { return route.index_name; }));
      const days = new Set(routes.map(function (route) { return route.day; }));
      const copies = routes.reduce(function (sum, route) {
        return sum + (Array.isArray(route.replicas) ? route.replicas.length : 0);
      }, 0);
      const events = routes.reduce(function (sum, route) {
        return sum + Number(route.event_count || 0);
      }, 0);
      const policies = indexEntries(data).filter(function (entry) { return Number(entry.retention_days || 0) > 0; }).length;

      const metrics = [
        { label: "Visible nodes", value: String(members.length) },
        { label: "Shard routes", value: String(routes.length) },
        { label: "Indexes", value: String(visibleIndexes.size) },
        { label: "Shard copies", value: String(copies) },
        { label: "Events", value: String(events) },
        { label: "Retention policies", value: String(policies) }
      ];

      summaryEl.innerHTML = metrics.map(function (metric) {
        return '<div class="metric"><div class="muted">' + metric.label + '</div><strong>' + metric.value + '</strong></div>';
      }).join("");
    }

    function formatBytes(sizeBytes) {
      const size = Number(sizeBytes || 0);
      if (!Number.isFinite(size) || size <= 0) return "0 B";

      const units = ["B", "KB", "MB", "GB", "TB"];
      let value = size;
      let unitIndex = 0;
      while (value >= 1024 && unitIndex < units.length - 1) {
        value /= 1024;
        unitIndex += 1;
      }
      const digits = value >= 100 || unitIndex === 0 ? 0 : value >= 10 ? 1 : 2;
      return value.toFixed(digits) + " " + units[unitIndex];
    }

    function indexRouteStats(data) {
      const stats = {};
      routingArray(data).forEach(function (route) {
        const key = route.index_name;
        if (!stats[key]) {
          stats[key] = { sizeBytes: 0, eventCount: 0, partial: false };
        }
        stats[key].sizeBytes += Number(route.size_bytes || 0);
        stats[key].eventCount += Number(route.event_count || 0);
        if (route.count_error) stats[key].partial = true;
      });
      return stats;
    }

    function fillRetentionForm(indexName, retentionDays) {
      retentionIndexEl.value = indexName || "";
      retentionDaysEl.value = retentionDays ? String(retentionDays) : "";
      if (indexName) retentionIndexEl.focus();
    }

    function renderRetention(data) {
      const entries = indexEntries(data);
      const routeStats = indexRouteStats(data);
      if (entries.length === 0) {
        retentionListEl.innerHTML = '<div class="empty">No indexes or retention policies are available yet.</div>';
        return;
      }

      const rows = entries.map(function (entry) {
        const days = Array.isArray(entry.days) ? entry.days : [];
        const retentionDays = Number(entry.retention_days || 0);
        const stats = routeStats[entry.name] || { sizeBytes: 0, eventCount: 0, partial: false };
        const daySummary = days.length === 0 ? "No routed days" : (days.length + " routed day" + (days.length === 1 ? "" : "s"));
        const retentionLabel = retentionDays > 0 ? (retentionDays + " days") : "Not set";
        const retentionClass = retentionDays > 0 ? "retention-pill" : "retention-pill warn";
        const latestDay = days.length > 0 ? days[days.length - 1] : "";
        const sizeSummary = formatBytes(stats.sizeBytes);
        const sizeDetail = stats.partial ? '<div class="muted">Partial from available shards</div>' : '<div class="muted">' + stats.eventCount + ' events</div>';

        return '' +
          '<tr>' +
            '<td><strong>' + entry.name + '</strong></td>' +
            '<td>' + daySummary + (latestDay ? '<div class="muted">Latest: ' + latestDay + '</div>' : '') + '</td>' +
            '<td><strong>' + sizeSummary + '</strong>' + sizeDetail + '</td>' +
            '<td><span class="' + retentionClass + '">' + retentionLabel + '</span></td>' +
            '<td><button class="secondary small-button" type="button" data-edit-index="' + entry.name + '" data-edit-retention="' + retentionDays + '">Edit</button></td>' +
          '</tr>';
      }).join("");

      retentionListEl.innerHTML = '' +
        '<div class="table-wrap">' +
          '<table>' +
            '<thead>' +
              '<tr>' +
                '<th>Index</th>' +
                '<th>Coverage</th>' +
                '<th>Size</th>' +
                '<th>Retention</th>' +
                '<th>Action</th>' +
              '</tr>' +
            '</thead>' +
            '<tbody>' + rows + '</tbody>' +
          '</table>' +
        '</div>';
    }

    function renderNodes(data) {
      const routes = filteredRoutes(data);
      const allRoutes = routingArray(data);
      const members = membersArray(data);
      const placementsByNode = buildNodePlacements(routes);
      const overallPlacementsByNode = buildNodePlacements(allRoutes);

      if (members.length === 0) {
        nodesEl.innerHTML = '<div class="empty">No nodes are currently registered.</div>';
        return;
      }

      nodesEl.innerHTML = members.map(function (member) {
        const stats = placementsByNode[member.id] || { total: 0, primary: 0, replica: 0, events: 0, placements: [] };
        const overallStats = overallPlacementsByNode[member.id] || { total: 0, primary: 0, replica: 0, events: 0, placements: [] };
        const status = nodeStatus(member, overallStats);
        const placementPreview = stats.placements.slice(0, 8).map(function (placement) {
          return '<span class="placement-pill">' + placement + '</span>';
        }).join("");
        const overflow = stats.placements.length > 8 ? '<span class="placement-pill">+' + (stats.placements.length - 8) + ' more</span>' : "";

        return '' +
          '<article class="node-card">' +
            '<div class="node-head">' +
              '<div>' +
                '<h3>' + member.id + '</h3>' +
                '<div class="muted">' + member.addr + '</div>' +
              '</div>' +
              '<span class="' + status.badgeClass + '">' + status.label + '</span>' +
            '</div>' +
            '<div class="node-stats">' +
              '<div class="node-stat"><div class="muted">Shard copies</div><strong>' + stats.total + '</strong></div>' +
              '<div class="node-stat"><div class="muted">Primary</div><strong>' + stats.primary + '</strong></div>' +
              '<div class="node-stat"><div class="muted">Replica</div><strong>' + stats.replica + '</strong></div>' +
              '<div class="node-stat"><div class="muted">Events</div><strong>' + stats.events + '</strong></div>' +
            '</div>' +
            '<div class="node-actions">' +
              '<div class="muted">' + status.note + '</div>' +
              '<button class="' + (status.actionDrain ? 'secondary' : 'primary') + '" type="button" data-node-id="' + member.id + '" data-drain="' + (status.actionDrain ? '1' : '0') + '">' + status.actionLabel + '</button>' +
            '</div>' +
            '<div class="placement-list">' + (placementPreview || '<span class="muted">No routed shards for current filter.</span>') + overflow + '</div>' +
          '</article>';
      }).join("");
    }

    function renderRouting(data) {
      const routes = filteredRoutes(data);
      if (routes.length === 0) {
        routingEl.innerHTML = '<div class="empty">No shard routes match the current filters.</div>';
        return;
      }

      const rows = routes.map(function (route) {
        const replicas = Array.isArray(route.replicas) ? route.replicas : [];
        const primary = replicas[0] || "unassigned";
        const replicaHTML = replicas.map(function (nodeID, idx) {
          const className = idx === 0 ? 'replica-pill primary-badge' : 'replica-pill';
          return '<span class="' + className + '">' + nodeID + '</span>';
        }).join("");
        const countText = route.count_error ? ('unavailable: ' + route.count_error) : String(Number(route.event_count || 0));
        return '' +
          '<tr>' +
            '<td><strong>' + route.index_name + '</strong></td>' +
            '<td>' + route.day + '</td>' +
            '<td>' + route.shard_id + '</td>' +
            '<td>' + countText + '</td>' +
            '<td><span class="primary-node">' + primary + '</span></td>' +
            '<td><div class="replicas">' + replicaHTML + '</div></td>' +
            '<td>' + (route.updated_at || "") + '</td>' +
          '</tr>';
      }).join("");

      routingEl.innerHTML = '' +
        '<div class="table-wrap">' +
          '<table>' +
            '<thead>' +
              '<tr>' +
                '<th>Index</th>' +
                '<th>Day</th>' +
                '<th>Shard</th>' +
                '<th>Events</th>' +
                '<th>Primary</th>' +
                '<th>Replicas</th>' +
                '<th>Updated</th>' +
              '</tr>' +
            '</thead>' +
            '<tbody>' + rows + '</tbody>' +
          '</table>' +
        '</div>';
    }

    function renderAll(data) {
      refreshFilters(data);
      renderSummary(data);
      renderRetention(data);
      renderNodes(data);
      renderRouting(data);
    }

    async function readError(response) {
      const text = await response.text();
      return text || ("Request failed with status " + response.status);
    }

    async function loadClusterState() {
      setStatus("Loading cluster state...");
      try {
        const responses = await Promise.all([
          fetch("/admin/routing?stats=1", {
            headers: { "Accept": "application/json" }
          }),
          fetch("/admin/indexes", {
            headers: { "Accept": "application/json" }
          })
        ]);
        if (!responses[0].ok) {
          throw new Error(await readError(responses[0]));
        }
        if (!responses[1].ok) {
          throw new Error(await readError(responses[1]));
        }
        const routingData = await responses[0].json();
        const indexData = await responses[1].json();
        rawData = Object.assign({}, routingData, { indexes: Array.isArray(indexData.indexes) ? indexData.indexes : [] });
        renderAll(rawData);
        setStatus("Cluster state refreshed.");
      } catch (error) {
        setStatus(error.message || "Failed to load cluster state.", true);
      }
    }

    async function updateNodeDrain(nodeID, drainRequested, button) {
      button.disabled = true;
      setStatus((drainRequested ? "Requesting drain for " : "Resuming ") + nodeID + "...");
      try {
        const response = await fetch("/admin/nodes/drain?node_id=" + encodeURIComponent(nodeID) + "&drain=" + (drainRequested ? "1" : "0"), {
          method: "POST",
          headers: { "Accept": "application/json" }
        });
        if (!response.ok) {
          const message = await response.text();
          throw new Error(message || ("Request failed with status " + response.status));
        }
        await loadClusterState();
        setStatus((drainRequested ? "Drain requested for " : "Drain cleared for ") + nodeID + ".");
      } catch (error) {
        setStatus(error.message || "Failed to update node drain state.", true);
      } finally {
        button.disabled = false;
      }
    }

    function setRetentionStatus(message, isError) {
      retentionStatusEl.textContent = message || "";
      retentionStatusEl.className = isError ? "status error" : "status";
    }

    async function applyIndexRetention(event) {
      event.preventDefault();

      const indexName = retentionIndexEl.value.trim();
      const retentionDays = Number(retentionDaysEl.value);
      if (!indexName) {
        setRetentionStatus("Choose or enter an index name.", true);
        return;
      }
      if (!Number.isInteger(retentionDays) || retentionDays <= 0) {
        setRetentionStatus("Retention days must be a positive whole number.", true);
        return;
      }

      setRetentionStatus("Saving retention for " + indexName + "...");
      try {
        const response = await fetch("/admin/index_retention?index=" + encodeURIComponent(indexName) + "&retention_days=" + encodeURIComponent(String(retentionDays)), {
          method: "POST",
          headers: { "Accept": "application/json" }
        });
        if (!response.ok) {
          throw new Error(await readError(response));
        }
        await loadClusterState();
        fillRetentionForm(indexName, retentionDays);
        setRetentionStatus("Retention updated for " + indexName + ".");
      } catch (error) {
        setRetentionStatus(error.message || "Failed to update retention.", true);
      }
    }

    async function clearIndexRetention() {
      const indexName = retentionIndexEl.value.trim();
      if (!indexName) {
        setRetentionStatus("Enter an index name to clear.", true);
        return;
      }

      setRetentionStatus("Clearing retention for " + indexName + "...");
      try {
        const response = await fetch("/admin/index_retention?index=" + encodeURIComponent(indexName), {
          method: "DELETE",
          headers: { "Accept": "application/json" }
        });
        if (!response.ok) {
          throw new Error(await readError(response));
        }
        await loadClusterState();
        fillRetentionForm(indexName, "");
        setRetentionStatus("Retention cleared for " + indexName + ".");
      } catch (error) {
        setRetentionStatus(error.message || "Failed to clear retention.", true);
      }
    }

    function ensureRefreshLoop() {
      if (refreshTimer) {
        clearInterval(refreshTimer);
        refreshTimer = null;
      }
      if (autoRefreshEl.checked) {
        refreshTimer = setInterval(loadClusterState, 5000);
      }
    }

    refreshBtn.addEventListener("click", loadClusterState);
    autoRefreshEl.addEventListener("change", ensureRefreshLoop);
    indexFilterEl.addEventListener("change", function () { if (rawData) renderAll(rawData); });
    dayFilterEl.addEventListener("change", function () { if (rawData) renderAll(rawData); });
    retentionFormEl.addEventListener("submit", applyIndexRetention);
    retentionClearEl.addEventListener("click", clearIndexRetention);
    nodesEl.addEventListener("click", function (event) {
      const button = event.target.closest("button[data-node-id]");
      if (!button) return;
      updateNodeDrain(button.dataset.nodeId, button.dataset.drain === "1", button);
    });
    retentionListEl.addEventListener("click", function (event) {
      const button = event.target.closest("button[data-edit-index]");
      if (!button) return;
      const indexName = button.dataset.editIndex || "";
      const retentionDays = Number(button.dataset.editRetention || 0);
      fillRetentionForm(indexName, retentionDays > 0 ? retentionDays : "");
      setRetentionStatus("Editing retention for " + indexName + ".");
    });

    ensureRefreshLoop();
    loadClusterState();
  </script>
</body>
</html>
`
