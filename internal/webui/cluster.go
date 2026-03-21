package webui

const ClusterPageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>elkgo cluster</title>
  <style>
    :root {
      --bg: #081117;
      --bg-soft: #0d1821;
      --panel: rgba(10, 24, 32, 0.88);
      --panel-strong: rgba(7, 18, 26, 0.96);
      --line: rgba(116, 205, 196, 0.15);
      --text: #eef8f6;
      --muted: #a7c1bb;
      --accent: #74d9cc;
      --accent-2: #f8be74;
      --good: #8df0b7;
      --warn: #ffd485;
      --shadow: 0 28px 80px rgba(0, 0, 0, 0.32);
      --radius: 22px;
    }

    * { box-sizing: border-box; }

    html, body {
      margin: 0;
      min-height: 100%;
      color: var(--text);
      font-family: "Avenir Next", "Segoe UI", "Trebuchet MS", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(116, 217, 204, 0.16), transparent 28%),
        radial-gradient(circle at top right, rgba(248, 190, 116, 0.13), transparent 22%),
        linear-gradient(180deg, #071017 0%, #0a1620 48%, #081117 100%);
    }

    body { padding: 28px 18px 48px; }

    .shell {
      width: min(1280px, 100%);
      margin: 0 auto;
      display: grid;
      gap: 20px;
      animation: rise 380ms ease-out both;
    }

    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
    }

    .hero {
      display: grid;
      grid-template-columns: 1.2fr 0.85fr;
      gap: 20px;
      align-items: start;
    }

    .intro, .controls, .summary, .nodes, .routing {
      padding: 24px;
    }

    .eyebrow, .section-title {
      text-transform: uppercase;
      letter-spacing: 0.16em;
      font-size: 12px;
    }

    .eyebrow { color: var(--accent); margin-bottom: 12px; }
    .section-title { color: var(--accent-2); margin-bottom: 14px; }

    h1, h2, h3 { margin: 0; }
    h1 {
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      font-size: clamp(2.1rem, 5vw, 4rem);
      line-height: 0.96;
      margin-bottom: 12px;
    }

    p, .muted, .empty, .status, .hint, label, th, td {
      color: var(--muted);
    }

    .intro p {
      margin: 0;
      max-width: 60ch;
      line-height: 1.65;
    }

    .nav {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 18px;
    }

    .nav a, button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      border-radius: 999px;
      padding: 11px 16px;
      text-decoration: none;
      font: inherit;
      cursor: pointer;
      transition: transform 160ms ease, filter 160ms ease, border-color 160ms ease;
    }

    .nav a.primary, button.primary {
      border: 0;
      color: #042028;
      font-weight: 700;
      background: linear-gradient(135deg, var(--accent) 0%, #56c0df 100%);
      box-shadow: 0 14px 34px rgba(86, 192, 223, 0.24);
    }

    .nav a.secondary, button.secondary {
      border: 1px solid rgba(116, 217, 204, 0.22);
      color: var(--text);
      background: transparent;
    }

    .nav a:hover, button:hover {
      transform: translateY(-1px);
      filter: brightness(1.04);
    }

    .controls-grid, .summary-grid, .nodes-grid {
      display: grid;
      gap: 14px;
    }

    .controls-grid {
      grid-template-columns: repeat(12, minmax(0, 1fr));
      align-items: end;
    }

    label {
      display: grid;
      gap: 8px;
      font-size: 0.95rem;
    }

    .span-6 { grid-column: span 6; }
    .span-4 { grid-column: span 4; }
    .span-3 { grid-column: span 3; }

    input, select {
      width: 100%;
      border: 1px solid rgba(116, 217, 204, 0.18);
      border-radius: 14px;
      padding: 13px 14px;
      font: inherit;
      color: var(--text);
      background: rgba(4, 15, 21, 0.76);
    }

    input:focus, select:focus {
      outline: none;
      border-color: var(--accent);
      background: rgba(4, 18, 25, 0.96);
    }

    .toggle {
      display: flex;
      align-items: center;
      gap: 10px;
      padding-bottom: 10px;
    }

    .summary-grid {
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }

    .metric {
      padding: 18px;
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.03);
      border: 1px solid rgba(255, 255, 255, 0.05);
    }

    .metric strong {
      display: block;
      font-size: 2rem;
      color: var(--text);
      margin-top: 8px;
    }

    .nodes-grid {
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    }

    .node-card {
      padding: 20px;
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.03);
      border: 1px solid rgba(255, 255, 255, 0.05);
      display: grid;
      gap: 14px;
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
      padding: 6px 10px;
      font-size: 0.85rem;
      border: 1px solid rgba(116, 217, 204, 0.18);
      background: rgba(116, 217, 204, 0.08);
      color: var(--text);
    }

    .badge.primary-badge {
      border-color: rgba(248, 190, 116, 0.24);
      background: rgba(248, 190, 116, 0.11);
    }

    .node-stats {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }

    .node-stat {
      flex: 1 1 90px;
      padding: 10px 12px;
      border-radius: 14px;
      background: rgba(1, 10, 15, 0.46);
      border: 1px solid rgba(255, 255, 255, 0.04);
    }

    .node-stat strong {
      display: block;
      font-size: 1.15rem;
      color: var(--text);
      margin-top: 4px;
    }

    .placement-list {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .placement-pill {
      padding: 7px 10px;
      border-radius: 999px;
      font-size: 0.84rem;
      border: 1px solid rgba(255, 255, 255, 0.05);
      background: rgba(255, 255, 255, 0.04);
      color: var(--text);
    }

    .table-wrap {
      overflow: auto;
      border-radius: 18px;
      border: 1px solid rgba(255, 255, 255, 0.05);
      background: rgba(1, 10, 15, 0.46);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 880px;
    }

    th, td {
      text-align: left;
      padding: 14px 16px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
      vertical-align: top;
    }

    th {
      position: sticky;
      top: 0;
      background: rgba(7, 18, 26, 0.98);
      color: var(--text);
      font-size: 0.9rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .replicas {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .replica-pill {
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(116, 217, 204, 0.08);
      border: 1px solid rgba(116, 217, 204, 0.14);
      color: var(--text);
      font-size: 0.85rem;
    }

    .replica-pill.primary-badge {
      background: rgba(248, 190, 116, 0.12);
      border-color: rgba(248, 190, 116, 0.24);
    }

    .primary-node {
      color: var(--good);
      font-weight: 700;
    }

    .status {
      min-height: 1.4em;
      margin-top: 10px;
    }

    .status.error { color: #ffb19c; }

    .empty {
      padding: 26px;
      text-align: center;
      border-radius: 18px;
      border: 1px dashed rgba(116, 217, 204, 0.18);
      background: rgba(255, 255, 255, 0.02);
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
      .hero, .summary-grid { grid-template-columns: 1fr; }
      .span-6, .span-4, .span-3 { grid-column: span 12; }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <article class="card intro">
        <div class="eyebrow">Cluster dashboard</div>
        <h1>See every node and every shard placement.</h1>
        <p>
          This view is meant to feel closer to a lightweight Cerebro-style routing console.
          It shows registered nodes, routing coverage, primary ownership, and replica placement
          using the live <code>/admin/routing</code> API.
        </p>
        <div class="nav">
          <a class="primary" href="/">Search UI</a>
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

    <section class="card nodes">
      <div class="section-title">Nodes</div>
      <div id="nodes" class="nodes-grid"></div>
    </section>

    <section class="card routing">
      <div class="section-title">Shard routing</div>
      <div id="routing"></div>
    </section>
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

    function refreshFilters(data) {
      const routes = routingArray(data);
      const currentIndex = indexFilterEl.value;
      const currentDay = dayFilterEl.value;
      const indexes = Array.from(new Set(routes.map(function (route) { return route.index_name; }))).sort();
      const days = Array.from(new Set(routes.map(function (route) { return route.day; }))).sort();

      indexFilterEl.innerHTML = '<option value="">All indexes</option>';
      dayFilterEl.innerHTML = '<option value="">All days</option>';

      indexes.forEach(function (indexName) {
        const option = document.createElement("option");
        option.value = indexName;
        option.textContent = indexName;
        indexFilterEl.appendChild(option);
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

    function renderSummary(data) {
      const routes = filteredRoutes(data);
      const members = membersArray(data);
      const indexes = new Set(routes.map(function (route) { return route.index_name; }));
      const days = new Set(routes.map(function (route) { return route.day; }));
      const copies = routes.reduce(function (sum, route) {
        return sum + (Array.isArray(route.replicas) ? route.replicas.length : 0);
      }, 0);

      const metrics = [
        { label: "Visible nodes", value: String(members.length) },
        { label: "Shard routes", value: String(routes.length) },
        { label: "Indexes", value: String(indexes.size) },
        { label: "Shard copies", value: String(copies) }
      ];

      summaryEl.innerHTML = metrics.map(function (metric) {
        return '<div class="metric"><div class="muted">' + metric.label + '</div><strong>' + metric.value + '</strong></div>';
      }).join("");
    }

    function renderNodes(data) {
      const routes = filteredRoutes(data);
      const members = membersArray(data);
      const placementsByNode = {};

      members.forEach(function (member) {
        placementsByNode[member.id] = {
          total: 0,
          primary: 0,
          replica: 0,
          placements: []
        };
      });

      routes.forEach(function (route) {
        (route.replicas || []).forEach(function (nodeID, idx) {
          if (!placementsByNode[nodeID]) {
            placementsByNode[nodeID] = { total: 0, primary: 0, replica: 0, placements: [] };
          }
          placementsByNode[nodeID].total += 1;
          if (idx === 0) {
            placementsByNode[nodeID].primary += 1;
          } else {
            placementsByNode[nodeID].replica += 1;
          }
          placementsByNode[nodeID].placements.push(route.index_name + "/" + route.day + "/s" + route.shard_id);
        });
      });

      if (members.length === 0) {
        nodesEl.innerHTML = '<div class="empty">No nodes are currently registered.</div>';
        return;
      }

      nodesEl.innerHTML = members.map(function (member) {
        const stats = placementsByNode[member.id] || { total: 0, primary: 0, replica: 0, placements: [] };
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
              '<span class="badge">active</span>' +
            '</div>' +
            '<div class="node-stats">' +
              '<div class="node-stat"><div class="muted">Shard copies</div><strong>' + stats.total + '</strong></div>' +
              '<div class="node-stat"><div class="muted">Primary</div><strong>' + stats.primary + '</strong></div>' +
              '<div class="node-stat"><div class="muted">Replica</div><strong>' + stats.replica + '</strong></div>' +
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
        return '' +
          '<tr>' +
            '<td><strong>' + route.index_name + '</strong></td>' +
            '<td>' + route.day + '</td>' +
            '<td>' + route.shard_id + '</td>' +
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
      renderNodes(data);
      renderRouting(data);
    }

    async function loadClusterState() {
      setStatus("Loading cluster state...");
      try {
        const response = await fetch("/admin/routing", {
          headers: { "Accept": "application/json" }
        });
        if (!response.ok) {
          const message = await response.text();
          throw new Error(message || ("Request failed with status " + response.status));
        }
        rawData = await response.json();
        renderAll(rawData);
        setStatus("Cluster state refreshed.");
      } catch (error) {
        setStatus(error.message || "Failed to load cluster state.", true);
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

    ensureRefreshLoop();
    loadClusterState();
  </script>
</body>
</html>
`
