package webui

const HomePageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>elkgo search</title>
  <style>
    :root {
      --bg: #08131a;
      --panel: rgba(9, 29, 39, 0.82);
      --panel-strong: rgba(8, 24, 33, 0.94);
      --line: rgba(115, 184, 170, 0.2);
      --text: #ecf6f3;
      --muted: #9db8b1;
      --accent: #7fe7c1;
      --accent-strong: #f2c66d;
      --danger: #ff9d85;
      --shadow: 0 24px 80px rgba(0, 0, 0, 0.35);
      --radius: 22px;
    }

    * {
      box-sizing: border-box;
    }

    html, body {
      margin: 0;
      min-height: 100%;
      background:
        radial-gradient(circle at top left, rgba(127, 231, 193, 0.16), transparent 30%),
        radial-gradient(circle at top right, rgba(242, 198, 109, 0.16), transparent 24%),
        linear-gradient(180deg, #071117 0%, #0b1a22 46%, #08131a 100%);
      color: var(--text);
      font-family: "Avenir Next", "Segoe UI", "Trebuchet MS", sans-serif;
    }

    body {
      padding: 32px 18px 64px;
    }

    .shell {
      width: min(1180px, 100%);
      margin: 0 auto;
      animation: rise 420ms ease-out both;
    }

    .hero {
      display: grid;
      gap: 22px;
      grid-template-columns: 1.35fr 0.9fr;
      align-items: start;
      margin-bottom: 22px;
    }

    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }

    .intro {
      padding: 28px;
    }

    .eyebrow {
      letter-spacing: 0.18em;
      text-transform: uppercase;
      color: var(--accent);
      font-size: 12px;
      margin-bottom: 14px;
    }

    h1 {
      margin: 0 0 14px;
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      font-size: clamp(2.2rem, 6vw, 4.4rem);
      line-height: 0.95;
      font-weight: 700;
    }

    .intro p, .sidebar p, .meta, .hint, .status, .empty, .pill {
      color: var(--muted);
    }

    .intro p {
      margin: 0;
      font-size: 1rem;
      line-height: 1.65;
      max-width: 62ch;
    }

    .sidebar {
      padding: 24px;
      display: grid;
      gap: 14px;
    }

    .sidebar h2, .results-head h2 {
      margin: 0;
      font-size: 1rem;
      text-transform: uppercase;
      letter-spacing: 0.14em;
      color: var(--accent-strong);
    }

    .panel {
      padding: 24px;
    }

    form {
      display: grid;
      gap: 16px;
    }

    .grid {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(12, minmax(0, 1fr));
    }

    label {
      display: grid;
      gap: 8px;
      font-size: 0.95rem;
      color: var(--text);
    }

    .span-12 { grid-column: span 12; }
    .span-6 { grid-column: span 6; }
    .span-4 { grid-column: span 4; }
    .span-3 { grid-column: span 3; }

    input, select {
      width: 100%;
      border: 1px solid rgba(127, 231, 193, 0.18);
      background: rgba(4, 15, 21, 0.72);
      color: var(--text);
      border-radius: 14px;
      padding: 14px 15px;
      font: inherit;
      transition: border-color 160ms ease, transform 160ms ease, background 160ms ease;
    }

    input:focus, select:focus {
      outline: none;
      border-color: var(--accent);
      background: rgba(4, 18, 25, 0.92);
      transform: translateY(-1px);
    }

    select {
      appearance: none;
    }

    .actions {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      align-items: center;
    }

    button, .actions a {
      border: 0;
      border-radius: 999px;
      background: linear-gradient(135deg, var(--accent) 0%, #5cc6dc 100%);
      color: #03202a;
      font: inherit;
      font-weight: 700;
      padding: 13px 18px;
      cursor: pointer;
      transition: transform 160ms ease, box-shadow 160ms ease, filter 160ms ease;
      box-shadow: 0 14px 30px rgba(92, 198, 220, 0.22);
      text-decoration: none;
    }

    button:hover, .actions a:hover {
      transform: translateY(-1px);
      filter: brightness(1.04);
    }

    button.secondary, .actions a {
      background: transparent;
      color: var(--text);
      border: 1px solid rgba(127, 231, 193, 0.25);
      box-shadow: none;
    }

    button.secondary {
      background: transparent;
      color: var(--text);
      border: 1px solid rgba(127, 231, 193, 0.25);
      box-shadow: none;
    }

    .status {
      min-height: 1.4em;
      font-size: 0.95rem;
    }

    .status.error {
      color: var(--danger);
    }

    .results {
      margin-top: 22px;
      display: grid;
      gap: 16px;
    }

    .results-head {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 18px;
      padding: 0 4px;
    }

    .pills {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }

    .pill {
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid rgba(255, 255, 255, 0.06);
      font-size: 0.9rem;
    }

    .result-card {
      padding: 22px;
      display: grid;
      gap: 14px;
      animation: rise 300ms ease-out both;
    }

    .result-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 7px 11px;
      background: rgba(127, 231, 193, 0.1);
      border: 1px solid rgba(127, 231, 193, 0.16);
      font-size: 0.9rem;
      color: var(--text);
    }

    .score {
      color: var(--accent-strong);
      font-weight: 700;
    }

    .doc-title {
      font-size: 1.1rem;
      margin: 0;
      color: var(--text);
    }

    pre {
      margin: 0;
      padding: 16px;
      overflow: auto;
      border-radius: 16px;
      background: rgba(1, 10, 15, 0.88);
      border: 1px solid rgba(127, 231, 193, 0.12);
      color: #d8f8ef;
      font-family: "IBM Plex Mono", "SFMono-Regular", Consolas, monospace;
      font-size: 0.9rem;
      line-height: 1.55;
    }

    .empty {
      padding: 28px;
      text-align: center;
      border: 1px dashed rgba(127, 231, 193, 0.18);
      border-radius: var(--radius);
      background: rgba(255, 255, 255, 0.02);
    }

    .errors {
      display: grid;
      gap: 10px;
    }

    .error-item {
      padding: 14px 16px;
      border-radius: 14px;
      border: 1px solid rgba(255, 157, 133, 0.2);
      background: rgba(73, 22, 13, 0.38);
      color: #ffd7ce;
    }

    a {
      color: var(--accent);
    }

    @keyframes rise {
      from {
        opacity: 0;
        transform: translateY(10px);
      }
      to {
        opacity: 1;
        transform: translateY(0);
      }
    }

    @media (max-width: 900px) {
      .hero {
        grid-template-columns: 1fr;
      }

      .span-6, .span-4, .span-3 {
        grid-column: span 12;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <article class="card intro">
        <div class="eyebrow">Distributed search console</div>
        <h1>Search the cluster without leaving the browser.</h1>
        <p>
          This page is a thin UI over the existing <code>/search</code> API. Fill in an index,
          pick a day or a day range, run your query, and inspect the matching documents below.
        </p>
      </article>
      <aside class="card sidebar">
        <h2>Quick notes</h2>
        <p>Use <strong>day</strong> for a single partition, or leave it empty and provide both <strong>day from</strong> and <strong>day to</strong>.</p>
        <p>The page keeps your inputs in the URL, so refreshing or sharing the search state is easy.</p>
        <p><a href="/admin/routing" target="_blank" rel="noreferrer">View routing JSON</a></p>
      </aside>
    </section>

    <section class="card panel">
      <form id="search-form">
        <div class="grid">
          <label class="span-4">
            <span>Index</span>
            <select id="index" name="index" required>
              <option value="">Loading indexes...</option>
            </select>
          </label>
          <label class="span-4">
            <span>Day</span>
            <input id="day" name="day" type="date">
          </label>
          <label class="span-4">
            <span>Top K</span>
            <input id="k" name="k" type="number" min="1" max="1000" value="10">
          </label>
          <label class="span-6">
            <span>Day From</span>
            <input id="day_from" name="day_from" type="date">
          </label>
          <label class="span-6">
            <span>Day To</span>
            <input id="day_to" name="day_to" type="date">
          </label>
          <label class="span-12">
            <span>Query</span>
            <input id="q" name="q" placeholder="Optional. Leave blank to match all documents.">
          </label>
        </div>
        <div class="actions">
          <button type="submit">Search cluster</button>
          <button type="button" class="secondary" id="reset-btn">Reset</button>
          <a href="/cluster">Cluster dashboard</a>
          <div class="hint">The UI calls <code>/search</code>. Leave query empty to list all documents for the selected day or range.</div>
        </div>
        <div id="index-catalog" class="hint">Loading available indexes...</div>
        <div id="status" class="status" aria-live="polite"></div>
      </form>
    </section>

    <section class="results">
      <div class="results-head">
        <h2>Results</h2>
        <div id="summary" class="pills"></div>
      </div>
      <div id="errors" class="errors"></div>
      <div id="results"></div>
    </section>
  </main>

  <script>
    const form = document.getElementById("search-form");
    const statusEl = document.getElementById("status");
    const resultsEl = document.getElementById("results");
    const errorsEl = document.getElementById("errors");
    const summaryEl = document.getElementById("summary");
    const resetBtn = document.getElementById("reset-btn");
    const indexCatalogEl = document.getElementById("index-catalog");
    let availableIndexes = [];
    let pendingIndexValue = "";

    const fields = {
      index: document.getElementById("index"),
      q: document.getElementById("q"),
      day: document.getElementById("day"),
      day_from: document.getElementById("day_from"),
      day_to: document.getElementById("day_to"),
      k: document.getElementById("k")
    };

    function setStatus(message, isError) {
      statusEl.textContent = message || "";
      statusEl.className = isError ? "status error" : "status";
    }

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
    }

    function docTitle(hit) {
      if (hit.source && hit.source.title) return String(hit.source.title);
      if (hit.source && hit.source.message) return String(hit.source.message);
      return "Document " + hit.doc_id;
    }

    function renderSummary(data) {
      summaryEl.innerHTML = "";
      const pills = [
        "index: " + data.index,
        "days: " + (Array.isArray(data.days) ? data.days.join(", ") : ""),
        "hits: " + (Array.isArray(data.hits) ? data.hits.length : 0),
        "k: " + data.k
      ];
      pills.forEach(function (text) {
        const pill = document.createElement("div");
        pill.className = "pill";
        pill.textContent = text;
        summaryEl.appendChild(pill);
      });
    }

    function renderErrors(partialErrors) {
      errorsEl.innerHTML = "";
      if (!Array.isArray(partialErrors) || partialErrors.length === 0) return;
      partialErrors.forEach(function (message) {
        const item = document.createElement("div");
        item.className = "error-item";
        item.textContent = message;
        errorsEl.appendChild(item);
      });
    }

    function renderResults(data) {
      resultsEl.innerHTML = "";
      const hits = Array.isArray(data.hits) ? data.hits : [];
      if (hits.length === 0) {
        resultsEl.innerHTML = '<div class="empty">No matching documents for this search.</div>';
        return;
      }

      hits.forEach(function (hit, index) {
        const card = document.createElement("article");
        card.className = "card result-card";
        card.style.animationDelay = (index * 35) + "ms";
        card.innerHTML =
          '<div class="result-meta">' +
            '<span class="badge score">score ' + escapeHtml(Number(hit.score || 0).toFixed(3)) + '</span>' +
            '<span class="badge">' + escapeHtml(hit.index || "") + '</span>' +
            '<span class="badge">' + escapeHtml(hit.day || "") + '</span>' +
            '<span class="badge">shard ' + escapeHtml(hit.shard || "") + '</span>' +
            '<span class="badge">' + escapeHtml(hit.doc_id || "") + '</span>' +
          '</div>' +
          '<h3 class="doc-title">' + escapeHtml(docTitle(hit)) + '</h3>' +
          '<pre>' + escapeHtml(JSON.stringify(hit.source || {}, null, 2)) + '</pre>';
        resultsEl.appendChild(card);
      });
    }

    function paramsFromForm() {
      const params = new URLSearchParams();
      Object.keys(fields).forEach(function (key) {
        const value = fields[key].value.trim();
        if (value !== "") params.set(key, value);
      });
      return params;
    }

    function applyParams(params) {
      Object.keys(fields).forEach(function (key) {
        const value = params.get(key) || (key === "k" ? "10" : "");
        if (key === "index") {
          pendingIndexValue = value;
          return;
        }
        fields[key].value = value;
      });
    }

    function applySuggestedDay() {
      if (fields.day.value || fields.day_from.value || fields.day_to.value) return;
      const match = availableIndexes.find(function (entry) {
        return entry.name === fields.index.value;
      });
      if (!match || !Array.isArray(match.days) || match.days.length === 0) return;
      fields.day.value = match.days[match.days.length - 1];
    }

    function renderAvailableIndexes(indexes) {
      availableIndexes = Array.isArray(indexes) ? indexes : [];
      fields.index.innerHTML = "";

      if (availableIndexes.length === 0) {
        fields.index.disabled = true;
        fields.index.innerHTML = '<option value="">No indexes available</option>';
        indexCatalogEl.textContent = "No searchable indexes yet. Bootstrap one or start the elkgo-testdata generator service.";
        return;
      }

      fields.index.disabled = false;
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.textContent = "Select an index";
      fields.index.appendChild(placeholder);

      availableIndexes.forEach(function (entry) {
        const option = document.createElement("option");
        option.value = entry.name;
        option.textContent = entry.name;
        fields.index.appendChild(option);
      });

      indexCatalogEl.textContent = "Available indexes: " + availableIndexes.map(function (entry) {
        const days = Array.isArray(entry.days) && entry.days.length > 0 ? " [" + entry.days.join(", ") + "]" : "";
        return entry.name + days;
      }).join(" | ");

      if (pendingIndexValue && availableIndexes.some(function (entry) { return entry.name === pendingIndexValue; })) {
        fields.index.value = pendingIndexValue;
      } else if (availableIndexes.length === 1) {
        fields.index.value = availableIndexes[0].name;
      } else {
        fields.index.value = "";
      }
      pendingIndexValue = "";
      applySuggestedDay();
    }

    async function loadAvailableIndexes() {
      try {
        const response = await fetch("/admin/indexes", {
          headers: { "Accept": "application/json" }
        });
        if (!response.ok) return;
        const data = await response.json();
        renderAvailableIndexes(data.indexes);
      } catch (_error) {
        indexCatalogEl.textContent = "Available indexes could not be loaded right now.";
      }
    }

    async function runSearch(pushState) {
      const params = paramsFromForm();
      if (!params.get("index")) {
        setStatus("Index is required.", true);
        return;
      }

      setStatus("Searching across assigned shards...");
      resultsEl.innerHTML = '<div class="empty">Loading results...</div>';
      errorsEl.innerHTML = "";

      const pageURL = params.toString() ? "/?" + params.toString() : "/";
      if (pushState) {
        window.history.replaceState({}, "", pageURL);
      }

      try {
        const response = await fetch("/search?" + params.toString(), {
          headers: { "Accept": "application/json" }
        });

        if (!response.ok) {
          const message = await response.text();
          throw new Error(message || ("Search failed with status " + response.status));
        }

        const data = await response.json();
        renderSummary(data);
        renderErrors(data.partial_errors);
        renderResults(data);
        setStatus("Search completed.");
        loadAvailableIndexes();
      } catch (error) {
        summaryEl.innerHTML = "";
        resultsEl.innerHTML = '<div class="empty">Search could not be completed.</div>';
        errorsEl.innerHTML = "";
        let message = error.message || "Search failed.";
        if (message.includes("routing not initialized")) {
          message += " Bootstrap an index/day first, or run the elkgo-testdata generator service.";
        }
        setStatus(message, true);
      }
    }

    form.addEventListener("submit", function (event) {
      event.preventDefault();
      runSearch(true);
    });

    resetBtn.addEventListener("click", function () {
      Object.keys(fields).forEach(function (key) {
        fields[key].value = key === "k" ? "10" : "";
      });
      summaryEl.innerHTML = "";
      errorsEl.innerHTML = "";
      resultsEl.innerHTML = '<div class="empty">Fill in the form and run a query to see results here.</div>';
      setStatus("");
      window.history.replaceState({}, "", "/");
    });

    fields.index.addEventListener("change", applySuggestedDay);

    const initialParams = new URLSearchParams(window.location.search);
    applyParams(initialParams);
    resultsEl.innerHTML = '<div class="empty">Fill in the form and run a query to see results here.</div>';
    loadAvailableIndexes();

    if (
      initialParams.get("index") &&
      (
        initialParams.get("q") ||
        initialParams.get("day") ||
        (initialParams.get("day_from") && initialParams.get("day_to"))
      )
    ) {
      runSearch(false);
    }
  </script>
</body>
</html>
`
