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

    .discover-layout {
      display: grid;
      gap: 18px;
      grid-template-columns: minmax(230px, 280px) minmax(0, 1fr);
      align-items: start;
    }

    .discover-sidebar,
    .discover-main,
    .timeline-panel,
    .events-panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }

    .discover-sidebar {
      padding: 18px 0;
      overflow: hidden;
    }

    .sidebar-section {
      padding: 0 18px;
    }

    .sidebar-section + .sidebar-section {
      margin-top: 18px;
      padding-top: 18px;
      border-top: 1px solid rgba(127, 231, 193, 0.12);
    }

    .sidebar-title,
    .timeline-title,
    .events-title,
    .field-name {
      letter-spacing: 0.14em;
      text-transform: uppercase;
      font-size: 0.72rem;
      color: var(--muted);
    }

    .sidebar-title,
    .timeline-title,
    .events-title {
      margin-bottom: 12px;
      font-weight: 700;
    }

    .sidebar-empty {
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.5;
    }

    .selected-fields {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .field-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 8px 12px;
      background: rgba(127, 231, 193, 0.12);
      border: 1px solid rgba(127, 231, 193, 0.16);
      color: var(--text);
      font-size: 0.86rem;
    }

    .field-pill button {
      border: 0;
      background: transparent;
      color: var(--muted);
      box-shadow: none;
      padding: 0;
      font-size: 1rem;
      line-height: 1;
      cursor: pointer;
    }

    .field-list {
      display: grid;
      gap: 8px;
    }

    .field-item {
      width: 100%;
      display: grid;
      grid-template-columns: 1fr auto auto;
      gap: 10px;
      align-items: center;
      padding: 10px 12px;
      border-radius: 14px;
      border: 1px solid rgba(127, 231, 193, 0.1);
      background: rgba(255, 255, 255, 0.03);
      color: var(--text);
      box-shadow: none;
      text-align: left;
    }

    .field-item:hover {
      transform: none;
      filter: none;
      border-color: rgba(127, 231, 193, 0.24);
      background: rgba(127, 231, 193, 0.08);
    }

    .field-item.active {
      border-color: rgba(127, 231, 193, 0.35);
      background: rgba(127, 231, 193, 0.12);
    }

    .field-item-name {
      font-weight: 600;
      word-break: break-word;
    }

    .field-item-count,
    .field-item-action {
      color: var(--muted);
      font-size: 0.82rem;
    }

    .discover-main {
      padding: 18px;
      display: grid;
      gap: 18px;
    }

    .timeline-panel {
      padding: 18px;
      background: rgba(5, 20, 27, 0.88);
    }

    .timeline-head,
    .events-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
      margin-bottom: 14px;
    }

    .timeline-meta,
    .events-meta {
      color: var(--muted);
      font-size: 0.9rem;
    }

    .timeline-chart {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(20px, 1fr));
      gap: 8px;
      align-items: end;
      min-height: 170px;
    }

    .timeline-bar {
      min-width: 0;
      display: grid;
      gap: 8px;
      justify-items: center;
      align-content: end;
    }

    .timeline-bar-fill {
      width: 100%;
      min-height: 8px;
      border-radius: 10px 10px 4px 4px;
      background: linear-gradient(180deg, #6ce3d0 0%, #3fa6c5 100%);
      box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.08);
    }

    .timeline-bar-label {
      width: 100%;
      text-align: center;
      font-size: 0.74rem;
      color: var(--muted);
      word-break: break-word;
    }

    .events-panel {
      overflow: hidden;
    }

    .events-head {
      padding: 18px 18px 12px;
      margin-bottom: 0;
      border-bottom: 1px solid rgba(127, 231, 193, 0.1);
    }

    .events-table {
      overflow-x: auto;
    }

    .events-header,
    .event-summary-row {
      display: grid;
      gap: 12px;
      align-items: start;
      min-width: 720px;
    }

    .events-header {
      padding: 10px 18px 12px;
      background: rgba(255, 255, 255, 0.02);
      border-bottom: 1px solid rgba(127, 231, 193, 0.08);
    }

    .events-header-cell {
      font-size: 0.75rem;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      color: var(--muted);
      font-weight: 700;
    }

    details.event-row {
      border-bottom: 1px solid rgba(127, 231, 193, 0.08);
    }

    details.event-row:last-child {
      border-bottom: 0;
    }

    details.event-row summary {
      list-style: none;
      cursor: pointer;
      padding: 14px 18px;
    }

    details.event-row summary::-webkit-details-marker {
      display: none;
    }

    details.event-row[open] summary {
      background: rgba(127, 231, 193, 0.05);
    }

    .event-time-cell {
      display: grid;
      gap: 4px;
      font-size: 0.92rem;
      color: var(--text);
    }

    .event-time-main {
      font-weight: 700;
    }

    .event-time-sub {
      color: var(--muted);
      font-size: 0.8rem;
      word-break: break-word;
    }

    .event-source-cell {
      display: grid;
      gap: 8px;
      min-width: 0;
    }

    .doc-title {
      font-size: 1rem;
      margin: 0;
      color: var(--text);
      line-height: 1.4;
      word-break: break-word;
    }

    .event-message {
      margin: 0;
      color: #d7eae4;
      line-height: 1.55;
      display: -webkit-box;
      -webkit-line-clamp: 3;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }

    .source-inline {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .source-pair,
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 6px 10px;
      border: 1px solid rgba(255, 255, 255, 0.08);
      background: rgba(255, 255, 255, 0.04);
      color: var(--text);
      font-size: 0.84rem;
      min-width: 0;
    }

    .source-pair-key {
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-size: 0.7rem;
    }

    .source-pair-value {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      max-width: 200px;
    }

    .badge.score {
      color: var(--accent-strong);
      font-weight: 700;
      background: rgba(242, 198, 109, 0.12);
      border-color: rgba(242, 198, 109, 0.18);
    }

    .detail-cell {
      color: var(--text);
      font-size: 0.9rem;
      line-height: 1.45;
      word-break: break-word;
    }

    .detail-cell.muted {
      color: var(--muted);
    }

    .event-body {
      padding: 0 18px 18px;
      display: grid;
      gap: 14px;
      background: rgba(255, 255, 255, 0.02);
    }

    .event-context {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      padding-top: 4px;
    }

    .field-grid {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }

    .field-card {
      padding: 12px 14px;
      border-radius: 16px;
      border: 1px solid rgba(127, 231, 193, 0.12);
      background: rgba(1, 11, 15, 0.56);
      min-width: 0;
    }

    .field-value {
      display: block;
      margin-top: 8px;
      color: var(--text);
      font-size: 0.94rem;
      line-height: 1.5;
      word-break: break-word;
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

      .result-card {
        grid-template-columns: 1fr;
      }

      .discover-layout {
        grid-template-columns: 1fr;
      }

      .discover-main {
        padding: 14px;
      }

      .timeline-chart {
        gap: 6px;
      }

      .events-header,
      .event-summary-row {
        min-width: 0;
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
          pick a day range, run your query, and inspect the matching documents below.
        </p>
      </article>
      <aside class="card sidebar">
        <h2>Quick notes</h2>
        <p>Use <strong>day from</strong> and <strong>day to</strong>. For a single partition, set them to the same date.</p>
        <p>The page keeps your inputs in the URL, so refreshing or sharing the search state is easy.</p>
        <p><a href="/admin/routing" target="_blank" rel="noreferrer">View routing JSON</a></p>
      </aside>
    </section>

    <section class="card panel">
      <form id="search-form">
        <div class="grid">
          <label class="span-8">
            <span>Index</span>
            <select id="index" name="index" required>
              <option value="">Loading indexes...</option>
            </select>
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
          <div class="hint">The UI calls <code>/search</code>. Leave query empty to list all documents for the selected range.</div>
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
    let selectedFields = [];
    let currentResultData = null;

    const fields = {
      index: document.getElementById("index"),
      q: document.getElementById("q"),
      day_from: document.getElementById("day_from"),
      day_to: document.getElementById("day_to"),
      k: document.getElementById("k")
    };

    function setStatus(message, isError) {
      statusEl.textContent = message || "";
      statusEl.className = isError ? "status error" : "status";
    }

    const TIMESTAMP_FIELDS = ["@timestamp", "timestamp", "event_time", "created", "created_at", "observed_at", "time"];
    const SUMMARY_FIELDS = ["title", "message", "summary", "description", "body", "event"];
    const CHIP_FIELDS = ["level", "severity", "service", "host", "hostname", "env", "environment", "dataset", "source", "status"];
    const HIDDEN_FIELDS = new Set(["id", "partition_day"].concat(TIMESTAMP_FIELDS, SUMMARY_FIELDS));

    function isPlainObject(value) {
      return value !== null && typeof value === "object" && !Array.isArray(value);
    }

    function isScalar(value) {
      return value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean";
    }

    function isCompactArray(value) {
      return Array.isArray(value) && value.length > 0 && value.length <= 6 && value.every(isScalar);
    }

    function firstPresentField(source, keys) {
      for (let i = 0; i < keys.length; i += 1) {
        const key = keys[i];
        if (source && source[key] !== undefined && source[key] !== null && source[key] !== "") {
          return source[key];
        }
      }
      return "";
    }

    function docTitle(hit) {
      const source = isPlainObject(hit.source) ? hit.source : {};
      const title = firstPresentField(source, ["title", "summary", "event", "message"]);
      if (title) return String(title);
      return "Document " + hit.doc_id;
    }

    function docMessage(hit) {
      const source = isPlainObject(hit.source) ? hit.source : {};
      const message = firstPresentField(source, ["message", "description", "body", "summary", "event"]);
      if (!message) return "";
      const title = docTitle(hit);
      if (String(message) === title) return "";
      return String(message);
    }

    function compactTimestamp(value, fallbackDay) {
      if (value === undefined || value === null || value === "") return fallbackDay || "No time";
      const parsed = new Date(value);
      if (Number.isNaN(parsed.getTime())) return String(value);
      return parsed.toLocaleString([], {
        month: "short",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
      });
    }

    function formatTimestamp(value, fallbackDay) {
      if (value === undefined || value === null || value === "") {
        return fallbackDay || "No timestamp";
      }
      const parsed = new Date(value);
      if (Number.isNaN(parsed.getTime())) {
        return String(value);
      }
      return parsed.toLocaleString([], {
        year: "numeric",
        month: "short",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
      });
    }

    function displayValue(value) {
      if (value === null) return "null";
      if (value === undefined) return "";
      if (typeof value === "string") return value;
      if (typeof value === "number" || typeof value === "boolean") return String(value);
      if (isCompactArray(value)) {
        return value.map(function (item) {
          return item === null ? "null" : String(item);
        }).join(" • ");
      }
      if (Array.isArray(value)) {
        return value.length + " items";
      }
      if (isPlainObject(value)) {
        return Object.keys(value).length + " fields";
      }
      return String(value);
    }

    function topLevelFieldStats(hits) {
      const counts = {};
      hits.forEach(function (hit) {
        const source = isPlainObject(hit.source) ? hit.source : {};
        Object.keys(source).forEach(function (key) {
          if (HIDDEN_FIELDS.has(key)) return;
          const value = source[key];
          if (!isScalar(value) && !isCompactArray(value)) return;
          counts[key] = (counts[key] || 0) + 1;
        });
      });
      return Object.keys(counts).map(function (key) {
        return { key: key, count: counts[key] };
      }).sort(function (a, b) {
        if (b.count === a.count) return a.key.localeCompare(b.key);
        return b.count - a.count;
      });
    }

    function normalizeSelectedFields(hits) {
      const stats = topLevelFieldStats(hits);
      const available = new Set(stats.map(function (entry) { return entry.key; }));

      selectedFields = selectedFields.filter(function (field) {
        return available.has(field);
      });

      if (selectedFields.length > 0) return;

      stats.forEach(function (entry) {
        if (selectedFields.length >= 3) return;
        if (CHIP_FIELDS.indexOf(entry.key) >= 0) return;
        selectedFields.push(entry.key);
      });
    }

    function extractChips(source) {
      const chips = [];
      CHIP_FIELDS.forEach(function (key) {
        if (!source || source[key] === undefined || source[key] === null || source[key] === "") return;
        if (!isScalar(source[key]) && !isCompactArray(source[key])) return;
        chips.push({ key: key, value: displayValue(source[key]) });
      });
      return chips.slice(0, 5);
    }

    function extractFieldCards(source) {
      const cards = [];
      const seen = new Set();

      Object.keys(source || {}).sort().forEach(function (key) {
        if (seen.has(key) || HIDDEN_FIELDS.has(key) || CHIP_FIELDS.indexOf(key) >= 0) return;
        const value = source[key];
        if (!isScalar(value) && !isCompactArray(value)) return;
        cards.push({ key: key, value: displayValue(value) });
        seen.add(key);
      });

      if (cards.length === 0) {
        CHIP_FIELDS.forEach(function (key) {
          if (!source || source[key] === undefined || source[key] === null || source[key] === "") return;
          if (!isScalar(source[key]) && !isCompactArray(source[key])) return;
          cards.push({ key: key, value: displayValue(source[key]) });
        });
      }

      return cards.slice(0, 10);
    }

    function inlineSourcePairs(source) {
      const pairs = [];
      Object.keys(source || {}).sort().forEach(function (key) {
        if (HIDDEN_FIELDS.has(key) || CHIP_FIELDS.indexOf(key) >= 0 || selectedFields.indexOf(key) >= 0) return;
        const value = source[key];
        if (!isScalar(value) && !isCompactArray(value)) return;
        pairs.push({ key: key, value: displayValue(value) });
      });
      return pairs.slice(0, 4);
    }

    function renderFieldSidebar(hits) {
      const sidebar = document.createElement("aside");
      sidebar.className = "discover-sidebar";

      const selectedSection = document.createElement("section");
      selectedSection.className = "sidebar-section";
      selectedSection.innerHTML = '<div class="sidebar-title">Selected Fields</div>';

      if (selectedFields.length === 0) {
        const empty = document.createElement("div");
        empty.className = "sidebar-empty";
        empty.textContent = "Choose fields to pin them as columns in the event stream.";
        selectedSection.appendChild(empty);
      } else {
        const selectedWrap = document.createElement("div");
        selectedWrap.className = "selected-fields";
        selectedFields.forEach(function (field) {
          const pill = document.createElement("div");
          pill.className = "field-pill";
          const label = document.createElement("span");
          label.textContent = field;
          pill.appendChild(label);
          const remove = document.createElement("button");
          remove.type = "button";
          remove.textContent = "×";
          remove.setAttribute("aria-label", "Remove " + field);
          remove.addEventListener("click", function () {
            selectedFields = selectedFields.filter(function (entry) { return entry !== field; });
            renderResults(currentResultData);
          });
          pill.appendChild(remove);
          selectedWrap.appendChild(pill);
        });
        selectedSection.appendChild(selectedWrap);
      }

      const availableSection = document.createElement("section");
      availableSection.className = "sidebar-section";
      availableSection.innerHTML = '<div class="sidebar-title">Available Fields</div>';

      const stats = topLevelFieldStats(hits);
      if (stats.length === 0) {
        const empty = document.createElement("div");
        empty.className = "sidebar-empty";
        empty.textContent = "No scalar top-level fields were found in the current result set.";
        availableSection.appendChild(empty);
      } else {
        const list = document.createElement("div");
        list.className = "field-list";
        stats.slice(0, 18).forEach(function (entry) {
          const button = document.createElement("button");
          button.type = "button";
          button.className = "field-item" + (selectedFields.indexOf(entry.key) >= 0 ? " active" : "");

          const name = document.createElement("span");
          name.className = "field-item-name";
          name.textContent = entry.key;
          button.appendChild(name);

          const count = document.createElement("span");
          count.className = "field-item-count";
          count.textContent = entry.count;
          button.appendChild(count);

          const action = document.createElement("span");
          action.className = "field-item-action";
          action.textContent = selectedFields.indexOf(entry.key) >= 0 ? "Pinned" : "Add";
          button.appendChild(action);

          button.addEventListener("click", function () {
            if (selectedFields.indexOf(entry.key) >= 0) {
              selectedFields = selectedFields.filter(function (field) { return field !== entry.key; });
            } else {
              selectedFields = selectedFields.concat(entry.key).slice(0, 6);
            }
            renderResults(currentResultData);
          });

          list.appendChild(button);
        });
        availableSection.appendChild(list);
      }

      sidebar.appendChild(selectedSection);
      sidebar.appendChild(availableSection);
      return sidebar;
    }

    function histogramBuckets(hits) {
      const points = hits.map(function (hit) {
        const source = isPlainObject(hit.source) ? hit.source : {};
        const raw = firstPresentField(source, TIMESTAMP_FIELDS);
        const parsed = new Date(raw);
        if (!raw || Number.isNaN(parsed.getTime())) return null;
        return parsed;
      }).filter(Boolean).sort(function (a, b) { return a - b; });

      if (points.length === 0) return [];

      const min = points[0].getTime();
      const max = points[points.length - 1].getTime();
      const bucketMs = (max - min) > (48 * 60 * 60 * 1000) ? (24 * 60 * 60 * 1000) : (60 * 60 * 1000);
      const buckets = {};

      points.forEach(function (point) {
        const stamp = point.getTime();
        const bucketStart = stamp - (stamp % bucketMs);
        const key = String(bucketStart);
        buckets[key] = (buckets[key] || 0) + 1;
      });

      return Object.keys(buckets).sort().map(function (key) {
        const start = new Date(Number(key));
        const label = bucketMs === (24 * 60 * 60 * 1000)
          ? start.toLocaleDateString([], { month: "short", day: "2-digit" })
          : start.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
        return { label: label, count: buckets[key] };
      });
    }

    function renderTimeline(hits) {
      const buckets = histogramBuckets(hits);
      const panel = document.createElement("section");
      panel.className = "timeline-panel";

      const head = document.createElement("div");
      head.className = "timeline-head";
      head.innerHTML = '<div class="timeline-title">Visible Hits Timeline</div><div class="timeline-meta">Based on the current result window</div>';
      panel.appendChild(head);

      if (buckets.length === 0) {
        const empty = document.createElement("div");
        empty.className = "sidebar-empty";
        empty.textContent = "No parsable timestamps were found in the visible hits.";
        panel.appendChild(empty);
        return panel;
      }

      const chart = document.createElement("div");
      chart.className = "timeline-chart";
      const maxCount = Math.max.apply(null, buckets.map(function (entry) { return entry.count; })) || 1;

      buckets.forEach(function (entry) {
        const bar = document.createElement("div");
        bar.className = "timeline-bar";
        bar.title = entry.label + ": " + entry.count + " hit" + (entry.count === 1 ? "" : "s");

        const fill = document.createElement("div");
        fill.className = "timeline-bar-fill";
        fill.style.height = Math.max(10, Math.round((entry.count / maxCount) * 120)) + "px";
        bar.appendChild(fill);

        const label = document.createElement("div");
        label.className = "timeline-bar-label";
        label.textContent = entry.label;
        bar.appendChild(label);

        chart.appendChild(bar);
      });

      panel.appendChild(chart);
      return panel;
    }

    function renderEventTable(hits) {
      const panel = document.createElement("section");
      panel.className = "events-panel";

      const head = document.createElement("div");
      head.className = "events-head";
      head.innerHTML = '<div class="events-title">Event Stream</div><div class="events-meta">' + hits.length + ' visible hit' + (hits.length === 1 ? '' : 's') + '</div>';
      panel.appendChild(head);

      const table = document.createElement("div");
      table.className = "events-table";
      const columns = ["170px", "minmax(320px, 1.6fr)"];
      selectedFields.forEach(function () {
        columns.push("minmax(150px, 1fr)");
      });
      const template = columns.join(" ");

      const header = document.createElement("div");
      header.className = "events-header";
      header.style.gridTemplateColumns = template;

      ["Time", "_source"].concat(selectedFields).forEach(function (labelText) {
        const cell = document.createElement("div");
        cell.className = "events-header-cell";
        cell.textContent = labelText;
        header.appendChild(cell);
      });
      table.appendChild(header);

      hits.forEach(function (hit) {
        const source = isPlainObject(hit.source) ? hit.source : {};
        const timestamp = firstPresentField(source, TIMESTAMP_FIELDS);
        const row = document.createElement("details");
        row.className = "event-row";

        const summary = document.createElement("summary");
        summary.className = "event-summary-row";
        summary.style.gridTemplateColumns = template;

        const timeCell = document.createElement("div");
        timeCell.className = "event-time-cell";
        const timeMain = document.createElement("span");
        timeMain.className = "event-time-main";
        timeMain.textContent = compactTimestamp(timestamp, hit.day || "");
        timeCell.appendChild(timeMain);

        const timeSub = document.createElement("span");
        timeSub.className = "event-time-sub";
        timeSub.textContent = timestamp ? String(timestamp) : (hit.day || "No timestamp");
        timeCell.appendChild(timeSub);
        summary.appendChild(timeCell);

        const sourceCell = document.createElement("div");
        sourceCell.className = "event-source-cell";
        const title = document.createElement("h3");
        title.className = "doc-title";
        title.textContent = docTitle(hit);
        sourceCell.appendChild(title);

        const message = docMessage(hit);
        if (message) {
          const messageEl = document.createElement("p");
          messageEl.className = "event-message";
          messageEl.textContent = message;
          sourceCell.appendChild(messageEl);
        }

        const inlinePairs = inlineSourcePairs(source);
        if (inlinePairs.length > 0) {
          const inlineWrap = document.createElement("div");
          inlineWrap.className = "source-inline";
          inlinePairs.forEach(function (entry) {
            const pair = document.createElement("div");
            pair.className = "source-pair";

            const pairKey = document.createElement("span");
            pairKey.className = "source-pair-key";
            pairKey.textContent = entry.key;
            pair.appendChild(pairKey);

            const pairValue = document.createElement("span");
            pairValue.className = "source-pair-value";
            pairValue.textContent = entry.value;
            pair.appendChild(pairValue);

            inlineWrap.appendChild(pair);
          });
          sourceCell.appendChild(inlineWrap);
        }
        summary.appendChild(sourceCell);

        selectedFields.forEach(function (field) {
          const detail = document.createElement("div");
          const value = source[field];
          detail.className = "detail-cell" + (value === undefined || value === null || value === "" ? " muted" : "");
          detail.textContent = value === undefined || value === null || value === "" ? "—" : displayValue(value);
          summary.appendChild(detail);
        });

        row.appendChild(summary);

        const body = document.createElement("div");
        body.className = "event-body";

        const context = document.createElement("div");
        context.className = "event-context";
        [
          "score " + Number(hit.score || 0).toFixed(3),
          hit.index || "",
          "day " + (hit.day || ""),
          "shard " + (hit.shard || ""),
          hit.doc_id || ""
        ].forEach(function (text, index) {
          const badge = document.createElement("span");
          badge.className = index === 0 ? "badge score" : "badge";
          badge.textContent = text;
          context.appendChild(badge);
        });
        body.appendChild(context);

        const fieldGrid = document.createElement("div");
        fieldGrid.className = "field-grid";
        extractFieldCards(source).forEach(function (entry) {
          const fieldCard = document.createElement("div");
          fieldCard.className = "field-card";

          const fieldName = document.createElement("span");
          fieldName.className = "field-name";
          fieldName.textContent = entry.key;
          fieldCard.appendChild(fieldName);

          const fieldValue = document.createElement("span");
          fieldValue.className = "field-value";
          fieldValue.textContent = entry.value;
          fieldCard.appendChild(fieldValue);

          fieldGrid.appendChild(fieldCard);
        });
        body.appendChild(fieldGrid);

        const pre = document.createElement("pre");
        pre.textContent = JSON.stringify(source, null, 2);
        body.appendChild(pre);

        row.appendChild(body);
        table.appendChild(row);
      });

      panel.appendChild(table);
      return panel;
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
      currentResultData = data;
      const hits = Array.isArray(data.hits) ? data.hits : [];
      if (hits.length === 0) {
        resultsEl.innerHTML = '<div class="empty">No matching documents for this search.</div>';
        return;
      }

      normalizeSelectedFields(hits);

      const layout = document.createElement("div");
      layout.className = "discover-layout";
      layout.appendChild(renderFieldSidebar(hits));

      const main = document.createElement("div");
      main.className = "discover-main";
      main.appendChild(renderTimeline(hits));
      main.appendChild(renderEventTable(hits));
      layout.appendChild(main);

      resultsEl.appendChild(layout);
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
      if (fields.day_from.value || fields.day_to.value) return;
      const match = availableIndexes.find(function (entry) {
        return entry.name === fields.index.value;
      });
      if (!match || !Array.isArray(match.days) || match.days.length === 0) return;
      const latestDay = match.days[match.days.length - 1];
      fields.day_from.value = latestDay;
      fields.day_to.value = latestDay;
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
        (initialParams.get("day_from") && initialParams.get("day_to"))
      )
    ) {
      runSearch(false);
    }
  </script>
</body>
</html>
`
