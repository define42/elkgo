package webui

const HomePageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>elkgo search</title>
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
      width: 100%;
      padding: 0;
      display: block;
    }

    .shell {
      width: 100%;
      min-height: 100vh;
      display: grid;
      gap: 0;
      align-content: start;
      justify-items: stretch;
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

    .search-panel {
      background: var(--surface-alt);
      border-bottom: 1px solid var(--line);
    }

    .toolbar-meta {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 7px 16px;
      border-bottom: 1px solid var(--line);
      background: var(--surface);
    }

    .hit-count {
      font-size: 0.95rem;
      font-weight: 700;
      white-space: nowrap;
    }

    .query-form {
      padding: 10px 12px 12px;
      display: grid;
      gap: 12px;
      border-bottom: 1px solid var(--line);
    }

    .grid {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(12, minmax(0, 1fr));
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

    .query-field > span {
      position: absolute;
      width: 1px;
      height: 1px;
      padding: 0;
      margin: -1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
      white-space: nowrap;
      border: 0;
    }

    .span-12 { grid-column: span 12; }
    .span-9 { grid-column: span 9; }
    .span-6 { grid-column: span 6; }
    .span-4 { grid-column: span 4; }
    .span-3 { grid-column: span 3; }
    .span-2 { grid-column: span 2; }

    .search-grid {
      gap: 10px;
      align-items: end;
    }

    .input-compact {
      max-width: none;
    }

    .hidden {
      display: none !important;
    }

    .query-box {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 0 12px;
      border: 1px solid var(--line-strong);
      border-radius: var(--radius);
      background: var(--panel);
    }

    .query-icon {
      color: var(--muted);
      font-size: 1rem;
      flex: 0 0 auto;
    }

    input, select {
      width: 100%;
      border: 1px solid var(--line-strong);
      background: var(--panel);
      color: var(--text);
      border-radius: var(--radius);
      padding: 10px 12px;
      font: inherit;
      box-shadow: none;
      transition: border-color 160ms ease, background 160ms ease;
    }

    .query-box input {
      border: 0;
      background: transparent;
      padding: 11px 0;
    }

    input:focus, select:focus {
      outline: none;
      border-color: var(--accent-strong);
      background: var(--panel-strong);
    }

    .query-box:focus-within {
      border-color: var(--accent-strong);
      background: var(--panel-strong);
    }

    .timebox-shell {
      position: relative;
      display: grid;
      gap: 6px;
      min-width: 0;
    }

    .timebox-control {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 0;
      border: 1px solid var(--line-strong);
      border-radius: var(--radius);
      background: var(--panel);
      overflow: hidden;
    }

    .timebox-trigger,
    .timebox-dates-btn {
      min-height: 58px;
      border: 0;
      border-radius: 0;
      background: transparent;
      color: var(--text);
      box-shadow: none;
    }

    .timebox-trigger {
      width: 100%;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      padding: 12px 16px;
      border-right: 1px solid var(--line);
      text-align: left;
    }

    .timebox-trigger:hover,
    .timebox-dates-btn:hover {
      background: rgba(255, 255, 255, 0.03);
    }

    .timebox-trigger-main {
      display: flex;
      align-items: center;
      gap: 14px;
      min-width: 0;
    }

    .timebox-trigger-copy {
      display: grid;
      gap: 4px;
      min-width: 0;
    }

    .timebox-value {
      font-size: 1.02rem;
      font-weight: 600;
      color: var(--text);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .timebox-caption {
      color: var(--muted);
      font-size: 0.8rem;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .timebox-calendar {
      width: 22px;
      height: 22px;
      border: 2px solid var(--accent-strong);
      border-radius: 4px;
      position: relative;
      flex: 0 0 auto;
    }

    .timebox-calendar::before,
    .timebox-calendar::after {
      content: "";
      position: absolute;
      background: var(--accent-strong);
    }

    .timebox-calendar::before {
      left: 3px;
      right: 3px;
      top: 6px;
      height: 2px;
    }

    .timebox-calendar::after {
      width: 10px;
      height: 2px;
      left: 4px;
      top: -4px;
      box-shadow: 8px 0 0 var(--accent-strong);
    }

    .timebox-caret {
      width: 12px;
      height: 12px;
      border-right: 2px solid var(--accent-strong);
      border-bottom: 2px solid var(--accent-strong);
      transform: rotate(45deg) translateY(-2px);
      flex: 0 0 auto;
    }

    .timebox-dates-btn {
      padding: 12px 18px;
      color: #72b6ff;
      font-size: 0.95rem;
      font-weight: 600;
      white-space: nowrap;
    }

    .timebox-popover {
      position: absolute;
      top: calc(100% + 16px);
      left: 0;
      z-index: 15;
      width: min(100%, 860px);
      padding: 22px 24px 24px;
      border-radius: 10px;
      border: 1px solid var(--line-strong);
      background: #1f2128;
      box-shadow: 0 18px 36px rgba(0, 0, 0, 0.42);
    }

    .timebox-popover-arrow {
      position: absolute;
      top: -10px;
      left: 42px;
      width: 20px;
      height: 20px;
      background: #1f2128;
      border-top: 1px solid var(--line-strong);
      border-left: 1px solid var(--line-strong);
      transform: rotate(45deg);
    }

    .timebox-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 18px;
    }

    .timebox-title,
    .timebox-section-title {
      font-size: 0.98rem;
      font-weight: 700;
      color: var(--text);
    }

    .timebox-section-title {
      margin-bottom: 14px;
    }

    .timebox-row {
      display: grid;
      grid-template-columns: minmax(140px, 1fr) minmax(120px, 1fr) minmax(160px, 1fr) auto;
      gap: 12px;
      align-items: center;
      padding-bottom: 18px;
      border-bottom: 1px solid var(--line);
      margin-bottom: 20px;
    }

    .timebox-row select,
    .timebox-row input,
    .timebox-date-grid input {
      min-height: 56px;
      font-size: 1rem;
    }

    .timebox-row button {
      min-height: 56px;
      min-width: 120px;
      font-size: 1rem;
    }

    .timebox-presets {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px 28px;
    }

    .timebox-preset {
      appearance: none;
      border: 0;
      padding: 0;
      background: transparent;
      color: #72b6ff;
      font-size: 0.98rem;
      font-weight: 500;
      text-align: left;
      box-shadow: none;
    }

    .timebox-preset:hover {
      color: #a6d4ff;
      background: transparent;
    }

    .timebox-preset.active {
      color: var(--text);
    }

    .timebox-date-panel {
      display: grid;
      gap: 14px;
      margin-top: 20px;
      padding-top: 18px;
      border-top: 1px solid var(--line);
    }

    .timebox-date-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }

    .timebox-date-grid label {
      gap: 8px;
    }

    .timebox-date-note {
      color: var(--muted);
      font-size: 0.8rem;
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
      border: 1px solid transparent;
      border-radius: var(--radius);
      background: #4aa3c8;
      color: #091116;
      font: inherit;
      font-weight: 700;
      padding: 10px 16px;
      cursor: pointer;
      transition: background 160ms ease, border-color 160ms ease, color 160ms ease;
      text-decoration: none;
      box-shadow: none;
    }

    button:hover, .actions a:hover {
      background: #63b5d7;
    }

    button:disabled,
    .actions a[aria-disabled="true"] {
      opacity: 0.42;
      cursor: not-allowed;
    }

    button:disabled:hover,
    .actions a[aria-disabled="true"]:hover {
      background: inherit;
    }

    button.secondary, .actions a {
      background: transparent;
      color: var(--text);
      border-color: var(--line-strong);
      box-shadow: none;
    }

    button.secondary {
      background: transparent;
      color: var(--text);
      border-color: var(--line-strong);
      box-shadow: none;
    }

    button.secondary:hover {
      background: rgba(255, 255, 255, 0.04);
    }

    .pills {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      padding: 10px 12px 12px;
      min-height: 46px;
      align-items: center;
    }

    .pill {
      padding: 5px 8px;
      border-radius: var(--radius);
      background: var(--panel);
      border: 1px solid var(--line);
      color: var(--muted);
      font-size: 0.82rem;
    }

    .status {
      min-height: 1.4em;
      font-size: 0.84rem;
      color: var(--muted);
      text-align: right;
    }

    .status.error {
      color: var(--danger);
    }

    .results {
      display: grid;
      gap: 0;
      align-content: start;
    }

    .discover-layout {
      display: grid;
      gap: 0;
      grid-template-columns: minmax(260px, 320px) minmax(0, 1fr);
      align-items: stretch;
      align-content: start;
    }

    .discover-sidebar {
      background: var(--surface);
      border-right: 1px solid var(--line);
      min-width: 0;
    }

    .sidebar-index {
      padding: 10px 16px;
      border-bottom: 1px solid var(--line);
      background: rgba(62, 168, 207, 0.24);
      font-size: 0.98rem;
      font-weight: 700;
      color: var(--text);
    }

    .sidebar-section {
      padding: 12px 16px;
    }

    .sidebar-section + .sidebar-section {
      border-top: 1px solid var(--line);
    }

    .sidebar-title,
    .timeline-title,
    .events-title,
    .field-name {
      letter-spacing: 0;
      text-transform: none;
      font-size: 0.88rem;
      color: var(--muted);
    }

    .sidebar-title,
    .timeline-title,
    .events-title {
      margin-bottom: 10px;
      font-weight: 700;
    }

    .sidebar-empty {
      color: var(--muted);
      font-size: 0.84rem;
      line-height: 1.5;
    }

    .selected-fields {
      display: grid;
      gap: 6px;
    }

    .field-pill {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      border-radius: var(--radius);
      padding: 8px 10px;
      background: var(--panel);
      border: 1px solid var(--line);
      color: var(--text);
      font-size: 0.86rem;
    }

    .field-pill.locked {
      background: transparent;
      border-style: dashed;
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
      gap: 2px;
      max-height: min(52vh, 640px);
      overflow-y: auto;
      padding-right: 4px;
    }

    .field-item {
      width: 100%;
      display: grid;
      grid-template-columns: 1fr auto auto;
      gap: 10px;
      align-items: center;
      padding: 9px 10px;
      border-radius: var(--radius);
      border: 1px solid transparent;
      background: transparent;
      color: var(--text);
      box-shadow: none;
      text-align: left;
    }

    .field-item:hover {
      transform: none;
      filter: none;
      border-color: var(--line);
      background: var(--panel);
    }

    .field-item.active {
      border-color: rgba(62, 168, 207, 0.42);
      background: rgba(62, 168, 207, 0.12);
    }

    .field-item-name {
      font-weight: 500;
      word-break: break-word;
    }

    .field-item-count,
    .field-item-action {
      color: var(--muted);
      font-size: 0.78rem;
    }

    .field-item-action {
      color: var(--accent-strong);
    }

    .discover-main {
      display: grid;
      gap: 0;
      min-width: 0;
      align-content: start;
    }

    .timeline-panel {
      padding: 16px 20px 18px;
      border-bottom: 1px solid var(--line);
      background: var(--surface-alt);
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
      font-size: 0.82rem;
    }

    .events-head-right,
    .pager {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }

    .pager button {
      padding: 7px 12px;
      min-width: 68px;
    }

    .timeline-chart {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(16px, 1fr));
      gap: 6px;
      align-items: end;
      min-height: 180px;
      padding: 8px 0 0 8px;
      border-left: 1px solid var(--line-strong);
      border-bottom: 1px solid var(--line-strong);
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
      border-radius: 0;
      background: rgba(0, 191, 179, 0.54);
      border: 1px solid rgba(0, 191, 179, 0.92);
      border-bottom: 0;
      box-shadow: none;
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
      min-width: 0;
    }

    .events-head {
      padding: 12px 18px;
      margin-bottom: 0;
      border-bottom: 1px solid var(--line);
      background: var(--surface);
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
      padding: 10px 18px;
      background: var(--surface);
      border-bottom: 1px solid var(--line);
    }

    .events-header-cell {
      font-size: 0.84rem;
      letter-spacing: 0;
      text-transform: none;
      color: var(--text);
      font-weight: 700;
      overflow-wrap: anywhere;
    }

    details.event-row {
      border-bottom: 1px solid var(--line);
      background: var(--bg);
    }

    details.event-row:last-child {
      border-bottom: 0;
    }

    details.event-row summary {
      list-style: none;
      cursor: pointer;
      padding: 14px 18px 14px 34px;
      position: relative;
    }

    details.event-row summary::-webkit-details-marker {
      display: none;
    }

    details.event-row summary::before {
      content: "▸";
      position: absolute;
      left: 14px;
      top: 15px;
      color: var(--muted);
      transition: transform 120ms ease;
    }

    details.event-row[open] summary {
      background: rgba(255, 255, 255, 0.02);
    }

    details.event-row[open] summary::before {
      transform: rotate(90deg);
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
      gap: 10px;
      min-width: 0;
    }

    .source-inline {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }

    .source-pair,
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: var(--radius);
      padding: 4px 6px;
      border: 0;
      background: #2a2d35;
      color: var(--text);
      font-family: "IBM Plex Mono", "SFMono-Regular", Consolas, monospace;
      font-size: 0.83rem;
      min-width: 0;
    }

    .source-pair-key {
      color: var(--muted);
      text-transform: none;
      letter-spacing: 0;
      font-size: inherit;
      font-weight: 700;
    }

    .source-pair-value {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      max-width: 320px;
    }

    .badge.score {
      color: #091116;
      font-weight: 700;
      background: #4aa3c8;
    }

    .detail-cell {
      color: var(--text);
      font-size: 0.88rem;
      line-height: 1.45;
      word-break: break-word;
    }

    .detail-cell.muted {
      color: var(--muted);
    }

    .event-body {
      padding: 0 18px 18px 34px;
      display: grid;
      gap: 14px;
      background: var(--surface-alt);
      border-top: 1px solid var(--line);
    }

    .event-context {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      padding-top: 4px;
    }

    .document-tabs {
      display: flex;
      gap: 0;
      border-bottom: 1px solid var(--line);
    }

    .document-tab {
      appearance: none;
      border: 0;
      border-right: 1px solid var(--line);
      background: transparent;
      color: var(--muted);
      padding: 10px 16px;
      font-size: 0.92rem;
      font-weight: 600;
      cursor: pointer;
      box-shadow: none;
    }

    .document-tab:last-child {
      border-right: 0;
    }

    .document-tab.active {
      background: var(--panel);
      color: var(--text);
    }

    .document-panel {
      border: 1px solid var(--line);
      border-top: 0;
      background: var(--panel);
    }

    .document-panel.hidden {
      display: none;
    }

    .document-field-table {
      display: grid;
    }

    .document-field-row {
      display: grid;
      grid-template-columns: minmax(200px, 260px) minmax(0, 1fr);
      border-top: 1px solid var(--line);
    }

    .document-field-row:first-child {
      border-top: 0;
    }

    .document-field-name,
    .document-field-value {
      min-width: 0;
      padding: 12px 14px;
      font-family: "IBM Plex Mono", "SFMono-Regular", Consolas, monospace;
      font-size: 0.9rem;
      line-height: 1.55;
    }

    .document-field-name {
      color: var(--muted);
      font-weight: 700;
      border-right: 1px solid var(--line);
      overflow-wrap: anywhere;
    }

    .document-field-value {
      color: var(--text);
      white-space: pre-wrap;
      word-break: break-word;
    }

    .document-empty {
      padding: 16px;
      color: var(--muted);
      font-size: 0.9rem;
    }

    pre {
      margin: 0;
      padding: 16px;
      overflow: auto;
      border-radius: var(--radius);
      background: var(--panel-strong);
      border: 1px solid var(--line);
      color: var(--text);
      font-family: "IBM Plex Mono", "SFMono-Regular", Consolas, monospace;
      font-size: 0.9rem;
      line-height: 1.55;
    }

    .document-json {
      border-radius: 0;
      border: 0;
      background: transparent;
    }

    .empty {
      padding: 28px;
      margin: 18px;
      text-align: center;
      border: 1px dashed var(--line);
      border-radius: var(--radius);
      background: var(--surface);
    }

    .errors {
      display: grid;
      gap: 10px;
      padding: 12px 16px 0;
    }

    .error-item {
      padding: 14px 16px;
      border-radius: var(--radius);
      border: 1px solid rgba(246, 111, 111, 0.28);
      background: var(--danger-soft);
      color: #ffd9d9;
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
      .menu,
      .toolbar-meta,
      .menu-links {
        flex-direction: column;
        align-items: stretch;
      }

      .span-9, .span-6, .span-4, .span-3, .span-2 {
        grid-column: span 12;
      }

      .timebox-control,
      .timebox-row,
      .timebox-date-grid,
      .timebox-presets {
        grid-template-columns: 1fr;
      }

      .timebox-dates-btn,
      .timebox-row button {
        width: 100%;
      }

      .timebox-popover {
        width: 100%;
        left: 0;
      }

      .menu-link {
        min-height: 40px;
      }

      .menu {
        padding: 10px 14px;
        align-items: stretch;
      }

      .discover-layout {
        grid-template-columns: 1fr;
      }

      .discover-sidebar {
        border-right: 0;
        border-bottom: 1px solid var(--line);
      }

      .discover-main {
        min-width: 0;
      }

      .timeline-chart {
        gap: 6px;
      }

      .events-head-right,
      .pager {
        justify-content: space-between;
      }

      .events-header,
      .event-summary-row {
        min-width: 0;
      }

      .status {
        text-align: left;
      }

      .source-pair-value {
        max-width: 100%;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <header class="menu">
      <div class="menu-brand">
        <span class="brand-badge">D</span>
        <div class="brand-copy">
          <div class="menu-label">elkgo</div>
          <div class="menu-title">Discover</div>
        </div>
      </div>
      <nav class="menu-links" aria-label="Primary">
        <a class="menu-link active" href="/">Search</a>
        <a class="menu-link" href="/cluster">Cluster dashboard</a>
      </nav>
    </header>

    <section class="search-panel">
      <div class="toolbar-meta">
        <div id="hit-count" class="hit-count">Discover ready</div>
        <div id="status" class="status" aria-live="polite"></div>
      </div>
      <form id="search-form" class="query-form">
        <label class="query-field">
          <span>Search</span>
          <div class="query-box">
            <span class="query-icon" aria-hidden="true">›</span>
            <input id="q" name="q" placeholder="Search... (e.g. status:200 AND extension:php)">
          </div>
        </label>
        <div class="grid search-grid">
          <label class="span-4">
            <span>Index</span>
            <select id="index" name="index" required>
              <option value="">Loading indexes...</option>
            </select>
          </label>
          <label class="span-2">
            <span>Top K</span>
            <input id="k" name="k" class="input-compact" type="number" min="1" max="1000" value="100">
          </label>
          <div class="timebox-shell span-6">
            <span>Time range</span>
            <div class="timebox-control">
              <button type="button" id="timebox-trigger" class="timebox-trigger" aria-expanded="false" aria-controls="timebox-popover">
                <span class="timebox-trigger-main">
                  <span class="timebox-calendar" aria-hidden="true"></span>
                  <span class="timebox-trigger-copy">
                    <span id="timebox-label" class="timebox-value">Last 15 minutes</span>
                    <span id="timebox-caption" class="timebox-caption">UTC window will update when you apply it.</span>
                  </span>
                </span>
                <span class="timebox-caret" aria-hidden="true"></span>
              </button>
              <button type="button" id="timebox-dates-btn" class="timebox-dates-btn">Show dates</button>
            </div>
            <div id="timebox-popover" class="timebox-popover hidden">
              <div class="timebox-popover-arrow" aria-hidden="true"></div>
              <div class="timebox-header">
                <div class="timebox-title">Quick select</div>
              </div>
              <div class="timebox-row">
                <select id="timebox-mode">
                  <option value="last">Last</option>
                </select>
                <input id="timebox-amount" type="number" min="1" step="1" value="15">
                <select id="timebox-unit">
                  <option value="minutes">minutes</option>
                  <option value="hours">hours</option>
                  <option value="days">days</option>
                  <option value="weeks">weeks</option>
                </select>
                <button type="button" id="timebox-apply-btn">Apply</button>
              </div>
              <div class="timebox-section">
                <div class="timebox-section-title">Commonly used</div>
                <div class="timebox-presets">
                  <button type="button" class="timebox-preset" data-timebox-preset="today">Today</button>
                  <button type="button" class="timebox-preset" data-timebox-preset="last-15-minutes">Last 15 minutes</button>
                  <button type="button" class="timebox-preset" data-timebox-preset="today-so-far">Today so far</button>
                  <button type="button" class="timebox-preset" data-timebox-preset="last-30-minutes">Last 30 minutes</button>
                  <button type="button" class="timebox-preset" data-timebox-preset="yesterday">Yesterday</button>
                  <button type="button" class="timebox-preset" data-timebox-preset="last-1-hour">Last 1 hour</button>
                  <button type="button" class="timebox-preset" data-timebox-preset="day-before-yesterday">Day before yesterday</button>
                  <button type="button" class="timebox-preset" data-timebox-preset="last-4-hours">Last 4 hours</button>
                  <button type="button" class="timebox-preset" data-timebox-preset="this-week">This week</button>
                  <button type="button" class="timebox-preset" data-timebox-preset="last-week">Last week</button>
                  <button type="button" class="timebox-preset" data-timebox-preset="last-12-hours">Last 12 hours</button>
                </div>
              </div>
              <div id="timebox-date-panel" class="timebox-date-panel hidden">
                <div class="timebox-section-title">Exact dates</div>
                <div class="timebox-date-grid">
                  <label>
                    <span>From</span>
                    <input id="timebox-date-from" type="datetime-local">
                  </label>
                  <label>
                    <span>To</span>
                    <input id="timebox-date-to" type="datetime-local">
                  </label>
                </div>
                <div class="timebox-date-note">Exact timestamp filters are applied in UTC, while the inputs follow your local browser time.</div>
              </div>
            </div>
          </div>
        </div>
        <input id="day_from" name="day_from" type="hidden">
        <input id="day_to" name="day_to" type="hidden">
        <input id="time_from" name="time_from" type="hidden">
        <input id="time_to" name="time_to" type="hidden">
        <div class="actions">
          <button type="submit">Refresh</button>
          <button type="button" class="secondary" id="reset-btn">Reset</button>
        </div>
      </form>
      <div id="summary" class="pills"></div>
    </section>

    <section class="results">
      <div id="errors" class="errors"></div>
      <div id="results"></div>
    </section>
  </main>

  <script>
    const form = document.getElementById("search-form");
    const statusEl = document.getElementById("status");
    const hitCountEl = document.getElementById("hit-count");
    const resultsEl = document.getElementById("results");
    const errorsEl = document.getElementById("errors");
    const summaryEl = document.getElementById("summary");
    const resetBtn = document.getElementById("reset-btn");
    const indexCatalogEl = document.getElementById("index-catalog");
    const DEFAULT_TOP_K = "100";
    let availableIndexes = [];
    let pendingIndexValue = "";
    let selectedFields = [];
    let currentResultData = null;
    let currentOffset = 0;

    const fields = {
      index: document.getElementById("index"),
      q: document.getElementById("q"),
      day_from: document.getElementById("day_from"),
      day_to: document.getElementById("day_to"),
      time_from: document.getElementById("time_from"),
      time_to: document.getElementById("time_to"),
      k: document.getElementById("k")
    };

    const timeboxEls = {
      trigger: document.getElementById("timebox-trigger"),
      label: document.getElementById("timebox-label"),
      caption: document.getElementById("timebox-caption"),
      datesBtn: document.getElementById("timebox-dates-btn"),
      popover: document.getElementById("timebox-popover"),
      mode: document.getElementById("timebox-mode"),
      amount: document.getElementById("timebox-amount"),
      unit: document.getElementById("timebox-unit"),
      applyBtn: document.getElementById("timebox-apply-btn"),
      datePanel: document.getElementById("timebox-date-panel"),
      dateFrom: document.getElementById("timebox-date-from"),
      dateTo: document.getElementById("timebox-date-to"),
      presets: Array.from(document.querySelectorAll("[data-timebox-preset]"))
    };

    const DEFAULT_TIMEBOX = Object.freeze({ kind: "relative", amount: 15, unit: "minutes" });
    let appliedTimebox = cloneTimebox(DEFAULT_TIMEBOX);
    let draftTimebox = cloneTimebox(DEFAULT_TIMEBOX);
    let timeboxDatesVisible = false;
    let timeboxCustomDirty = false;

    function setStatus(message, isError) {
      statusEl.textContent = message || "";
      statusEl.className = isError ? "status error" : "status";
    }

    function setHitCount(message) {
      hitCountEl.textContent = message || "Discover ready";
    }

    function normalizeTimeboxUnit(unit) {
      switch (String(unit || "").toLowerCase()) {
        case "hours":
        case "days":
        case "weeks":
          return String(unit).toLowerCase();
        default:
          return "minutes";
      }
    }

    function cloneTimebox(state) {
      if (!state || typeof state !== "object") {
        return { kind: "relative", amount: 15, unit: "minutes" };
      }
      if (state.kind === "named") {
        return { kind: "named", preset: String(state.preset || "today") };
      }
      if (state.kind === "custom") {
        return { kind: "custom", from: String(state.from || ""), to: String(state.to || "") };
      }
      return {
        kind: "relative",
        amount: Math.max(1, Math.floor(Number(state.amount) || 15)),
        unit: normalizeTimeboxUnit(state.unit)
      };
    }

    function padDatePart(value) {
      return String(value).padStart(2, "0");
    }

    function formatDayValue(date) {
      return date.toISOString().slice(0, 10);
    }

    function isValidDate(value) {
      return value instanceof Date && !Number.isNaN(value.getTime());
    }

    function startOfUTCDay(date) {
      return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 0, 0, 0, 0));
    }

    function endOfUTCDay(date) {
      return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 23, 59, 59, 999));
    }

    function startOfUTCWeek(date) {
      const start = startOfUTCDay(date);
      const weekday = start.getUTCDay();
      const offset = weekday === 0 ? 6 : weekday - 1;
      start.setUTCDate(start.getUTCDate() - offset);
      return start;
    }

    function shiftUTC(date, amount, unit) {
      const shifted = new Date(date.getTime());
      switch (unit) {
        case "hours":
          shifted.setUTCHours(shifted.getUTCHours() + amount);
          break;
        case "days":
          shifted.setUTCDate(shifted.getUTCDate() + amount);
          break;
        case "weeks":
          shifted.setUTCDate(shifted.getUTCDate() + (amount * 7));
          break;
        default:
          shifted.setUTCMinutes(shifted.getUTCMinutes() + amount);
          break;
      }
      return shifted;
    }

    function relativeTimeboxLabel(amount, unit) {
      const baseUnit = amount === 1 ? unit.replace(/s$/, "") : unit;
      return "Last " + amount + " " + baseUnit;
    }

    function presetTimeboxState(preset) {
      switch (preset) {
        case "today":
        case "today-so-far":
        case "yesterday":
        case "day-before-yesterday":
        case "this-week":
        case "last-week":
          return { kind: "named", preset: preset };
        case "last-30-minutes":
          return { kind: "relative", amount: 30, unit: "minutes" };
        case "last-1-hour":
          return { kind: "relative", amount: 1, unit: "hours" };
        case "last-4-hours":
          return { kind: "relative", amount: 4, unit: "hours" };
        case "last-12-hours":
          return { kind: "relative", amount: 12, unit: "hours" };
        case "last-15-minutes":
        default:
          return { kind: "relative", amount: 15, unit: "minutes" };
      }
    }

    function timeboxPresetKey(state) {
      const normalized = cloneTimebox(state);
      if (normalized.kind === "named") {
        return normalized.preset;
      }
      if (normalized.kind === "relative") {
        if (normalized.amount === 15 && normalized.unit === "minutes") return "last-15-minutes";
        if (normalized.amount === 30 && normalized.unit === "minutes") return "last-30-minutes";
        if (normalized.amount === 1 && normalized.unit === "hours") return "last-1-hour";
        if (normalized.amount === 4 && normalized.unit === "hours") return "last-4-hours";
        if (normalized.amount === 12 && normalized.unit === "hours") return "last-12-hours";
      }
      return "";
    }

    function computeTimeboxWindow(state) {
      const normalized = cloneTimebox(state);
      const now = new Date();

      if (normalized.kind === "named") {
        switch (normalized.preset) {
          case "today": {
            const start = startOfUTCDay(now);
            return { start: start, end: endOfUTCDay(now), label: "Today" };
          }
          case "today-so-far": {
            return { start: startOfUTCDay(now), end: now, label: "Today so far" };
          }
          case "yesterday": {
            const yesterday = shiftUTC(startOfUTCDay(now), -1, "days");
            return { start: startOfUTCDay(yesterday), end: endOfUTCDay(yesterday), label: "Yesterday" };
          }
          case "day-before-yesterday": {
            const previous = shiftUTC(startOfUTCDay(now), -2, "days");
            return { start: startOfUTCDay(previous), end: endOfUTCDay(previous), label: "Day before yesterday" };
          }
          case "last-week": {
            const currentWeekStart = startOfUTCWeek(now);
            const lastWeekStart = shiftUTC(currentWeekStart, -1, "weeks");
            const lastWeekEnd = new Date(currentWeekStart.getTime() - 1);
            return { start: lastWeekStart, end: lastWeekEnd, label: "Last week" };
          }
          case "this-week":
          default:
            return { start: startOfUTCWeek(now), end: now, label: "This week" };
        }
      }

      if (normalized.kind === "custom") {
        const start = new Date(normalized.from);
        const end = new Date(normalized.to);
        if (isValidDate(start) && isValidDate(end) && end >= start) {
          return { start: start, end: end, label: "Custom range" };
        }
      }

      const end = now;
      const start = shiftUTC(end, -normalized.amount, normalized.unit);
      return { start: start, end: end, label: relativeTimeboxLabel(normalized.amount, normalized.unit) };
    }

    function formatTimeboxInstant(date) {
      return date.toLocaleString([], {
        timeZone: "UTC",
        year: "numeric",
        month: "short",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit"
      }).replace(",", "") + " UTC";
    }

    function formatTimeboxCaption(window) {
      return formatTimeboxInstant(window.start) + " -> " + formatTimeboxInstant(window.end);
    }

    function toLocalDateTimeInputValue(date) {
      if (!isValidDate(date)) return "";
      return [
        date.getFullYear(),
        padDatePart(date.getMonth() + 1),
        padDatePart(date.getDate())
      ].join("-") + "T" + [
        padDatePart(date.getHours()),
        padDatePart(date.getMinutes())
      ].join(":");
    }

    function appendTimeboxParams(params) {
      const state = cloneTimebox(appliedTimebox);
      params.set("timebox_kind", state.kind);
      if (state.kind === "relative") {
        params.set("timebox_amount", String(state.amount));
        params.set("timebox_unit", state.unit);
        return;
      }
      if (state.kind === "named") {
        params.set("timebox_preset", state.preset);
      }
    }

    function deriveTimeboxFromParams(params) {
      const kind = params.get("timebox_kind");
      if (kind === "relative") {
        return {
          kind: "relative",
          amount: Math.max(1, Math.floor(Number(params.get("timebox_amount")) || 15)),
          unit: normalizeTimeboxUnit(params.get("timebox_unit"))
        };
      }
      if (kind === "named") {
        return { kind: "named", preset: String(params.get("timebox_preset") || "today") };
      }
      if (kind === "custom") {
        return {
          kind: "custom",
          from: String(params.get("time_from") || ""),
          to: String(params.get("time_to") || "")
        };
      }

      const timeFrom = params.get("time_from");
      const timeTo = params.get("time_to");
      if (timeFrom && timeTo) {
        return { kind: "custom", from: timeFrom, to: timeTo };
      }

      const dayFrom = params.get("day_from");
      const dayTo = params.get("day_to");
      if (dayFrom && dayTo) {
        return {
          kind: "custom",
          from: dayFrom + "T00:00:00Z",
          to: dayTo + "T23:59:59Z"
        };
      }

      return cloneTimebox(DEFAULT_TIMEBOX);
    }

    function syncTimeboxControls() {
      const state = cloneTimebox(draftTimebox);
      if (state.kind === "relative") {
        timeboxEls.mode.value = "last";
        timeboxEls.amount.value = String(state.amount);
        timeboxEls.unit.value = state.unit;
      }

      if (timeboxDatesVisible) {
        const preview = computeTimeboxWindow(state);
        timeboxEls.dateFrom.value = state.kind === "custom" ? toLocalDateTimeInputValue(new Date(state.from)) : toLocalDateTimeInputValue(preview.start);
        timeboxEls.dateTo.value = state.kind === "custom" ? toLocalDateTimeInputValue(new Date(state.to)) : toLocalDateTimeInputValue(preview.end);
      }

      const presetKey = timeboxPresetKey(state);
      timeboxEls.presets.forEach(function (button) {
        button.classList.toggle("active", button.dataset.timeboxPreset === presetKey);
      });
    }

    function setTimeboxDatesVisible(visible) {
      timeboxDatesVisible = !!visible;
      timeboxEls.datePanel.classList.toggle("hidden", !timeboxDatesVisible);
      timeboxEls.datesBtn.textContent = timeboxDatesVisible ? "Hide dates" : "Show dates";
      if (timeboxDatesVisible) {
        timeboxCustomDirty = draftTimebox.kind === "custom";
        syncTimeboxControls();
        return;
      }
      timeboxCustomDirty = false;
    }

    function syncSearchWindowFromTimebox() {
      const window = computeTimeboxWindow(appliedTimebox);
      fields.day_from.value = formatDayValue(window.start);
      fields.day_to.value = formatDayValue(window.end);
      fields.time_from.value = window.start.toISOString();
      fields.time_to.value = window.end.toISOString();
      timeboxEls.label.textContent = window.label;
      timeboxEls.caption.textContent = formatTimeboxCaption(window);
      return window;
    }

    function applyTimeboxState(state) {
      appliedTimebox = cloneTimebox(state);
      draftTimebox = cloneTimebox(state);
      syncSearchWindowFromTimebox();
      syncTimeboxControls();
    }

    function openTimeboxPopover(showDates) {
      draftTimebox = cloneTimebox(appliedTimebox);
      timeboxEls.popover.classList.remove("hidden");
      timeboxEls.trigger.setAttribute("aria-expanded", "true");
      if (typeof showDates === "boolean") {
        setTimeboxDatesVisible(showDates);
      } else {
        setTimeboxDatesVisible(timeboxDatesVisible);
      }
      syncTimeboxControls();
    }

    function closeTimeboxPopover() {
      timeboxEls.popover.classList.add("hidden");
      timeboxEls.trigger.setAttribute("aria-expanded", "false");
    }

    function customTimeboxFromInputs() {
      const rawFrom = String(timeboxEls.dateFrom.value || "").trim();
      const rawTo = String(timeboxEls.dateTo.value || "").trim();
      if (rawFrom === "" && rawTo === "") return null;
      if (rawFrom === "" || rawTo === "") {
        throw new Error("Provide both exact dates before applying.");
      }

      const start = new Date(rawFrom);
      const end = new Date(rawTo);
      if (!isValidDate(start) || !isValidDate(end)) {
        throw new Error("Invalid exact date selection.");
      }
      if (end < start) {
        throw new Error("The end date must be after the start date.");
      }
      return { kind: "custom", from: start.toISOString(), to: end.toISOString() };
    }

    function applyTimeboxSelection() {
      try {
        const useCustom = timeboxDatesVisible && (timeboxCustomDirty || draftTimebox.kind === "custom");
        const custom = useCustom ? customTimeboxFromInputs() : null;
        applyTimeboxState(custom || draftTimebox);
        closeTimeboxPopover();
        currentOffset = 0;
        runSearch(true);
      } catch (error) {
        setStatus(error.message || "Could not apply the selected time range.", true);
      }
    }

    const TIMESTAMP_FIELDS = ["@timestamp", "timestamp", "event_time", "created", "created_at", "observed_at", "time"];
    const SUMMARY_FIELDS = ["title", "message", "summary", "description", "body", "event"];
    const CHIP_FIELDS = ["level", "severity", "service", "host", "hostname", "env", "environment", "dataset", "source", "status"];
    const INLINE_HIDDEN_FIELDS = new Set(["id", "partition_day"].concat(TIMESTAMP_FIELDS));
    const SOURCE_PRIORITY_FIELDS = ["message", "event", "summary", "title", "request", "response", "status", "tags", "url", "path", "service", "host", "hostname", "clientip", "ip", "agent"];
    const DEFAULT_PINNED_SKIP_FIELDS = new Set(["id", "partition_day"].concat(TIMESTAMP_FIELDS, SUMMARY_FIELDS));
    const FIELD_PRIORITY_FIELDS = TIMESTAMP_FIELDS.concat(["id", "partition_day"], SUMMARY_FIELDS, SOURCE_PRIORITY_FIELDS);
    const flatSourceCache = typeof WeakMap === "function" ? new WeakMap() : null;

    function isPlainObject(value) {
      return value !== null && typeof value === "object" && !Array.isArray(value);
    }

    function isScalar(value) {
      return value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean";
    }

    function isCompactArray(value) {
      return Array.isArray(value) && value.length > 0 && value.length <= 6 && value.every(isScalar);
    }

    function leafFieldName(field) {
      const parts = String(field || "").split(".");
      return parts[parts.length - 1] || String(field || "");
    }

    function fieldPriority(field) {
      const exact = FIELD_PRIORITY_FIELDS.indexOf(field);
      if (exact >= 0) return exact;
      const leaf = FIELD_PRIORITY_FIELDS.indexOf(leafFieldName(field));
      if (leaf >= 0) return FIELD_PRIORITY_FIELDS.length + leaf;
      return FIELD_PRIORITY_FIELDS.length * 2;
    }

    function compareFieldKeys(a, b) {
      const aPriority = fieldPriority(a);
      const bPriority = fieldPriority(b);
      if (aPriority !== bPriority) return aPriority - bPriority;
      return a.localeCompare(b);
    }

    function mergeFlattenedValue(existing, incoming) {
      const values = [];
      const seen = new Set();

      function append(value) {
        if (value === undefined) return;
        const marker = value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean"
          ? typeof value + ":" + String(value)
          : JSON.stringify(value);
        if (seen.has(marker)) return;
        seen.add(marker);
        values.push(value);
      }

      (Array.isArray(existing) ? existing : [existing]).forEach(append);
      (Array.isArray(incoming) ? incoming : [incoming]).forEach(append);

      if (values.length === 0) return undefined;
      return values.length === 1 ? values[0] : values;
    }

    function flattenSourceFields(source) {
      if (!isPlainObject(source)) return {};
      if (flatSourceCache && flatSourceCache.has(source)) {
        return flatSourceCache.get(source);
      }

      const flat = {};

      function append(path, value) {
        if (!path || value === undefined) return;
        flat[path] = mergeFlattenedValue(flat[path], value);
      }

      function visit(value, path) {
        if (isPlainObject(value)) {
          Object.keys(value).sort(compareFieldKeys).forEach(function (key) {
            visit(value[key], path ? path + "." + key : key);
          });
          return;
        }

        if (Array.isArray(value)) {
          if (value.length === 0) {
            append(path, []);
            return;
          }

          const hasNestedItems = value.some(function (item) {
            return isPlainObject(item) || Array.isArray(item);
          });

          if (!hasNestedItems) {
            append(path, value);
            return;
          }

          value.forEach(function (item) {
            if (isPlainObject(item) || Array.isArray(item)) {
              visit(item, path);
              return;
            }
            append(path, item);
          });
          return;
        }

        append(path, value);
      }

      visit(source, "");
      if (flatSourceCache) {
        flatSourceCache.set(source, flat);
      }
      return flat;
    }

    function getFieldValue(source, field) {
      const flat = flattenSourceFields(source);
      if (Object.prototype.hasOwnProperty.call(flat, field)) {
        return flat[field];
      }
      if (source && Object.prototype.hasOwnProperty.call(source, field)) {
        return source[field];
      }
      return undefined;
    }

    function firstPresentField(source, keys) {
      for (let i = 0; i < keys.length; i += 1) {
        const key = keys[i];
        const value = getFieldValue(source, key);
        if (value !== undefined && value !== null && value !== "") {
          return value;
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
      if (Array.isArray(value) && value.length === 0) return "[]";
      if (Array.isArray(value) && value.every(isScalar)) {
        const preview = value.slice(0, 6).map(function (item) {
          return item === null ? "null" : String(item);
        });
        if (value.length <= 6) return preview.join(" • ");
        return preview.join(" • ") + " • +" + (value.length - 6) + " more";
      }
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

    function flattenedFieldStats(hits) {
      const counts = {};
      hits.forEach(function (hit) {
        const source = isPlainObject(hit.source) ? hit.source : {};
        Object.keys(flattenSourceFields(source)).forEach(function (key) {
          counts[key] = (counts[key] || 0) + 1;
        });
      });
      return Object.keys(counts).map(function (key) {
        return { key: key, count: counts[key] };
      }).sort(function (a, b) {
        if (b.count === a.count) return compareFieldKeys(a.key, b.key);
        return b.count - a.count;
      });
    }

    function normalizeSelectedFields(hits) {
      const stats = flattenedFieldStats(hits);
      const available = new Set(stats.map(function (entry) { return entry.key; }));

      selectedFields = selectedFields.filter(function (field) {
        return available.has(field);
      });

      if (selectedFields.length > 0) return;

      stats.forEach(function (entry) {
        if (selectedFields.length >= 3) return;
        if (CHIP_FIELDS.indexOf(entry.key) >= 0) return;
        if (DEFAULT_PINNED_SKIP_FIELDS.has(entry.key)) return;
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

    function documentFieldEntries(source) {
      const flat = flattenSourceFields(source);
      return Object.keys(flat).sort(compareFieldKeys).map(function (key) {
        return { key: key, value: displayValue(flat[key]) };
      });
    }

    function inlineSourcePairs(source) {
      const flat = flattenSourceFields(source);
      return Object.keys(flat).filter(function (key) {
        if (INLINE_HIDDEN_FIELDS.has(key) || selectedFields.indexOf(key) >= 0) return false;
        const value = flat[key];
        return value !== undefined && value !== null && value !== "";
      }).sort(compareFieldKeys).map(function (key) {
        return { key: key, value: displayValue(flat[key]) };
      }).slice(0, 16);
    }

    function renderFieldSidebar(hits) {
      const sidebar = document.createElement("aside");
      sidebar.className = "discover-sidebar";

      const currentIndex = currentResultData && currentResultData.index && currentResultData.index !== "_all"
        ? currentResultData.index
        : "all indexes";
      const indexBanner = document.createElement("div");
      indexBanner.className = "sidebar-index";
      indexBanner.textContent = currentIndex;
      sidebar.appendChild(indexBanner);

      const selectedSection = document.createElement("section");
      selectedSection.className = "sidebar-section";
      selectedSection.innerHTML = '<div class="sidebar-title">Selected fields</div>';

      const selectedWrap = document.createElement("div");
      selectedWrap.className = "selected-fields";
      const sourcePill = document.createElement("div");
      sourcePill.className = "field-pill locked";
      sourcePill.textContent = "_source";
      selectedWrap.appendChild(sourcePill);

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

      const availableSection = document.createElement("section");
      availableSection.className = "sidebar-section";
      availableSection.innerHTML = '<div class="sidebar-title">Available fields</div>';

      const stats = flattenedFieldStats(hits);
      if (stats.length === 0) {
        const empty = document.createElement("div");
        empty.className = "sidebar-empty";
        empty.textContent = "No flattened fields were found in the current result set.";
        availableSection.appendChild(empty);
      } else {
        const list = document.createElement("div");
        list.className = "field-list";
        stats.forEach(function (entry) {
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

    function histogramData(hits) {
      const points = hits.map(function (hit) {
        const source = isPlainObject(hit.source) ? hit.source : {};
        const raw = firstPresentField(source, TIMESTAMP_FIELDS);
        const parsed = new Date(raw);
        if (!raw || Number.isNaN(parsed.getTime())) return null;
        return parsed;
      }).filter(Boolean).sort(function (a, b) { return a - b; });

      if (points.length === 0) {
        return { buckets: [], bucketMs: 0, start: null, end: null };
      }

      const min = points[0].getTime();
      const max = points[points.length - 1].getTime();
      const span = Math.max(max - min, 1);
      let bucketMs = 30 * 1000;
      if (span > 15 * 60 * 1000) bucketMs = 60 * 1000;
      if (span > 2 * 60 * 60 * 1000) bucketMs = 5 * 60 * 1000;
      if (span > 12 * 60 * 60 * 1000) bucketMs = 30 * 60 * 1000;
      if (span > 2 * 24 * 60 * 60 * 1000) bucketMs = 2 * 60 * 60 * 1000;
      if (span > 14 * 24 * 60 * 60 * 1000) bucketMs = 24 * 60 * 60 * 1000;
      const buckets = {};

      points.forEach(function (point) {
        const stamp = point.getTime();
        const bucketStart = stamp - (stamp % bucketMs);
        const key = String(bucketStart);
        buckets[key] = (buckets[key] || 0) + 1;
      });

      return {
        bucketMs: bucketMs,
        start: points[0],
        end: points[points.length - 1],
        buckets: Object.keys(buckets).sort().map(function (key) {
          const start = new Date(Number(key));
          const label = bucketMs >= (24 * 60 * 60 * 1000)
            ? start.toLocaleDateString([], { month: "short", day: "2-digit" })
            : start.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
          return { label: label, count: buckets[key] };
        })
      };
    }

    function bucketLabel(bucketMs) {
      if (bucketMs >= 24 * 60 * 60 * 1000) {
        const days = Math.round(bucketMs / (24 * 60 * 60 * 1000));
        return days + " day" + (days === 1 ? "" : "s");
      }
      if (bucketMs >= 60 * 60 * 1000) {
        const hours = Math.round(bucketMs / (60 * 60 * 1000));
        return hours + " hour" + (hours === 1 ? "" : "s");
      }
      if (bucketMs >= 60 * 1000) {
        const minutes = Math.round(bucketMs / (60 * 1000));
        return minutes + " minute" + (minutes === 1 ? "" : "s");
      }
      const seconds = Math.round(bucketMs / 1000);
      return seconds + " second" + (seconds === 1 ? "" : "s");
    }

    function timelineRangeLabel(start, end) {
      if (!start || !end) return "";
      const formatter = {
        month: "short",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
      };
      return start.toLocaleString([], formatter) + " - " + end.toLocaleString([], formatter);
    }

    function renderTimeline(hits) {
      const histogram = histogramData(hits);
      const buckets = histogram.buckets;
      const panel = document.createElement("section");
      panel.className = "timeline-panel";

      const head = document.createElement("div");
      head.className = "timeline-head";
      const rangeText = timelineRangeLabel(histogram.start, histogram.end);
      const intervalText = histogram.bucketMs ? "timestamp per " + bucketLabel(histogram.bucketMs) : "";
      head.innerHTML = '<div class="timeline-title">Histogram</div><div class="timeline-meta">' + [rangeText, intervalText].filter(Boolean).join(" · ") + '</div>';
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

    function renderDocumentView(source) {
      const wrapper = document.createElement("div");

      const tabs = document.createElement("div");
      tabs.className = "document-tabs";

      const tableTab = document.createElement("button");
      tableTab.type = "button";
      tableTab.className = "document-tab active";
      tableTab.textContent = "Table";
      tabs.appendChild(tableTab);

      const jsonTab = document.createElement("button");
      jsonTab.type = "button";
      jsonTab.className = "document-tab";
      jsonTab.textContent = "JSON";
      tabs.appendChild(jsonTab);

      const tablePanel = document.createElement("div");
      tablePanel.className = "document-panel";
      const fieldTable = document.createElement("div");
      fieldTable.className = "document-field-table";
      const entries = documentFieldEntries(source);

      if (entries.length === 0) {
        const empty = document.createElement("div");
        empty.className = "document-empty";
        empty.textContent = "No fields were found in this document.";
        fieldTable.appendChild(empty);
      } else {
        entries.forEach(function (entry) {
          const row = document.createElement("div");
          row.className = "document-field-row";

          const name = document.createElement("div");
          name.className = "document-field-name";
          name.textContent = entry.key;
          row.appendChild(name);

          const value = document.createElement("div");
          value.className = "document-field-value";
          value.textContent = entry.value || "—";
          row.appendChild(value);

          fieldTable.appendChild(row);
        });
      }
      tablePanel.appendChild(fieldTable);

      const jsonPanel = document.createElement("div");
      jsonPanel.className = "document-panel hidden";
      const pre = document.createElement("pre");
      pre.className = "document-json";
      pre.textContent = JSON.stringify(source, null, 2);
      jsonPanel.appendChild(pre);

      function setView(view) {
        const showingTable = view === "table";
        tableTab.classList.toggle("active", showingTable);
        jsonTab.classList.toggle("active", !showingTable);
        tablePanel.classList.toggle("hidden", !showingTable);
        jsonPanel.classList.toggle("hidden", showingTable);
      }

      tableTab.addEventListener("click", function () { setView("table"); });
      jsonTab.addEventListener("click", function () { setView("json"); });

      wrapper.appendChild(tabs);
      wrapper.appendChild(tablePanel);
      wrapper.appendChild(jsonPanel);
      return wrapper;
    }

    function renderEventTable(hits) {
      const panel = document.createElement("section");
      panel.className = "events-panel";

      const head = document.createElement("div");
      head.className = "events-head";
      const headLeft = document.createElement("div");
      const title = document.createElement("div");
      title.className = "events-title";
      title.textContent = "Event Stream";
      headLeft.appendChild(title);

      const meta = document.createElement("div");
      meta.className = "events-meta";
      meta.textContent = hits.length + " visible hit" + (hits.length === 1 ? "" : "s");
      headLeft.appendChild(meta);
      head.appendChild(headLeft);

      const headRight = document.createElement("div");
      headRight.className = "events-head-right";
      const from = currentResultData && typeof currentResultData.from === "number" ? currentResultData.from : 0;

      const range = document.createElement("div");
      range.className = "events-meta";
      range.textContent = (from + 1) + "-" + (from + hits.length);
      headRight.appendChild(range);

      const pager = document.createElement("div");
      pager.className = "pager";

      const prevBtn = document.createElement("button");
      prevBtn.type = "button";
      prevBtn.className = "secondary";
      prevBtn.textContent = "Prev";
      prevBtn.disabled = from <= 0;
      prevBtn.addEventListener("click", function () {
        currentOffset = Math.max(0, from - (currentResultData && currentResultData.k ? currentResultData.k : Number(DEFAULT_TOP_K)));
        runSearch(true);
      });
      pager.appendChild(prevBtn);

      const nextBtn = document.createElement("button");
      nextBtn.type = "button";
      nextBtn.className = "secondary";
      nextBtn.textContent = "Next";
      nextBtn.disabled = !(currentResultData && currentResultData.has_more);
      nextBtn.addEventListener("click", function () {
        currentOffset = from + (currentResultData && currentResultData.k ? currentResultData.k : Number(DEFAULT_TOP_K));
        runSearch(true);
      });
      pager.appendChild(nextBtn);

      headRight.appendChild(pager);
      head.appendChild(headRight);
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
        const inlinePairs = inlineSourcePairs(source);
        if (inlinePairs.length > 0) {
          const inlineWrap = document.createElement("div");
          inlineWrap.className = "source-inline";
          inlinePairs.forEach(function (entry) {
            const pair = document.createElement("div");
            pair.className = "source-pair";

            const pairKey = document.createElement("span");
            pairKey.className = "source-pair-key";
            pairKey.textContent = entry.key + ":";
            pair.appendChild(pairKey);

            const pairValue = document.createElement("span");
            pairValue.className = "source-pair-value";
            pairValue.textContent = entry.value;
            pair.appendChild(pairValue);

            inlineWrap.appendChild(pair);
          });
          sourceCell.appendChild(inlineWrap);
        } else {
          const fallback = document.createElement("div");
          fallback.className = "detail-cell muted";
          fallback.textContent = JSON.stringify(source);
          sourceCell.appendChild(fallback);
        }
        summary.appendChild(sourceCell);

        selectedFields.forEach(function (field) {
          const detail = document.createElement("div");
          const value = getFieldValue(source, field);
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
          { text: "score " + Number(hit.score || 0).toFixed(3), score: true },
          { text: hit.index || "" },
          { text: hit.day ? "day " + hit.day : "" },
          { text: hit.shard !== undefined && hit.shard !== null && hit.shard !== "" ? "shard " + hit.shard : "" },
          { text: hit.doc_id || "" }
        ].forEach(function (entry) {
          if (!entry.text) return;
          const badge = document.createElement("span");
          badge.className = entry.score ? "badge score" : "badge";
          badge.textContent = entry.text;
          context.appendChild(badge);
        });
        body.appendChild(context);
        body.appendChild(renderDocumentView(source));

        row.appendChild(body);
        table.appendChild(row);
      });

      panel.appendChild(table);
      return panel;
    }

    function renderSummary(data) {
      summaryEl.innerHTML = "";
      const hits = Array.isArray(data.hits) ? data.hits.length : 0;
      const indexLabel = !data.index || data.index === "_all" ? "all indexes" : data.index;
      const timeboxWindow = computeTimeboxWindow(appliedTimebox);
      setHitCount(hits + " hit" + (hits === 1 ? "" : "s"));
      pendingIndexValue = data.index || "_all";
      if (fields.index.querySelector('option[value="' + pendingIndexValue + '"]')) {
        fields.index.value = pendingIndexValue;
      }

      const pills = [
        indexLabel,
        timeboxWindow.label,
        Array.isArray(data.days) && data.days.length > 0 ? data.days.join(" -> ") : "",
        "top " + data.k
      ];
      if (data.q) pills.unshift(data.q);

      pills.forEach(function (text) {
        if (!text) return;
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
      currentOffset = data && typeof data.from === "number" ? data.from : 0;
      const hits = Array.isArray(data.hits) ? data.hits : [];
      if (hits.length === 0) {
        setHitCount("0 hits");
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
      syncSearchWindowFromTimebox();
      const params = new URLSearchParams();
      Object.keys(fields).forEach(function (key) {
        const value = key === "index" && !fields[key].value.trim()
          ? pendingIndexValue.trim()
          : fields[key].value.trim();
        if (value !== "") params.set(key, value);
      });
      appendTimeboxParams(params);
      if (currentOffset > 0) {
        params.set("from", String(currentOffset));
      }
      return params;
    }

    function applyParams(params) {
      Object.keys(fields).forEach(function (key) {
        const value = params.get(key) || (key === "k" ? DEFAULT_TOP_K : "");
        if (key === "index") {
          pendingIndexValue = value;
          return;
        }
        fields[key].value = value;
      });
      const rawFrom = params.get("from");
      const parsedFrom = rawFrom ? Number(rawFrom) : 0;
      currentOffset = Number.isFinite(parsedFrom) && parsedFrom > 0 ? Math.floor(parsedFrom) : 0;
      applyTimeboxState(deriveTimeboxFromParams(params));
    }

    function renderAvailableIndexes(indexes) {
      availableIndexes = Array.isArray(indexes) ? indexes : [];
      fields.index.innerHTML = "";

      if (availableIndexes.length === 0) {
        fields.index.disabled = true;
        fields.index.innerHTML = '<option value="">No indexes available</option>';
        if (indexCatalogEl) {
          indexCatalogEl.textContent = "";
        }
        return;
      }

      fields.index.disabled = false;
      const placeholder = document.createElement("option");
      placeholder.value = "_all";
      placeholder.textContent = "All indexes";
      fields.index.appendChild(placeholder);

      availableIndexes.forEach(function (entry) {
        const option = document.createElement("option");
        option.value = entry.name;
        option.textContent = entry.name;
        fields.index.appendChild(option);
      });

      if (indexCatalogEl) {
        indexCatalogEl.textContent = "";
      }

      const desiredIndex = pendingIndexValue || fields.index.value;

      if (desiredIndex === "" || desiredIndex === "_all") {
        fields.index.value = "_all";
      } else if (desiredIndex && availableIndexes.some(function (entry) { return entry.name === desiredIndex; })) {
        fields.index.value = desiredIndex;
      } else if (availableIndexes.length > 1) {
        fields.index.value = "_all";
      } else {
        fields.index.value = availableIndexes[0].name;
      }
      pendingIndexValue = "";
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
        if (indexCatalogEl) {
          indexCatalogEl.textContent = "";
        }
      }
    }

    async function runSearch(pushState) {
      const params = paramsFromForm();
      setStatus("Searching across assigned shards...");
      setHitCount("Searching...");
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
        setHitCount("Search failed");
        let message = error.message || "Search failed.";
        if (message.includes("routing not initialized")) {
          message += " Bootstrap an index/day first, or run the elkgo-testdata generator service.";
        }
        setStatus(message, true);
      }
    }

    form.addEventListener("submit", function (event) {
      event.preventDefault();
      currentOffset = 0;
      runSearch(true);
    });

    timeboxEls.trigger.addEventListener("click", function () {
      if (timeboxEls.popover.classList.contains("hidden")) {
        openTimeboxPopover(false);
        return;
      }
      closeTimeboxPopover();
    });

    timeboxEls.datesBtn.addEventListener("click", function () {
      if (timeboxEls.popover.classList.contains("hidden")) {
        openTimeboxPopover(true);
        return;
      }
      setTimeboxDatesVisible(!timeboxDatesVisible);
    });

    timeboxEls.amount.addEventListener("input", function () {
      draftTimebox = {
        kind: "relative",
        amount: Math.max(1, Math.floor(Number(timeboxEls.amount.value) || 1)),
        unit: normalizeTimeboxUnit(timeboxEls.unit.value)
      };
      timeboxCustomDirty = false;
      syncTimeboxControls();
    });

    timeboxEls.unit.addEventListener("change", function () {
      draftTimebox = {
        kind: "relative",
        amount: Math.max(1, Math.floor(Number(timeboxEls.amount.value) || 1)),
        unit: normalizeTimeboxUnit(timeboxEls.unit.value)
      };
      timeboxCustomDirty = false;
      syncTimeboxControls();
    });

    timeboxEls.dateFrom.addEventListener("input", function () {
      timeboxCustomDirty = true;
    });

    timeboxEls.dateTo.addEventListener("input", function () {
      timeboxCustomDirty = true;
    });

    timeboxEls.applyBtn.addEventListener("click", applyTimeboxSelection);

    timeboxEls.presets.forEach(function (button) {
      button.addEventListener("click", function () {
        draftTimebox = presetTimeboxState(button.dataset.timeboxPreset);
        timeboxCustomDirty = false;
        syncTimeboxControls();
      });
    });

    document.addEventListener("click", function (event) {
      const target = event.target;
      if (timeboxEls.popover.classList.contains("hidden")) return;
      if (timeboxEls.popover.contains(target) || timeboxEls.trigger.contains(target) || timeboxEls.datesBtn.contains(target)) return;
      closeTimeboxPopover();
    });

    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape") {
        closeTimeboxPopover();
      }
    });

    resetBtn.addEventListener("click", function () {
      Object.keys(fields).forEach(function (key) {
        fields[key].value = key === "k" ? DEFAULT_TOP_K : "";
      });
      applyTimeboxState(DEFAULT_TIMEBOX);
      closeTimeboxPopover();
      setTimeboxDatesVisible(false);
      currentOffset = 0;
      summaryEl.innerHTML = "";
      errorsEl.innerHTML = "";
      resultsEl.innerHTML = "";
      setHitCount("Discover ready");
      setStatus("");
      window.history.replaceState({}, "", "/");
    });

    const initialParams = new URLSearchParams(window.location.search);
    applyParams(initialParams);
    resultsEl.innerHTML = "";
    setHitCount("Discover ready");
    setTimeboxDatesVisible(false);
    loadAvailableIndexes();

    if (
      initialParams.get("index") &&
      (
        initialParams.get("q") ||
        (initialParams.get("day_from") && initialParams.get("day_to")) ||
        (initialParams.get("time_from") && initialParams.get("time_to"))
      )
    ) {
      runSearch(false);
    }
  </script>
</body>
</html>
`
