// Markup for GET /logs.
//
// Body markup extracted verbatim from the Express handler (lib/server/app.js
// lines 47405-47957), which built the whole document inline.
// The document shell now comes from app/layout.jsx, the <style> block from
// ./logs.css, and any static <script> from public/js/.

import { renderTopNav } from "@/lib/ui/nav.js";

function renderLogsPage() {
  return `      ${renderTopNav("logs")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Message Logging</div>
              <h1>WeSolveHR // Logs Console</h1>
              <div class="subtitle">Inbound command visibility for tracing, debugging, and audit review</div>
            </div>
          </div>

<div class="stats-grid" id="logsStats"></div>

<div class="panel filters-panel">
  <div class="filters-grid">
    <input id="filterSearch" placeholder="Search message or SID" />
    <input id="filterUser" placeholder="Filter by username / phone" />
    <select id="filterOutcome">
      <option value="">All outcomes</option>
      <option value="completed">Completed</option>
      <option value="failed">Failed</option>
      <option value="processing">Processing</option>
      <option value="unknown">Unknown</option>
    </select>
    <input id="filterDay" type="date" />
    <input id="filterMonth" type="month" />
    <button onclick="loadLogs()">Apply</button>
    <button onclick="clearLogFilters()">Clear</button>
  </div>
</div>

<div class="panel">
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Time</th>
          <th>Sender</th>
          <th>Command</th>
          <th>Outcome</th>
          <th>Type</th>
          <th>Exception</th>
          <th>SID</th>
        </tr>
      </thead>
      <tbody id="logRows"></tbody>
    </table>
  </div>
</div>
        </div>

        <script src="/js/logs.js"></script>
      `;
}

export { renderLogsPage };
