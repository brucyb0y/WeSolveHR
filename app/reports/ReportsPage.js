// Markup for GET /reports (org view).
//
// Body markup extracted verbatim from renderReportsPage() (lib/server/app.js
// lines 29958-30602). The document shell now comes from
// app/layout.jsx, the <style> block from ./reports.css, and the inline
// <script> from public/js/.

import { getReportDateString } from "@/lib/data/reports.js";
import { escapeHtml, formatDateOnly } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderReportsPage(data) {
  const reportDate = data?.reportDate || getReportDateString();
  const users = data?.users || [];
  const compliance = data?.compliance || {
    full: [],
    partial: [],
    missing: [],
    onLeave: [],
    off: [],
  };
  return `
          ${renderTopNav("reports")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Daily Reporting</div>
              <h1>WeSolveHR // Reports</h1>
              <div class="subtitle">Attendance-day so far. Task narratives + extra work + open/blocked snapshot.</div>
            </div>
          </div>

          <div class="panel" style="padding:14px 16px; margin-bottom:16px;">
<strong>Date:</strong> ${escapeHtml(formatDateOnly(reportDate))} 
<span class="muted">(6:00 AM → next day 6:00 AM IST)</span>
</div>

<div id="reportsSummary">
  <div class="panel" style="padding:18px; margin-bottom:16px;">
    <div class="muted">Loading summary...</div>
  </div>
</div>

          <div class="panel" style="padding:14px 16px; margin-bottom:16px;">
            <input
              id="reportSearch"
              type="text"
              placeholder="Search user name"
              oninput="filterReports()"
              style="width:100%; padding:12px 14px; border-radius:12px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);"
            />
          </div>

          <div id="reportsGrid" class="reports-grid">
            <div class="panel" style="padding:18px;">
              <div class="muted">Loading reports...</div>
            </div>
          </div>
        </div>

        <div id="taskModal" class="modal-backdrop" onclick="closeTaskModal(event)">
          <div class="modal-card" onclick="event.stopPropagation()">
            <div class="modal-head">
              <div>
                <div class="eyebrow">Task detail</div>
                <h2 id="modalTitle" class="modal-title">Loading...</h2>
              </div>
              <button class="modal-close" onclick="closeTaskModal()">Close</button>
            </div>

            <div id="modalBody">
              <div class="muted">Loading task details...</div>
            </div>
          </div>
        </div>

        <script src="/js/reports.js"></script>
      
  `;
}

export {
  renderReportsPage,
};
