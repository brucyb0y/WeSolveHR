// Markup for GET /attendance.
//
// Body markup extracted verbatim from the Express handler (lib/server/app.js
// lines 45224-46101), which built the whole document inline.
// The document shell now comes from app/layout.jsx, the <style> block from
// ./attendance.css, and any static <script> from public/js/.

import { renderTopNav } from "@/lib/ui/nav.js";

function renderAttendancePage() {
  return `      ${renderTopNav("attendance")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">WeSolveHR</div>
              <h1>Attendance</h1>
              <div class="subtitle">Team attendance overview and exceptions</div>
            </div>
          </div>

          <div id="statsGrid" class="stats">
            <div class="stat-card"><div class="stat-label">Loading</div><div class="stat-value">...</div><div class="stat-note">Fetching attendance</div></div>
          </div>

          <div class="tabbar">
            <button class="tab-btn active" data-tab="overview">Live Overview</button>
            <button class="tab-btn" data-tab="exceptions">Late & Exceptions</button>
            <button class="tab-btn" data-tab="leave">Leave & No Update</button>
            <button class="tab-btn" data-tab="summary">Team Summary</button>
          </div>

<div class="grid-3">
  <div class="panel">
    <h2 style="margin-top:0;">Needs attention now</h2>
    <div id="attentionNow" class="alert-list">
      <div class="loading-state">Loading...</div>
    </div>
  </div>

  <div class="panel">
    <h2 style="margin-top:0;">Careless login</h2>
    <div id="carelessLoginList" class="alert-list">
      <div class="loading-state">Loading...</div>
    </div>
  </div>

  <div class="panel">
    <h2 style="margin-top:0;">Live grouped view</h2>
    <div id="liveGroups" class="alert-list">
      <div class="loading-state">Loading...</div>
    </div>
  </div>
</div>
            
            <div class="panel">
  <h2 style="margin-top:0;">Weekly & Monthly Insights</h2>

  <div class="insight-section">
    <div class="insight-section-title">This week</div>
    <div id="weeklyInsightsGrid" class="insight-grid">
      <div class="loading-state">Loading weekly insights...</div>
    </div>
  </div>

  <div class="insight-section" style="margin-top:18px;">
    <div class="insight-section-title">This month</div>
    <div id="monthlyInsightsGrid" class="insight-grid">
      <div class="loading-state">Loading monthly insights...</div>
    </div>
  </div>
</div>

            <div class="panel">
              <h2 style="margin-top:0;">Live employee table</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
<th>Name</th>
<th>Role</th>
<th>Expected Login</th>
<th>Current Status</th>
<th>Since</th>
<th>Worked Today</th>
<th>Break Today</th>
<th>First Login</th>
<th>Late</th>
<th>Leave</th>
<th>Flags</th>
                    </tr>
                  </thead>
                  <tbody id="attendanceTableBody">
<tr><td colspan="11" class="loading-state">Loading attendance...</td></tr>
</tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-exceptions" class="tab-panel">
            <div class="panel">
              <h2 style="margin-top:0;">Late & exception cases</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
<th>Name</th>
<th>Role</th>
<th>Expected Login</th>
<th>Status</th>
<th>Worked</th>
<th>Break</th>
<th>First Login</th>
<th>Late</th>
<th>Flags</th>
                    </tr>
                  </thead>
                  <tbody id="exceptionsTableBody">
                    <tr><td colspan="9" class="loading-state">Loading exceptions...</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-leave" class="tab-panel">
            <div class="grid-2">
              <div class="panel">
                <h2 style="margin-top:0;">On leave today</h2>
                <div id="leaveList" class="alert-list">
                  <div class="loading-state">Loading...</div>
                </div>
              </div>

              <div class="panel">
                <h2 style="margin-top:0;">No update yet</h2>
                <div id="noUpdateList" class="alert-list">
                  <div class="loading-state">Loading...</div>
                </div>
              </div>
            </div>
          </div>

          <div id="tab-summary" class="tab-panel">
            <div class="panel">
              <h2 style="margin-top:0;">Team summary</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Role</th>
                      <th>Status</th>
                      <th>Worked</th>
                      <th>Break</th>
                      <th>First Login</th>
                      <th>Late</th>
                      <th>Flags</th>
                    </tr>
                  </thead>
                  <tbody id="summaryTableBody">
                    <tr><td colspan="8" class="loading-state">Loading summary...</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        

        <div id="pageLoadingOverlay" class="loading-overlay">
          <div class="loading-card">
            <div class="loading-spinner"></div>
            <div style="font-weight:700;">Opening attendance details...</div>
          </div>
        </div>

        <script src="/js/attendance.js"></script>
      `;
}

export { renderAttendancePage };
