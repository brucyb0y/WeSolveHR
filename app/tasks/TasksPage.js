// Markup for GET /tasks.
//
// Body markup extracted verbatim from the Express handler (lib/server/app.js
// lines 44191-45188), which built the whole document inline.
// The document shell now comes from app/layout.jsx, the <style> block from
// ./tasks.css, and any static <script> from public/js/.

import { renderTopNav } from "@/lib/ui/nav.js";

function renderTasksPage() {
  return `      ${renderTopNav("tasks")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Task Operations</div>
              <h1>WeSolveHR // Tasks Console</h1>
              <div class="subtitle">Filter and inspect work across the team without changing backend behavior</div>
            </div>
          </div>

<div class="panel task-table-panel">
  <div class="controls">
  <input id="search" placeholder="Search task title or ID" />
  <select id="assignee"><option value="">All assignees</option></select>

  <select id="business">
    <option value="">All clients</option>
    <option value="joolian">Joolian</option>
    <option value="wesolve">WeSolve</option>
    <option value="rasset">Rasset</option>
    <option value="navii">Navii</option>
    <option value="general">General</option>
  </select>

<select id="area">
  <option value="">All areas</option>
  <option value="pricing">Pricing</option>
  <option value="marketing">Marketing</option>
  <option value="prospect fu">Prospect FU</option>
  <option value="pm">PM</option>
  <option value="escalation">Escalation</option>
  <option value="contractors hiring">Contractors Hiring</option>
  <option value="product dev">Product Dev</option>
  <option value="pitch practice">Pitch Practice</option>
  <option value="b2c leads gen">B2C Leads Gen</option>
  <option value="b2b leads gen">B2B Leads Gen</option>
  <option value="website dev">Website Dev</option>
  <option value="competitors calling">Competitors Calling</option>
  <option value="prospects calling">Prospects Calling</option>
  <option value="research">Research</option>
  <option value="strategy">Strategy</option>
</select>

  <select id="status">
    <option value="">All active status</option>
    <option value="open">Open</option>
    <option value="in_progress">In progress</option>
    <option value="blocked">Blocked</option>
  </select>

<select id="priority">
  <option value="">All priority</option>
  <option value="low">Low</option>
  <option value="medium">Medium</option>
  <option value="high">High</option>
  <option value="urgent">Urgent</option>
</select>

<select id="progressBucket" multiple size="1">
<option value="not_begun" selected>Not begun</option>
  <option value="zero_to_fifty" selected>0–50% complete</option>
  <option value="fifty_to_hundred" selected>50–100% complete</option>
  <option value="complete">100% complete</option>
    <option value="hide_cancelled" selected>Hide Cancelled</option>
<option value="only_cancelled">Cancelled only</option>
</select>

<label><input type="checkbox" id="blocked" /> Blocked only</label>
<label><input type="checkbox" id="overdue" /> Overdue only</label>
<button onclick="loadTasks()">Apply</button>
</div>
</div>
<div id="activeSpecialFilters" class="muted" style="margin: 8px 0 12px;"></div>
<div class="panel">
  <div id="statusText">Loading tasks...</div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
    <th>ID</th>
    <th>Title</th>
    <th>Business</th>
    <th>Assignee</th>
    <th>Status</th>
    <th>Priority</th>
    <th>Deadline</th>
    <th>Blocker</th>
        </tr>
      </thead>
      <tbody id="taskRows"></tbody>
    </table>
  </div>
</div>

<div id="taskModal" class="modal">
  <div class="modal-backdrop" onclick="closeTaskModal()"></div>
  <div class="modal-card">
    <div class="modal-header">
      <div id="modalTitle" class="modal-title">Task Detail</div>
      <button class="modal-close" onclick="closeTaskModal()">✕</button>
    </div>
    <div id="modalBody" class="modal-body">
      <div class="muted">Loading task details...</div>
    </div>
  </div>
</div>
</div><!-- /.wrap: the original markup never closed this -->
<script src="/js/tasks.js"></script>
      `;
}

export { renderTasksPage };
