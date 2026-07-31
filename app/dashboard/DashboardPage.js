// Markup for GET /dashboard.
//
// Body markup extracted verbatim from renderDashboardPage() (lib/server/app.js
// lines 33026-33717). The document shell now comes from
// app/layout.jsx, the <style> block from ./dashboard.css, and the inline
// <script> from public/js/.

import { escapeHtml, normalizeText } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderDashboardPage(data) {
  const summary = data?.summary || {};
  const userTaskStats = data?.user_task_stats || [];
  const taskGroups = data?.task_groups || {};

  const summaryCards = [
    {
      label: "Open Tasks",
      value: summary.open_tasks ?? 0,
      note: "All active tasks",
      cardClass: "info",
    },
    {
      label: "Overdue",
      value: summary.overdue_tasks ?? 0,
      note: "Past deadline and still active",
      cardClass: "danger",
    },
    {
      label: "Blocked",
      value: summary.blocked_tasks ?? 0,
      note: "Tasks currently blocked",
      cardClass: "warn",
    },
    {
      label: "High Priority",
      value: summary.high_priority_tasks ?? 0,
      note: "High + urgent active tasks",
      cardClass: "warn",
    },
    {
      label: "Stale Tasks",
      value: summary.stale_tasks ?? 0,
      note: "No updates in 5+ days",
      cardClass: "danger",
    },
    {
      label: "Team Members",
      value: summary.team_members ?? 0,
      note: "People in task dashboard",
      cardClass: "success",
    },
    {
      label: "Online Now",
      value: summary.employees_online ?? 0,
      note: "Logged in / back",
      cardClass: "success",
    },
    {
      label: "On Leave",
      value: summary.employees_on_leave ?? 0,
      note: "Planned leave today",
      cardClass: "info",
    },
    {
      label: "Missing Reports",
      value: summary.missing_reports_today ?? 0,
      note: "Missing report today",
      cardClass: "danger",
    },
    {
      label: "Late Today",
      value:
        (summary.late_today_approved ?? 0) +
        (summary.late_today_unapproved ?? 0),
      note: "Approved + not approved",
      cardClass: "warn",
    },
  ];

  const summaryCardsHtml = summaryCards
    .map(
      (card) => `
        <div class="stat-card ${card.cardClass}">
          <div class="stat-label">${escapeHtml(card.label)}</div>
          <div class="stat-value">${escapeHtml(card.value)}</div>
          <div class="stat-note">${escapeHtml(card.note)}</div>
        </div>
      `,
    )
    .join("");

  const sortedUsers = [...userTaskStats].sort(
    (a, b) => (b.load_score || 0) - (a.load_score || 0),
  );

  const userRows = sortedUsers.length
    ? sortedUsers
        .map(
          (row) => `
            <tr class="health-${normalizeText(row.health)}">
              <td>
                <span class="task-link" onclick="goToTaskFilter(${Number(row.user_id)}, 'all', this)">
                  ${escapeHtml(row.name || "-")}
                </span>
              </td>
              <td>${escapeHtml(row.role || "-")}</td>
              <td>
                <span class="task-link" onclick="goToTaskFilter(${Number(row.user_id)}, 'open', this)">
                  ${escapeHtml(row.open_count ?? 0)}
                </span>
              </td>
              <td>
                <span class="task-link" onclick="goToTaskFilter(${Number(row.user_id)}, 'blocked', this)">
                  ${escapeHtml(row.blocked_count ?? 0)}
                </span>
              </td>
              <td>
                <span class="task-link" onclick="goToTaskFilter(${Number(row.user_id)}, 'not_started', this)">
                  ${escapeHtml(row.not_started_count ?? 0)}
                </span>
              </td>
              <td>
                <span class="task-link" onclick="goToTaskFilter(${Number(row.user_id)}, 'overdue', this)">
                  ${escapeHtml(row.overdue_count ?? 0)}
                </span>
              </td>
              <td>
                <span class="task-link" onclick="goToTaskFilter(${Number(row.user_id)}, 'blocked_on_them', this)">
                  ${escapeHtml(row.waiting_on_them_count ?? 0)}
                </span>
              </td>
              <td>${escapeHtml(row.high_priority_count ?? 0)}</td>
              <td>${escapeHtml(row.stale_count ?? 0)}</td>
              <td>${escapeHtml(row.load_score ?? 0)}</td>
              <td><span class="mini-badge health-pill ${normalizeText(row.health).replace(/\s+/g, "-")}">${escapeHtml(row.health || "Healthy")}</span></td>
            </tr>
          `,
        )
        .join("")
    : `
      <tr>
        <td colspan="11" class="empty-cell">No task load data found.</td>
      </tr>
    `;

  function renderMiniTaskRows(rows, typeLabel) {
    if (!rows?.length) {
      return `<tr><td colspan="5" class="empty-cell">No ${escapeHtml(typeLabel)} tasks</td></tr>`;
    }

    return rows
      .map(
        (task) => `
        <tr>
          <td>#${escapeHtml(task.task_no || task.id)}</td>
          <td>${escapeHtml(task.title || "-")}</td>
          <td>${escapeHtml(task.priority || "-")}</td>
          <td>${escapeHtml(task.status || "-")}</td>
          <td>${escapeHtml(task.deadline || "-")}</td>
        </tr>
      `,
      )
      .join("");
  }

  return `
          ${renderTopNav("dashboard")}
        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">WeSolveHR</div>
              <h1>Dashboard</h1>
              <div class="subtitle">Company-wide overview dashboard</div>
            </div>
          </div>

          <div class="stats">
            ${summaryCardsHtml}
          </div>

          <div class="tabbar">
            <button class="tab-btn active" data-tab="overview">Overview</button>
            <button class="tab-btn" data-tab="taskload">Task Load by User</button>
            <button class="tab-btn" data-tab="attention">Needs Attention</button>
            <button class="tab-btn" data-tab="taskviews">Task Views</button>
          </div>

          <div id="tab-overview" class="tab-panel active">
            <div class="panel">
              <h2 style="margin-top:0;">Leadership snapshot</h2>
              <div class="kpi-inline">
                <div class="kpi-chip">Online now: ${escapeHtml(summary.employees_online ?? 0)}</div>
                <div class="kpi-chip">On break: ${escapeHtml(summary.employees_on_break ?? 0)}</div>
                <div class="kpi-chip">No attendance yet: ${escapeHtml(summary.employees_no_attendance ?? 0)}</div>
                <div class="kpi-chip">Approved late: ${escapeHtml(summary.late_today_approved ?? 0)}</div>
                <div class="kpi-chip">Late not approved: ${escapeHtml(summary.late_today_unapproved ?? 0)}</div>
              </div>
            </div>

            <div class="panel">
              <h2 style="margin-top:0;">Task load by user</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Role</th>
                      <th>Open</th>
                      <th>Blocked</th>
                      <th>Not Started</th>
                      <th>Overdue</th>
                      <th>Blocked On Them</th>
                      <th>High Priority</th>
                      <th>Stale</th>
                      <th>Load Score</th>
                      <th>Health</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${userRows}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-taskload" class="tab-panel">
            <div class="panel">
              <h2 style="margin-top:0;">Full task load by user</h2>
              <div class="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Role</th>
                      <th>Open</th>
                      <th>Blocked</th>
                      <th>Not Started</th>
                      <th>Overdue</th>
                      <th>Blocked On Them</th>
                      <th>High Priority</th>
                      <th>Stale</th>
                      <th>Load Score</th>
                      <th>Health</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${userRows}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="tab-attention" class="tab-panel">
            <div class="grid-2">
              <div class="panel">
                <h2 style="margin-top:0;">Immediate attention</h2>
                <div class="alert-list">
                  <div class="alert-item">Overdue tasks: <strong>${escapeHtml(summary.overdue_tasks ?? 0)}</strong></div>
                  <div class="alert-item">Blocked tasks: <strong>${escapeHtml(summary.blocked_tasks ?? 0)}</strong></div>
                  <div class="alert-item">Missing reports today: <strong>${escapeHtml(summary.missing_reports_today ?? 0)}</strong></div>
                  <div class="alert-item">No attendance today: <strong>${escapeHtml(summary.employees_no_attendance ?? 0)}</strong></div>
                  <div class="alert-item">Stale tasks (5+ days no update): <strong>${escapeHtml(summary.stale_tasks ?? 0)}</strong></div>
                </div>
              </div>

              <div class="panel">
                <h2 style="margin-top:0;">Most overloaded people</h2>
                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Name</th>
                        <th>Open</th>
                        <th>Overdue</th>
                        <th>Blocked</th>
                        <th>Score</th>
                        <th>Health</th>
                      </tr>
                    </thead>
                    <tbody>
                      ${sortedUsers
                        .slice(0, 8)
                        .map(
                          (row) => `
                          <tr>
                            <td>${escapeHtml(row.name)}</td>
                            <td>${escapeHtml(row.open_count ?? 0)}</td>
                            <td>${escapeHtml(row.overdue_count ?? 0)}</td>
                            <td>${escapeHtml(row.blocked_count ?? 0)}</td>
                            <td>${escapeHtml(row.load_score ?? 0)}</td>
                            <td><span class="mini-badge health-pill ${normalizeText(row.health).replace(/\s+/g, "-")}">${escapeHtml(row.health)}</span></td>
                          </tr>
                        `,
                        )
                        .join("")}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>

          <div id="tab-taskviews" class="tab-panel">
            <div class="grid-2">
              <div class="panel">
                <h2 style="margin-top:0;">Top overdue tasks</h2>
                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Title</th>
                        <th>Priority</th>
                        <th>Status</th>
                        <th>Deadline</th>
                      </tr>
                    </thead>
                    <tbody>${renderMiniTaskRows(taskGroups.overdue, "overdue")}</tbody>
                  </table>
                </div>
              </div>

              <div class="panel">
                <h2 style="margin-top:0;">Top blocked tasks</h2>
                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Title</th>
                        <th>Priority</th>
                        <th>Status</th>
                        <th>Deadline</th>
                      </tr>
                    </thead>
                    <tbody>${renderMiniTaskRows(taskGroups.blocked, "blocked")}</tbody>
                  </table>
                </div>
              </div>

              <div class="panel">
                <h2 style="margin-top:0;">High priority tasks</h2>
                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Title</th>
                        <th>Priority</th>
                        <th>Status</th>
                        <th>Deadline</th>
                      </tr>
                    </thead>
                    <tbody>${renderMiniTaskRows(taskGroups.high_priority, "high priority")}</tbody>
                  </table>
                </div>
              </div>

              <div class="panel">
                <h2 style="margin-top:0;">Stale tasks</h2>
                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Title</th>
                        <th>Priority</th>
                        <th>Status</th>
                        <th>Deadline</th>
                      </tr>
                    </thead>
                    <tbody>${renderMiniTaskRows(taskGroups.stale, "stale")}</tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div id="pageLoadingOverlay" class="loading-overlay">
          <div class="loading-card">
            <div class="loading-spinner"></div>
            <div id="pageLoadingTitle">Opening task list...</div>
          </div>
        </div>

        <script src="/js/dashboard.js"></script>
      
  `;
}

export {
  renderDashboardPage,
};
