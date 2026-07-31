// Markup for GET /my-dashboard.
//
// Body markup extracted verbatim from renderMyDashboardPage() (lib/server/app.js
// lines 38588-38827). The document shell now comes from
// app/layout.jsx, the <style> block from ./my-dashboard.css, and the inline
// <script> from public/js/.

import { escapeHtml } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderMyDashboardPage(data) {
  const user = data?.user || {};
  const taskData = data?.taskData || {};
  const myAttendance = data?.myAttendance || null;
  const reportData = data?.reportData || {};

  const counts = taskData?.counts || {};
  const pendingTasks = taskData?.tabs?.pending || [];
  const blockedTasks = taskData?.tabs?.blocked || [];
  const doneTodayTasks = taskData?.tabs?.done_today || [];

  const reportSummary =
    reportData?.summary_text ||
    reportData?.narrative ||
    "No report summary available for today.";

  return `
            ${renderTopNav("dashboard")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Personal Workspace</div>
              <h1>Welcome, ${escapeHtml(user.name || "User")}</h1>
              <div class="subtitle">
                Your tasks, attendance, and report summary in one place
              </div>
            </div>
          </div>

          <div class="stats">
            <div class="stat-card">
              <div class="stat-label">Pending Tasks</div>
              <div class="stat-value">${escapeHtml(counts.pending || 0)}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Blocked Tasks</div>
              <div class="stat-value">${escapeHtml(counts.blocked || 0)}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Done Today</div>
              <div class="stat-value">${escapeHtml(counts.done_today || 0)}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Attendance Status</div>
              <div class="stat-value">${escapeHtml(myAttendance?.status || "-")}</div>
            </div>
          </div>

          <div class="grid">
            <div class="panel">
              <h2>My Tasks</h2>
              <div class="task-list">
                ${
                  pendingTasks.length
                    ? pendingTasks
                        .slice(0, 8)
                        .map(
                          (task) => `
                      <div class="task-card">
                        <div class="task-title">#${escapeHtml(task.task_no || task.id)} — ${escapeHtml(task.title || "")}</div>
                        <div class="task-meta">
                          Status: ${escapeHtml(task.status || "-")}<br />
                          Priority: ${escapeHtml(task.priority || "-")}<br />
                          Progress: ${escapeHtml(task.progress ?? 0)}%<br />
                          Deadline: ${escapeHtml(task.deadline || "-")}
                        </div>
                      </div>
                    `,
                        )
                        .join("")
                    : `<div class="muted">No pending tasks.</div>`
                }
              </div>
            </div>

            <div style="display:grid; gap:16px;">
              <div class="panel">
                <h2>My Attendance</h2>
                <div class="attendance-line">
Status: ${escapeHtml(myAttendance?.status || "-")}
Login: ${escapeHtml(myAttendance?.login_time || "-")}
Break: ${escapeHtml(myAttendance?.break_time || "-")}
Logout: ${escapeHtml(myAttendance?.logout_time || "-")}
Worked: ${escapeHtml(myAttendance?.worked_duration || "-")}
                </div>
              </div>

              <div class="panel">
                <h2>Today’s Report</h2>
                <div class="report-box">${escapeHtml(reportSummary)}</div>
              </div>

              <div class="panel">
                <h2>Quick Links</h2>
                <div class="attendance-line">
<a href="/tasks/user/${escapeHtml(user.id)}" style="color: var(--primary);">Open my full task workspace</a><br />
<a href="/attendance" style="color: var(--primary);">Open attendance page</a><br />
<a href="/reports?userId=${escapeHtml(user.id)}" style="color: var(--primary);">Open my reports</a>
                </div>
              </div>
            </div>
          </div>
        </div>
      
  `;
}

export {
  renderMyDashboardPage,
};
