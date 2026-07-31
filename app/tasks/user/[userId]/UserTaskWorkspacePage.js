// Markup for GET /tasks/user/:userId.
//
// Body markup extracted verbatim from renderUserTaskWorkspacePage() (lib/server/app.js
// lines 1843-2509). The document shell now comes from
// app/layout.jsx, the <style> block from ./user-task-workspace.css, and the inline
// <script> from public/js/.

import { renderUserWorkspaceHistoryLine } from "@/lib/data/tasks.js";
import { badgeClass, escapeHtml, formatDateTime } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderUserTaskWorkspacePage(data) {
  const user = data?.user;
  const counts = data?.counts || {};
  const selectedTab = data?.selectedTab || "pending";
  const tabs = data?.tabs || {};

  const selectedItems = tabs[selectedTab] || [];

  const chip = (key, label, count) => `
    <a
      href="/tasks/user/${user.id}?tab=${key}"
      class="workspace-chip ${selectedTab === key ? "active" : ""}"
    >
      ${label} (${count || 0})
    </a>
  `;

  const taskCardsHtml = selectedItems.length
    ? selectedItems
        .map(
          (task) => `
        <div class="workspace-task-card">
          <div class="workspace-task-top">
            <div>
<a
  href="#"
  class="workspace-task-id-link"
  onclick="event.preventDefault(); event.stopPropagation(); openUserWorkspaceTaskDetail(${Number(task.task_no || task.id)})"
>
                #${escapeHtml(task.task_no || task.id)}
              </a>
            </div>
            <div class="${badgeClass(task.status)}">${escapeHtml(task.status || "")}</div>
          </div>

          <div class="workspace-task-title">${escapeHtml(task.title || "")}</div>

          <div class="workspace-task-meta">
            <div><strong>Business:</strong> ${escapeHtml(task.business || "-")}</div>
            <div><strong>Area:</strong> ${escapeHtml(task.area || "-")}</div>
            <div><strong>Owners:</strong> ${escapeHtml((task.owner_names || []).join(", ") || "-")}</div>
            <div><strong>Priority:</strong> ${escapeHtml(task.priority || "-")}</div>
            <div><strong>Progress:</strong> ${escapeHtml(task.progress ?? 0)}%</div>
            <div><strong>Deadline:</strong> ${escapeHtml(task.deadline || "-")}</div>
            <div><strong>Blocker:</strong> ${escapeHtml(task.blocker_note || "-")}</div>
            <div><strong>Latest update:</strong> ${escapeHtml(task.latest_update_text || "No updates yet")}</div>
            <div><strong>Updated by:</strong> ${escapeHtml(task.latest_updated_by || "-")}</div>
            <div><strong>Updated at:</strong> ${escapeHtml(task.latest_update_at ? formatDateTime(task.latest_update_at) : "-")}</div>
          </div>

          ${
            Array.isArray(task.mini_history) && task.mini_history.length
              ? `
                <div class="workspace-mini-timeline">
                  <div class="workspace-mini-timeline-title">Recent flow</div>
                  ${task.mini_history
                    .map(
                      (item) => `
                    <div class="workspace-mini-timeline-item">
                      <div class="workspace-mini-timeline-time">
                        ${escapeHtml(formatDateTime(item.created_at))}
                      </div>
                      <div class="workspace-mini-timeline-text">
                        ${escapeHtml(renderUserWorkspaceHistoryLine(item))}
                        ${item.changed_by_name ? `<span class="workspace-mini-timeline-by">by ${escapeHtml(item.changed_by_name)}</span>` : ""}
                      </div>
                    </div>
                  `,
                    )
                    .join("")}
                </div>
              `
              : ""
          }
        </div>
      `,
        )
        .join("")
    : `<div class="panel" style="padding:16px;">No items found in this tab.</div>`;

  const historyCardsHtml = selectedItems.length
    ? selectedItems
        .map(
          (item) => `
        <div class="workspace-task-card">
          <div class="workspace-task-top">
            <div>
              <a
                href="#"
                class="workspace-task-id-link"
                onclick="event.preventDefault(); event.stopPropagation(); openUserWorkspaceTaskDetail(${Number(item.task_no || item.task_id)})"
              >
                Task #${escapeHtml(item.task_no || item.task_id)}
              </a>
            </div>
            <div class="muted">${escapeHtml(formatDateTime(item.created_at))}</div>
          </div>

          <div class="workspace-task-title">${escapeHtml(renderUserWorkspaceHistoryLine(item))}</div>

<div class="workspace-task-meta">
  <div><strong>Updated by:</strong> ${escapeHtml(item.changed_by_name || "-")}</div>
  <div><strong>Type:</strong> ${escapeHtml(item.change_type || "-")}</div>
  <div><strong>Field:</strong> ${escapeHtml(item.field_name || "-")}</div>
</div>
        </div>
      `,
        )
        .join("")
    : `<div class="panel" style="padding:16px;">No progress updates found.</div>`;

  return `
            ${renderTopNav("tasks")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">User Task Workspace</div>
              <h1>${escapeHtml(user?.name || "Unknown user")}</h1>
              <div class="subtitle">Focused task workspace for one user</div>
            </div>
            <a class="back-link" href="/tasks">← Back to Tasks</a>
          </div>

          <div class="stats">
            <div class="stat-card">
              <div class="stat-label">Pending</div>
              <div class="stat-value">${counts.pending || 0}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Blocked</div>
              <div class="stat-value">${counts.blocked || 0}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Done today</div>
              <div class="stat-value">${counts.done_today || 0}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Deleted</div>
              <div class="stat-value">${counts.deleted || 0}</div>
            </div>
          </div>

          <div class="workspace-chip-row">
            ${chip("pending", "Pending", counts.pending)}
            ${chip("blocked", "Blocked", counts.blocked)}
            ${chip("blocked_on_me", "Blocked on me", counts.blocked_on_me)}
            ${chip("done_today", "Done today", counts.done_today)}
            ${chip("deleted", "Deleted", counts.deleted)}
            ${chip("progress_updates", "Progress updates", counts.progress_updates)}
          </div>

          <div class="workspace-list">
            ${selectedTab === "progress_updates" ? historyCardsHtml : taskCardsHtml}
          </div>

                    <div id="taskModal" class="task-modal" onclick="closeUserWorkspaceTaskDetail(event)">
            <div class="task-modal-card" onclick="event.stopPropagation()">
              <div class="task-modal-head">
                <div id="taskModalTitle" style="font-size:22px; font-weight:800;">Task detail</div>
                <button type="button" class="task-modal-close" onclick="closeUserWorkspaceTaskDetail()">Close</button>
              </div>
              <div id="taskModalBody" class="muted">Loading...</div>
            </div>
          </div>
        </div>
        <script src="/js/user-task-workspace.js"></script>
      
  `;
}

export {
  renderUserTaskWorkspacePage,
};
