// Markup for GET /bugs.
//
// Body markup extracted verbatim from renderStage0BugBoardPage() (lib/server/app.js
// lines 1535-1841). The document shell now comes from
// app/layout.jsx, the <style> block from ./bugs.css, and the inline
// <script> from public/js/.

import { STAGE0_BUG_COLUMNS, bugSeverityBadgeClass, bugStatusBadgeClass } from "@/lib/data/bugs.js";
import { escapeHtml } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderStage0BugBoardPage(data) {
  const summary = data?.summary || {};
  const columns = data?.columns || [];
  const users = data?.users || [];

  const columnHtml = columns
    .map((column) => {
      const cardsHtml = (column.items || []).length
        ? column.items
            .map((bug) => {
              return `
                <div class="bug-card" data-id="${escapeHtml(bug.id)}">
                  <div class="bug-top">
                    <div class="bug-id">#${escapeHtml(bug.id)}</div>
                    <div class="bug-badges">
                      <span class="${bugSeverityBadgeClass(bug.severity)}">${escapeHtml(bug.severity)}</span>
                      <span class="${bugStatusBadgeClass(bug.status)}">${escapeHtml(bug.status)}</span>
                    </div>
                  </div>

                  <div class="bug-title">${escapeHtml(bug.title)}</div>

                  ${
                    bug.description
                      ? `<div class="bug-desc">${escapeHtml(bug.description)}</div>`
                      : ""
                  }

                  <div class="bug-meta">
                    <div><strong>Assignee:</strong> ${escapeHtml(bug.assigned_to_name || "-")}</div>
                    <div><strong>Created by:</strong> ${escapeHtml(bug.created_by_name || "-")}</div>
                    <div><strong>Created:</strong> ${escapeHtml(bug.created_at_text || "-")}</div>
                  </div>

                  ${
                    bug.source_message_sid ||
                    bug.source_phone_number ||
                    bug.source_message_text
                      ? `
                        <div class="bug-source">
                          ${bug.source_message_sid ? `<div><strong>SID:</strong> ${escapeHtml(bug.source_message_sid)}</div>` : ""}
                          ${bug.source_phone_number ? `<div><strong>Phone:</strong> ${escapeHtml(bug.source_phone_number)}</div>` : ""}
                          ${bug.source_message_text ? `<div><strong>Message:</strong> ${escapeHtml(bug.source_message_text)}</div>` : ""}
                        </div>
                      `
                      : ""
                  }

                  <div class="bug-actions" style="margin-top:10px; display:flex; gap:8px; flex-wrap:wrap;">
                    <select onchange="updateBug(${bug.id}, { board_column: this.value })"
                      style="padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);">
                      ${STAGE0_BUG_COLUMNS.map(
                        (col) => `
                        <option value="${escapeHtml(col)}" ${bug.board_column === col ? "selected" : ""}>${escapeHtml(col)}</option>
                      `,
                      ).join("")}
                    </select>

                    <select onchange="updateBug(${bug.id}, { severity: this.value })"
                      style="padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);">
                      ${["P0", "P1", "P2"]
                        .map(
                          (sev) => `
                        <option value="${sev}" ${bug.severity === sev ? "selected" : ""}>${sev}</option>
                      `,
                        )
                        .join("")}
                    </select>

                    <select onchange="updateBug(${bug.id}, { status: this.value })"
                      style="padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);">
                      ${["open", "in_progress", "blocked", "done"]
                        .map(
                          (st) => `
                        <option value="${st}" ${bug.status === st ? "selected" : ""}>${st}</option>
                      `,
                        )
                        .join("")}
                    </select>

                    <select onchange="updateBug(${bug.id}, { assigned_to_user_id: this.value || null })"
                      style="padding:8px; border-radius:8px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);">
                      <option value="">Unassigned</option>
                      ${users
                        .map(
                          (u) => `
                        <option value="${u.id}" ${String(bug.assigned_to_user_id || "") === String(u.id) ? "selected" : ""}>${escapeHtml(u.name)}</option>
                      `,
                        )
                        .join("")}
                    </select>
                  </div>
                </div>
              `;
            })
            .join("")
        : `<div class="empty-col">No bugs here</div>`;

      return `
        <div class="board-col">
          <div class="board-col-head">
            <div class="board-col-title">${escapeHtml(column.name)}</div>
            <div class="board-col-count">${escapeHtml(column.count)}</div>
          </div>
          <div class="board-col-body">
            ${cardsHtml}
          </div>
        </div>
      `;
    })
    .join("");

  return `
            ${renderTopNav("bugs")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Stage 0 Stability</div>
              <h1>Bug Board</h1>
              <div class="subtitle">Parsing, idempotency, Twilio, DB failures, dashboard/logs, infra, unknown issues.</div>
            </div>
          </div>

          <div class="stats">
            <div class="stat-card"><div class="stat-label">Total</div><div class="stat-value">${escapeHtml(summary.total ?? 0)}</div></div>
            <div class="stat-card"><div class="stat-label">P0</div><div class="stat-value">${escapeHtml(summary.p0 ?? 0)}</div></div>
            <div class="stat-card"><div class="stat-label">P1</div><div class="stat-value">${escapeHtml(summary.p1 ?? 0)}</div></div>
            <div class="stat-card"><div class="stat-label">P2</div><div class="stat-value">${escapeHtml(summary.p2 ?? 0)}</div></div>
            <div class="stat-card"><div class="stat-label">Open</div><div class="stat-value">${escapeHtml(summary.open ?? 0)}</div></div>
            <div class="stat-card"><div class="stat-label">In Progress</div><div class="stat-value">${escapeHtml(summary.in_progress ?? 0)}</div></div>
            <div class="stat-card"><div class="stat-label">Blocked</div><div class="stat-value">${escapeHtml(summary.blocked ?? 0)}</div></div>
          </div>

          <div class="panel" style="margin-bottom: 18px; padding: 16px;">
            <h2 style="margin-top:0;">Create bug</h2>
            <div style="display:grid; grid-template-columns: 2fr 1.2fr 1fr 1fr; gap:10px; margin-bottom:10px;">
              <input id="bugTitle" placeholder="Bug title" style="padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);" />
              <select id="bugColumn" style="padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);">
                ${STAGE0_BUG_COLUMNS.map((x) => `<option value="${escapeHtml(x)}">${escapeHtml(x)}</option>`).join("")}
              </select>
              <select id="bugSeverity" style="padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);">
                <option value="P0">P0</option>
                <option value="P1">P1</option>
                <option value="P2">P2</option>
              </select>
              <button onclick="createBug()" style="padding:10px 14px; border-radius:10px; border:1px solid var(--line); background:var(--primary-soft); color:var(--text); font-weight:700;">Create</button>
            </div>

            <textarea id="bugDescription" placeholder="Description" style="width:100%; min-height:90px; padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);"></textarea>

            <div style="display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-top:10px;">
              <input id="bugSourceSid" placeholder="Source Message SID (optional)" style="padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);" />
              <input id="bugSourcePhone" placeholder="Source Phone (optional)" style="padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);" />
            </div>
            <textarea id="bugSourceText" placeholder="Source message text (optional)" style="width:100%; min-height:70px; margin-top:10px; padding:10px; border-radius:10px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:var(--text);"></textarea>
          </div>

          <div class="board">
            ${columnHtml}
          </div>
        </div>

        <script src="/js/bugs.js"></script>
      
  `;
}

export {
  renderStage0BugBoardPage,
};
