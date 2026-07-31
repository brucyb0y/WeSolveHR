// Markup for GET /clients.
//
// Body markup extracted verbatim from renderClientsListPage() (lib/server/app.js
// lines 3397-3734). The document shell now comes from
// app/layout.jsx, the <style> block from ./clients.css, and the inline
// <script> from public/js/.

import { escapeHtml } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderClientsListPage({ clients = [], summary = {} } = {}) {
  const rowsHtml = clients.length
    ? clients
        .map((client) => {
          const serviceNames = (client.service_names || []).join(", ") || "-";

          return `
            <tr>
              <td>
                <div style="font-weight:800;">
                  <a href="/clients/${client.id}" style="color:var(--text-strong); text-decoration:none;">
                    ${escapeHtml(client.name)}
                  </a>
                </div>
                <div class="muted">${escapeHtml(client.company_name || "-")}</div>
              </td>
              <td>${escapeHtml(serviceNames)}</td>
              <td>${escapeHtml(client.project_manager_name || "-")}</td>
              <td><span class="badge badge-info">${escapeHtml(client.status || "-")}</span></td>
              <td><span class="${client.health_status === "at_risk" ? "badge badge-danger" : client.health_status === "watch" ? "badge badge-warn" : "badge badge-ok"}">${escapeHtml(client.health_status || "-")}</span></td>
              <td>${escapeHtml(client.open_work_count || 0)}</td>
              <td>${escapeHtml(client.waiting_count || 0)}</td>
              <td>${escapeHtml(client.last_update_text || "-")}</td>
<td>
  <button class="action-kebab" type="button" onclick="toggleClientActionsMenu(event, ${Number(client.id)})">⋯</button>

  <div id="clientActionsMenu-${Number(client.id)}" class="floating-actions-menu">
    <a href="/clients/${client.id}/edit">Edit Client</a>
    <a href="/clients/${client.id}/reset">Reset Workspace</a>
    <a href="/clients/${client.id}">Open Workspace</a>
  </div>
</td>
            </tr>
          `;
        })
        .join("")
    : `
      <tr>
        <td colspan="9" class="empty">
          No clients yet. Click “New Client” to start.
        </td>
      </tr>
    `;

  return `
            ${renderTopNav("clients")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Client Workspace</div>
              <h1>Clients</h1>
              <div class="subtitle">Internal consulting CRM layer for client work, updates, actions, documents, and progress.</div>
            </div>

            <a class="action-btn" href="/clients/new">+ New Client</a>
          </div>

          <div class="stats">
            <div class="stat-card">
              <div class="stat-label">Total Clients</div>
              <div class="stat-value">${summary.total || 0}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Active</div>
              <div class="stat-value">${summary.active || 0}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Waiting on Client</div>
              <div class="stat-value">${summary.waiting || 0}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">At Risk</div>
              <div class="stat-value">${summary.atRisk || 0}</div>
            </div>
          </div>

          <div class="panel">
            <table>
              <thead>
                <tr>
                  <th>Client</th>
                  <th>Services</th>
                  <th>Project Manager</th>
                  <th>Status</th>
                  <th>Health</th>
                  <th>Open Work</th>
                  <th>Waiting</th>
                  <th>Last Update</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                ${rowsHtml}
              </tbody>
            </table>
          </div>
        </div>
        <script src="/js/clients.js"></script>
      
  `;
}

export {
  renderClientsListPage,
};
