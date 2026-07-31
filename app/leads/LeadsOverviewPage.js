// Markup for GET /leads.
//
// Body markup extracted verbatim from renderLeadsOverviewPage() (lib/server/app.js
// lines 24472-24821). The document shell now comes from
// app/layout.jsx, the <style> block from ./leads.css, and the inline
// <script> from public/js/.

import { badgeClass, escapeHtml, formatDateTime } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderLeadsOverviewPage(data) {
  const summary = data?.summary || {};
  const businesses = data?.businesses || [];
  const recent = data?.recent || [];

  const businessRowsHtml = businesses.length
    ? businesses
        .map((b) => {
          const attention =
            Number(b.in_progress || 0) > 0 || Number(b.leads || 0) > 0;
          return `
            <tr class="business-row" onclick="window.location.href='/leads/${encodeURIComponent(b.business)}'">
              <td>
                <div class="business-name">${escapeHtml(b.label || b.business)}</div>
                <div class="business-subtitle">${escapeHtml(b.business)}</div>
              </td>
              <td><strong>${escapeHtml(b.total || 0)}</strong></td>
              <td>${escapeHtml(b.leads || 0)}</td>
              <td>
                <span class="${Number(b.in_progress || 0) > 0 ? "badge badge-warn" : "badge badge-muted"}">
                  ${escapeHtml(b.in_progress || 0)}
                </span>
              </td>
              <td>
                <span class="badge badge-ok">${escapeHtml(b.completed || 0)}</span>
              </td>
              <td>
                <span class="${attention ? "attention-dot active" : "attention-dot"}"></span>
                ${attention ? "Needs review" : "Clean"}
              </td>
              <td class="open-cell">Open →</td>
            </tr>
          `;
        })
        .join("")
    : `<tr><td colspan="7" class="empty-cell">No businesses found.</td></tr>`;

  const recentRowsHtml = recent.length
    ? recent
        .map(
          (lead) => `
            <tr>
              <td>${escapeHtml(formatDateTime(lead.created_at))}</td>
              <td><a href="/leads/${encodeURIComponent(lead.business)}">${escapeHtml(lead.business)}</a></td>
              <td>${escapeHtml(lead.lead_phone)}</td>
              <td>${escapeHtml(lead.sender_phone)}</td>
              <td><span class="${badgeClass(lead.status)}">${escapeHtml(lead.status)}</span></td>
            </tr>
          `,
        )
        .join("")
    : `<tr><td colspan="5" class="empty-cell">No recent voice uploads.</td></tr>`;

  return `
            ${renderTopNav("leads")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Voice Upload Inbox</div>
              <h1>Leads Overview</h1>
              <div class="subtitle">Voice leads received from WhatsApp, grouped by business.</div>
            </div>
          </div>

          <div class="stats">
            <div class="stat-card"><div class="stat-label">Total</div><div class="stat-value">${summary.total || 0}</div></div>
            <div class="stat-card"><div class="stat-label">Leads</div><div class="stat-value">${summary.leads || 0}</div></div>
            <div class="stat-card"><div class="stat-label">In Progress</div><div class="stat-value">${summary.in_progress || 0}</div></div>
            <div class="stat-card"><div class="stat-label">Completed</div><div class="stat-value">${summary.completed || 0}</div></div>
          </div>

          <div class="panel">
            <div class="toolbar">
              <div>
                <div class="toolbar-title">Businesses</div>
                <div class="subtitle">Click any row to open that business lead inbox.</div>
              </div>

              <div class="toolbar-actions">
                <span class="filter-chip">All: ${businesses.length}</span>
                <span class="filter-chip">Needs Review: ${businesses.filter((b) => Number(b.in_progress || 0) > 0 || Number(b.leads || 0) > 0).length}</span>
              </div>
            </div>

            <table>
              <thead>
                <tr>
                  <th>Business</th>
                  <th>Total</th>
                  <th>Leads</th>
                  <th>In Progress</th>
                  <th>Completed</th>
                  <th>Status</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>${businessRowsHtml}</tbody>
            </table>
          </div>

          <div class="panel recent-panel">
            <div class="toolbar">
              <div class="toolbar-title">Recent Voice Uploads</div>
            </div>

            <table>
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Business</th>
                  <th>Lead Phone</th>
                  <th>Uploaded By</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>${recentRowsHtml}</tbody>
            </table>
          </div>
        </div>
      
  `;
}

export {
  renderLeadsOverviewPage,
};
