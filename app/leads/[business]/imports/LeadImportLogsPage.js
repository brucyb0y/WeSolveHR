// Markup for GET /leads/:business/imports.
//
// Body markup extracted verbatim from renderLeadImportLogsPage() (lib/server/app.js
// lines 38407-38586). The document shell now comes from
// app/layout.jsx, the <style> block from ./lead-imports.css, and the inline
// <script> from public/js/.

import { escapeHtml, formatDateTime } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";

function renderLeadImportLogsPage({
  business,
  logs = [],
  rows = [],
  selectedImportId = null,
  search = "",
  date = "",
  status = "",
  uploadedBy = "",
}) {
  const logRowsHtml = logs.length
    ? logs
        .map(
          (log) => `
          <tr>
            <td>
              <a href="/leads/${business}/imports?import_id=${log.id}&search=${encodeURIComponent(search)}&date=${encodeURIComponent(date)}&status=${encodeURIComponent(status)}&uploaded_by=${encodeURIComponent(uploadedBy)}">
                #${escapeHtml(log.id)}
              </a>
              <div class="muted">${escapeHtml(log.file_name || "-")}</div>
            </td>
            <td>${escapeHtml(log.created_at ? formatDateTime(log.created_at) : "-")}</td>
            <td>${escapeHtml(log.uploaded_by_name || "-")}</td>
            <td><span class="badge badge-info">${escapeHtml(log.status || "-")}</span></td>
            <td>${escapeHtml(log.total_rows || 0)}</td>
            <td>${escapeHtml(log.inserted_count || 0)}</td>
            <td>${escapeHtml(log.duplicate_count || 0)}</td>
            <td>${escapeHtml(log.skipped_count || 0)}</td>
            <td>${escapeHtml(log.error_count || 0)}</td>
          </tr>
        `,
        )
        .join("")
    : `<tr><td colspan="9" class="empty-cell">No import logs found.</td></tr>`;

  const detailRowsHtml = rows.length
    ? rows
        .map(
          (r) => `
          <tr>
            <td>${escapeHtml(r.row_number || "-")}</td>
            <td><span class="badge ${r.status === "success" ? "badge-ok" : r.status === "duplicate" ? "badge-warn" : r.status === "error" ? "badge-danger" : "badge-muted"}">${escapeHtml(r.status)}</span></td>
            <td>${escapeHtml(r.phone || "-")}</td>
            <td>${escapeHtml(r.company || "-")}</td>
            <td>${escapeHtml(r.website || "-")}</td>
            <td>${escapeHtml(r.message || "-")}</td>
            <td>${escapeHtml(r.existing_lead_id || "-")}</td>
          </tr>
        `,
        )
        .join("")
    : `<tr><td colspan="7" class="empty-cell">Select an import to view row details.</td></tr>`;

  return `
            ${renderTopNav("leads")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <h1>${escapeHtml(business)} Import Logs</h1>
              <div class="subtitle">Excel upload history, duplicates skipped, errors, day-wise search, and uploader tracking.</div>
            </div>
            <a class="btn" href="/leads/${encodeURIComponent(business)}">← Back to Leads</a>
          </div>

          <div class="panel">
            <form class="search-row" method="GET" action="/leads/${encodeURIComponent(business)}/imports">
              <input name="search" value="${escapeHtml(search)}" placeholder="Search file, uploader, phone, company..." />
              <input type="date" name="date" value="${escapeHtml(date)}" />
              <input name="uploaded_by" value="${escapeHtml(uploadedBy)}" placeholder="Uploaded by..." />
              <select name="status">
                <option value="">All statuses</option>
                <option value="completed" ${status === "completed" ? "selected" : ""}>Completed</option>
                <option value="processing" ${status === "processing" ? "selected" : ""}>Processing</option>
                <option value="failed" ${status === "failed" ? "selected" : ""}>Failed</option>
              </select>
              <button class="btn btn-primary" type="submit">Search</button>
              <a class="btn" href="/leads/${encodeURIComponent(business)}/imports">Clear</a>
            </form>
          </div>

          <div class="panel">
            <h2>Uploads</h2>
            <table>
              <thead>
                <tr>
                  <th>Import</th>
                  <th>Uploaded At</th>
                  <th>Uploaded By</th>
                  <th>Status</th>
                  <th>Total</th>
                  <th>Inserted</th>
                  <th>Duplicates</th>
                  <th>Skipped</th>
                  <th>Errors</th>
                </tr>
              </thead>
              <tbody>${logRowsHtml}</tbody>
            </table>
          </div>

          <div class="panel">
            <h2>Selected Import Details ${selectedImportId ? `#${escapeHtml(selectedImportId)}` : ""}</h2>
            <table>
              <thead>
                <tr>
                  <th>Excel Row</th>
                  <th>Status</th>
                  <th>Phone</th>
                  <th>Company</th>
                  <th>Website</th>
                  <th>Message</th>
                  <th>Lead ID</th>
                </tr>
              </thead>
              <tbody>${detailRowsHtml}</tbody>
            </table>
          </div>
        </div>
      
  `;
}

export {
  renderLeadImportLogsPage,
};
