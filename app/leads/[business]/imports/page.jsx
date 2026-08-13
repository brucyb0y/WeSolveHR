// /leads/:business/imports — replaces renderLeadImportLogsPage() +
// app.get("/leads/:business/imports").
//
// Entirely server-rendered: the filter bar is a plain GET form that reloads the
// page with new query params, exactly as before, so this page ships no
// JavaScript.

import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import {
  DASHBOARD_ORG_ID,
  getBusinessCanonicalName,
  formatDateTime,
} from "@/lib/server/app.js";
import { getLeadImportLogsData } from "@/lib/data/lead-imports";
import styles from "./imports.module.css";

export const metadata = { title: "Lead Import Logs" };
export const dynamic = "force-dynamic";

const LOG_COLUMNS = [
  "Import",
  "Uploaded At",
  "Uploaded By",
  "Status",
  "Total",
  "Inserted",
  "Duplicates",
  "Skipped",
  "Errors",
];

const DETAIL_COLUMNS = [
  "Excel Row",
  "Status",
  "Phone",
  "Company",
  "Website",
  "Message",
  "Lead ID",
];

const STATUS_OPTIONS = [
  { value: "", label: "All statuses" },
  { value: "completed", label: "Completed" },
  { value: "processing", label: "Processing" },
  { value: "failed", label: "Failed" },
];

function rowStatusBadge(status) {
  if (status === "success") return `${styles.badge} ${styles.badgeOk}`;
  if (status === "duplicate") return `${styles.badge} ${styles.badgeWarn}`;
  if (status === "error") return `${styles.badge} ${styles.badgeDanger}`;
  return `${styles.badge} ${styles.badgeMuted}`;
}

export default async function LeadImportsPage({ params, searchParams }) {
  const user = await requireDashboardUser();
  const { business: businessParam } = await params;
  const sp = await searchParams;

  const business = getBusinessCanonicalName(businessParam);

  const search = String(sp?.search || "").trim();
  const date = String(sp?.date || "").trim();
  const status = String(sp?.status || "").trim();
  const uploadedBy = String(sp?.uploaded_by || "").trim();
  const selectedImportId = sp?.import_id ? Number(sp.import_id) : null;

  // The original read org_id off req.session.user, which the session never
  // populates, so this always resolved to DASHBOARD_ORG_ID. Kept identical.
  const { logs, rows } = await getLeadImportLogsData({
    orgId: DASHBOARD_ORG_ID,
    business,
    search,
    date,
    status,
    uploadedBy,
    selectedImportId,
  });

  const importsHref = `/leads/${encodeURIComponent(business)}/imports`;

  const logHref = (logId) => {
    const qs = new URLSearchParams({
      import_id: String(logId),
      search,
      date,
      status,
      uploaded_by: uploadedBy,
    });
    return `${importsHref}?${qs.toString()}`;
  };

  return (
    <>
      <TopNav active="leads" user={user} />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <h1>{business} Import Logs</h1>
            <div className={styles.subtitle}>
              Excel upload history, duplicates skipped, errors, day-wise search,
              and uploader tracking.
            </div>
          </div>
          <a
            className={styles.btn}
            href={`/leads/${encodeURIComponent(business)}`}
          >
            ← Back to Leads
          </a>
        </div>

        <div className={styles.panel}>
          <form className={styles.searchRow} method="GET" action={importsHref}>
            <input
              name="search"
              defaultValue={search}
              placeholder="Search file, uploader, phone, company..."
            />
            <input type="date" name="date" defaultValue={date} />
            <input
              name="uploaded_by"
              defaultValue={uploadedBy}
              placeholder="Uploaded by..."
            />
            <select name="status" defaultValue={status}>
              {STATUS_OPTIONS.map((option) => (
                <option value={option.value} key={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
            <button
              className={`${styles.btn} ${styles.btnPrimary}`}
              type="submit"
            >
              Search
            </button>
            <a className={styles.btn} href={importsHref}>
              Clear
            </a>
          </form>
        </div>

        <div className={styles.panel}>
          <h2>Uploads</h2>
          <table>
            <thead>
              <tr>
                {LOG_COLUMNS.map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {logs.length ? (
                logs.map((log) => (
                  <tr key={log.id}>
                    <td>
                      <a href={logHref(log.id)}>#{log.id}</a>
                      <div className="muted">{log.file_name || "-"}</div>
                    </td>
                    <td>
                      {log.created_at ? formatDateTime(log.created_at) : "-"}
                    </td>
                    <td>{log.uploaded_by_name || "-"}</td>
                    <td>
                      <span className={`${styles.badge} ${styles.badgeInfo}`}>
                        {log.status || "-"}
                      </span>
                    </td>
                    <td>{log.total_rows || 0}</td>
                    <td>{log.inserted_count || 0}</td>
                    <td>{log.duplicate_count || 0}</td>
                    <td>{log.skipped_count || 0}</td>
                    <td>{log.error_count || 0}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={9} className="empty-cell">
                    No import logs found.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className={styles.panel}>
          <h2>
            Selected Import Details {selectedImportId ? `#${selectedImportId}` : ""}
          </h2>
          <table>
            <thead>
              <tr>
                {DETAIL_COLUMNS.map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.length ? (
                rows.map((r) => (
                  <tr key={r.id ?? r.row_number}>
                    <td>{r.row_number || "-"}</td>
                    <td>
                      <span className={rowStatusBadge(r.status)}>
                        {r.status}
                      </span>
                    </td>
                    <td>{r.phone || "-"}</td>
                    <td>{r.company || "-"}</td>
                    <td>{r.website || "-"}</td>
                    <td>{r.message || "-"}</td>
                    <td>{r.existing_lead_id || "-"}</td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={7} className="empty-cell">
                    Select an import to view row details.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
