// Lead Import Logs (Server Component). Replaces the GET /leads/:business/imports
// Express handler + renderLeadImportLogsPage(). The page has no client-side
// interactivity — the search box is a native GET form and selecting an import is
// a plain link — so everything renders on the server from params/searchParams.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import { getBusinessCanonicalName, getLeadImportLogs } from "@/lib/services/leads.js";
import { formatDateTime } from "@/lib/utils/datetime.js";
import styles from "./imports.module.css";

export const metadata = { title: "Lead Import Logs | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const DETAIL_BADGE_CLASS = {
  success: "badgeOk",
  duplicate: "badgeWarn",
  error: "badgeDanger",
};

function importHref(business, importId, filters) {
  const qs = new URLSearchParams({
    import_id: String(importId),
    search: filters.search,
    date: filters.date,
    status: filters.status,
    uploaded_by: filters.uploadedBy,
  });
  return `/leads/${business}/imports?${qs.toString()}`;
}

export default async function LeadImportLogsPage({ params, searchParams }) {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const { business: rawBusiness } = await params;
  const sp = await searchParams;
  const business = getBusinessCanonicalName(rawBusiness);

  const filters = {
    search: String(sp.search || "").trim(),
    date: String(sp.date || "").trim(),
    status: String(sp.status || "").trim(),
    uploadedBy: String(sp.uploaded_by || "").trim(),
  };
  const selectedImportId = sp.import_id ? Number(sp.import_id) : null;

  const { logs, rows } = await getLeadImportLogs({
    orgId: user.org_id || DASHBOARD_ORG_ID,
    business,
    ...filters,
    selectedImportId,
  });

  const backHref = `/leads/${encodeURIComponent(business)}`;
  const formAction = `/leads/${encodeURIComponent(business)}/imports`;

  return (
    <>
      <TopNav active="leads" />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <h1>{business} Import Logs</h1>
            <div className={styles.subtitle}>
              Excel upload history, duplicates skipped, errors, day-wise search,
              and uploader tracking.
            </div>
          </div>
          <a className={styles.btn} href={backHref}>
            ← Back to Leads
          </a>
        </div>

        <div className={styles.panel}>
          <form className={styles.searchRow} method="GET" action={formAction}>
            <input
              name="search"
              defaultValue={filters.search}
              placeholder="Search file, uploader, phone, company..."
            />
            <input type="date" name="date" defaultValue={filters.date} />
            <input
              name="uploaded_by"
              defaultValue={filters.uploadedBy}
              placeholder="Uploaded by..."
            />
            <select name="status" defaultValue={filters.status}>
              <option value="">All statuses</option>
              <option value="completed">Completed</option>
              <option value="processing">Processing</option>
              <option value="failed">Failed</option>
            </select>
            <button className={`${styles.btn} ${styles.btnPrimary}`} type="submit">
              Search
            </button>
            <a className={styles.btn} href={formAction}>
              Clear
            </a>
          </form>
        </div>

        <div className={styles.panel}>
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
            <tbody>
              {logs.length ? (
                logs.map((log) => (
                  <tr key={log.id}>
                    <td>
                      <a href={importHref(business, log.id, filters)}>
                        #{log.id}
                      </a>
                      <div className="muted">{log.file_name || "-"}</div>
                    </td>
                    <td>{log.created_at ? formatDateTime(log.created_at) : "-"}</td>
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
          <h2>Selected Import Details {selectedImportId ? `#${selectedImportId}` : ""}</h2>
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
            <tbody>
              {rows.length ? (
                rows.map((r, i) => (
                  <tr key={r.id ?? i}>
                    <td>{r.row_number || "-"}</td>
                    <td>
                      <span
                        className={`${styles.badge} ${
                          styles[DETAIL_BADGE_CLASS[r.status] || "badgeMuted"]
                        }`}
                      >
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
