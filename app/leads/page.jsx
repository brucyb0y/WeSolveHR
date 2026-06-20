// Leads Overview (Server Component). Replaces GET /leads +
// renderLeadsOverviewPage(): authenticate, compute the per-business counts +
// recent uploads on the server, and hand the click-to-open businesses table to
// a small client island. The recent-uploads table is static (plain links).

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import { getLeadsOverviewData } from "@/lib/services/leads.js";
import { badgeKind } from "@/lib/utils/badge.js";
import { formatDateTime } from "@/lib/utils/datetime.js";
import LeadsBusinessTable from "./LeadsBusinessTable.jsx";
import styles from "./leads-overview.module.css";

export const metadata = { title: "Leads | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const STAT_CARDS = [
  { label: "Total", field: "total" },
  { label: "Leads", field: "leads" },
  { label: "In Progress", field: "in_progress" },
  { label: "Completed", field: "completed" },
];

// The overview's <style> only defines ok/warn/muted; other kinds fall back to
// the base badge, matching the original page.
const BADGE_CLASS = {
  ok: "badgeOk",
  warn: "badgeWarn",
  muted: "badgeMuted",
};

export default async function LeadsOverviewPage() {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const data = await getLeadsOverviewData(user.org_id || DASHBOARD_ORG_ID);
  const summary = data?.summary || {};
  const businesses = data?.businesses || [];
  const recent = data?.recent || [];

  const needsReviewCount = businesses.filter(
    (b) => Number(b.in_progress || 0) > 0 || Number(b.leads || 0) > 0,
  ).length;

  const statusBadge = (status) =>
    `${styles.badge} ${styles[BADGE_CLASS[badgeKind(status)]] || ""}`;

  return (
    <>
      <TopNav active="leads" />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Voice Upload Inbox</div>
            <h1>Leads Overview</h1>
            <div className={styles.subtitle}>
              Voice leads received from WhatsApp, grouped by business.
            </div>
          </div>
        </div>

        <div className={styles.stats}>
          {STAT_CARDS.map((card) => (
            <div className={styles.statCard} key={card.field}>
              <div className={styles.statLabel}>{card.label}</div>
              <div className={styles.statValue}>{summary[card.field] || 0}</div>
            </div>
          ))}
        </div>

        <div className={styles.panel}>
          <div className={styles.toolbar}>
            <div>
              <div className={styles.toolbarTitle}>Businesses</div>
              <div className={styles.subtitle}>
                Click any row to open that business lead inbox.
              </div>
            </div>

            <div className={styles.toolbarActions}>
              <span className={styles.filterChip}>All: {businesses.length}</span>
              <span className={styles.filterChip}>
                Needs Review: {needsReviewCount}
              </span>
            </div>
          </div>

          <LeadsBusinessTable businesses={businesses} />
        </div>

        <div className={`${styles.panel} ${styles.recentPanel}`}>
          <div className={styles.toolbar}>
            <div className={styles.toolbarTitle}>Recent Voice Uploads</div>
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
            <tbody>
              {recent.length ? (
                recent.map((lead) => (
                  <tr key={lead.id}>
                    <td>{formatDateTime(lead.created_at)}</td>
                    <td>
                      <a href={`/leads/${encodeURIComponent(lead.business)}`}>
                        {lead.business}
                      </a>
                    </td>
                    <td>{lead.lead_phone}</td>
                    <td>{lead.sender_phone}</td>
                    <td>
                      <span className={statusBadge(lead.status)}>
                        {lead.status}
                      </span>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={5} className="empty-cell">
                    No recent voice uploads.
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
