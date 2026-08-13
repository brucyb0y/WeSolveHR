// /leads — replaces renderLeadsOverviewPage() + app.get("/leads").

import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import {
  DASHBOARD_ORG_ID,
  getLeadsOverviewData,
  formatDateTime,
  badgeClass,
} from "@/lib/server/app.js";
import ClickableRow from "./ClickableRow";
import styles from "./leads.module.css";

export const metadata = { title: "Leads | WeSolveHR" };
export const dynamic = "force-dynamic";

// badgeClass() returns global class names ("badge badge-warn"). Map them onto
// this page's CSS-module classes. badge-danger and badge-info are intentionally
// unmapped: the original page never defined them, so those statuses showed the
// pill shape with no fill, and that is preserved here.
const BADGE_MODULE_CLASSES = {
  badge: styles.badge,
  "badge-ok": styles.badgeOk,
  "badge-warn": styles.badgeWarn,
  "badge-muted": styles.badgeMuted,
};

function badgeClasses(value) {
  return badgeClass(value)
    .split(" ")
    .map((name) => BADGE_MODULE_CLASSES[name])
    .filter(Boolean)
    .join(" ");
}

const STAT_CARDS = [
  { label: "Total", key: "total" },
  { label: "Leads", key: "leads" },
  { label: "In Progress", key: "in_progress" },
  { label: "Completed", key: "completed" },
];

const BUSINESS_COLUMNS = [
  "Business",
  "Total",
  "Leads",
  "In Progress",
  "Completed",
  "Status",
  "Action",
];

const RECENT_COLUMNS = [
  "Time",
  "Business",
  "Lead Phone",
  "Uploaded By",
  "Status",
];

export default async function LeadsPage() {
  const user = await requireDashboardUser();

  // The original read org_id off req.session.user (which the session never
  // populates — only userId is stored), so this always fell through to
  // DASHBOARD_ORG_ID. Kept identical rather than silently rescoping the page.
  const data = await getLeadsOverviewData(DASHBOARD_ORG_ID);

  const summary = data?.summary || {};
  const businesses = data?.businesses || [];
  const recent = data?.recent || [];

  const needsReviewCount = businesses.filter(
    (b) => Number(b.in_progress || 0) > 0 || Number(b.leads || 0) > 0,
  ).length;

  return (
    <>
      <TopNav active="leads" user={user} />

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
            <div className={styles.statCard} key={card.key}>
              <div className={styles.statLabel}>{card.label}</div>
              <div className={styles.statValue}>{summary[card.key] || 0}</div>
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

          <table>
            <thead>
              <tr>
                {BUSINESS_COLUMNS.map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {businesses.length ? (
                businesses.map((b) => {
                  const attention =
                    Number(b.in_progress || 0) > 0 || Number(b.leads || 0) > 0;

                  return (
                    <ClickableRow
                      key={b.business}
                      className={styles.businessRow}
                      href={`/leads/${encodeURIComponent(b.business)}`}
                    >
                      <td>
                        <div className={styles.businessName}>
                          {b.label || b.business}
                        </div>
                        <div className={styles.businessSubtitle}>
                          {b.business}
                        </div>
                      </td>
                      <td>
                        <strong>{b.total || 0}</strong>
                      </td>
                      <td>{b.leads || 0}</td>
                      <td>
                        <span
                          className={
                            Number(b.in_progress || 0) > 0
                              ? `${styles.badge} ${styles.badgeWarn}`
                              : `${styles.badge} ${styles.badgeMuted}`
                          }
                        >
                          {b.in_progress || 0}
                        </span>
                      </td>
                      <td>
                        <span className={`${styles.badge} ${styles.badgeOk}`}>
                          {b.completed || 0}
                        </span>
                      </td>
                      <td>
                        <span
                          className={
                            attention
                              ? `${styles.attentionDot} ${styles.attentionDotActive}`
                              : styles.attentionDot
                          }
                        />
                        {attention ? "Needs review" : "Clean"}
                      </td>
                      <td className={styles.openCell}>Open →</td>
                    </ClickableRow>
                  );
                })
              ) : (
                <tr>
                  <td colSpan={7} className={styles.emptyCell}>
                    No businesses found.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className={`${styles.panel} ${styles.recentPanel}`}>
          <div className={styles.toolbar}>
            <div className={styles.toolbarTitle}>Recent Voice Uploads</div>
          </div>

          <table>
            <thead>
              <tr>
                {RECENT_COLUMNS.map((column) => (
                  <th key={column}>{column}</th>
                ))}
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
                      <span className={badgeClasses(lead.status)}>
                        {lead.status}
                      </span>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={5} className={styles.emptyCell}>
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
