"use client";

// Businesses table for the Leads Overview. Replaces the per-row
// onclick="window.location.href=..." in renderLeadsOverviewPage(). Uses a hard
// navigation (matching the original) because /leads/:business is still served as
// HTML by the dispatch shim rather than an RSC page.

import styles from "./leads-overview.module.css";

export default function LeadsBusinessTable({ businesses }) {
  function openBusiness(business) {
    window.location.href = `/leads/${encodeURIComponent(business)}`;
  }

  return (
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
      <tbody>
        {businesses.length ? (
          businesses.map((b) => {
            const attention =
              Number(b.in_progress || 0) > 0 || Number(b.leads || 0) > 0;
            return (
              <tr
                key={b.business}
                className={styles.businessRow}
                onClick={() => openBusiness(b.business)}
              >
                <td>
                  <div className={styles.businessName}>{b.label || b.business}</div>
                  <div className={styles.businessSubtitle}>{b.business}</div>
                </td>
                <td>
                  <strong>{b.total || 0}</strong>
                </td>
                <td>{b.leads || 0}</td>
                <td>
                  <span
                    className={`${styles.badge} ${
                      Number(b.in_progress || 0) > 0
                        ? styles.badgeWarn
                        : styles.badgeMuted
                    }`}
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
                    className={`${styles.attentionDot} ${
                      attention ? styles.active : ""
                    }`}
                  />
                  {attention ? "Needs review" : "Clean"}
                </td>
                <td className={styles.openCell}>Open →</td>
              </tr>
            );
          })
        ) : (
          <tr>
            <td colSpan={7} className="empty-cell">
              No businesses found.
            </td>
          </tr>
        )}
      </tbody>
    </table>
  );
}
