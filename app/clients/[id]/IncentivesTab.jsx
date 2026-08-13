"use client";

// Incentives tab — replaces incentivesTabHtml and archiveIncentive.
//
// Internal-only: the INTERNAL chip beside the heading is the signal that none of
// this reaches the client dashboard. Attribution and commission amounts are not
// mirrored to /client-view/[token].
//
// Rows arrive pre-decorated from page.jsx with gtmName and leadLabel, so the
// lead lookup map the original rebuilt per render is not needed here.

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./workspace.module.css";
import { incentiveStatusClass, VIS_CHIP } from "./badges";

export default function IncentivesTab({ clientId, incentives, onAdd, onEdit }) {
  const router = useRouter();
  const [busyId, setBusyId] = useState(null);

  const totalIncentive = incentives.reduce(
    (n, i) => n + (Number(i.amount) || 0),
    0,
  );

  async function archive(id) {
    if (!confirm("Archive this incentive?")) return;

    setBusyId(id);
    try {
      const res = await fetch(`/api/clients/${clientId}/incentives/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ archive: true }),
      });
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to archive incentive");
        return;
      }

      router.refresh();
    } catch {
      alert("Failed to archive incentive");
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div className={styles.panel}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          gap: 12,
          alignItems: "center",
          marginBottom: 14,
        }}
      >
        <div>
          <h2 style={{ margin: 0 }}>
            Incentives <span className={VIS_CHIP.internal}>INTERNAL</span>
          </h2>
          <div className={styles.sectionSubtitle}>
            Attribution · commission · credit log (internal only)
          </div>
        </div>
        <button
          className={`${styles.btn} ${styles.btnPrimary}`}
          type="button"
          onClick={onAdd}
        >
          + Add Incentive
        </button>
      </div>

      <div className={styles.workSummaryChips} style={{ marginBottom: 12 }}>
        <span className={styles.summaryChip}>Entries {incentives.length}</span>
        <span className={styles.summaryChip}>
          Paid {incentives.filter((i) => i.status === "paid").length}
        </span>
        <span className={styles.summaryChip}>
          Total amount {totalIncentive}
        </span>
      </div>

      <div style={{ overflowX: "auto" }}>
        <table
          className={styles.workTable}
          style={{ width: "100%", borderCollapse: "collapse" }}
        >
          <thead>
            <tr>
              <th style={{ textAlign: "left" }}>Title</th>
              <th style={{ textAlign: "left" }}>GTM (attribution)</th>
              <th style={{ textAlign: "left" }}>Lead</th>
              <th style={{ textAlign: "left" }}>Amount</th>
              <th style={{ textAlign: "left" }}>Status</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {incentives.length ? (
              incentives.map((i) => (
                <tr key={i.id}>
                  <td style={{ fontWeight: 700 }}>{i.title || "-"}</td>
                  <td>{i.gtmName}</td>
                  <td>{i.leadLabel}</td>
                  <td>{Number(i.amount) || 0}</td>
                  <td>
                    <span className={incentiveStatusClass(i.status)}>
                      {i.status || "pending"}
                    </span>
                  </td>
                  <td>
                    <button
                      className={styles.btn}
                      type="button"
                      onClick={() => onEdit(i.id)}
                    >
                      Edit
                    </button>
                    <button
                      className={styles.btn}
                      type="button"
                      disabled={busyId === i.id}
                      onClick={() => archive(i.id)}
                    >
                      Archive
                    </button>
                  </td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={6} className={styles.meta}>
                  No incentives logged yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
