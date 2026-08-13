"use client";

// The manually-authored Weekly Report cards at the bottom of the Report tab —
// replaces reportsCardsHtml, updateReport and archiveReport.
//
// This is only the PM-published half of the Report tab. The auto-computed
// daily/weekly/funnel subviews above it come from buildClientAutoReportSections
// and are still rendered through the shared chart kit; they convert together
// with /client-view's copy so the two pages cannot drift apart.
//
// Publish state and client visibility are two different switches:
//   * is_published  — whether the report is finalised at all.
//   * is_client_visible — whether it is mirrored to the client dashboard once
//     published (set in the report modal).
// A report that is published but not client-visible stays internal, which is why
// both the Published/Draft badge and the CLIENT/INTERNAL chip are shown.

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./workspace.module.css";
import { VIS_CHIP } from "./badges";

function ReportSection({ label, value }) {
  if (!value) return null;
  return (
    <div style={{ marginTop: 8 }}>
      <strong>{label}:</strong>
      <div className={styles.meta} style={{ whiteSpace: "pre-wrap" }}>
        {value}
      </div>
    </div>
  );
}

export default function ReportsPanel({ clientId, reports, onAdd, onEdit }) {
  const router = useRouter();
  const [busyId, setBusyId] = useState(null);

  async function patch(id, body, failMessage) {
    setBusyId(id);
    try {
      const res = await fetch(`/api/clients/${clientId}/reports/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || failMessage);
        return;
      }

      router.refresh();
    } catch {
      alert(failMessage);
    } finally {
      setBusyId(null);
    }
  }

  function archive(id) {
    if (!confirm("Archive this report?")) return;
    patch(id, { archive: true }, "Failed to archive report");
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
            Weekly Report <span className={VIS_CHIP.client}>CLIENT</span>
          </h2>
          <div className={styles.sectionSubtitle}>
            PM publishes · visible to client when published
          </div>
        </div>
        <button
          className={`${styles.btn} ${styles.btnPrimary}`}
          type="button"
          onClick={onAdd}
        >
          + New Report
        </button>
      </div>

      <div className={styles.standardList}>
        {reports.length ? (
          reports.map((r) => {
            const period =
              r.period_label ||
              (r.week_start ? `Week of ${r.week_start}` : "Report");
            return (
              <div className={styles.standardCard} key={r.id}>
                <div className={styles.standardCardTop}>
                  <div>
                    <div className={styles.standardCardTitle}>{period}</div>
                    <div className={styles.meta}>{r.createdText}</div>
                  </div>
                  <div
                    style={{
                      display: "flex",
                      gap: 8,
                      flexWrap: "wrap",
                      justifyContent: "flex-end",
                    }}
                  >
                    <span
                      className={`${styles.badge} ${
                        r.is_published ? styles.badgeOk : styles.badgeMuted
                      }`}
                    >
                      {r.is_published ? "Published" : "Draft"}
                    </span>
                    <span
                      className={
                        r.is_client_visible
                          ? VIS_CHIP.client
                          : VIS_CHIP.internal
                      }
                    >
                      {r.is_client_visible ? "CLIENT" : "INTERNAL"}
                    </span>
                  </div>
                </div>

                <ReportSection label="Summary" value={r.summary} />
                <ReportSection label="Highlights" value={r.highlights} />
                <ReportSection label="Lowlights / Risks" value={r.lowlights} />
                <ReportSection
                  label="Next Week Plan"
                  value={r.next_week_plan}
                />

                <div
                  className={styles.workCardActions}
                  style={{ marginTop: 12 }}
                >
                  <button
                    className={styles.btn}
                    type="button"
                    disabled={busyId === r.id}
                    onClick={() =>
                      patch(
                        r.id,
                        r.is_published
                          ? { unpublish: true }
                          : { publish: true },
                        `Failed to ${r.is_published ? "unpublish" : "publish"} report`,
                      )
                    }
                  >
                    {r.is_published ? "Unpublish" : "Publish"}
                  </button>
                  <button
                    className={styles.btn}
                    type="button"
                    onClick={() => onEdit(r.id)}
                  >
                    Edit
                  </button>
                  <button
                    className={styles.btn}
                    type="button"
                    disabled={busyId === r.id}
                    onClick={() => archive(r.id)}
                  >
                    Archive
                  </button>
                </div>
              </div>
            );
          })
        ) : (
          <div className={styles.meta}>
            No weekly reports yet. Publish the first weekly update for this
            client.
          </div>
        )}
      </div>
    </div>
  );
}
