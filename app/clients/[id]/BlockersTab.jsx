"use client";

// Blockers tab — replaces the blockersTabHtml block and the updateBlocker /
// archiveBlocker handlers.
//
// The Start / Resolve buttons are conditional in the same way as the original:
// Start only shows while a blocker is still "open", and Resolve disappears once
// it is resolved — so neither button can move a blocker to the state it is
// already in.
//
// Rows arrive pre-decorated from page.jsx (owner name, related work item title,
// formatted date); this component only renders and mutates.

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./workspace.module.css";
import {
  priorityBadgeClass,
  blockerStatusClass,
  mutedBadgeClass,
  blockerSideLabel,
  humanizeStatus,
} from "./badges";

export default function BlockersTab({ clientId, blockers, onAdd, onEdit }) {
  const router = useRouter();
  const [busyId, setBusyId] = useState(null);

  const openCount = blockers.filter(
    (b) => b.resolution_status !== "resolved",
  ).length;

  const countBy = (status) =>
    blockers.filter((b) => b.resolution_status === status).length;

  async function patch(id, body, failMessage) {
    setBusyId(id);
    try {
      const res = await fetch(`/api/clients/${clientId}/blockers/${id}`, {
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

  const setStatus = (id, resolution_status) =>
    patch(id, { resolution_status }, "Failed to update blocker");

  function archive(id) {
    if (!confirm("Archive this blocker?")) return;
    patch(id, { archive: true }, "Failed to archive blocker");
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
          <h2 style={{ margin: 0 }}>Blockers</h2>
          <div className={styles.sectionSubtitle}>
            Internal &amp; client-side blockers · ownership · resolution status
          </div>
        </div>
        <button
          className={`${styles.btn} ${styles.btnPrimary}`}
          type="button"
          onClick={onAdd}
        >
          + Add Blocker
        </button>
      </div>

      {openCount ? (
        <div className={styles.alertStrip}>
          <span>
            ⛔ {openCount} open blocker{openCount === 1 ? "" : "s"}
          </span>
        </div>
      ) : null}

      <div className={styles.workSummaryChips} style={{ marginBottom: 12 }}>
        <span className={styles.summaryChip}>Total {blockers.length}</span>
        <span className={styles.summaryChip}>Open {countBy("open")}</span>
        <span className={styles.summaryChip}>
          In progress {countBy("in_progress")}
        </span>
        <span className={styles.summaryChip}>
          Resolved {countBy("resolved")}
        </span>
      </div>

      <div className={styles.standardList}>
        {blockers.length ? (
          blockers.map((b) => (
            <div className={styles.standardCard} key={b.id}>
              <div className={styles.standardCardTop}>
                <div>
                  <div className={styles.standardCardTitle}>
                    {b.title || "Untitled blocker"}
                  </div>
                  <div className={styles.meta}>
                    {b.description || "No description"}
                  </div>
                </div>
                <div
                  style={{
                    display: "flex",
                    gap: 8,
                    flexWrap: "wrap",
                    justifyContent: "flex-end",
                  }}
                >
                  <span className={mutedBadgeClass()}>
                    {blockerSideLabel(b.blocker_side)}
                  </span>
                  <span className={priorityBadgeClass(b.priority)}>
                    {b.priority || "medium"}
                  </span>
                  <span className={blockerStatusClass(b.resolution_status)}>
                    {humanizeStatus(b.resolution_status || "open")}
                  </span>
                </div>
              </div>

              <div className={styles.workCardMeta}>
                <div>
                  <strong>Owner:</strong> {b.ownerName}
                </div>
                <div>
                  <strong>Related work item:</strong> {b.relatedTitle}
                </div>
                <div>
                  <strong>Created:</strong> {b.createdText}
                </div>
              </div>

              <div className={styles.workCardActions}>
                {b.resolution_status === "open" ? (
                  <button
                    className={styles.btn}
                    type="button"
                    disabled={busyId === b.id}
                    onClick={() => setStatus(b.id, "in_progress")}
                  >
                    Start
                  </button>
                ) : null}
                {b.resolution_status !== "resolved" ? (
                  <button
                    className={styles.btn}
                    type="button"
                    disabled={busyId === b.id}
                    onClick={() => setStatus(b.id, "resolved")}
                  >
                    Resolve
                  </button>
                ) : null}
                <button
                  className={styles.btn}
                  type="button"
                  onClick={() => onEdit(b.id)}
                >
                  Edit
                </button>
                <button
                  className={styles.btn}
                  type="button"
                  disabled={busyId === b.id}
                  onClick={() => archive(b.id)}
                >
                  Archive
                </button>
              </div>
            </div>
          ))
        ) : (
          <div className={styles.meta}>
            No blockers logged. Add one when something is blocking progress.
          </div>
        )}
      </div>
    </div>
  );
}
