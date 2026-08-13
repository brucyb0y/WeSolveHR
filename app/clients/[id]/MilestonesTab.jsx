"use client";

// Milestones tab — replaces the milestones panel, closeMilestone and
// archiveMilestone.
//
// Closing and archiving are genuinely different calls, preserved as-is:
//   close   -> PUT  /api/clients/:id/milestones/:mid   { status: "closed" }
//   archive -> POST /api/clients/:id/milestones/:mid/archive
// They are separate endpoints, not two flags on one request.
//
// linkedCount is the number of work items pointing at this milestone. It is
// compared as a STRING on both sides — work_items.milestone_id and the
// milestone id can arrive as either number or string depending on the query
// path, and a strict === would silently show "0 work items" for every one.

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./workspace.module.css";

export default function MilestonesTab({
  clientId,
  milestones,
  workItems,
  onAdd,
  onEdit,
}) {
  const router = useRouter();
  const [busyId, setBusyId] = useState(null);

  const linkedCount = (milestoneId) =>
    workItems.filter(
      (w) => String(w.milestone_id || "") === String(milestoneId),
    ).length;

  async function send(id, { path = "", method, body }, failMessage) {
    setBusyId(id);
    try {
      const res = await fetch(
        `/api/clients/${clientId}/milestones/${id}${path}`,
        {
          method,
          headers: { "Content-Type": "application/json" },
          ...(body ? { body: JSON.stringify(body) } : {}),
        },
      );
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

  function close(id) {
    if (!confirm("Close this milestone?")) return;
    send(
      id,
      { method: "PUT", body: { status: "closed" } },
      "Failed to close milestone",
    );
  }

  function archive(id) {
    if (
      !confirm(
        "Archive this milestone? Work items will remain, but the milestone will be hidden.",
      )
    )
      return;
    send(
      id,
      { path: "/archive", method: "POST" },
      "Failed to archive milestone",
    );
  }

  return (
    <div className={styles.panel}>
      <div className={styles.sectionHead}>
        <div>
          <h2 className={styles.sectionTitle}>Milestones</h2>
          <div className={styles.sectionSubtitle}>
            Project checkpoints connected to work items.
          </div>
        </div>
        <button
          className={`${styles.btn} ${styles.btnPrimary}`}
          type="button"
          onClick={onAdd}
        >
          + Add Milestone
        </button>
      </div>

      <div className={styles.standardList}>
        {milestones.length ? (
          milestones.map((m) => (
            <div className={styles.standardCard} key={m.id}>
              <div className={styles.standardCardTop}>
                <div>
                  <div className={styles.standardCardTitle}>
                    {m.title || "Milestone"}
                  </div>
                  <div className={styles.meta}>{m.notes || ""}</div>
                </div>
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  <span className={`${styles.badge} ${styles.badgeInfo}`}>
                    {m.status || "planned"}
                  </span>
                  <span className={`${styles.badge} ${styles.badgeMuted}`}>
                    {linkedCount(m.id)} work items
                  </span>
                </div>
              </div>

              <div className={styles.standardCardMeta}>
                <div>
                  <strong>Due:</strong> {m.due_date || "-"}
                </div>
                <div>
                  <strong>Updated:</strong> {m.updatedText || "-"}
                </div>
              </div>

              <div className={styles.standardCardActions}>
                <button
                  className={styles.btn}
                  type="button"
                  onClick={() => onEdit(m.id)}
                >
                  Edit
                </button>
                <button
                  className={styles.btn}
                  type="button"
                  disabled={busyId === m.id}
                  onClick={() => close(m.id)}
                >
                  Close
                </button>
                <button
                  className={styles.btn}
                  type="button"
                  disabled={busyId === m.id}
                  onClick={() => archive(m.id)}
                >
                  Archive
                </button>
              </div>
            </div>
          ))
        ) : (
          <div className={styles.meta}>No milestones yet.</div>
        )}
      </div>
    </div>
  );
}
