"use client";

// Task tab: work-item cards with quick status changes, archive, and the
// add/edit modal trigger.
//
// Dependency handling is the part worth preserving carefully: a work item shows
// as "blocked" (and takes the warn badge) when its dependency exists and is not
// done — the stored status is still todo/in_progress, the label is derived.
// A completed dependency flips the text to "Dependency complete".
//
// Rows arrive pre-decorated from page.jsx (owner name, dependency text, badge
// classes, overdue flag) so this file imports nothing server-only.

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import styles from "./workspace.module.css";

export default function TaskTab({
  clientId,
  workItems,
  chips,
  alertStrip,
  onAdd,
}) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [busy, setBusy] = useState(false);

  async function patchWorkItem(id, body, failMessage) {
    setBusy(true);
    try {
      const res = await fetch(`/api/client-work-items/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const json = await res.json();
      if (!json.ok) {
        alert(json.error || failMessage);
        return;
      }
      startTransition(() => router.refresh());
    } catch {
      alert(failMessage);
    } finally {
      setBusy(false);
    }
  }

  const quickUpdate = (id, status) =>
    patchWorkItem(id, { status }, "Failed to update work item");

  const archive = (id) => {
    if (!confirm("Archive this work item?")) return;
    patchWorkItem(id, { is_active: false }, "Failed to archive work item");
  };

  const disabled = busy || isPending;

  return (
    <div className={styles.panel}>
      <div className={styles.panelHeadRow}>
        <div>
          <h2 className={styles.panelHeadFlush}>Task</h2>
          <div className={styles.workSummaryChips}>
            {chips.map((c) => (
              <span className={styles.summaryChip} key={c.label}>
                {c.label} {c.value}
              </span>
            ))}
          </div>
        </div>

        <button
          className={`${styles.btn} ${styles.btnPrimary}`}
          type="button"
          onClick={onAdd}
        >
          + Add Work Item
        </button>
      </div>

      {alertStrip}

      <div className={styles.workCardList}>
        {workItems.length ? (
          workItems.map((w) => (
            <div className={styles.workCard} key={w.id}>
              <div className={styles.workCardTop}>
                <div>
                  <div className={styles.workCardTitle}>
                    {w.title || "Untitled"}
                  </div>
                  <div className={styles.meta}>
                    {w.description || "No description"}
                  </div>
                </div>

                <div className={styles.workCardBadges}>
                  {w.isOverdue ? (
                    <span className={styles.overduePill}>Overdue</span>
                  ) : null}
                  <span className={w.statusBadgeClass}>{w.statusLabel}</span>
                  <span className={w.priorityBadgeClass}>
                    {w.priority || "medium"}
                  </span>
                </div>
              </div>

              <div className={styles.workCardMeta}>
                <div>
                  <strong>Owner:</strong> {w.ownerName}
                </div>
                <div>
                  <strong>Due:</strong> {w.due_date || "-"}
                </div>
                <div>
                  <strong>Depends:</strong> {w.dependencyText}
                </div>
                <div>
                  <strong>Milestone:</strong> {w.milestoneTitle}
                </div>
                <div>
                  <strong>Last updated:</strong> {w.updatedAtText}
                </div>
              </div>

              <div className={styles.workCardActions}>
                <button
                  className={styles.btn}
                  type="button"
                  onClick={() => onAdd(w.id)}
                >
                  Open / Edit
                </button>
                <button
                  className={styles.btn}
                  type="button"
                  disabled={disabled}
                  onClick={() => quickUpdate(w.id, "in_progress")}
                >
                  Start
                </button>
                <button
                  className={styles.btn}
                  type="button"
                  disabled={disabled}
                  onClick={() => quickUpdate(w.id, "done")}
                >
                  Done
                </button>
                <button
                  className={styles.btn}
                  type="button"
                  disabled={disabled}
                  onClick={() => archive(w.id)}
                >
                  Archive
                </button>
              </div>
            </div>
          ))
        ) : (
          <div className={styles.meta}>
            No work items yet. Add the first work item for this client.
          </div>
        )}
      </div>
    </div>
  );
}
