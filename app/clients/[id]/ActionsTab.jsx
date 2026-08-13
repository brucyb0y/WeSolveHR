"use client";

// Actions Needed tab — simple client / WeSolve follow-ups with add, edit and
// archive.
//
// Archive posts to the action's archive endpoint and refreshes the server
// component rather than reloading the document.

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import styles from "./workspace.module.css";

export default function ActionsTab({ clientId, actions, onAdd, onEdit }) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const [busy, setBusy] = useState(false);

  async function archiveAction(id) {
    if (!confirm("Archive this action?")) return;

    setBusy(true);
    try {
      const res = await fetch(
        `/api/clients/${clientId}/actions/${id}/archive`,
        { method: "POST", headers: { "Content-Type": "application/json" } },
      );
      const json = await res.json();
      if (!json.ok) {
        alert(json.error || "Failed to archive action");
        return;
      }
      startTransition(() => router.refresh());
    } catch {
      alert("Failed to archive action");
    } finally {
      setBusy(false);
    }
  }

  const disabled = busy || isPending;

  return (
    <div className={styles.panel}>
      <div className={styles.panelHeadRow}>
        <div>
          <h2 className={styles.panelHeadFlush}>Actions Needed</h2>
          <div className={styles.meta}>
            Track simple client or WeSolve follow-ups.
          </div>
        </div>
        <button
          className={`${styles.btn} ${styles.btnPrimary}`}
          type="button"
          onClick={() => onAdd()}
        >
          + Add Action
        </button>
      </div>

      {actions.length ? (
        actions.map((a) => (
          <div className={styles.workCard} key={a.id}>
            <div className={styles.workCardTop}>
              <div>
                <div className={styles.workCardTitle}>{a.title}</div>
                <div className={styles.meta}>{a.notes || ""}</div>
              </div>
              <div className={styles.badgeRow}>
                <span className={`${styles.badge} ${styles.badgeInfo}`}>
                  {a.status || "Open"}
                </span>
                <span className={`${styles.badge} ${styles.badgeMuted}`}>
                  {a.priority || "Medium"}
                </span>
              </div>
            </div>

            <div className={styles.workCardMeta}>
              <div>
                <strong>Owner:</strong> {a.owner_type || "-"}
                {a.owner_name ? ` · ${a.owner_name}` : ""}
              </div>
              <div>
                <strong>Due:</strong> {a.due_date || "-"}
              </div>
              <div>
                <strong>Updated:</strong> {a.updatedAtText}
              </div>
            </div>

            <div className={styles.workCardActions}>
              <button
                className={styles.btn}
                type="button"
                onClick={() => onEdit(a.id)}
              >
                Edit
              </button>
              <button
                className={styles.btn}
                type="button"
                disabled={disabled}
                onClick={() => archiveAction(a.id)}
              >
                Archive
              </button>
            </div>
          </div>
        ))
      ) : (
        <div className={styles.meta}>No actions yet.</div>
      )}
    </div>
  );
}
