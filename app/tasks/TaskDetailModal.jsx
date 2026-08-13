"use client";

// Task detail dialog, fed by /api/reports/task/:taskNo.
//
// /reports has its own variant of this with a slightly different layout and a
// different renderHistoryDetail (it lacks the two generic fallbacks at the end
// of the chain below), so the two are deliberately not shared yet.

import { useEffect, useState } from "react";
import styles from "./tasks.module.css";

// Ported verbatim from the page's renderHistoryDetail(). Output is plain text
// rendered into a white-space: pre-wrap box, so the "\n"s are meaningful.
function historyDetail(item) {
  const oldValue = item.oldValue || {};
  const newValue = item.newValue || {};

  if (item.changeType === "task_created") return "Task created";

  if (item.changeType === "status_change") {
    const note = newValue.note ? `\nNote: ${newValue.note}` : "";
    return (
      `Status: ${oldValue.status || "-"} → ${newValue.status || "-"}` +
      `\nProgress: ${oldValue.progress ?? "-"}% → ${newValue.progress ?? "-"}%` +
      note
    );
  }

  if (item.changeType === "progress_change") {
    const note = newValue.note ? `\nNote: ${newValue.note}` : "";
    return `Progress: ${oldValue.progress ?? 0}% → ${newValue.progress ?? 0}%${note}`;
  }

  if (item.changeType === "owner_change") {
    const oldOwners = Array.isArray(oldValue.owners)
      ? oldValue.owners.join(", ")
      : "-";
    const newOwners = Array.isArray(newValue.owners)
      ? newValue.owners.join(", ")
      : "-";
    return `Owners: ${oldOwners} → ${newOwners}`;
  }

  if (item.changeType === "deadline_change") {
    return `Deadline: ${oldValue.deadline || "-"} → ${newValue.deadline || "-"}`;
  }

  if (item.fieldName === "blocker_note") {
    return [
      `Blocker: ${newValue.blocker_note || "-"}`,
      newValue.note ? `Note: ${newValue.note}` : null,
    ]
      .filter(Boolean)
      .join("\n");
  }

  if (item.fieldName === "title") {
    return `Title: ${oldValue.title || "-"} → ${newValue.title || "-"}`;
  }
  if (item.fieldName === "detail") return "Detail updated";
  if (item.fieldName === "priority") {
    return `Priority: ${oldValue.priority || "-"} → ${newValue.priority || "-"}`;
  }
  if (item.fieldName === "business") {
    return `Business: ${oldValue.business || "-"} → ${newValue.business || "-"}`;
  }
  if (item.fieldName === "area") {
    return `Area: ${oldValue.area || "-"} → ${newValue.area || "-"}`;
  }

  if (item.fieldName) {
    return `${item.fieldName || "Field"}: ${JSON.stringify(oldValue)} → ${JSON.stringify(newValue)}`;
  }

  return JSON.stringify(newValue || {});
}

export default function TaskDetailModal({ taskNo, onClose }) {
  const [task, setTask] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const onKeyDown = (e) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError("");
    setTask(null);

    (async () => {
      try {
        const res = await fetch(`/api/reports/task/${taskNo}`);
        const json = await res.json();
        if (cancelled) return;

        if (!json.ok) {
          setError(json.error || "Failed to load task");
          return;
        }
        setTask(json.data || {});
      } catch (err) {
        console.error("openTaskDetail error:", err);
        if (!cancelled) setError("Could not load task detail");
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [taskNo]);

  const title = task
    ? `#${task.taskNo || task.id} — ${task.title || "Untitled"}`
    : `Task #${taskNo}`;

  const meta = task
    ? [
        { label: "Owners", value: (task.owners || []).join(", ") || "-" },
        { label: "Status", value: task.status || "-" },
        { label: "Priority", value: task.priority || "-" },
        { label: "Progress", value: `${task.progress ?? 0}%` },
        { label: "Deadline", value: task.deadline || "-" },
        {
          label: "Business / Area",
          value: `${task.business || "-"} / ${task.area || "-"}`,
        },
      ]
    : [];

  return (
    <div className={`${styles.modal} ${styles.open}`}>
      <div className={styles.modalBackdrop} onClick={onClose} />
      <div className={styles.modalCard}>
        <div className={styles.modalHeader}>
          <div className={styles.modalTitle}>{title}</div>
          <button className={styles.modalClose} onClick={onClose}>
            ✕
          </button>
        </div>

        <div className={styles.modalBody}>
          {loading ? (
            <div className="muted">Loading task details...</div>
          ) : error ? (
            <div className="muted">{error}</div>
          ) : (
            <>
              <div className={styles.modalMetaGrid}>
                {meta.map((m) => (
                  <div className={styles.modalMetaBox} key={m.label}>
                    <div className={styles.modalMetaLabel}>{m.label}</div>
                    <div>{m.value}</div>
                  </div>
                ))}
              </div>

              {task.detail || task.blockerNote ? (
                <div className={styles.modalSection}>
                  <h3>Details</h3>
                  {task.detail ? (
                    <div
                      className={`${styles.modalMetaBox} ${styles.detailBox}`}
                    >
                      <div className={styles.modalMetaLabel}>Detail</div>
                      <div>{task.detail}</div>
                    </div>
                  ) : null}
                  {task.blockerNote ? (
                    <div className={styles.modalMetaBox}>
                      <div className={styles.modalMetaLabel}>Blocker</div>
                      <div>{task.blockerNote}</div>
                    </div>
                  ) : null}
                </div>
              ) : null}

              <div className={styles.modalSection}>
                <h3>History</h3>
                <div className={styles.historyList}>
                  {(task.history || []).length ? (
                    task.history.map((item, i) => (
                      <div className={styles.historyItem} key={i}>
                        <div className={styles.historyTop}>
                          <strong>{item.changeType || "-"}</strong>
                          <span>
                            {item.at || "-"} • {item.by || "-"}
                          </span>
                        </div>
                        <div className={styles.historyDetail}>
                          {historyDetail(item)}
                        </div>
                      </div>
                    ))
                  ) : (
                    <div className="muted">No recent history</div>
                  )}
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
