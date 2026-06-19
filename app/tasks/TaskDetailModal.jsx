"use client";

// Task detail modal. Ported from openTaskDetail() / renderHistoryDetail() in the
// GET /tasks handler. Fetches /api/reports/task/:taskNo (still served by the
// dispatch shim) and renders meta + change history. Mounted only while open.

import { useEffect, useState } from "react";
import styles from "./tasks.module.css";

function renderHistoryDetail(item) {
  const oldValue = item.oldValue || {};
  const newValue = item.newValue || {};

  if (item.changeType === "task_created") return "Task created";

  if (item.changeType === "status_change") {
    const note = newValue.note ? "\nNote: " + newValue.note : "";
    return (
      "Status: " + (oldValue.status || "-") + " → " + (newValue.status || "-") +
      "\nProgress: " + (oldValue.progress ?? "-") + "% → " + (newValue.progress ?? "-") + "%" +
      note
    );
  }

  if (item.changeType === "progress_change") {
    const note = newValue.note ? "\nNote: " + newValue.note : "";
    return "Progress: " + (oldValue.progress ?? 0) + "% → " + (newValue.progress ?? 0) + "%" + note;
  }

  if (item.changeType === "owner_change") {
    const oldOwners = Array.isArray(oldValue.owners) ? oldValue.owners.join(", ") : "-";
    const newOwners = Array.isArray(newValue.owners) ? newValue.owners.join(", ") : "-";
    return "Owners: " + oldOwners + " → " + newOwners;
  }

  if (item.changeType === "deadline_change") {
    return "Deadline: " + (oldValue.deadline || "-") + " → " + (newValue.deadline || "-");
  }

  if (item.fieldName === "blocker_note") {
    return [
      "Blocker: " + (newValue.blocker_note || "-"),
      newValue.note ? "Note: " + newValue.note : null,
    ]
      .filter(Boolean)
      .join("\n");
  }

  if (item.fieldName === "title") {
    return "Title: " + (oldValue.title || "-") + " → " + (newValue.title || "-");
  }

  if (item.fieldName === "detail") return "Detail updated";

  if (item.fieldName === "priority") {
    return "Priority: " + (oldValue.priority || "-") + " → " + (newValue.priority || "-");
  }

  if (item.fieldName === "business") {
    return "Business: " + (oldValue.business || "-") + " → " + (newValue.business || "-");
  }

  if (item.fieldName === "area") {
    return "Area: " + (oldValue.area || "-") + " → " + (newValue.area || "-");
  }

  if (item.fieldName) {
    return (item.fieldName || "Field") + ": " +
      JSON.stringify(oldValue) + " → " + JSON.stringify(newValue);
  }

  return JSON.stringify(newValue || {});
}

export default function TaskDetailModal({ taskNo, onClose }) {
  const [task, setTask] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  // Close on Escape, matching the original keydown handler.
  useEffect(() => {
    const onKey = (e) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError("");
    setTask(null);

    fetch("/api/reports/task/" + taskNo)
      .then((r) => r.json())
      .then((json) => {
        if (!alive) return;
        if (!json.ok) setError(json.error || "Failed to load task");
        else setTask(json.data || {});
        setLoading(false);
      })
      .catch((err) => {
        console.error("openTaskDetail error:", err);
        if (!alive) return;
        setError("Could not load task detail");
        setLoading(false);
      });

    return () => {
      alive = false;
    };
  }, [taskNo]);

  const headerTitle = task
    ? "#" + (task.taskNo || task.id) + " — " + (task.title || "Untitled")
    : "Task #" + taskNo;

  const history = task?.history || [];

  return (
    <div className={styles.modal}>
      <div className={styles.modalBackdrop} onClick={onClose}></div>
      <div className={styles.modalCard}>
        <div className={styles.modalHeader}>
          <div className={styles.modalTitle}>{headerTitle}</div>
          <button className={styles.modalClose} onClick={onClose}>
            ✕
          </button>
        </div>
        <div className={styles.modalBody}>
          {loading ? <div className="muted">Loading task details...</div> : null}
          {error ? <div className="muted">{error}</div> : null}

          {task ? (
            <>
              <div className={styles.modalMetaGrid}>
                <div className={styles.modalMetaBox}>
                  <div className={styles.modalMetaLabel}>Owners</div>
                  <div>{(task.owners || []).join(", ") || "-"}</div>
                </div>
                <div className={styles.modalMetaBox}>
                  <div className={styles.modalMetaLabel}>Status</div>
                  <div>{task.status || "-"}</div>
                </div>
                <div className={styles.modalMetaBox}>
                  <div className={styles.modalMetaLabel}>Priority</div>
                  <div>{task.priority || "-"}</div>
                </div>
                <div className={styles.modalMetaBox}>
                  <div className={styles.modalMetaLabel}>Progress</div>
                  <div>{task.progress ?? 0}%</div>
                </div>
                <div className={styles.modalMetaBox}>
                  <div className={styles.modalMetaLabel}>Deadline</div>
                  <div>{task.deadline || "-"}</div>
                </div>
                <div className={styles.modalMetaBox}>
                  <div className={styles.modalMetaLabel}>Business / Area</div>
                  <div>{(task.business || "-") + " / " + (task.area || "-")}</div>
                </div>
              </div>

              {task.detail || task.blockerNote ? (
                <div className={styles.modalSection}>
                  <h3>Details</h3>
                  {task.detail ? (
                    <div className={styles.modalMetaBox} style={{ marginBottom: 10 }}>
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
                  {history.length ? (
                    history.map((item, i) => (
                      <div className={styles.historyItem} key={i}>
                        <div className={styles.historyTop}>
                          <strong>{item.changeType || "-"}</strong>
                          <span>
                            {(item.at || "-") + " • " + (item.by || "-")}
                          </span>
                        </div>
                        <div className={styles.historyDetail}>
                          {renderHistoryDetail(item)}
                        </div>
                      </div>
                    ))
                  ) : (
                    <div className="muted">No recent history</div>
                  )}
                </div>
              </div>
            </>
          ) : null}
        </div>
      </div>
    </div>
  );
}
