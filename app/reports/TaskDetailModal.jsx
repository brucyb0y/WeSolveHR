"use client";

// Task detail dialog for both report views, fed by /api/reports/task/:taskNo.
//
// Takes `styles` as a prop because the two views keep separate CSS modules
// (see multiday.module.css for why); the modal class names are identical in
// both, so one component serves them.
//
// Note: the old implementation injected task.detail, task.blockerNote and the
// history detail into innerHTML unescaped. React escapes them, which closes
// that hole; the only visible difference would be for values that contain
// markup, which these fields are not meant to.

import { useEffect, useState } from "react";

// Ported from the /reports renderHistoryDetail(). Distinct from the /tasks and
// user-workspace variants: this one spells out task_created and ends with a
// plain "Updated" fallback rather than dumping JSON.
function historyDetail(item) {
  const oldValue = item.oldValue || {};
  const newValue = item.newValue || {};

  if (item.changeType === "task_created") {
    const owners =
      Array.isArray(newValue.owners) && newValue.owners.length
        ? newValue.owners.join(", ")
        : "-";
    return [
      "Created task",
      `Title: ${newValue.title || "-"}`,
      `Owners: ${owners}`,
      `Priority: ${newValue.priority || "-"}`,
      `Deadline: ${newValue.deadline || "-"}`,
      `Business / Area: ${newValue.business || "-"} / ${newValue.area || "-"}`,
    ].join("\n");
  }

  if (item.changeType === "status_change") {
    const note = newValue.note ? `\nNote: ${newValue.note}` : "";
    return (
      `Status: ${oldValue.status || "-"} → ${newValue.status || "-"}` +
      `\nProgress: ${oldValue.progress ?? "-"}% → ${newValue.progress ?? "-"}%` +
      note
    );
  }

  if (item.changeType === "progress_change") {
    return [
      `Progress: ${oldValue.progress ?? "-"}% → ${newValue.progress ?? "-"}%`,
      `Status: ${oldValue.status || "-"} → ${newValue.status || "-"}`,
      newValue.note ? `Note: ${newValue.note}` : null,
    ]
      .filter(Boolean)
      .join("\n");
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

  return "Updated";
}

export default function TaskDetailModal({ styles, taskNo, onClose }) {
  const [task, setTask] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

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
      } catch {
        if (!cancelled) setError("Failed to load task detail");
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [taskNo]);

  const meta = task
    ? [
        { label: "Owners", value: (task.owners || []).join(", ") || "-" },
        { label: "Status", value: task.status || "-" },
        { label: "Priority", value: task.priority || "-" },
        { label: "Progress", value: `${task.progress ?? "-"}%` },
        { label: "Deadline", value: task.deadline || "-" },
        {
          label: "Business / Area",
          value: `${task.business || "-"} / ${task.area || "-"}`,
        },
      ]
    : [];

  return (
    <div
      className={`${styles.modalBackdrop} ${styles.open}`}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className={styles.modalCard}>
        <div className={styles.modalHead}>
          <div>
            <div className={styles.eyebrow}>Task detail</div>
            <h2 className={styles.modalTitle}>
              {task
                ? `#${task.taskNo || task.id} — ${task.title || "Untitled"}`
                : `Task #${taskNo}`}
            </h2>
          </div>
          <button className={styles.modalClose} onClick={onClose}>
            Close
          </button>
        </div>

        <div>
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

              <div className={styles.reportSection}>
                <div className={styles.sectionTitle}>Detail</div>
                <div>
                  {task.detail ? (
                    task.detail
                  ) : (
                    <span className="muted">No detail</span>
                  )}
                </div>
              </div>

              <div className={styles.reportSection}>
                <div className={styles.sectionTitle}>Blocker</div>
                <div>
                  {task.blockerNote ? (
                    task.blockerNote
                  ) : (
                    <span className="muted">No blocker</span>
                  )}
                </div>
              </div>

              <div className={styles.reportSection}>
                <div className={styles.sectionTitle}>Recent history</div>
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
