"use client";

// Task cards / progress-update cards for one user, plus the detail modal and
// the periodic refresh.
//
// Two notes on fidelity:
//
//   * The old page refreshed with window.location.reload() every 60s and on
//     tab focus. That is router.refresh() here, which re-runs the server
//     component and swaps in fresh data without tearing down the document. The
//     visible difference is that an open modal and the scroll position now
//     survive a refresh instead of being thrown away mid-read.
//   * Each card's mini-timeline line and the history-card titles are rendered
//     by renderUserWorkspaceHistoryLine(), a server helper. Those strings are
//     precomputed in page.jsx and arrive as `line`, so this file needs nothing
//     from lib/server/app.js.

import { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./workspace.module.css";

const REFRESH_MS = 60000;

// Ported from renderUserWorkspaceTaskHistoryDetail(). Note this variant has no
// "task_created" branch — unlike the one on /tasks — so that case falls through
// to the generic fieldName/JSON fallbacks, as before.
function historyDetail(item) {
  const oldValue = item.oldValue || {};
  const newValue = item.newValue || {};
  const note = newValue.note ? `\nNote: ${newValue.note}` : "";

  if (item.changeType === "progress_change") {
    return `Progress: ${oldValue.progress ?? 0}% → ${newValue.progress ?? 0}%${note}`;
  }
  if (item.changeType === "status_change") {
    return `Status: ${oldValue.status || "-"} → ${newValue.status || "-"}${note}`;
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
  if (item.fieldName === "blocker_note") {
    return [
      `Blocker: ${newValue.blocker_note || "-"}`,
      newValue.note ? `Note: ${newValue.note}` : null,
    ]
      .filter(Boolean)
      .join("\n");
  }
  if (item.fieldName) {
    return `${item.fieldName || "Field"}: ${JSON.stringify(oldValue)} → ${JSON.stringify(newValue)}`;
  }
  return JSON.stringify(newValue || {});
}

function TaskModal({ taskNo, onClose }) {
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
      } catch (err) {
        if (!cancelled) setError(err?.message || "Failed to load task");
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
        { label: "Progress", value: `${task.progress ?? 0}%` },
        { label: "Deadline", value: task.deadline || "-" },
        {
          label: "Business / Area",
          value: `${task.business || "-"} / ${task.area || "-"}`,
        },
      ]
    : [];

  return (
    <div
      className={`${styles.taskModal} ${styles.open}`}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className={styles.taskModalCard}>
        <div className={styles.taskModalHead}>
          <div className={styles.taskModalTitle}>
            {task
              ? `#${task.taskNo || task.id} — ${task.title || "Untitled"}`
              : `Task #${taskNo}`}
          </div>
          <button
            type="button"
            className={styles.taskModalClose}
            onClick={onClose}
          >
            Close
          </button>
        </div>

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
                  <div className={`${styles.modalMetaBox} ${styles.detailBox}`}>
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
          </>
        )}
      </div>
    </div>
  );
}

export default function WorkspaceList({ items, isProgressTab }) {
  const router = useRouter();
  const [openTaskNo, setOpenTaskNo] = useState(null);

  const refresh = useCallback(() => router.refresh(), [router]);

  useEffect(() => {
    const timer = setInterval(refresh, REFRESH_MS);
    const onVisible = () => {
      if (document.visibilityState === "visible") refresh();
    };
    const onKeyDown = (e) => {
      if (e.key === "Escape") setOpenTaskNo(null);
    };

    document.addEventListener("visibilitychange", onVisible);
    document.addEventListener("keydown", onKeyDown);

    return () => {
      clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisible);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [refresh]);

  if (!items.length) {
    return (
      <div className={styles.panel}>
        {isProgressTab
          ? "No progress updates found."
          : "No items found in this tab."}
      </div>
    );
  }

  return (
    <>
      {isProgressTab
        ? items.map((item, i) => (
            <div className={styles.workspaceTaskCard} key={item.id ?? i}>
              <div className={styles.workspaceTaskTop}>
                <div>
                  <button
                    type="button"
                    className={styles.workspaceTaskIdLink}
                    onClick={() => setOpenTaskNo(item.task_no || item.task_id)}
                  >
                    Task #{item.task_no || item.task_id}
                  </button>
                </div>
                <div className="muted">{item.created_at_text}</div>
              </div>

              <div className={styles.workspaceTaskTitle}>{item.line}</div>

              <div className={styles.workspaceTaskMeta}>
                <div>
                  <strong>Updated by:</strong> {item.changed_by_name || "-"}
                </div>
                <div>
                  <strong>Type:</strong> {item.change_type || "-"}
                </div>
                <div>
                  <strong>Field:</strong> {item.field_name || "-"}
                </div>
              </div>
            </div>
          ))
        : items.map((task) => (
            <div className={styles.workspaceTaskCard} key={task.id}>
              <div className={styles.workspaceTaskTop}>
                <div>
                  <button
                    type="button"
                    className={styles.workspaceTaskIdLink}
                    onClick={() => setOpenTaskNo(task.task_no || task.id)}
                  >
                    #{task.task_no || task.id}
                  </button>
                </div>
                <div className={task.statusBadgeClass}>{task.status || ""}</div>
              </div>

              <div className={styles.workspaceTaskTitle}>{task.title || ""}</div>

              <div className={styles.workspaceTaskMeta}>
                <div>
                  <strong>Business:</strong> {task.business || "-"}
                </div>
                <div>
                  <strong>Area:</strong> {task.area || "-"}
                </div>
                <div>
                  <strong>Owners:</strong>{" "}
                  {(task.owner_names || []).join(", ") || "-"}
                </div>
                <div>
                  <strong>Priority:</strong> {task.priority || "-"}
                </div>
                <div>
                  <strong>Progress:</strong> {task.progress ?? 0}%
                </div>
                <div>
                  <strong>Deadline:</strong> {task.deadline || "-"}
                </div>
                <div>
                  <strong>Blocker:</strong> {task.blocker_note || "-"}
                </div>
                <div>
                  <strong>Latest update:</strong>{" "}
                  {task.latest_update_text || "No updates yet"}
                </div>
                <div>
                  <strong>Updated by:</strong> {task.latest_updated_by || "-"}
                </div>
                <div>
                  <strong>Updated at:</strong> {task.latest_update_at_text}
                </div>
              </div>

              {Array.isArray(task.mini_history) && task.mini_history.length ? (
                <div className={styles.workspaceMiniTimeline}>
                  <div className={styles.workspaceMiniTimelineTitle}>
                    Recent flow
                  </div>
                  {task.mini_history.map((item, i) => (
                    <div className={styles.workspaceMiniTimelineItem} key={i}>
                      <div className={styles.workspaceMiniTimelineTime}>
                        {item.created_at_text}
                      </div>
                      <div className={styles.workspaceMiniTimelineText}>
                        {item.line}
                        {item.changed_by_name ? (
                          <span className={styles.workspaceMiniTimelineBy}>
                            by {item.changed_by_name}
                          </span>
                        ) : null}
                      </div>
                    </div>
                  ))}
                </div>
              ) : null}
            </div>
          ))}

      {openTaskNo !== null ? (
        <TaskModal taskNo={openTaskNo} onClose={() => setOpenTaskNo(null)} />
      ) : null}
    </>
  );
}
