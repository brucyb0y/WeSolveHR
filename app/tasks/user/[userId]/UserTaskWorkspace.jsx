"use client";

// User task workspace body. Ported from renderUserTaskWorkspacePage() in
// lib/server/app.js. Tab chips are server-navigated links (?tab=...); task/
// history cards are rendered from the server-provided data; the task-detail
// modal fetches /api/reports/task/:taskNo (dispatch shim). The original's 60s
// location.reload() + on-focus reload become router.refresh().

import { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { badgeKind } from "@/lib/utils/badge.js";
import { formatDateTime } from "@/lib/utils/datetime.js";
import {
  renderUserWorkspaceHistoryLine,
  renderTaskHistoryDetail,
} from "@/lib/utils/taskHistory.js";
import styles from "./user-task-workspace.module.css";

const TABS = [
  { key: "pending", label: "Pending" },
  { key: "blocked", label: "Blocked" },
  { key: "blocked_on_me", label: "Blocked on me" },
  { key: "done_today", label: "Done today" },
  { key: "deleted", label: "Deleted" },
  { key: "progress_updates", label: "Progress updates" },
];

const STAT_CARDS = [
  { label: "Pending", field: "pending" },
  { label: "Blocked", field: "blocked" },
  { label: "Done today", field: "done_today" },
  { label: "Deleted", field: "deleted" },
];

const BADGE_CLASS = {
  ok: "badgeOk",
  warn: "badgeWarn",
  danger: "badgeDanger",
  info: "badgeInfo",
  muted: "badgeMuted",
};

function statusBadge(status) {
  return `${styles.badge} ${styles[BADGE_CLASS[badgeKind(status)]] || styles.badgeMuted}`;
}

export default function UserTaskWorkspace({ data }) {
  const router = useRouter();
  const user = data?.user || {};
  const counts = data?.counts || {};
  const selectedTab = data?.selectedTab || "pending";
  const tabs = data?.tabs || {};
  const selectedItems = tabs[selectedTab] || [];

  const [modal, setModal] = useState(null); // null | { taskNo, status, task, error }

  const openDetail = useCallback(async (taskNo) => {
    setModal({ taskNo, status: "loading", task: null, error: "" });
    try {
      const res = await fetch("/api/reports/task/" + taskNo);
      const json = await res.json();
      if (!json.ok) {
        setModal({ taskNo, status: "error", task: null, error: json.error || "Failed to load task" });
        return;
      }
      setModal({ taskNo, status: "ready", task: json.data || {}, error: "" });
    } catch (error) {
      setModal({ taskNo, status: "error", task: null, error: error?.message || "Failed to load task" });
    }
  }, []);

  const closeDetail = useCallback(() => setModal(null), []);

  useEffect(() => {
    const refresh = () => router.refresh();
    const timer = setInterval(refresh, 60000);
    const onVisible = () => {
      if (document.visibilityState === "visible") refresh();
    };
    const onKey = (e) => {
      if (e.key === "Escape") closeDetail();
    };
    document.addEventListener("visibilitychange", onVisible);
    document.addEventListener("keydown", onKey);
    return () => {
      clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisible);
      document.removeEventListener("keydown", onKey);
    };
  }, [router, closeDetail]);

  const modalTask = modal?.task;
  const modalTitle =
    modal?.status === "ready" && modalTask
      ? `#${modalTask.taskNo || modalTask.id} — ${modalTask.title || "Untitled"}`
      : `Task #${modal?.taskNo ?? ""}`;

  return (
    <>
      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>User Task Workspace</div>
            <h1>{user?.name || "Unknown user"}</h1>
            <div className={styles.subtitle}>Focused task workspace for one user</div>
          </div>
          <a className={styles.backLink} href="/tasks">
            ← Back to Tasks
          </a>
        </div>

        <div className={styles.stats}>
          {STAT_CARDS.map((card) => (
            <div className={styles.statCard} key={card.field}>
              <div className={styles.statLabel}>{card.label}</div>
              <div className={styles.statValue}>{counts[card.field] || 0}</div>
            </div>
          ))}
        </div>

        <div className={styles.workspaceChipRow}>
          {TABS.map((tab) => (
            <a
              key={tab.key}
              href={`/tasks/user/${user.id}?tab=${tab.key}`}
              className={`${styles.workspaceChip} ${selectedTab === tab.key ? styles.active : ""}`}
            >
              {tab.label} ({counts[tab.key] || 0})
            </a>
          ))}
        </div>

        <div className={styles.workspaceList}>
          {selectedTab === "progress_updates" ? (
            selectedItems.length ? (
              selectedItems.map((item, i) => (
                <div className={styles.workspaceTaskCard} key={i}>
                  <div className={styles.workspaceTaskTop}>
                    <div>
                      <button
                        className={styles.workspaceTaskIdLink}
                        onClick={() => openDetail(item.task_no || item.task_id)}
                      >
                        Task #{item.task_no || item.task_id}
                      </button>
                    </div>
                    <div className="muted">{formatDateTime(item.created_at)}</div>
                  </div>
                  <div className={styles.workspaceTaskTitle}>
                    {renderUserWorkspaceHistoryLine(item)}
                  </div>
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
            ) : (
              <div className={styles.panel}>No progress updates found.</div>
            )
          ) : selectedItems.length ? (
            selectedItems.map((task) => (
              <div className={styles.workspaceTaskCard} key={task.id}>
                <div className={styles.workspaceTaskTop}>
                  <div>
                    <button
                      className={styles.workspaceTaskIdLink}
                      onClick={() => openDetail(task.task_no || task.id)}
                    >
                      #{task.task_no || task.id}
                    </button>
                  </div>
                  <div className={statusBadge(task.status)}>{task.status || ""}</div>
                </div>

                <div className={styles.workspaceTaskTitle}>{task.title || ""}</div>

                <div className={styles.workspaceTaskMeta}>
                  <div><strong>Business:</strong> {task.business || "-"}</div>
                  <div><strong>Area:</strong> {task.area || "-"}</div>
                  <div>
                    <strong>Owners:</strong>{" "}
                    {(task.owner_names || []).join(", ") || "-"}
                  </div>
                  <div><strong>Priority:</strong> {task.priority || "-"}</div>
                  <div><strong>Progress:</strong> {task.progress ?? 0}%</div>
                  <div><strong>Deadline:</strong> {task.deadline || "-"}</div>
                  <div><strong>Blocker:</strong> {task.blocker_note || "-"}</div>
                  <div>
                    <strong>Latest update:</strong>{" "}
                    {task.latest_update_text || "No updates yet"}
                  </div>
                  <div>
                    <strong>Updated by:</strong> {task.latest_updated_by || "-"}
                  </div>
                  <div>
                    <strong>Updated at:</strong>{" "}
                    {task.latest_update_at
                      ? formatDateTime(task.latest_update_at)
                      : "-"}
                  </div>
                </div>

                {Array.isArray(task.mini_history) && task.mini_history.length ? (
                  <div className={styles.workspaceMiniTimeline}>
                    <div className={styles.workspaceMiniTimelineTitle}>Recent flow</div>
                    {task.mini_history.map((item, i) => (
                      <div className={styles.workspaceMiniTimelineItem} key={i}>
                        <div className={styles.workspaceMiniTimelineTime}>
                          {formatDateTime(item.created_at)}
                        </div>
                        <div className={styles.workspaceMiniTimelineText}>
                          {renderUserWorkspaceHistoryLine(item)}
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
            ))
          ) : (
            <div className={styles.panel}>No items found in this tab.</div>
          )}
        </div>
      </div>

      <div
        className={`${styles.taskModal} ${modal ? styles.open : ""}`}
        onClick={(e) => {
          if (e.target === e.currentTarget) closeDetail();
        }}
      >
        <div className={styles.taskModalCard}>
          <div className={styles.taskModalHead}>
            <div style={{ fontSize: 22, fontWeight: 800 }}>{modalTitle}</div>
            <button
              type="button"
              className={styles.taskModalClose}
              onClick={closeDetail}
            >
              Close
            </button>
          </div>

          <div>
            {!modal || modal.status === "loading" ? (
              <div className="muted">Loading task details...</div>
            ) : modal.status === "error" ? (
              <div className="muted">{modal.error}</div>
            ) : modalTask ? (
              <>
                <div className={styles.modalMetaGrid}>
                  <div className={styles.modalMetaBox}>
                    <div className={styles.modalMetaLabel}>Owners</div>
                    <div>{(modalTask.owners || []).join(", ") || "-"}</div>
                  </div>
                  <div className={styles.modalMetaBox}>
                    <div className={styles.modalMetaLabel}>Status</div>
                    <div>{modalTask.status || "-"}</div>
                  </div>
                  <div className={styles.modalMetaBox}>
                    <div className={styles.modalMetaLabel}>Priority</div>
                    <div>{modalTask.priority || "-"}</div>
                  </div>
                  <div className={styles.modalMetaBox}>
                    <div className={styles.modalMetaLabel}>Progress</div>
                    <div>{modalTask.progress ?? 0}%</div>
                  </div>
                  <div className={styles.modalMetaBox}>
                    <div className={styles.modalMetaLabel}>Deadline</div>
                    <div>{modalTask.deadline || "-"}</div>
                  </div>
                  <div className={styles.modalMetaBox}>
                    <div className={styles.modalMetaLabel}>Business / Area</div>
                    <div>{(modalTask.business || "-") + " / " + (modalTask.area || "-")}</div>
                  </div>
                </div>

                {modalTask.detail || modalTask.blockerNote ? (
                  <div className={styles.modalSection}>
                    <h3>Details</h3>
                    {modalTask.detail ? (
                      <div className={styles.modalMetaBox} style={{ marginBottom: 10 }}>
                        <div className={styles.modalMetaLabel}>Detail</div>
                        <div>{modalTask.detail}</div>
                      </div>
                    ) : null}
                    {modalTask.blockerNote ? (
                      <div className={styles.modalMetaBox}>
                        <div className={styles.modalMetaLabel}>Blocker</div>
                        <div>{modalTask.blockerNote}</div>
                      </div>
                    ) : null}
                  </div>
                ) : null}

                <div className={styles.modalSection}>
                  <h3>History</h3>
                  {(modalTask.history || []).length ? (
                    modalTask.history.map((item, i) => (
                      <div className={styles.historyItem} key={i}>
                        <div className={styles.historyTop}>
                          <strong>{item.changeType || "-"}</strong>
                          <span>{(item.at || "-") + " • " + (item.by || "-")}</span>
                        </div>
                        <div className={styles.historyDetail}>
                          {renderTaskHistoryDetail(item)}
                        </div>
                      </div>
                    ))
                  ) : (
                    <div className="muted">No recent history</div>
                  )}
                </div>
              </>
            ) : null}
          </div>
        </div>
      </div>
    </>
  );
}
