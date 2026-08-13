"use client";

// Linked Tasks — the second panel on the Task tab.
//
// These are NOT client work items. They are rows from the org-wide task system
// whose free-text `business` field names this client, surfaced here so a PM can
// see and adjust them without leaving the workspace. Edits write straight back
// to that system via /api/clients/:id/linked-tasks/:taskId, which is why this
// is a separate panel with its own endpoint rather than part of the work-item
// list above it.
//
// Every control saves immediately on change — there is no Save button, matching
// the original. A failed write reverts the control so it cannot show a value
// the task system rejected.

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./workspace.module.css";

const STATUSES = ["open", "in_progress", "blocked", "done"];
const PRIORITIES = ["low", "medium", "high", "urgent"];

export default function LinkedTasksPanel({ clientId, linkedTasks }) {
  const router = useRouter();
  const [busyId, setBusyId] = useState(null);

  async function update(taskId, field, value, revert) {
    setBusyId(taskId);
    try {
      const res = await fetch(
        `/api/clients/${clientId}/linked-tasks/${taskId}`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ [field]: value }),
        },
      );
      const json = await res.json();

      if (!json.ok) {
        alert(json.error || "Failed to update task");
        revert?.();
        return;
      }

      router.refresh();
    } catch {
      alert("Failed to update task");
      revert?.();
    } finally {
      setBusyId(null);
    }
  }

  // Restores the element's previous value when the write fails.
  const onSelect = (taskId, field) => (e) => {
    const el = e.target;
    const previous = el.dataset.prev ?? el.value;
    update(taskId, field, el.value, () => {
      el.value = previous;
    });
  };

  return (
    <div className={styles.panel}>
      <h2>Linked Tasks</h2>
      <div className={styles.sectionSubtitle}>
        Tasks from the task system where this client is set as the business ·
        update status, priority &amp; progress inline
      </div>

      <div className={styles.workCardList}>
        {linkedTasks.length ? (
          linkedTasks.map((t) => (
            <div className={styles.workCard} key={t.id}>
              <div className={styles.workCardTop}>
                <div>
                  {/* Links through to the assignee's task list — only when the
                      task actually has an assignee to link to. */}
                  {t.openHref ? (
                    <a
                      className={styles.workCardTitle}
                      href={t.openHref}
                      style={{ textDecoration: "none" }}
                    >
                      #{t.taskRefNo} · {t.title || "Untitled"}
                    </a>
                  ) : (
                    <div className={styles.workCardTitle}>
                      #{t.taskRefNo} · {t.title || "Untitled"}
                    </div>
                  )}
                </div>

                <div
                  style={{
                    display: "flex",
                    gap: 8,
                    flexWrap: "wrap",
                    justifyContent: "flex-end",
                    alignItems: "center",
                  }}
                >
                  <select
                    className={styles.stageSelect}
                    title="Status"
                    disabled={busyId === t.id}
                    defaultValue={t.status || "open"}
                    onFocus={(e) => {
                      e.target.dataset.prev = e.target.value;
                    }}
                    onChange={onSelect(t.id, "status")}
                  >
                    {STATUSES.map((s) => (
                      <option value={s} key={s}>
                        {s.replace("_", " ")}
                      </option>
                    ))}
                  </select>

                  <select
                    className={styles.stageSelect}
                    title="Priority"
                    disabled={busyId === t.id}
                    defaultValue={t.priority || "medium"}
                    onFocus={(e) => {
                      e.target.dataset.prev = e.target.value;
                    }}
                    onChange={onSelect(t.id, "priority")}
                  >
                    {PRIORITIES.map((p) => (
                      <option value={p} key={p}>
                        {p}
                      </option>
                    ))}
                  </select>

                  <label
                    style={{
                      fontSize: 12,
                      whiteSpace: "nowrap",
                      display: "inline-flex",
                      alignItems: "center",
                      gap: 4,
                    }}
                    title="Show this task on the client's external dashboard"
                  >
                    <input
                      type="checkbox"
                      defaultChecked={!!t.is_client_visible}
                      disabled={busyId === t.id}
                      onChange={(e) => {
                        const el = e.target;
                        const previous = !el.checked;
                        update(t.id, "is_client_visible", el.checked, () => {
                          el.checked = previous;
                        });
                      }}
                    />{" "}
                    Client
                  </label>
                </div>
              </div>

              <div className={styles.workCardMeta}>
                <div>
                  <strong>Owner:</strong> {t.ownerName}
                </div>
                <div>
                  <strong>Area:</strong> {t.area || "-"}
                </div>
                <div>
                  <strong>Progress:</strong>{" "}
                  <input
                    type="number"
                    min="0"
                    max="100"
                    step="5"
                    defaultValue={Number(t.progress) || 0}
                    disabled={busyId === t.id}
                    style={{
                      width: 62,
                      padding: "4px 6px",
                      borderRadius: 8,
                      border: "1px solid var(--line)",
                      background: "rgba(255,255,255,0.04)",
                      color: "var(--text)",
                    }}
                    onFocus={(e) => {
                      e.target.dataset.prev = e.target.value;
                    }}
                    onChange={onSelect(t.id, "progress")}
                  />
                  %
                </div>
                <div>
                  <strong>Due:</strong> {t.deadline || "-"}
                </div>
                <div>
                  <strong>Last updated:</strong> {t.updatedText}
                </div>
              </div>
            </div>
          ))
        ) : (
          // The original rendered an empty string here, so the panel shows its
          // heading with nothing under it when no task names this client.
          <></>
        )}
      </div>
    </div>
  );
}
