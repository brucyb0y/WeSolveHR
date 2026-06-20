"use client";

// Task-detail modal shared by both report views. Replaces the inline
// openTaskDetail()/closeTaskModal()/renderHistoryDetail() scripts in
// renderReportsPage()/renderMultiDayUserReportsPage(). It registers
// window.openTaskDetail so the injected card fragments' inline
// onclick="openTaskDetail(n)" (and the multi-day task links) keep working, then
// fetches /api/reports/task/:taskNo (dispatch shim) and renders the detail.

import { useEffect, useState } from "react";

function renderHistoryDetail(item) {
  const oldValue = item.oldValue || {};
  const newValue = item.newValue || {};

  if (item.changeType === "task_created") {
    const owners =
      Array.isArray(newValue.owners) && newValue.owners.length
        ? newValue.owners.join(", ")
        : "-";
    return [
      "Created task",
      "Title: " + (newValue.title || "-"),
      "Owners: " + owners,
      "Priority: " + (newValue.priority || "-"),
      "Deadline: " + (newValue.deadline || "-"),
      "Business / Area: " +
        (newValue.business || "-") +
        " / " +
        (newValue.area || "-"),
    ].join("\n");
  }

  if (item.changeType === "status_change") {
    const note = newValue.note ? "\nNote: " + newValue.note : "";
    return (
      "Status: " +
      (oldValue.status || "-") +
      " → " +
      (newValue.status || "-") +
      "\nProgress: " +
      (oldValue.progress ?? "-") +
      "% → " +
      (newValue.progress ?? "-") +
      "%" +
      note
    );
  }

  if (item.changeType === "progress_change") {
    return [
      "Progress: " +
        (oldValue.progress ?? "-") +
        "% → " +
        (newValue.progress ?? "-") +
        "%",
      "Status: " + (oldValue.status || "-") + " → " + (newValue.status || "-"),
      newValue.note ? "Note: " + newValue.note : null,
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
    return "Owners: " + oldOwners + " → " + newOwners;
  }

  if (item.changeType === "deadline_change") {
    return (
      "Deadline: " + (oldValue.deadline || "-") + " → " + (newValue.deadline || "-")
    );
  }

  if (item.fieldName === "blocker_note") {
    return [
      "Blocker: " + (newValue.blocker_note || "-"),
      newValue.note ? "Note: " + newValue.note : null,
    ]
      .filter(Boolean)
      .join("\n");
  }

  if (item.fieldName === "title")
    return "Title: " + (oldValue.title || "-") + " → " + (newValue.title || "-");
  if (item.fieldName === "detail") return "Detail updated";
  if (item.fieldName === "priority")
    return "Priority: " + (oldValue.priority || "-") + " → " + (newValue.priority || "-");
  if (item.fieldName === "business")
    return "Business: " + (oldValue.business || "-") + " → " + (newValue.business || "-");
  if (item.fieldName === "area")
    return "Area: " + (oldValue.area || "-") + " → " + (newValue.area || "-");

  return "Updated";
}

export default function TaskModal() {
  const [open, setOpen] = useState(false);
  const [taskNo, setTaskNo] = useState(null);
  const [task, setTask] = useState(null);
  const [status, setStatus] = useState("loading"); // loading | ready | error
  const [errorMsg, setErrorMsg] = useState("");

  useEffect(() => {
    window.openTaskDetail = async (no) => {
      setTaskNo(no);
      setTask(null);
      setStatus("loading");
      setErrorMsg("");
      setOpen(true);
      try {
        const res = await fetch("/api/reports/task/" + no);
        const json = await res.json();
        if (!json.ok) {
          setStatus("error");
          setErrorMsg(json.error || "Failed to load task");
          return;
        }
        setTask(json.data || {});
        setStatus("ready");
      } catch (error) {
        setStatus("error");
        setErrorMsg("Failed to load task detail");
      }
    };
    return () => {
      delete window.openTaskDetail;
    };
  }, []);

  const title =
    status === "ready" && task
      ? `#${task.taskNo || task.id} — ${task.title || "Untitled"}`
      : `Task #${taskNo ?? ""}`;

  return (
    <div
      className={`modal-backdrop ${open ? "open" : ""}`}
      onClick={(e) => {
        if (e.target === e.currentTarget) setOpen(false);
      }}
    >
      <div className="modal-card">
        <div className="modal-head">
          <div>
            <div className="eyebrow">Task detail</div>
            <h2 className="modal-title">{title}</h2>
          </div>
          <button className="modal-close" onClick={() => setOpen(false)}>
            Close
          </button>
        </div>

        <div>
          {status === "loading" ? (
            <div className="muted">Loading task details...</div>
          ) : status === "error" ? (
            <div className="muted">{errorMsg}</div>
          ) : task ? (
            <>
              <div className="modal-meta-grid">
                <div className="modal-meta-box">
                  <div className="modal-meta-label">Owners</div>
                  <div>{(task.owners || []).join(", ") || "-"}</div>
                </div>
                <div className="modal-meta-box">
                  <div className="modal-meta-label">Status</div>
                  <div>{task.status || "-"}</div>
                </div>
                <div className="modal-meta-box">
                  <div className="modal-meta-label">Priority</div>
                  <div>{task.priority || "-"}</div>
                </div>
                <div className="modal-meta-box">
                  <div className="modal-meta-label">Progress</div>
                  <div>{task.progress ?? "-"}%</div>
                </div>
                <div className="modal-meta-box">
                  <div className="modal-meta-label">Deadline</div>
                  <div>{task.deadline || "-"}</div>
                </div>
                <div className="modal-meta-box">
                  <div className="modal-meta-label">Business / Area</div>
                  <div>
                    {(task.business || "-") + " / " + (task.area || "-")}
                  </div>
                </div>
              </div>

              <div className="report-section">
                <div className="section-title">Detail</div>
                <div>
                  {task.detail ? task.detail : <span className="muted">No detail</span>}
                </div>
              </div>

              <div className="report-section">
                <div className="section-title">Blocker</div>
                <div>
                  {task.blockerNote ? (
                    task.blockerNote
                  ) : (
                    <span className="muted">No blocker</span>
                  )}
                </div>
              </div>

              <div className="report-section">
                <div className="section-title">Recent history</div>
                <div className="history-list">
                  {(task.history || []).length ? (
                    task.history.map((item) => (
                      <div className="history-item" key={item.id}>
                        <div className="history-top">
                          <strong>{item.changeType || "-"}</strong>
                          <span>
                            {(item.at || "-") + " • " + (item.by || "-")}
                          </span>
                        </div>
                        <div className="history-detail">
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
