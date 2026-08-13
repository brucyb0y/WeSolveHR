"use client";

// Tasks console: filter bar, results table and the detail modal.
//
// Behaviour preserved from the inline script, including two things that are
// easy to "improve" by accident:
//
//   * changing a filter does NOT refetch. Only the Apply button, the 60s poll
//     and the visibility listener call loadTasks(). Filter changes merely clear
//     the hidden waitingOn filter.
//   * the waitingOn filter arrives via the URL, is dropped as soon as any other
//     filter is touched, and its Clear button also strips it from the URL.
//
// Initial filter values come from the server as props rather than being read
// off window.location, so the first render is already correct.

import { useCallback, useEffect, useRef, useState } from "react";
import TaskDetailModal from "./TaskDetailModal";
import styles from "./tasks.module.css";

const REFRESH_MS = 60000;

const BUSINESS_OPTIONS = [
  { value: "", label: "All clients" },
  { value: "joolian", label: "Joolian" },
  { value: "wesolve", label: "WeSolve" },
  { value: "rasset", label: "Rasset" },
  { value: "navii", label: "Navii" },
  { value: "general", label: "General" },
];

const AREA_OPTIONS = [
  { value: "", label: "All areas" },
  { value: "pricing", label: "Pricing" },
  { value: "marketing", label: "Marketing" },
  { value: "prospect fu", label: "Prospect FU" },
  { value: "pm", label: "PM" },
  { value: "escalation", label: "Escalation" },
  { value: "contractors hiring", label: "Contractors Hiring" },
  { value: "product dev", label: "Product Dev" },
  { value: "pitch practice", label: "Pitch Practice" },
  { value: "b2c leads gen", label: "B2C Leads Gen" },
  { value: "b2b leads gen", label: "B2B Leads Gen" },
  { value: "website dev", label: "Website Dev" },
  { value: "competitors calling", label: "Competitors Calling" },
  { value: "prospects calling", label: "Prospects Calling" },
  { value: "research", label: "Research" },
  { value: "strategy", label: "Strategy" },
];

const STATUS_OPTIONS = [
  { value: "", label: "All active status" },
  { value: "open", label: "Open" },
  { value: "in_progress", label: "In progress" },
  { value: "blocked", label: "Blocked" },
];

const PRIORITY_OPTIONS = [
  { value: "", label: "All priority" },
  { value: "low", label: "Low" },
  { value: "medium", label: "Medium" },
  { value: "high", label: "High" },
  { value: "urgent", label: "Urgent" },
];

const PROGRESS_OPTIONS = [
  { value: "not_begun", label: "Not begun" },
  { value: "zero_to_fifty", label: "0–50% complete" },
  { value: "fifty_to_hundred", label: "50–100% complete" },
  { value: "complete", label: "100% complete" },
  { value: "hide_cancelled", label: "Hide Cancelled" },
  { value: "only_cancelled", label: "Cancelled only" },
];

const COLUMNS = [
  "ID",
  "Title",
  "Business",
  "Assignee",
  "Status",
  "Priority",
  "Deadline",
  "Blocker",
];

function isOverdueTask(task) {
  const status = String(task.status || "").toLowerCase();
  return (
    !!task.deadline &&
    status !== "done" &&
    status !== "cancelled" &&
    new Date(`${task.deadline}T23:59:59`) < new Date()
  );
}

export default function TasksConsole({ initialFilters, initialWaitingOn }) {
  const [filters, setFilters] = useState(initialFilters);
  const [waitingOn, setWaitingOn] = useState(initialWaitingOn);
  const [users, setUsers] = useState([]);
  const [rows, setRows] = useState([]);
  const [statusText, setStatusText] = useState("Loading tasks...");
  const [openTaskNo, setOpenTaskNo] = useState(null);

  // Read through refs so the poll and visibility listener never capture a stale
  // filter snapshot.
  const filtersRef = useRef(filters);
  filtersRef.current = filters;
  const waitingOnRef = useRef(waitingOn);
  waitingOnRef.current = waitingOn;

  const loadTasks = useCallback(async () => {
    const f = filtersRef.current;
    const params = new URLSearchParams();

    if (waitingOnRef.current) params.set("waitingOn", waitingOnRef.current);
    if (f.search.trim()) params.set("search", f.search.trim());
    if (f.assignee) params.set("assignee", f.assignee);
    if (f.business) params.set("business", f.business);
    if (f.area) params.set("area", f.area);
    if (f.status) params.set("status", f.status);
    if (f.priority) params.set("priority", f.priority);
    if (f.blocked) params.set("blocked", "true");
    if (f.overdue) params.set("overdue", "true");
    for (const bucket of f.progressBucket) {
      params.append("progressBucket", bucket);
    }

    setStatusText("Loading tasks...");

    try {
      const res = await fetch(`/api/tasks?${params.toString()}`);
      const json = await res.json();

      if (!json.ok) {
        setStatusText(
          `Could not load tasks: ${json.error || "unknown error"}`,
        );
        setRows([]);
        console.error("loadTasks api error:", json);
        return;
      }

      const data = json.data || [];
      setRows(data);
      setStatusText(
        data.length === 0
          ? "No tasks found"
          : `${data.length} task${data.length === 1 ? "" : "s"} shown`,
      );
    } catch (error) {
      console.error("loadTasks failed:", error);
      setStatusText("Failed to initialize tasks page");
      setRows([]);
    }
  }, []);

  // Assignee options, then the first load — same order as the original init.
  useEffect(() => {
    (async () => {
      try {
        const res = await fetch("/api/users");
        const json = await res.json();
        if (json.ok) setUsers(json.data || []);
      } catch (error) {
        console.error("loadUsers fatal error:", error);
      } finally {
        loadTasks();
      }
    })();
  }, [loadTasks]);

  useEffect(() => {
    const timer = setInterval(loadTasks, REFRESH_MS);
    const onVisible = () => {
      if (document.visibilityState === "visible") loadTasks();
    };
    document.addEventListener("visibilitychange", onVisible);

    return () => {
      clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [loadTasks]);

  // Touching any filter drops the hidden waitingOn filter, as before.
  const update = (key) => (value) => {
    setWaitingOn("");
    setFilters((f) => ({ ...f, [key]: value }));
  };

  function clearWaitingOnFilter() {
    setWaitingOn("");
    const url = new URL(window.location.href);
    url.searchParams.delete("waitingOn");
    window.history.replaceState({}, "", url.toString());
    loadTasks();
  }

  const waitingOnName =
    users.find((u) => String(u.id) === String(waitingOn))?.name ||
    "Selected user";

  return (
    <>
      <div className={`${styles.panel} ${styles.taskTablePanel}`}>
        <div className={styles.controls}>
          <input
            placeholder="Search task title or ID"
            value={filters.search}
            onChange={(e) => update("search")(e.target.value)}
          />

          <select
            value={filters.assignee}
            onChange={(e) => update("assignee")(e.target.value)}
          >
            <option value="">All assignee</option>
            {users.map((u) => (
              <option value={String(u.id)} key={u.id}>
                {u.name}
              </option>
            ))}
          </select>

          <select
            value={filters.business}
            onChange={(e) => update("business")(e.target.value)}
          >
            {BUSINESS_OPTIONS.map((o) => (
              <option value={o.value} key={o.value}>
                {o.label}
              </option>
            ))}
          </select>

          <select
            value={filters.area}
            onChange={(e) => update("area")(e.target.value)}
          >
            {AREA_OPTIONS.map((o) => (
              <option value={o.value} key={o.value}>
                {o.label}
              </option>
            ))}
          </select>

          <select
            value={filters.status}
            onChange={(e) => update("status")(e.target.value)}
          >
            {STATUS_OPTIONS.map((o) => (
              <option value={o.value} key={o.value}>
                {o.label}
              </option>
            ))}
          </select>

          <select
            value={filters.priority}
            onChange={(e) => update("priority")(e.target.value)}
          >
            {PRIORITY_OPTIONS.map((o) => (
              <option value={o.value} key={o.value}>
                {o.label}
              </option>
            ))}
          </select>

          <select
            multiple
            size={1}
            value={filters.progressBucket}
            onChange={(e) =>
              update("progressBucket")(
                Array.from(e.target.selectedOptions).map((o) => o.value),
              )
            }
          >
            {PROGRESS_OPTIONS.map((o) => (
              <option value={o.value} key={o.value}>
                {o.label}
              </option>
            ))}
          </select>

          <label>
            <input
              type="checkbox"
              checked={filters.blocked}
              onChange={(e) => update("blocked")(e.target.checked)}
            />{" "}
            Blocked only
          </label>
          <label>
            <input
              type="checkbox"
              checked={filters.overdue}
              onChange={(e) => update("overdue")(e.target.checked)}
            />{" "}
            Overdue only
          </label>
          <button onClick={loadTasks}>Apply</button>
        </div>
      </div>

      <div className={`muted ${styles.specialFilters}`}>
        {waitingOn ? (
          <>
            Filtered: waiting on <strong>{waitingOnName}</strong>{" "}
            <button type="button" onClick={clearWaitingOnFilter}>
              Clear
            </button>
          </>
        ) : null}
      </div>

      <div className={styles.panel}>
        <div className={styles.statusText}>{statusText}</div>
        <div className={styles.tableWrap}>
          <table>
            <thead>
              <tr>
                {COLUMNS.map((c) => (
                  <th key={c}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((task) => {
                const status = String(task.status || "").toLowerCase();
                const isBlocked = status === "blocked";
                const isOverdue = isOverdueTask(task);
                const taskNo = task.task_no || task.id;
                const owners = Array.isArray(task.owners) ? task.owners : [];

                const rowClass = [
                  isBlocked ? styles.taskRowBlocked : "",
                  isOverdue ? styles.taskRowOverdue : "",
                ]
                  .filter(Boolean)
                  .join(" ");

                return (
                  <tr
                    className={rowClass}
                    key={task.id ?? taskNo}
                    onClick={() => setOpenTaskNo(taskNo)}
                  >
                    <td>
                      <span
                        className="task-link"
                        onClick={(e) => {
                          e.stopPropagation();
                          setOpenTaskNo(taskNo);
                        }}
                      >
                        #{taskNo}
                      </span>
                    </td>
                    <td>{task.title || ""}</td>
                    <td>{task.business || "-"}</td>
                    <td>
                      {owners.length ? (
                        owners.map((owner) => (
                          <a
                            className={styles.ownerChipLink}
                            href={`/tasks/user/${owner.user_id || owner.id}`}
                            key={owner.user_id || owner.id}
                            onClick={(e) => e.stopPropagation()}
                          >
                            {owner.name || "Unknown"}
                          </a>
                        ))
                      ) : (
                        <span className="muted">Unassigned</span>
                      )}
                    </td>
                    <td>{task.status || ""}</td>
                    <td>{task.priority || ""}</td>
                    <td>{task.deadline || "-"}</td>
                    <td>{task.blocker_note || "-"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {openTaskNo !== null ? (
        <TaskDetailModal
          taskNo={openTaskNo}
          onClose={() => setOpenTaskNo(null)}
        />
      ) : null}
    </>
  );
}
