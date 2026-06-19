"use client";

// Tasks console. Ported from the GET /tasks handler in lib/server/app.js — the
// filters, URL-param init, owner drill-downs, periodic refresh, and task-detail
// modal are now React state. All data still comes from /api/users and /api/tasks
// (served by the dispatch shim); behavior is unchanged.

import { useCallback, useEffect, useRef, useState } from "react";
import styles from "./tasks.module.css";
import TaskDetailModal from "./TaskDetailModal.jsx";

const BUSINESS_OPTIONS = [
  { value: "joolian", label: "Joolian" },
  { value: "wesolve", label: "WeSolve" },
  { value: "rasset", label: "Rasset" },
  { value: "navii", label: "Navii" },
  { value: "general", label: "General" },
];

const AREA_OPTIONS = [
  "pricing", "marketing", "prospect fu", "pm", "escalation",
  "contractors hiring", "product dev", "pitch practice", "b2c leads gen",
  "b2b leads gen", "website dev", "competitors calling", "prospects calling",
  "research", "strategy",
];

const STATUS_OPTIONS = [
  { value: "open", label: "Open" },
  { value: "in_progress", label: "In progress" },
  { value: "blocked", label: "Blocked" },
];

const PRIORITY_OPTIONS = ["low", "medium", "high", "urgent"];

const PROGRESS_OPTIONS = [
  { value: "not_begun", label: "Not begun" },
  { value: "zero_to_fifty", label: "0–50% complete" },
  { value: "fifty_to_hundred", label: "50–100% complete" },
  { value: "complete", label: "100% complete" },
  { value: "hide_cancelled", label: "Hide Cancelled" },
  { value: "only_cancelled", label: "Cancelled only" },
];

const DEFAULT_BUCKETS = [
  "not_begun",
  "zero_to_fifty",
  "fifty_to_hundred",
  "hide_cancelled",
];

const BLOCKED_BUCKETS = [
  "not_begun",
  "zero_to_fifty",
  "fifty_to_hundred",
  "complete",
  "hide_cancelled",
];

const EMPTY_FILTERS = {
  search: "",
  assignee: "",
  business: "",
  area: "",
  status: "",
  priority: "",
  blocked: false,
  overdue: false,
  progressBuckets: DEFAULT_BUCKETS,
};

function titleCase(s) {
  return s.replace(/\b\w/g, (c) => c.toUpperCase());
}

function buildTaskQuery(filters, waitingOn) {
  const params = new URLSearchParams();
  if (waitingOn) params.set("waitingOn", waitingOn);
  if (filters.search) params.set("search", filters.search);
  if (filters.assignee) params.set("assignee", filters.assignee);
  if (filters.business) params.set("business", filters.business);
  if (filters.area) params.set("area", filters.area);
  if (filters.status) params.set("status", filters.status);
  if (filters.priority) params.set("priority", filters.priority);
  if (filters.blocked) params.set("blocked", "true");
  if (filters.overdue) params.set("overdue", "true");
  for (const bucket of filters.progressBuckets) {
    params.append("progressBucket", bucket);
  }
  return params.toString();
}

export default function TasksConsole() {
  const [users, setUsers] = useState([]);
  const [tasks, setTasks] = useState([]);
  const [statusText, setStatusText] = useState("Loading tasks...");
  const [filters, setFilters] = useState(EMPTY_FILTERS);
  const [waitingOn, setWaitingOn] = useState("");
  const [detailTaskNo, setDetailTaskNo] = useState(null);

  // Refs hold the latest filters/waitingOn for the timer + visibility refresh
  // (which must not capture a stale closure).
  const filtersRef = useRef(filters);
  const waitingOnRef = useRef(waitingOn);
  useEffect(() => {
    filtersRef.current = filters;
  }, [filters]);
  useEffect(() => {
    waitingOnRef.current = waitingOn;
  }, [waitingOn]);

  const loadTasks = useCallback(async (currentFilters, currentWaitingOn) => {
    setStatusText("Loading tasks...");
    try {
      const qs = buildTaskQuery(currentFilters, currentWaitingOn);
      const res = await fetch("/api/tasks?" + qs);
      const json = await res.json();

      if (!json.ok) {
        setStatusText("Could not load tasks: " + (json.error || "unknown error"));
        setTasks([]);
        console.error("loadTasks api error:", json);
        return;
      }

      const rows = json.data || [];
      setTasks(rows);
      setStatusText(
        rows.length === 0
          ? "No tasks found"
          : rows.length + " task" + (rows.length === 1 ? "" : "s") + " shown",
      );
    } catch (error) {
      console.error("loadTasks fatal error:", error);
      setStatusText("Could not load tasks");
      setTasks([]);
    }
  }, []);

  const loadUsers = useCallback(async () => {
    try {
      const res = await fetch("/api/users");
      const json = await res.json();
      if (json.ok) setUsers(json.data || []);
    } catch (error) {
      console.error("loadUsers fatal error:", error);
    }
  }, []);

  // Init: read filters from URL (mirrors applyFiltersFromUrl), then load.
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const urlBuckets = params.getAll("progressBucket");
    const blocked = params.get("blocked") === "true";

    let buckets;
    if (urlBuckets.length) buckets = urlBuckets;
    else if (blocked) buckets = BLOCKED_BUCKETS;
    else buckets = DEFAULT_BUCKETS;

    const initFilters = {
      search: params.get("search") || "",
      assignee: params.get("assignee") || "",
      business: params.get("business") || "",
      area: params.get("area") || "",
      status: params.get("status") || "",
      priority: params.get("priority") || "",
      blocked,
      overdue: params.get("overdue") === "true",
      progressBuckets: buckets,
    };
    const initWaitingOn = params.get("waitingOn") || "";

    setFilters(initFilters);
    setWaitingOn(initWaitingOn);
    loadUsers();
    loadTasks(initFilters, initWaitingOn);
  }, [loadTasks, loadUsers]);

  // Periodic refresh + refresh on tab focus, using the latest filters.
  useEffect(() => {
    const id = setInterval(() => {
      loadTasks(filtersRef.current, waitingOnRef.current);
    }, 60000);

    const onVisible = () => {
      if (document.visibilityState === "visible") {
        loadTasks(filtersRef.current, waitingOnRef.current);
      }
    };
    document.addEventListener("visibilitychange", onVisible);

    return () => {
      clearInterval(id);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [loadTasks]);

  // Changing any filter clears the (hidden) waiting-on filter, matching
  // clearHiddenWaitingOn in the original.
  function updateFilter(patch) {
    setFilters((prev) => ({ ...prev, ...patch }));
    setWaitingOn("");
  }

  function onProgressChange(e) {
    const selected = Array.from(e.target.selectedOptions).map((o) => o.value);
    updateFilter({ progressBuckets: selected });
  }

  function clearWaitingOnFilter() {
    setWaitingOn("");
    const url = new URL(window.location.href);
    url.searchParams.delete("waitingOn");
    window.history.replaceState({}, "", url.toString());
    loadTasks(filters, "");
  }

  const waitingOnName =
    users.find((u) => String(u.id) === String(waitingOn))?.name || "Selected user";

  return (
    <div className={styles.wrap}>
      <div className={styles.topbar}>
        <div>
          <div className={styles.eyebrow}>Task Operations</div>
          <h1 className={styles.title}>WeSolveHR // Tasks Console</h1>
          <div className={styles.subtitle}>
            Filter and inspect work across the team without changing backend behavior
          </div>
        </div>
      </div>

      <div className={`${styles.panel} ${styles.taskTablePanel}`}>
        <div className={styles.controls}>
          <input
            placeholder="Search task title or ID"
            value={filters.search}
            onChange={(e) => updateFilter({ search: e.target.value })}
          />

          <select
            value={filters.assignee}
            onChange={(e) => updateFilter({ assignee: e.target.value })}
          >
            <option value="">All assignees</option>
            {users.map((u) => (
              <option key={u.id} value={String(u.id)}>
                {u.name}
              </option>
            ))}
          </select>

          <select
            value={filters.business}
            onChange={(e) => updateFilter({ business: e.target.value })}
          >
            <option value="">All clients</option>
            {BUSINESS_OPTIONS.map((b) => (
              <option key={b.value} value={b.value}>
                {b.label}
              </option>
            ))}
          </select>

          <select
            value={filters.area}
            onChange={(e) => updateFilter({ area: e.target.value })}
          >
            <option value="">All areas</option>
            {AREA_OPTIONS.map((a) => (
              <option key={a} value={a}>
                {titleCase(a)}
              </option>
            ))}
          </select>

          <select
            value={filters.status}
            onChange={(e) => updateFilter({ status: e.target.value })}
          >
            <option value="">All active status</option>
            {STATUS_OPTIONS.map((s) => (
              <option key={s.value} value={s.value}>
                {s.label}
              </option>
            ))}
          </select>

          <select
            value={filters.priority}
            onChange={(e) => updateFilter({ priority: e.target.value })}
          >
            <option value="">All priority</option>
            {PRIORITY_OPTIONS.map((p) => (
              <option key={p} value={p}>
                {titleCase(p)}
              </option>
            ))}
          </select>

          <select
            multiple
            size={1}
            value={filters.progressBuckets}
            onChange={onProgressChange}
          >
            {PROGRESS_OPTIONS.map((p) => (
              <option key={p.value} value={p.value}>
                {p.label}
              </option>
            ))}
          </select>

          <label>
            <input
              type="checkbox"
              checked={filters.blocked}
              onChange={(e) => updateFilter({ blocked: e.target.checked })}
            />{" "}
            Blocked only
          </label>
          <label>
            <input
              type="checkbox"
              checked={filters.overdue}
              onChange={(e) => updateFilter({ overdue: e.target.checked })}
            />{" "}
            Overdue only
          </label>
          <button onClick={() => loadTasks(filters, waitingOn)}>Apply</button>
        </div>
      </div>

      <div className={`muted ${styles.specialFilters}`}>
        {waitingOn ? (
          <>
            Filtered: waiting on <strong>{waitingOnName}</strong>{" "}
            <button
              type="button"
              onClick={clearWaitingOnFilter}
              style={{ marginLeft: 8 }}
            >
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
                <th>ID</th>
                <th>Title</th>
                <th>Business</th>
                <th>Assignee</th>
                <th>Status</th>
                <th>Priority</th>
                <th>Deadline</th>
                <th>Blocker</th>
              </tr>
            </thead>
            <tbody>
              {tasks.map((task) => {
                const status = String(task.status || "").toLowerCase();
                const isBlocked = status === "blocked";
                const isOverdue =
                  !!task.deadline &&
                  status !== "done" &&
                  status !== "cancelled" &&
                  new Date(task.deadline + "T23:59:59") < new Date();

                const rowClass = [
                  isBlocked ? styles.rowBlocked : "",
                  isOverdue ? styles.rowOverdue : "",
                ]
                  .filter(Boolean)
                  .join(" ");

                const taskNo = task.task_no || task.id;
                const owners = Array.isArray(task.owners) ? task.owners : [];

                return (
                  <tr
                    key={task.id ?? taskNo}
                    className={rowClass || undefined}
                    onClick={() => setDetailTaskNo(taskNo)}
                  >
                    <td>
                      <span
                        className={styles.taskLink}
                        onClick={(e) => {
                          e.stopPropagation();
                          setDetailTaskNo(taskNo);
                        }}
                      >
                        #{taskNo}
                      </span>
                    </td>
                    <td>{task.title || ""}</td>
                    <td>{task.business || "-"}</td>
                    <td>
                      {owners.length ? (
                        owners.map((owner) => {
                          const id = owner.user_id || owner.id;
                          return (
                            <a
                              key={id}
                              className={styles.ownerChipLink}
                              href={"/tasks/user/" + id}
                              onClick={(e) => e.stopPropagation()}
                            >
                              {owner.name || "Unknown"}
                            </a>
                          );
                        })
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

      {detailTaskNo != null ? (
        <TaskDetailModal
          taskNo={detailTaskNo}
          onClose={() => setDetailTaskNo(null)}
        />
      ) : null}
    </div>
  );
}
