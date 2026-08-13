"use client";

// Task-load-by-user table with the per-cell drill-down into /tasks.
//
// It owns the navigation overlay itself rather than taking a callback: a server
// component cannot hand a function to a client component, and keeping the
// behaviour here means the table can be dropped into either tab as a plain
// element.
//
// buildTaskFilterHref() is ported verbatim from goToTaskFilter(), including
// which progressBucket values each drill-down appends — those have to keep
// matching what /tasks reads. Note `blocked_on_them` deliberately does NOT set
// `assignee`; it sets `waitingOn` instead.

import { useState } from "react";
import styles from "./dashboard.module.css";

const COLUMNS = [
  "Name",
  "Role",
  "Open",
  "Blocked",
  "Not Started",
  "Overdue",
  "Blocked On Them",
  "High Priority",
  "Stale",
  "Load Score",
  "Health",
];

const OVERLAY_TITLES = {
  blocked_on_them: "Opening blocked tasks waiting on this person...",
  blocked: "Opening blocked tasks...",
  overdue: "Opening overdue tasks...",
  not_started: "Opening not started tasks...",
};

export function buildTaskFilterHref(userId, type) {
  const params = new URLSearchParams();

  if (type !== "blocked_on_them") params.set("assignee", String(userId));

  if (type === "blocked") {
    params.set("blocked", "true");
    for (const b of [
      "not_begun",
      "zero_to_fifty",
      "fifty_to_hundred",
      "complete",
      "hide_cancelled",
    ]) {
      params.append("progressBucket", b);
    }
  }

  if (type === "overdue") params.set("overdue", "true");

  if (type === "not_started") {
    params.append("progressBucket", "not_begun");
    params.append("progressBucket", "hide_cancelled");
  }

  if (type === "open" || type === "all") {
    for (const b of [
      "not_begun",
      "zero_to_fifty",
      "fifty_to_hundred",
      "hide_cancelled",
    ]) {
      params.append("progressBucket", b);
    }
  }

  if (type === "blocked_on_them") {
    params.set("waitingOn", String(userId));
    params.set("blocked", "true");
  }

  return `/tasks?${params.toString()}`;
}

export default function TaskLoadTable({ rows }) {
  const [overlay, setOverlay] = useState(null);

  function drill(userId, type) {
    setOverlay(OVERLAY_TITLES[type] || "Opening task list...");
    window.location.href = buildTaskFilterHref(userId, type);
  }

  return (
    <>
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
            {rows.length ? (
              rows.map((row) => (
                <tr className={row.rowClass} key={row.user_id}>
                  <td>
                    <span
                      className={styles.taskLink}
                      onClick={() => drill(row.user_id, "all")}
                    >
                      {row.name || "-"}
                    </span>
                  </td>
                  <td>{row.role || "-"}</td>
                  {[
                    ["open", row.open_count],
                    ["blocked", row.blocked_count],
                    ["not_started", row.not_started_count],
                    ["overdue", row.overdue_count],
                    ["blocked_on_them", row.waiting_on_them_count],
                  ].map(([type, value]) => (
                    <td key={type}>
                      <span
                        className={styles.taskLink}
                        onClick={() => drill(row.user_id, type)}
                      >
                        {value ?? 0}
                      </span>
                    </td>
                  ))}
                  <td>{row.high_priority_count ?? 0}</td>
                  <td>{row.stale_count ?? 0}</td>
                  <td>{row.load_score ?? 0}</td>
                  <td>
                    <span
                      className={`${styles.miniBadge} ${styles.healthPill} ${row.healthClass}`}
                    >
                      {row.health || "Healthy"}
                    </span>
                  </td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={11} className="empty-cell">
                  No task load data found.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <div className={`${styles.loadingOverlay} ${overlay ? styles.show : ""}`}>
        <div className={styles.loadingCard}>
          <div className={styles.loadingSpinner} />
          <div>{overlay || "Opening task list..."}</div>
        </div>
      </div>
    </>
  );
}
