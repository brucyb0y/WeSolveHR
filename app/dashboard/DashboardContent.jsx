"use client";

// Interactive dashboard body. Ported from renderDashboardPage() in
// lib/server/app.js — the tab switching, the task-link drill-downs, and the
// loading overlay are now React state instead of hand-written DOM scripts. The
// data object is computed on the server (getDashboardData) and passed in.

import { useState } from "react";
import { useRouter } from "next/navigation";
import styles from "./dashboard.module.css";

const TABS = [
  { key: "overview", label: "Overview" },
  { key: "taskload", label: "Task Load by User" },
  { key: "attention", label: "Needs Attention" },
  { key: "taskviews", label: "Task Views" },
];

const SUMMARY_CARDS = [
  { label: "Open Tasks", field: "open_tasks", note: "All active tasks" },
  { label: "Overdue", field: "overdue_tasks", note: "Past deadline and still active" },
  { label: "Blocked", field: "blocked_tasks", note: "Tasks currently blocked" },
  { label: "High Priority", field: "high_priority_tasks", note: "High + urgent active tasks" },
  { label: "Stale Tasks", field: "stale_tasks", note: "No updates in 5+ days" },
  { label: "Team Members", field: "team_members", note: "People in task dashboard" },
  { label: "Online Now", field: "employees_online", note: "Logged in / back" },
  { label: "On Leave", field: "employees_on_leave", note: "Planned leave today" },
  { label: "Missing Reports", field: "missing_reports_today", note: "Missing report today" },
];

function normalizeText(text) {
  return String(text || "").trim().toLowerCase();
}

const HEALTH_PILL_CLASS = {
  healthy: styles.healthy,
  watch: styles.watch,
  "high risk": styles.highRisk,
  critical: styles.critical,
};

const ROW_HEALTH_CLASS = {
  healthy: styles.rowHealthy,
  watch: styles.rowWatch,
  "high risk": styles.rowHighRisk,
  critical: styles.rowCritical,
};

export default function DashboardContent({ data }) {
  const router = useRouter();
  const [activeTab, setActiveTab] = useState("overview");
  const [loading, setLoading] = useState({ show: false, title: "Opening task list..." });

  const summary = data?.summary || {};
  const userTaskStats = data?.user_task_stats || [];
  const taskGroups = data?.task_groups || {};

  const sortedUsers = [...userTaskStats].sort(
    (a, b) => (b.load_score || 0) - (a.load_score || 0),
  );

  const lateToday =
    (summary.late_today_approved ?? 0) + (summary.late_today_unapproved ?? 0);

  function goToTaskFilter(userId, type) {
    const params = new URLSearchParams();

    if (type !== "blocked_on_them") {
      params.set("assignee", String(userId));
    }

    if (type === "blocked") {
      params.set("blocked", "true");
      params.append("progressBucket", "not_begun");
      params.append("progressBucket", "zero_to_fifty");
      params.append("progressBucket", "fifty_to_hundred");
      params.append("progressBucket", "complete");
      params.append("progressBucket", "hide_cancelled");
    }

    if (type === "overdue") {
      params.set("overdue", "true");
    }

    if (type === "not_started") {
      params.append("progressBucket", "not_begun");
      params.append("progressBucket", "hide_cancelled");
    }

    if (type === "open" || type === "all") {
      params.append("progressBucket", "not_begun");
      params.append("progressBucket", "zero_to_fifty");
      params.append("progressBucket", "fifty_to_hundred");
      params.append("progressBucket", "hide_cancelled");
    }

    if (type === "blocked_on_them") {
      params.set("waitingOn", String(userId));
      params.set("blocked", "true");
    }

    let title = "Opening task list...";
    if (type === "blocked_on_them") title = "Opening blocked tasks waiting on this person...";
    else if (type === "blocked") title = "Opening blocked tasks...";
    else if (type === "overdue") title = "Opening overdue tasks...";
    else if (type === "not_started") title = "Opening not started tasks...";

    setLoading({ show: true, title });
    router.push(`/tasks?${params.toString()}`);
  }

  function TaskLink({ userId, type, children }) {
    return (
      <span className={styles.taskLink} onClick={() => goToTaskFilter(userId, type)}>
        {children}
      </span>
    );
  }

  function HealthPill({ health }) {
    const key = normalizeText(health);
    return (
      <span className={`${styles.miniBadge} ${HEALTH_PILL_CLASS[key] || ""}`}>
        {health || "Healthy"}
      </span>
    );
  }

  function UserLoadTable() {
    if (!sortedUsers.length) {
      return (
        <tr>
          <td colSpan={11} className="empty-cell">
            No task load data found.
          </td>
        </tr>
      );
    }

    return sortedUsers.map((row) => (
      <tr key={row.user_id} className={ROW_HEALTH_CLASS[normalizeText(row.health)] || ""}>
        <td>
          <TaskLink userId={row.user_id} type="all">
            {row.name || "-"}
          </TaskLink>
        </td>
        <td>{row.role || "-"}</td>
        <td>
          <TaskLink userId={row.user_id} type="open">
            {row.open_count ?? 0}
          </TaskLink>
        </td>
        <td>
          <TaskLink userId={row.user_id} type="blocked">
            {row.blocked_count ?? 0}
          </TaskLink>
        </td>
        <td>
          <TaskLink userId={row.user_id} type="not_started">
            {row.not_started_count ?? 0}
          </TaskLink>
        </td>
        <td>
          <TaskLink userId={row.user_id} type="overdue">
            {row.overdue_count ?? 0}
          </TaskLink>
        </td>
        <td>
          <TaskLink userId={row.user_id} type="blocked_on_them">
            {row.waiting_on_them_count ?? 0}
          </TaskLink>
        </td>
        <td>{row.high_priority_count ?? 0}</td>
        <td>{row.stale_count ?? 0}</td>
        <td>{row.load_score ?? 0}</td>
        <td>
          <HealthPill health={row.health} />
        </td>
      </tr>
    ));
  }

  function MiniTaskRows({ rows, typeLabel }) {
    if (!rows?.length) {
      return (
        <tr>
          <td colSpan={5} className="empty-cell">
            No {typeLabel} tasks
          </td>
        </tr>
      );
    }
    return rows.map((task) => (
      <tr key={task.id}>
        <td>#{task.task_no || task.id}</td>
        <td>{task.title || "-"}</td>
        <td>{task.priority || "-"}</td>
        <td>{task.status || "-"}</td>
        <td>{task.deadline || "-"}</td>
      </tr>
    ));
  }

  const loadTableHead = (
    <tr>
      <th>Name</th>
      <th>Role</th>
      <th>Open</th>
      <th>Blocked</th>
      <th>Not Started</th>
      <th>Overdue</th>
      <th>Blocked On Them</th>
      <th>High Priority</th>
      <th>Stale</th>
      <th>Load Score</th>
      <th>Health</th>
    </tr>
  );

  const miniTaskHead = (
    <tr>
      <th>ID</th>
      <th>Title</th>
      <th>Priority</th>
      <th>Status</th>
      <th>Deadline</th>
    </tr>
  );

  return (
    <div className={styles.wrap}>
      <div className={styles.topbar}>
        <div>
          <div className={styles.eyebrow}>WeSolveHR</div>
          <h1 className={styles.title}>Dashboard</h1>
          <div className={styles.subtitle}>Company-wide overview dashboard</div>
        </div>
      </div>

      <div className={styles.stats}>
        {SUMMARY_CARDS.map((card) => (
          <div className={styles.statCard} key={card.field}>
            <div className={styles.statLabel}>{card.label}</div>
            <div className={styles.statValue}>{summary[card.field] ?? 0}</div>
            <div className={styles.statNote}>{card.note}</div>
          </div>
        ))}
        <div className={styles.statCard}>
          <div className={styles.statLabel}>Late Today</div>
          <div className={styles.statValue}>{lateToday}</div>
          <div className={styles.statNote}>Approved + not approved</div>
        </div>
      </div>

      <div className={styles.tabbar}>
        {TABS.map((tab) => (
          <button
            key={tab.key}
            className={`${styles.tabBtn}${activeTab === tab.key ? ` ${styles.active}` : ""}`}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeTab === "overview" ? (
        <div>
          <div className={styles.panel}>
            <h2>Leadership snapshot</h2>
            <div className={styles.kpiInline}>
              <div className={styles.kpiChip}>Online now: {summary.employees_online ?? 0}</div>
              <div className={styles.kpiChip}>On break: {summary.employees_on_break ?? 0}</div>
              <div className={styles.kpiChip}>No attendance yet: {summary.employees_no_attendance ?? 0}</div>
              <div className={styles.kpiChip}>Approved late: {summary.late_today_approved ?? 0}</div>
              <div className={styles.kpiChip}>Late not approved: {summary.late_today_unapproved ?? 0}</div>
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Task load by user</h2>
            <div className={styles.tableWrap}>
              <table>
                <thead>{loadTableHead}</thead>
                <tbody>
                  <UserLoadTable />
                </tbody>
              </table>
            </div>
          </div>
        </div>
      ) : null}

      {activeTab === "taskload" ? (
        <div className={styles.panel}>
          <h2>Full task load by user</h2>
          <div className={styles.tableWrap}>
            <table>
              <thead>{loadTableHead}</thead>
              <tbody>
                <UserLoadTable />
              </tbody>
            </table>
          </div>
        </div>
      ) : null}

      {activeTab === "attention" ? (
        <div className={styles.grid2}>
          <div className={styles.panel}>
            <h2>Immediate attention</h2>
            <div className={styles.alertList}>
              <div className={styles.alertItem}>
                Overdue tasks: <strong>{summary.overdue_tasks ?? 0}</strong>
              </div>
              <div className={styles.alertItem}>
                Blocked tasks: <strong>{summary.blocked_tasks ?? 0}</strong>
              </div>
              <div className={styles.alertItem}>
                Missing reports today: <strong>{summary.missing_reports_today ?? 0}</strong>
              </div>
              <div className={styles.alertItem}>
                No attendance today: <strong>{summary.employees_no_attendance ?? 0}</strong>
              </div>
              <div className={styles.alertItem}>
                Stale tasks (5+ days no update): <strong>{summary.stale_tasks ?? 0}</strong>
              </div>
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Most overloaded people</h2>
            <div className={styles.tableWrap}>
              <table>
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Open</th>
                    <th>Overdue</th>
                    <th>Blocked</th>
                    <th>Score</th>
                    <th>Health</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedUsers.slice(0, 8).map((row) => (
                    <tr key={row.user_id}>
                      <td>{row.name}</td>
                      <td>{row.open_count ?? 0}</td>
                      <td>{row.overdue_count ?? 0}</td>
                      <td>{row.blocked_count ?? 0}</td>
                      <td>{row.load_score ?? 0}</td>
                      <td>
                        <HealthPill health={row.health} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      ) : null}

      {activeTab === "taskviews" ? (
        <div className={styles.grid2}>
          <div className={styles.panel}>
            <h2>Top overdue tasks</h2>
            <div className={styles.tableWrap}>
              <table>
                <thead>{miniTaskHead}</thead>
                <tbody>
                  <MiniTaskRows rows={taskGroups.overdue} typeLabel="overdue" />
                </tbody>
              </table>
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Top blocked tasks</h2>
            <div className={styles.tableWrap}>
              <table>
                <thead>{miniTaskHead}</thead>
                <tbody>
                  <MiniTaskRows rows={taskGroups.blocked} typeLabel="blocked" />
                </tbody>
              </table>
            </div>
          </div>

          <div className={styles.panel}>
            <h2>High priority tasks</h2>
            <div className={styles.tableWrap}>
              <table>
                <thead>{miniTaskHead}</thead>
                <tbody>
                  <MiniTaskRows rows={taskGroups.high_priority} typeLabel="high priority" />
                </tbody>
              </table>
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Stale tasks</h2>
            <div className={styles.tableWrap}>
              <table>
                <thead>{miniTaskHead}</thead>
                <tbody>
                  <MiniTaskRows rows={taskGroups.stale} typeLabel="stale" />
                </tbody>
              </table>
            </div>
          </div>
        </div>
      ) : null}

      <div className={`${styles.loadingOverlay}${loading.show ? ` ${styles.show}` : ""}`}>
        <div className={styles.loadingCard}>
          <div className={styles.loadingSpinner}></div>
          <div>{loading.title}</div>
        </div>
      </div>
    </div>
  );
}
