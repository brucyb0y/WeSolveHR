// /dashboard — replaces renderDashboardPage() + app.get("/dashboard").
//
// Non-managers are bounced to /my-dashboard, as before. The tab markup here was
// already correct (a real #tab-overview existed), unlike /attendance.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import {
  DASHBOARD_ORG_ID,
  getDashboardData,
  isManagerOrAdmin,
} from "@/lib/server/app.js";
import DashboardTabs from "./DashboardTabs";
import TaskLoadTable from "./TaskLoadTable";
import styles from "./dashboard.module.css";

export const metadata = { title: "Dashboard" };
export const dynamic = "force-dynamic";

const MINI_COLUMNS = ["ID", "Title", "Priority", "Status", "Deadline"];

// normalizeText(health) + spaces->dashes, mapped to this module's classes.
const HEALTH_CLASS = {
  healthy: styles.healthy,
  watch: styles.watch,
  "high-risk": styles.highRisk,
  critical: styles.critical,
};

const ROW_CLASS = {
  healthy: styles.rowHealthy,
  watch: styles.rowWatch,
  "high-risk": styles.rowHighRisk,
  critical: styles.rowCritical,
};

const healthKey = (health) =>
  String(health || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "-");

function MiniTaskTable({ rows, typeLabel }) {
  return (
    <div className={styles.tableWrap}>
      <table>
        <thead>
          <tr>
            {MINI_COLUMNS.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows?.length ? (
            rows.map((task) => (
              <tr key={task.id ?? task.task_no}>
                <td>#{task.task_no || task.id}</td>
                <td>{task.title || "-"}</td>
                <td>{task.priority || "-"}</td>
                <td>{task.status || "-"}</td>
                <td>{task.deadline || "-"}</td>
              </tr>
            ))
          ) : (
            <tr>
              <td colSpan={5} className="empty-cell">
                No {typeLabel} tasks
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

export default async function DashboardPage() {
  const user = await requireDashboardUser();

  if (user && !isManagerOrAdmin(user)) redirect("/my-dashboard");

  const data = await getDashboardData(DASHBOARD_ORG_ID);

  const summary = data?.summary || {};
  const taskGroups = data?.task_groups || {};

  const sortedUsers = [...(data?.user_task_stats || [])]
    .sort((a, b) => (b.load_score || 0) - (a.load_score || 0))
    .map((row) => {
      const key = healthKey(row.health);
      return {
        ...row,
        healthClass: HEALTH_CLASS[key] || "",
        rowClass: ROW_CLASS[key] || "",
      };
    });

  // The tone class on each card (info/danger/warn/success) was never defined in
  // the page's stylesheet, so every card already looked identical. Not carried
  // over rather than inventing colours the page has never shown.
  const summaryCards = [
    ["Open Tasks", summary.open_tasks ?? 0, "All active tasks"],
    ["Overdue", summary.overdue_tasks ?? 0, "Past deadline and still active"],
    ["Blocked", summary.blocked_tasks ?? 0, "Tasks currently blocked"],
    [
      "High Priority",
      summary.high_priority_tasks ?? 0,
      "High + urgent active tasks",
    ],
    ["Stale Tasks", summary.stale_tasks ?? 0, "No updates in 5+ days"],
    ["Team Members", summary.team_members ?? 0, "People in task dashboard"],
    ["Online Now", summary.employees_online ?? 0, "Logged in / back"],
    ["On Leave", summary.employees_on_leave ?? 0, "Planned leave today"],
    [
      "Missing Reports",
      summary.missing_reports_today ?? 0,
      "Missing report today",
    ],
    [
      "Late Today",
      (summary.late_today_approved ?? 0) + (summary.late_today_unapproved ?? 0),
      "Approved + not approved",
    ],
  ];

  const kpis = [
    ["Online now", summary.employees_online ?? 0],
    ["On break", summary.employees_on_break ?? 0],
    ["No attendance yet", summary.employees_no_attendance ?? 0],
    ["Approved late", summary.late_today_approved ?? 0],
    ["Late not approved", summary.late_today_unapproved ?? 0],
  ];

  const alerts = [
    ["Overdue tasks", summary.overdue_tasks ?? 0],
    ["Blocked tasks", summary.blocked_tasks ?? 0],
    ["Missing reports today", summary.missing_reports_today ?? 0],
    ["No attendance today", summary.employees_no_attendance ?? 0],
    ["Stale tasks (5+ days no update)", summary.stale_tasks ?? 0],
  ];

  const tabs = [
    {
      key: "overview",
      label: "Overview",
      content: (
        <>
          <div className={styles.panel}>
            <h2>Leadership snapshot</h2>
            <div className={styles.kpiInline}>
              {kpis.map(([label, value]) => (
                <div className={styles.kpiChip} key={label}>
                  {label}: {value}
                </div>
              ))}
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Task load by user</h2>
            <TaskLoadTable rows={sortedUsers} />
          </div>
        </>
      ),
    },
    {
      key: "taskload",
      label: "Task Load by User",
      content: (
        <div className={styles.panel}>
          <h2>Full task load by user</h2>
          <TaskLoadTable rows={sortedUsers} />
        </div>
      ),
    },
    {
      key: "attention",
      label: "Needs Attention",
      content: (
        <div className={styles.grid2}>
          <div className={styles.panel}>
            <h2>Immediate attention</h2>
            <div className={styles.alertList}>
              {alerts.map(([label, value]) => (
                <div className={styles.alertItem} key={label}>
                  {label}: <strong>{value}</strong>
                </div>
              ))}
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Most overloaded people</h2>
            <div className={styles.tableWrap}>
              <table>
                <thead>
                  <tr>
                    {["Name", "Open", "Overdue", "Blocked", "Score", "Health"].map(
                      (c) => (
                        <th key={c}>{c}</th>
                      ),
                    )}
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
                        <span
                          className={`${styles.miniBadge} ${styles.healthPill} ${row.healthClass}`}
                        >
                          {row.health}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      ),
    },
    {
      key: "taskviews",
      label: "Task Views",
      content: (
        <div className={styles.grid2}>
          <div className={styles.panel}>
            <h2>Top overdue tasks</h2>
            <MiniTaskTable rows={taskGroups.overdue} typeLabel="overdue" />
          </div>
          <div className={styles.panel}>
            <h2>Top blocked tasks</h2>
            <MiniTaskTable rows={taskGroups.blocked} typeLabel="blocked" />
          </div>
          <div className={styles.panel}>
            <h2>High priority tasks</h2>
            <MiniTaskTable
              rows={taskGroups.high_priority}
              typeLabel="high priority"
            />
          </div>
          <div className={styles.panel}>
            <h2>Stale tasks</h2>
            <MiniTaskTable rows={taskGroups.stale} typeLabel="stale" />
          </div>
        </div>
      ),
    },
  ];

  return (
    <>
      <TopNav active="dashboard" user={user} />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>WeSolveHR</div>
            <h1>Dashboard</h1>
            <div className={styles.subtitle}>Company-wide overview dashboard</div>
          </div>
        </div>

        <div className={styles.stats}>
          {summaryCards.map(([label, value, note]) => (
            <div className={styles.statCard} key={label}>
              <div className={styles.statLabel}>{label}</div>
              <div className={styles.statValue}>{value}</div>
              <div className={styles.statNote}>{note}</div>
            </div>
          ))}
        </div>

        <DashboardTabs tabs={tabs} />
      </div>
    </>
  );
}
