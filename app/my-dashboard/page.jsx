// My Dashboard (Server Component). Replaces GET /my-dashboard +
// renderMyDashboardPage(): any logged-in user; managers/admins bounce to
// /dashboard. Aggregates tasks + attendance + today's report on the server.
// No client interactivity, so it renders entirely on the server.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser, isManagerOrAdmin } from "@/lib/services/auth.js";
import { getMyDashboardData } from "@/lib/services/my-dashboard.js";
import styles from "./my-dashboard.module.css";

export const metadata = { title: "My Dashboard | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export default async function MyDashboardPage() {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  if (isManagerOrAdmin(user)) {
    redirect("/dashboard");
  }

  const data = await getMyDashboardData(user);
  const counts = data.taskData?.counts || {};
  const pendingTasks = data.taskData?.tabs?.pending || [];
  const myAttendance = data.myAttendance;
  const reportSummary =
    data.reportData?.summary_text ||
    data.reportData?.narrative ||
    "No report summary available for today.";

  const stats = [
    { label: "Pending Tasks", value: counts.pending || 0 },
    { label: "Blocked Tasks", value: counts.blocked || 0 },
    { label: "Done Today", value: counts.done_today || 0 },
    { label: "Attendance Status", value: myAttendance?.status || "-" },
  ];

  return (
    <>
      <TopNav active="dashboard" />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Personal Workspace</div>
            <h1>Welcome, {user.name || "User"}</h1>
            <div className={styles.subtitle}>
              Your tasks, attendance, and report summary in one place
            </div>
          </div>
        </div>

        <div className={styles.stats}>
          {stats.map((s) => (
            <div className={styles.statCard} key={s.label}>
              <div className={styles.statLabel}>{s.label}</div>
              <div className={styles.statValue}>{s.value}</div>
            </div>
          ))}
        </div>

        <div className={styles.grid}>
          <div className={styles.panel}>
            <h2>My Tasks</h2>
            <div className={styles.taskList}>
              {pendingTasks.length ? (
                pendingTasks.slice(0, 8).map((task) => (
                  <div className={styles.taskCard} key={task.id}>
                    <div className={styles.taskTitle}>
                      #{task.task_no || task.id} — {task.title || ""}
                    </div>
                    <div className={styles.taskMeta}>
                      Status: {task.status || "-"}
                      <br />
                      Priority: {task.priority || "-"}
                      <br />
                      Progress: {task.progress ?? 0}%
                      <br />
                      Deadline: {task.deadline || "-"}
                    </div>
                  </div>
                ))
              ) : (
                <div className="muted">No pending tasks.</div>
              )}
            </div>
          </div>

          <div className={styles.sideColumn}>
            <div className={styles.panel}>
              <h2>My Attendance</h2>
              <div className={styles.attendanceLine}>
                {`Status: ${myAttendance?.status || "-"}
Login: ${myAttendance?.login_time || "-"}
Break: ${myAttendance?.break_time || "-"}
Logout: ${myAttendance?.logout_time || "-"}
Worked: ${myAttendance?.worked_duration || "-"}`}
              </div>
            </div>

            <div className={styles.panel}>
              <h2>Today’s Report</h2>
              <div className={styles.reportBox}>{reportSummary}</div>
            </div>

            <div className={styles.panel}>
              <h2>Quick Links</h2>
              <div className={styles.attendanceLine}>
                <a href={`/tasks/user/${user.id}`} className={styles.quickLink}>
                  Open my full task workspace
                </a>
                <br />
                <a href="/attendance" className={styles.quickLink}>
                  Open attendance page
                </a>
                <br />
                <a href={`/reports?userId=${user.id}`} className={styles.quickLink}>
                  Open my reports
                </a>
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
