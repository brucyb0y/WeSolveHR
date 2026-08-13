// /my-dashboard — replaces renderMyDashboardPage() + app.get("/my-dashboard").
//
// Same guard as the original: a real session is required, and managers/admins
// are bounced to the full /dashboard.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav";
import { requireUser } from "@/lib/auth";
import {
  isManagerOrAdmin,
  getUserTaskWorkspaceData,
  getAttendancePageData,
  getDailyNarrativeReport,
  getReportDateString,
} from "@/lib/server/app.js";
import styles from "./my-dashboard.module.css";

export const metadata = { title: "My Dashboard" };
export const dynamic = "force-dynamic";

export default async function MyDashboardPage() {
  const user = await requireUser();

  if (isManagerOrAdmin(user)) redirect("/dashboard");

  const [taskData, attendanceData, reportData] = await Promise.all([
    getUserTaskWorkspaceData({
      userId: user.id,
      orgId: user.org_id,
      tab: "pending",
    }),
    getAttendancePageData(user.org_id),
    getDailyNarrativeReport({
      orgId: user.org_id,
      reportDate: getReportDateString(),
      userId: user.id,
    }),
  ]);

  const myAttendance =
    (Array.isArray(attendanceData?.rows) ? attendanceData.rows : []).find(
      (row) => Number(row.user_id) === Number(user.id),
    ) || null;

  const counts = taskData?.counts || {};
  const pendingTasks = taskData?.tabs?.pending || [];

  const reportSummary =
    reportData?.summary_text ||
    reportData?.narrative ||
    "No report summary available for today.";

  // These two blocks are white-space: pre-wrap, so the newlines below are load
  // bearing — they are what puts each field on its own line. Written as
  // explicit strings rather than JSX lines because JSX collapses whitespace.
  const attendanceText = [
    "",
    `Status: ${myAttendance?.status || "-"}`,
    `Login: ${myAttendance?.login_time || "-"}`,
    `Break: ${myAttendance?.break_time || "-"}`,
    `Logout: ${myAttendance?.logout_time || "-"}`,
    `Worked: ${myAttendance?.worked_duration || "-"}`,
    "",
  ].join("\n");

  return (
    <>
      <TopNav active="dashboard" user={user} />

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
          <div className={styles.statCard}>
            <div className={styles.statLabel}>Pending Tasks</div>
            <div className={styles.statValue}>{counts.pending || 0}</div>
          </div>
          <div className={styles.statCard}>
            <div className={styles.statLabel}>Blocked Tasks</div>
            <div className={styles.statValue}>{counts.blocked || 0}</div>
          </div>
          <div className={styles.statCard}>
            <div className={styles.statLabel}>Done Today</div>
            <div className={styles.statValue}>{counts.done_today || 0}</div>
          </div>
          <div className={styles.statCard}>
            <div className={styles.statLabel}>Attendance Status</div>
            <div className={styles.statValue}>
              {myAttendance?.status || "-"}
            </div>
          </div>
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
                      Progress: {task.progress ?? 0}%<br />
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
              <div className={styles.attendanceLine}>{attendanceText}</div>
            </div>

            <div className={styles.panel}>
              <h2>Today’s Report</h2>
              <div className={styles.reportBox}>{reportSummary}</div>
            </div>

            <div className={styles.panel}>
              <h2>Quick Links</h2>
              <div className={styles.attendanceLine}>
                {"\n"}
                <a href={`/tasks/user/${user.id}`} className={styles.quickLink}>
                  Open my full task workspace
                </a>
                <br />
                {"\n"}
                <a href="/attendance" className={styles.quickLink}>
                  Open attendance page
                </a>
                <br />
                {"\n"}
                <a href={`/reports?userId=${user.id}`} className={styles.quickLink}>
                  Open my reports
                </a>
                {"\n"}
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
