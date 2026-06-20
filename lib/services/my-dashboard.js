// Personal dashboard data. Aggregates the same three subsystems the GET
// /my-dashboard handler used (task workspace, attendance, daily report). Those
// data functions still live in lib/server/app.js and are re-exported from there
// until their own slices migrate; this composition matches the original handler.

import {
  getUserTaskWorkspaceData,
  getAttendancePageData,
  getDailyNarrativeReport,
  getReportDateString,
} from "@/lib/server/app.js";

export async function getMyDashboardData(user) {
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

  const myAttendanceRows = Array.isArray(attendanceData?.rows)
    ? attendanceData.rows.filter((row) => Number(row.user_id) === Number(user.id))
    : [];

  return {
    user,
    taskData,
    myAttendance: myAttendanceRows[0] || null,
    reportData,
  };
}
