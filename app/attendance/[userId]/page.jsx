// Employee attendance detail (Server Component). Replaces GET /attendance/:userId:
// authenticate, resolve the days (1/7) + month from the query, load the overview
// on the server, and hand it to the AttendanceDetail client island (tabs + the
// red-reports lookup). Org-scoped to DASHBOARD_ORG_ID as the original was, and
// the top nav highlights "reports" (matching renderTopNav("reports")).

import { notFound, redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import {
  getEmployeeAttendanceOverview,
  getAttendanceMonthNavigation,
} from "@/lib/services/attendance.js";
import AttendanceDetail from "./AttendanceDetail.jsx";

export const metadata = { title: "Employee Attendance | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export default async function AttendanceDetailPage({ params, searchParams }) {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const { userId: rawUserId } = await params;
  const sp = await searchParams;
  const userId = Number(rawUserId);
  if (!userId) {
    notFound();
  }

  const days = Number(sp.days) === 7 ? 7 : 1;
  const monthNav = getAttendanceMonthNavigation(sp.month);

  const data = await getEmployeeAttendanceOverview(userId, DASHBOARD_ORG_ID, {
    days,
    monthNav,
  });

  return (
    <>
      <TopNav active="reports" />
      <AttendanceDetail
        employee={data.employee}
        today={data.today}
        monthly={data.monthly}
        history={data.history}
        recentAudit={data.recent_audit}
        selectedDays={days}
        monthNav={monthNav}
      />
    </>
  );
}
