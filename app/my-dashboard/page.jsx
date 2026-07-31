// GET /my-dashboard — ported from lib/server/app.js lines 36859-36909.

import { redirect, unstable_rethrow } from "next/navigation";
import { requireUserLoginPage } from "@/lib/server/auth.js";
import { isManagerOrAdmin } from "@/lib/server/users.js";
import { getUserTaskWorkspaceData } from "@/lib/data/tasks.js";
import { getAttendancePageData } from "@/lib/data/attendance-core.js";
import { getDailyNarrativeReport, getReportDateString } from "@/lib/data/reports.js";
import { escapeHtml } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderMyDashboardPage } from "./MyDashboardPage.js";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./my-dashboard.css";

export const metadata = { title: "My Dashboard" };
export const dynamic = "force-dynamic";

export default async function MyDashboardPage() {
  const user = await requireUserLoginPage();

  if (isManagerOrAdmin(user)) redirect("/dashboard");

  try {
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
      ? attendanceData.rows.filter(
          (row) => Number(row.user_id) === Number(user.id),
        )
      : [];

    const myAttendance = myAttendanceRows[0] || null;

    return (
      <RawHtml
        html={renderMyDashboardPage({ user, taskData, myAttendance, reportData })}
      />
    );
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("My dashboard error:", error);
    return (
      <RawHtml
        html={`
          ${renderTopNav("dashboard")}
          <pre>${escapeHtml(error?.stack || error?.message || String(error))}</pre>
        `}
      />
    );
  }
}
