// Reports (Server Component). Replaces GET /reports. Three cases, matching the
// original handler: no userId → team view (client console that injects the
// summary/cards fragments); userId + days>1 → multi-day per-user view; userId +
// days=1 → single-day per-user view. Org-scoped to DASHBOARD_ORG_ID. The
// per-user view intentionally has no top nav (the original didn't render one).

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import {
  getDailyNarrativeReport,
  getMultiDayNarrativeReport,
  getReportDateString,
} from "@/lib/services/reports.js";
import ReportsConsole from "./ReportsConsole.jsx";
import MultiDayReports from "./MultiDayReports.jsx";
import "./reports.css";

export const metadata = { title: "Reports | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export default async function ReportsPage({ searchParams }) {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const sp = await searchParams;
  const userId = sp.userId ? Number(sp.userId) : null;
  const days = sp.days ? Number(sp.days) : 1;
  const reportDate = String(sp.date || "").trim() || getReportDateString();

  if (userId) {
    const safeDays = Math.max(1, Number(days || 1));
    let data;
    if (safeDays > 1) {
      data = await getMultiDayNarrativeReport({
        orgId: DASHBOARD_ORG_ID,
        userId,
        days: safeDays,
        endDate: reportDate,
      });
    } else {
      const daily = await getDailyNarrativeReport({
        orgId: DASHBOARD_ORG_ID,
        reportDate,
        userId,
      });
      data = {
        mode: "multi_day_user",
        userId,
        endDate: reportDate,
        days: 1,
        dailyReports: [daily],
      };
    }

    return (
      <div className="reports-page">
        <MultiDayReports data={data} />
      </div>
    );
  }

  // Team view: only the date display is needed on the server; the summary +
  // cards load client-side from /api/reports/*.
  return (
    <>
      <TopNav active="reports" />
      <div className="reports-page">
        <ReportsConsole reportDate={reportDate} />
      </div>
    </>
  );
}
