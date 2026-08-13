// /reports — replaces renderReportsPage() AND renderMultiDayUserReportsPage()
// plus app.get("/reports").
//
// Both had to convert together: ?userId=N renders the multi-day view, which
// every report card links to as "Last 7 days", so converting one alone would
// have broken that link.
//
// The overview no longer triple-queries. Previously the handler ran
// getDailyNarrativeReport() server-side, then the browser fetched
// /api/reports/summary and /api/reports/cards, each of which ran that same
// query again and returned an HTML string for innerHTML. Now it runs once with
// includeUsers:true and the result renders directly. Both endpoints still
// exist and still work — nothing else depends on them, but they are unchanged.

import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import {
  DASHBOARD_ORG_ID,
  getDailyNarrativeReport,
  getMultiDayNarrativeReport,
  getReportDateString,
  formatDateOnly,
} from "@/lib/server/app.js";
import ReportsView from "./ReportsView";
import MultiDayView from "./MultiDayView";
import styles from "./reports.module.css";

export const metadata = { title: "Reports" };
export const dynamic = "force-dynamic";

// Mirrors summarizeUserMultiDayReport().
function summarizeMultiDay(dailyReports) {
  const totals = {
    totalWorkingDays: 0,
    fullDays: 0,
    partialDays: 0,
    missingDays: 0,
    leaveDays: 0,
    offDays: 0,
  };

  for (const daily of dailyReports || []) {
    const user = (daily.users || [])[0];
    if (!user) continue;

    const weight = Number(user.workDayWeight || 0);
    totals.totalWorkingDays += weight;

    if (user.reportStatus === "full") totals.fullDays += weight;
    else if (user.reportStatus === "partial") totals.partialDays += weight;
    else if (user.reportStatus === "missing") totals.missingDays += weight;
    else if (user.reportStatus === "leave") totals.leaveDays += 1;
    else if (user.reportStatus === "off") totals.offDays += 1;
  }

  return totals;
}

export default async function ReportsPage({ searchParams }) {
  const user = await requireDashboardUser();
  const sp = await searchParams;

  const userId = sp?.userId ? Number(sp.userId) : null;
  const days = sp?.days ? Number(sp.days) : 1;
  const reportDate = String(sp?.date || "").trim() || getReportDateString();

  // ---- per-user multi-day view ----
  if (userId) {
    const safeDays = Math.max(1, Number(days || 1));

    const data =
      safeDays > 1
        ? await getMultiDayNarrativeReport({
            orgId: DASHBOARD_ORG_ID,
            userId,
            days: safeDays,
            endDate: reportDate,
          })
        : {
            mode: "multi_day_user",
            userId,
            endDate: reportDate,
            days: 1,
            dailyReports: [
              await getDailyNarrativeReport({
                orgId: DASHBOARD_ORG_ID,
                reportDate,
                userId,
              }),
            ],
          };

    const dailyReports = data?.dailyReports || [];
    const viewDays = data?.days || 7;

    const firstUser =
      dailyReports?.[0]?.users?.[0] ||
      dailyReports?.find((d) => (d.users || []).length)?.users?.[0] ||
      null;

    const pageTitle = firstUser
      ? viewDays === 1
        ? `${firstUser.userName} — Today Report`
        : `${firstUser.userName} — Last ${viewDays} Days`
      : viewDays === 1
        ? "Today Report"
        : `Last ${viewDays} Days Report`;

    const dayCards = dailyReports.map((daily) => ({
      reportDate: daily.reportDate,
      reportDateText: formatDateOnly(daily.reportDate),
      user: (daily.users || [])[0] || null,
    }));

    return (
      <MultiDayView
        pageTitle={pageTitle}
        days={viewDays}
        summary={summarizeMultiDay(dailyReports)}
        dayCards={dayCards}
      />
    );
  }

  // ---- org-wide overview ----
  const data = await getDailyNarrativeReport({
    orgId: DASHBOARD_ORG_ID,
    reportDate,
    userId: null,
    includeUsers: true,
  });

  const compliance = data?.compliance || {};
  const resolvedDate = data?.reportDate || reportDate;

  // The "off" chip is titled "Sunday Off" when the report date is a Sunday.
  const isSunday = new Date(`${resolvedDate}T12:00:00`).getDay() === 0;

  return (
    <>
      <TopNav active="reports" user={user} />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Daily Reporting</div>
            <h1>WeSolveHR // Reports</h1>
            <div className={styles.subtitle}>
              Attendance-day so far. Task narratives + extra work + open/blocked
              snapshot.
            </div>
          </div>
        </div>

        <ReportsView
          users={data?.users || []}
          reportDateText={formatDateOnly(resolvedDate)}
          compliance={{
            full: compliance.full || [],
            partial: compliance.partial || [],
            missing: compliance.missing || [],
            onLeave: compliance.onLeave || [],
            off: compliance.off || [],
            offTitle: isSunday ? "Sunday Off" : "Off / not expected",
          }}
        />
      </div>
    </>
  );
}
