// GET /reports — ported from lib/server/app.js lines 36917-36967.
//
// Two mutually exclusive screens live on this URL. Their CSS collides, so each
// stylesheet is served from public/css/ and linked only when its view renders.

import { cache } from "react";
import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import {
  getDailyNarrativeReport,
  getMultiDayNarrativeReport,
  getReportDateString,
} from "@/lib/data/reports.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderReportsPage } from "./ReportsPage.js";
import {
  multiDayUserReportsTitle,
  renderMultiDayUserReportsPage,
} from "./MultiDayUserReportsPage.js";
import { unstable_rethrow } from "next/navigation";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";

export const dynamic = "force-dynamic";

function parseQuery(searchParams) {
  return {
    userId: searchParams.userId ? Number(searchParams.userId) : null,
    days: searchParams.days ? Number(searchParams.days) : 1,
    reportDate: String(searchParams.date || "").trim() || getReportDateString(),
  };
}

// cache() keeps generateMetadata and the body on a single fetch per request.
const loadReport = cache(async (userId, days, reportDate) => {
  if (userId) {
    const safeDays = Math.max(1, Number(days || 1));
    if (safeDays > 1) {
      return {
        view: "user",
        data: await getMultiDayNarrativeReport({
          orgId: DASHBOARD_ORG_ID,
          userId,
          days: safeDays,
          endDate: reportDate,
        }),
      };
    }
    const daily = await getDailyNarrativeReport({
      orgId: DASHBOARD_ORG_ID,
      reportDate,
      userId,
    });
    return {
      view: "user",
      data: {
        mode: "multi_day_user",
        userId,
        endDate: reportDate,
        days: 1,
        dailyReports: [daily],
      },
    };
  }

  return {
    view: "org",
    data: await getDailyNarrativeReport({
      orgId: DASHBOARD_ORG_ID,
      reportDate,
      userId: null,
      includeUsers: false,
    }),
  };
});

export async function generateMetadata({ searchParams }) {
  const q = parseQuery(await searchParams);
  if (!q.userId) return { title: "Reports" };
  const { data } = await loadReport(q.userId, q.days, q.reportDate);
  return { title: multiDayUserReportsTitle(data) };
}

export default async function ReportsPage({ searchParams }) {
  await requireDashboardAuthPage();
  const q = parseQuery(await searchParams);

  try {
    const { view, data } = await loadReport(q.userId, q.days, q.reportDate);
    const href = view === "user" ? "/css/reports-user.css" : "/css/reports.css";
    const html =
      view === "user"
        ? renderMultiDayUserReportsPage(data)
        : renderReportsPage(data);

    return (
      <>
        <link rel="stylesheet" href={href} precedence="page" />
        <RawHtml html={html} />
      </>
    );
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("Reports page error:", error);
    return <RawHtml html="Failed to load reports page" />;
  }
}
