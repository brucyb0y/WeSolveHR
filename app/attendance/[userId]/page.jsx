// GET /attendance/:userId — ported from lib/server/app.js lines 37145-37207.

import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import {
  getAttendanceMonthNavigation,
  getEmployeeAttendanceOverview,
} from "@/lib/data/attendance-core.js";
import { escapeHtml } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderEmployeeAttendancePage } from "./EmployeeAttendancePage.js";
import { unstable_rethrow } from "next/navigation";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./employee-attendance.css";

export const metadata = { title: "Employee Attendance" };
export const dynamic = "force-dynamic";

// As on /dashboard, the error screen restyles <body>, so its CSS stays inline
// and only reaches the DOM when the error branch renders. Verbatim from the
// original handler (lib/server/app.js lines 37172-37192).
const ERROR_CSS = `
            body {
              font-family: Arial, sans-serif;
              background: #0f172a;
              color: white;
              padding: 40px;
            }
            .box {
              max-width: 800px;
              margin: 0 auto;
              padding: 24px;
              border-radius: 16px;
              background: rgba(255,255,255,0.06);
              border: 1px solid rgba(255,255,255,0.1);
            }
            pre {
              white-space: pre-wrap;
              word-break: break-word;
              color: #fca5a5;
            }
            a { color: #93c5fd; }
`;

export default async function EmployeeAttendancePage({ params, searchParams }) {
  await requireDashboardAuthPage();
  const { userId: rawUserId } = await params;
  const query = await searchParams;

  const userId = Number(rawUserId);
  if (!userId) return <RawHtml html="Invalid user id" />;

  try {
    const days = Number(query.days) === 7 ? 7 : 1;
    const monthNav = getAttendanceMonthNavigation(query.month);

    const data = await getEmployeeAttendanceOverview(userId, DASHBOARD_ORG_ID, {
      days,
      monthNav,
    });

    return (
      <RawHtml
        html={renderEmployeeAttendancePage({ ...data, selectedDays: days, monthNav })}
      />
    );
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("Employee attendance page error:", error);
    return (
      <RawHtml
        html={`
          <style>${ERROR_CSS}</style>
        ${renderTopNav("attendance")}
          <div class="box">
            <h1>Employee attendance failed to load</h1>
            <pre>${escapeHtml(error?.message || String(error))}</pre>
            <p><a href="/attendance">Back to attendance</a></p>
          </div>
    `}
      />
    );
  }
}
