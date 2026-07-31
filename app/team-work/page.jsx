// GET /team-work — ported from lib/server/app.js lines 46238-47058.

import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import { getTodayDateStringInTimeZone } from "@/lib/server/time.js";
import { loadTeamWorkData } from "@/lib/data/team-work.js";
import { getRecentTeamWorkLogs } from "@/lib/data/team-work-logs.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderTeamWorkPage } from "./TeamWorkPage.js";
import { unstable_rethrow } from "next/navigation";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./team-work.css";

export const metadata = { title: "Team Work" };
// One of the two screens that carried its own viewport meta.
export const viewport = { width: "device-width", initialScale: 1 };
export const dynamic = "force-dynamic";

export default async function TeamWorkPage({ searchParams }) {
  const user = await requireDashboardAuthPage();
  const orgId = user?.org_id || DASHBOARD_ORG_ID;
  const query = await searchParams;

  const today = getTodayDateStringInTimeZone();
  let initialDate = String(query.date || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(initialDate)) initialDate = today;

  let data;
  try {
    data = await loadTeamWorkData(orgId, initialDate);
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("/team-work load error:", error);
    data = {
      date: initialDate,
      tablesMissing: false,
      columns: [],
      members: [],
      hours: {},
    };
  }
  const logs = await getRecentTeamWorkLogs(orgId, 40);
  // Pre-serialised here, exactly as the original handler did, because the view
  // interpolates it straight into a <script> body. Escaping "<" keeps a value
  // containing "</script>" from closing the tag early.
  const bootstrap = JSON.stringify({ ...data, logs }).replace(/</g, "\\u003c");

  return <RawHtml html={renderTeamWorkPage({ initialDate, today, bootstrap })} />;
}
