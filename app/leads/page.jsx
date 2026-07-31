// GET /leads — ported from lib/server/app.js lines 34365-34374.

import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import { getLeadsOverviewData } from "@/lib/data/leads.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderLeadsOverviewPage } from "./LeadsOverviewPage.js";
import { unstable_rethrow } from "next/navigation";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./leads.css";

export const metadata = { title: "Leads | WeSolveHR" };
export const dynamic = "force-dynamic";

export default async function LeadsPage() {
  await requireDashboardAuthPage();
  try {
    // The original read req.session?.user?.org_id, a key the session never
    // holds (login stores userId), so this always resolved to the default org.
    const data = await getLeadsOverviewData(DASHBOARD_ORG_ID);
    return <RawHtml html={renderLeadsOverviewPage(data)} />;
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("GET /leads error:", error);
    return <RawHtml html="Failed to load leads page" />;
  }
}
