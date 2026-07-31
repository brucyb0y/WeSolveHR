// GET /bugs — ported from lib/server/app.js lines 43955-43970.

import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import { getStage0BugBoardData } from "@/lib/data/bugs.js";
import { escapeHtml } from "@/lib/ui/html.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderStage0BugBoardPage } from "./BugBoardPage.js";
import { unstable_rethrow } from "next/navigation";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./bugs.css";

export const metadata = { title: "Stage 0 Bug Board" };
export const dynamic = "force-dynamic";

export default async function BugBoardPage() {
  await requireDashboardAuthPage();
  try {
    const data = await getStage0BugBoardData(DASHBOARD_ORG_ID);
    return <RawHtml html={renderStage0BugBoardPage(data)} />;
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("Bug board page error:", error);
    return (
      <RawHtml
        html={`<pre>${escapeHtml(error?.stack || error?.message || String(error))}</pre>`}
      />
    );
  }
}
