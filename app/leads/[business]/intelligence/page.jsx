// GET /leads/:business/intelligence — ported from lib/server/app.js lines 34376-34429.

import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import { getBusinessCanonicalName, getBusinessLeadsData } from "@/lib/data/leads.js";
import {
  getLatestLeadAIIntelligenceRun,
  getLeadAIIntelligenceHistory,
} from "@/lib/data/lead-intelligence.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderBusinessLeadIntelligencePage } from "./LeadIntelligencePage.js";
import { unstable_rethrow } from "next/navigation";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./lead-intelligence.css";

export const dynamic = "force-dynamic";

const ALLOWED_TIMEFRAMES = [
  "today",
  "yesterday",
  "this_week",
  "this_month",
  "all_history",
  "cumulative",
];

export async function generateMetadata({ params }) {
  const { business } = await params;
  return { title: `${getBusinessCanonicalName(business)} Intelligence` };
}

export default async function LeadIntelligencePage({ params, searchParams }) {
  const actingUser = await requireDashboardAuthPage();
  const { business: rawBusiness } = await params;
  const query = await searchParams;

  try {
    const orgId = actingUser?.org_id || DASHBOARD_ORG_ID;
    const business = getBusinessCanonicalName(rawBusiness);

    const timeframe = ALLOWED_TIMEFRAMES.includes(query.timeframe)
      ? query.timeframe
      : "today";

    const data = await getBusinessLeadsData(orgId, business, "all", "", 1, {});

    data.timeframe = timeframe;

    data.aiRun = await getLatestLeadAIIntelligenceRun({
      orgId,
      business,
      timeframe,
    });

    data.aiHistoryRuns = await getLeadAIIntelligenceHistory({
      orgId,
      business,
      limit: 20,
    });

    return <RawHtml html={renderBusinessLeadIntelligencePage(data)} />;
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("GET /leads/:business/intelligence error:", error);
    return <RawHtml html={"Failed to load lead intelligence: " + error.message} />;
  }
}
