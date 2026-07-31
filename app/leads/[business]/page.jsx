// GET /leads/:business — ported from lib/server/app.js lines 34703-34755.

import { cache } from "react";
import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import { getBusinessLeadsData } from "@/lib/data/leads.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderBusinessLeadsPage } from "./BusinessLeadsPage.js";
import { unstable_rethrow } from "next/navigation";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./business-leads.css";

export const dynamic = "force-dynamic";

const ALLOWED_TABS = [
  "all",
  "b2b",
  "b2c",
  "in_progress",
  "completed",
  "voice_inbox",
];

const loadLeads = cache((business, tab, search, page, filtersJson) =>
  getBusinessLeadsData(
    DASHBOARD_ORG_ID,
    business,
    tab,
    search,
    page,
    JSON.parse(filtersJson),
  ),
);

function readQuery(params, searchParams) {
  const selectedTab = ALLOWED_TABS.includes(searchParams.tab)
    ? searchParams.tab
    : "all";
  return {
    business: params.business,
    selectedTab,
    search: String(searchParams.search || "").trim(),
    page: Number(searchParams.page || 1),
    embed: searchParams.embed === "1",
    filters: {
      industry: searchParams.industry || "",
      capability: searchParams.capability || "",
      entity_type: searchParams.entity_type || "",
      status: searchParams.status || "",
      city: searchParams.city || "",
      state: searchParams.state || "",
      assigned_to: searchParams.assigned_to || "",
      qualified: searchParams.qualified || "",
      worth_talking: searchParams.worth_talking || "",
      has_call_transcription: searchParams.has_call_transcription || "",
    },
  };
}

export async function generateMetadata({ params }) {
  const { business } = await params;
  return { title: `${business} Leads | WeSolveHR` };
}

export default async function BusinessLeadsPage({ params, searchParams }) {
  await requireDashboardAuthPage();
  const q = readQuery(await params, await searchParams);

  try {
    const data = await loadLeads(
      q.business,
      q.selectedTab,
      q.search,
      q.page,
      JSON.stringify(q.filters),
    );
    data.embed = q.embed;
    return <RawHtml html={renderBusinessLeadsPage(data)} />;
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("GET /leads/:business error:", error);
    return (
      <RawHtml
        html={
          "Failed to load business leads page: " + (error.message || String(error))
        }
      />
    );
  }
}
