// Business leads (Server Component). Replaces GET /leads/:business: authenticate,
// resolve tab/search/page/filters from the query, load the data on the server,
// and hand it to the client console. Supports the embedded (iframe) variant used
// by the client workspace Leads tab. The business param is used raw, matching the
// original handler.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import { getBusinessLeadsData } from "@/lib/services/leads.js";
import BusinessLeadsConsole from "./BusinessLeadsConsole.jsx";
import "./business-leads.css";

export const metadata = { title: "Leads | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const ALLOWED_TABS = ["all", "b2b", "b2c", "in_progress", "completed", "voice_inbox"];

export default async function BusinessLeadsPage({ params, searchParams }) {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const { business } = await params;
  const sp = await searchParams;

  const selectedTab = ALLOWED_TABS.includes(sp.tab) ? sp.tab : "all";
  const search = String(sp.search || "").trim();
  const page = Number(sp.page || 1);

  const data = await getBusinessLeadsData(
    user.org_id || DASHBOARD_ORG_ID,
    business,
    selectedTab,
    search,
    page,
    {
      industry: sp.industry || "",
      capability: sp.capability || "",
      entity_type: sp.entity_type || "",
      status: sp.status || "",
      city: sp.city || "",
      state: sp.state || "",
      assigned_to: sp.assigned_to || "",
      qualified: sp.qualified || "",
      worth_talking: sp.worth_talking || "",
      has_call_transcription: sp.has_call_transcription || "",
    },
  );

  data.embed = sp.embed === "1";

  return (
    <>
      {data.embed ? null : <TopNav active="leads" />}
      <div className={`business-leads-page${data.embed ? " embed" : ""}`}>
        <BusinessLeadsConsole data={data} />
      </div>
    </>
  );
}
