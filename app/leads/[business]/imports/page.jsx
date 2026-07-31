// GET /leads/:business/imports — ported from lib/server/app.js lines 39132-39230.

import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import { getBusinessCanonicalName } from "@/lib/data/leads.js";
import { getLeadImportLogsData } from "@/lib/data/lead-imports.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderLeadImportLogsPage } from "./LeadImportLogsPage.js";
import { unstable_rethrow } from "next/navigation";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./lead-imports.css";

export const metadata = { title: "Lead Import Logs" };
export const dynamic = "force-dynamic";

export default async function LeadImportLogsPage({ params, searchParams }) {
  await requireDashboardAuthPage();
  const { business: rawBusiness } = await params;
  const query = await searchParams;

  try {
    // As on /leads, the original read a session key that is never set, so the
    // org always resolved to the default one.
    const orgId = DASHBOARD_ORG_ID;
    const business = getBusinessCanonicalName(rawBusiness);

    const search = String(query.search || "").trim();
    const date = String(query.date || "").trim();
    const status = String(query.status || "").trim();
    const uploadedBy = String(query.uploaded_by || "").trim();
    const selectedImportId = query.import_id ? Number(query.import_id) : null;

    const { logs, rows } = await getLeadImportLogsData({
      orgId,
      business,
      search,
      date,
      status,
      uploadedBy,
      selectedImportId,
    });

    return (
      <RawHtml
        html={renderLeadImportLogsPage({
          business,
          logs,
          rows,
          selectedImportId,
          search,
          date,
          status,
          uploadedBy,
        })}
      />
    );
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("GET /leads/:business/imports error:", error);
    return (
      <RawHtml
        html={"Failed to load import logs: " + (error.message || String(error))}
      />
    );
  }
}
