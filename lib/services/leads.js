// Leads data service. Server-side data access for the leads pages, ported from
// the corresponding handlers in lib/server/app.js. Queries are kept identical to
// the originals so page output is unchanged.

import { supabase } from "@/lib/db/supabase.js";
import {
  getBusinessLeadsData,
  buildLeadIntelligenceMetrics,
  getLeadAIIntelligenceHistory,
  getLatestLeadAIIntelligenceRun,
} from "@/lib/server/app.js";

// Re-export the business-leads data builder (used by the leads/[business] pages).
export { getBusinessLeadsData };

// Lead intelligence page data. Mirrors the GET /leads/:business/intelligence
// handler: base business-lead rows + computed metrics + the latest AI run +
// recent AI run history.
export async function getBusinessLeadIntelligenceData({ orgId, business, timeframe }) {
  const data = await getBusinessLeadsData(orgId, business, "all", "", 1, {});
  const metrics = buildLeadIntelligenceMetrics(
    data.businessRows || [],
    data.voiceRows || [],
    timeframe,
  );
  const aiRun = await getLatestLeadAIIntelligenceRun({ orgId, business, timeframe });
  const aiHistoryRuns = await getLeadAIIntelligenceHistory({
    orgId,
    business,
    limit: 20,
  });

  return {
    business,
    timeframe,
    metrics,
    aiRun: aiRun || null,
    aiHistoryRuns: aiHistoryRuns || [],
  };
}

// Static lead businesses (ported from LEAD_BUSINESSES in lib/server/app.js). A
// client can also act as a virtual lead-business ("client:<id>") backed by the
// shared client_leads table — handled where business pages are converted.
export const LEAD_BUSINESSES = [
  { business: "rasset", label: "Rasset", table: "rasset_leads", active: true },
  { business: "joolian", label: "Joolian", table: "joolian_leads", active: true },
];

export function getActiveLeadBusinesses() {
  return LEAD_BUSINESSES.filter((x) => x.active);
}

export function getBusinessConfig(business) {
  const key = String(business || "")
    .trim()
    .toLowerCase();
  return getActiveLeadBusinesses().find((x) => x.business === key) || null;
}

// Canonical business key (folds the rasset.ai / joolian.ai aliases). Ported
// verbatim from getBusinessCanonicalName() in lib/server/app.js.
export function getBusinessCanonicalName(input) {
  const key = String(input || "")
    .trim()
    .toLowerCase();

  const aliases = {
    rasset: "rasset",
    rassetai: "rasset",
    "rasset.ai": "rasset",
    joolian: "joolian",
    joolianai: "joolian",
    "joolian.ai": "joolian",
  };

  return aliases[key] || key;
}

// Import history + (optionally) the per-row detail for one selected import.
// Mirrors the GET /leads/:business/imports handler in lib/server/app.js.
export async function getLeadImportLogs({
  orgId,
  business,
  search = "",
  date = "",
  status = "",
  uploadedBy = "",
  selectedImportId = null,
}) {
  let query = supabase
    .from("lead_import_logs")
    .select("*")
    .eq("org_id", orgId)
    .eq("business", business)
    .order("created_at", { ascending: false })
    .limit(100);

  if (status) {
    query = query.eq("status", status);
  }

  if (uploadedBy) {
    query = query.ilike("uploaded_by_name", `%${uploadedBy}%`);
  }

  if (date) {
    const nextDate = new Date(`${date}T00:00:00+05:30`);
    nextDate.setDate(nextDate.getDate() + 1);

    query = query
      .gte("created_at", `${date}T00:00:00+05:30`)
      .lt("created_at", nextDate.toISOString());
  }

  if (search) {
    query = query.or(
      [`file_name.ilike.%${search}%`, `uploaded_by_name.ilike.%${search}%`].join(
        ",",
      ),
    );
  }

  const { data: logs, error: logsError } = await query;
  if (logsError) throw logsError;

  let rows = [];

  if (selectedImportId) {
    let rowQuery = supabase
      .from("lead_import_log_rows")
      .select("*")
      .eq("org_id", orgId)
      .eq("business", business)
      .eq("import_id", selectedImportId)
      .order("row_number", { ascending: true })
      .limit(500);

    if (search) {
      rowQuery = rowQuery.or(
        [
          `phone.ilike.%${search}%`,
          `company.ilike.%${search}%`,
          `website.ilike.%${search}%`,
          `message.ilike.%${search}%`,
          `status.ilike.%${search}%`,
        ].join(","),
      );
    }

    const { data: rowData, error: rowError } = await rowQuery;
    if (rowError) throw rowError;
    rows = rowData || [];
  }

  return { logs: logs || [], rows };
}

// Leads overview: per-business counts + recent voice uploads. Mirrors
// getLeadsOverviewData() in lib/server/app.js.
export async function getLeadsOverviewData(orgId) {
  const { data: voiceRows, error: voiceError } = await supabase
    .from("lead_voice_uploads")
    .select(
      "id, business, lead_phone, sender_phone, status, media_content_type, created_at",
    )
    .eq("org_id", orgId)
    .order("created_at", { ascending: false });

  if (voiceError) throw voiceError;

  const businessTables = getActiveLeadBusinesses();
  const businesses = [];

  for (const item of businessTables) {
    let query = supabase
      .from(item.table)
      .select("id, status", { count: "exact" })
      .eq("org_id", orgId);

    if (item.table === "rasset_leads") {
      query = query.or("is_deleted.is.null,is_deleted.eq.false");
    }

    const { data, error, count } = await query;

    if (!error) {
      const rows = data || [];
      businesses.push({
        business: item.business,
        label: item.label || item.business,
        total: count || rows.length,
        leads: rows.filter(
          (x) => !["in_progress", "completed"].includes(x.status),
        ).length,
        in_progress: rows.filter((x) => x.status === "in_progress").length,
        completed: rows.filter((x) => x.status === "completed").length,
        voice_uploads: (voiceRows || []).filter((x) => x.business === item.business)
          .length,
      });
    }
  }

  return {
    summary: {
      total: businesses.reduce((sum, x) => sum + x.total, 0),
      leads: businesses.reduce((sum, x) => sum + x.leads, 0),
      in_progress: businesses.reduce((sum, x) => sum + x.in_progress, 0),
      completed: businesses.reduce((sum, x) => sum + x.completed, 0),
      voice_uploads: (voiceRows || []).length,
    },
    businesses,
    recent: (voiceRows || []).slice(0, 20),
  };
}
