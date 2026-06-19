// Leads data service. Server-side data access for the leads pages, ported from
// the corresponding handlers in lib/server/app.js. Queries are kept identical to
// the originals so page output is unchanged.

import { supabase } from "@/lib/db/supabase.js";

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
