// Lead import-log reads. Extracted from the GET /leads/:business/imports
// handler in the original monolith (lib/server/app.js lines 39132-39230); the
// queries and filter handling are unchanged, only the Express request/response
// plumbing is replaced by arguments and a return value.

import { supabase } from "../server/supabase.js";

async function getLeadImportLogsData({
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
      [
        `file_name.ilike.%${search}%`,
        `uploaded_by_name.ilike.%${search}%`,
      ].join(","),
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

export { getLeadImportLogsData };
