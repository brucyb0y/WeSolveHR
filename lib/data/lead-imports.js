// Data for /leads/:business/imports — the query body of
// app.get("/leads/:business/imports").
//
// Filters are unchanged: status and uploaded_by narrow the log list, `date` is
// treated as an IST calendar day, and `search` matches file name or uploader on
// the logs and phone/company/website/message/status on the selected import's
// rows.

import { supabase } from "@/lib/server/app.js";

const LOG_LIMIT = 100;
const ROW_LIMIT = 500;

export async function getLeadImportLogsData({
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
    .limit(LOG_LIMIT);

  if (status) query = query.eq("status", status);
  if (uploadedBy) query = query.ilike("uploaded_by_name", `%${uploadedBy}%`);

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
      .limit(ROW_LIMIT);

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
