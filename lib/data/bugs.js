// Stage 0 bug board: column definitions, badge helpers and the board query.
// Extracted verbatim from the original monolith.

import { supabase } from "../server/supabase.js";
import { formatDateTime } from "../ui/html.js";

const STAGE0_BUG_COLUMNS = [
  "Parsing",
  "Duplicate / idempotency",
  "Webhook / Twilio",
  "DB / save failure",
  "Dashboard / logs",
  "Infra / regional access",
  "Unknown",
];

function bugSeveritySortWeight(severity) {
  if (severity === "P0") return 0;
  if (severity === "P1") return 1;
  return 2;
}

function bugSeverityBadgeClass(severity) {
  if (severity === "P0") return "badge badge-danger";
  if (severity === "P1") return "badge badge-warn";
  return "badge badge-info";
}

function bugStatusBadgeClass(status) {
  if (status === "done") return "badge badge-ok";
  if (status === "blocked") return "badge badge-danger";
  if (status === "in_progress") return "badge badge-info";
  return "badge badge-warn";
}

async function getStage0BugBoardData(orgId) {
  const { data, error } = await supabase
    .from("stage0_bug_board")
    .select(
      `
      id,
      org_id,
      title,
      description,
      board_column,
      severity,
      status,
      source_message_sid,
      source_phone_number,
      source_message_text,
      created_by_user_id,
      assigned_to_user_id,
      created_at,
      updated_at,
      creator:users!stage0_bug_board_created_by_user_id_fkey(name),
      assignee:users!stage0_bug_board_assigned_to_user_id_fkey(name)
    `,
    )
    .eq("org_id", orgId)
    .order("created_at", { ascending: false });

  if (error) throw error;

  const rows = (data || []).map((row) => ({
    id: row.id,
    title: row.title || "",
    description: row.description || "",
    board_column: row.board_column || "Unknown",
    severity: row.severity || "P2",
    status: row.status || "open",
    source_message_sid: row.source_message_sid || "",
    source_phone_number: row.source_phone_number || "",
    source_message_text: row.source_message_text || "",
    created_by_name: row.creator?.name || "-",
    assigned_to_name: row.assignee?.name || "-",
    assigned_to_user_id: row.assigned_to_user_id || null,
    created_at: row.created_at,
    updated_at: row.updated_at,
    created_at_text: row.created_at ? formatDateTime(row.created_at) : "-",
    updated_at_text: row.updated_at ? formatDateTime(row.updated_at) : "-",
  }));

  const grouped = {};
  for (const column of STAGE0_BUG_COLUMNS) grouped[column] = [];

  for (const row of rows) {
    if (!grouped[row.board_column]) grouped[row.board_column] = [];
    grouped[row.board_column].push(row);
  }

  return {
    summary: {
      total: rows.length,
      p0: rows.filter((x) => x.severity === "P0").length,
      p1: rows.filter((x) => x.severity === "P1").length,
      p2: rows.filter((x) => x.severity === "P2").length,
      open: rows.filter((x) => x.status === "open").length,
      in_progress: rows.filter((x) => x.status === "in_progress").length,
      blocked: rows.filter((x) => x.status === "blocked").length,
    },
    columns: STAGE0_BUG_COLUMNS.map((name) => ({
      name,
      count: (grouped[name] || []).length,
      items: (grouped[name] || []).sort((a, b) => {
        if (
          bugSeveritySortWeight(a.severity) !==
          bugSeveritySortWeight(b.severity)
        ) {
          return (
            bugSeveritySortWeight(a.severity) -
            bugSeveritySortWeight(b.severity)
          );
        }
        return new Date(b.created_at || 0) - new Date(a.created_at || 0);
      }),
    })),
  };
}

export {
  STAGE0_BUG_COLUMNS,
  bugSeverityBadgeClass,
  bugSeveritySortWeight,
  bugStatusBadgeClass,
  getStage0BugBoardData,
};
