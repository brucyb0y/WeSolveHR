// Recent team-work activity log, extracted verbatim from the original monolith.

import { supabase } from "../server/supabase.js";
import { isMissingTableError } from "./team-work.js";

async function getRecentTeamWorkLogs(orgId, limit = 40) {
  const { data, error } = await supabase
    .from("team_work_logs")
    .select("*")
    .eq("org_id", orgId)
    .order("created_at", { ascending: false })
    .limit(limit);
  if (error) {
    if (isMissingTableError(error)) return [];
    console.error("getRecentTeamWorkLogs error:", error.message);
    return [];
  }
  return data || [];
}

export {
  getRecentTeamWorkLogs,
};
