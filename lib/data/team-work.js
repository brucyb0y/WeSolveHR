// Team-work board data, extracted verbatim from the original monolith.

import { supabase } from "../server/supabase.js";

const TEAM_WORK_TEAMS = ["LEADS", "GTM"];

function normalizeTeamWorkTeam(value) {
  const t = String(value || "").trim().toUpperCase();
  return TEAM_WORK_TEAMS.includes(t) ? t : "LEADS";
}

function isMissingTableError(error) {
  if (!error) return false;
  const msg = String(error.message || error.code || "").toLowerCase();
  return (
    error.code === "42P01" ||
    msg.includes("does not exist") ||
    msg.includes("could not find the table") ||
    msg.includes("schema cache")
  );
}

async function loadTeamWorkData(orgId, workDate) {
  const result = {
    date: workDate,
    tablesMissing: false,
    columns: [],
    members: [],
    hours: {},
  };

  const { data: columns, error: colErr } = await supabase
    .from("team_work_columns")
    .select("id, label, sort_order")
    .eq("org_id", orgId)
    .eq("is_active", true)
    .order("sort_order", { ascending: true })
    .order("id", { ascending: true });
  if (colErr) {
    if (isMissingTableError(colErr)) {
      result.tablesMissing = true;
      return result;
    }
    throw colErr;
  }
  result.columns = (columns || []).map((c) => ({
    id: c.id,
    label: c.label || "",
  }));

  const { data: members, error: memErr } = await supabase
    .from("team_work_members")
    .select("id, name, team, responsibility, sort_order")
    .eq("org_id", orgId)
    .eq("is_active", true)
    .order("team", { ascending: true })
    .order("sort_order", { ascending: true })
    .order("id", { ascending: true });
  if (memErr) {
    if (isMissingTableError(memErr)) {
      result.tablesMissing = true;
      return result;
    }
    throw memErr;
  }
  result.members = (members || []).map((m) => ({
    id: m.id,
    name: m.name || "",
    team: normalizeTeamWorkTeam(m.team),
    responsibility: m.responsibility || "",
  }));

  const { data: hours, error: hoursErr } = await supabase
    .from("team_work_hours")
    .select("member_id, column_id, hours")
    .eq("org_id", orgId)
    .eq("work_date", workDate);
  if (hoursErr) {
    if (isMissingTableError(hoursErr)) {
      result.tablesMissing = true;
      return result;
    }
    throw hoursErr;
  }
  for (const h of hours || []) {
    result.hours[`${h.member_id}:${h.column_id}`] = Number(h.hours) || 0;
  }

  return result;
}

export {
  TEAM_WORK_TEAMS,
  isMissingTableError,
  loadTeamWorkData,
  normalizeTeamWorkTeam,
};
