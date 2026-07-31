// Stored client report summaries and goals, extracted verbatim from the
// original monolith.

import { supabase } from "../server/supabase.js";

const CLIENT_REPORT_MAX_WEEKS = 104;

async function getLatestClientReportSummaries(orgId, clientId) {
  // `daily`/`weekly` = the most recent of each (used by the Overview + Daily
  // tab). `weeklyByDate` maps each week's Monday date → that week's stored weekly
  // summary, so every Week N tab can show its own. Enough rows to cover ~2 years
  // of weekly history plus recent dailies.
  const out = { daily: null, weekly: null, weeklyByDate: {} };
  try {
    const { data, error } = await supabase
      .from("client_report_ai_summaries")
      .select("*")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .order("created_at", { ascending: false })
      .limit(160);
    if (error) {
      console.error("getLatestClientReportSummaries error:", error.message);
      return out;
    }
    for (const row of data || []) {
      if (!out[row.period]) out[row.period] = row;
      if (row.period === "weekly" && row.summary_date) {
        const key = String(row.summary_date).slice(0, 10);
        // Rows are newest-first, so keep the first (latest) per week date.
        if (!out.weeklyByDate[key]) out.weeklyByDate[key] = row;
      }
    }
  } catch (e) {
    console.error("getLatestClientReportSummaries threw:", e.message);
  }
  return out;
}

async function getClientGoals(orgId, clientId) {
  try {
    const { data, error } = await supabase
      .from("client_goals")
      .select("*")
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .maybeSingle();
    if (error) {
      console.error("getClientGoals error:", error.message);
      return null;
    }
    return data || null;
  } catch (e) {
    console.error("getClientGoals threw:", e.message);
    return null;
  }
}

export {
  CLIENT_REPORT_MAX_WEEKS,
  getClientGoals,
  getLatestClientReportSummaries,
};
