// Stored AI intelligence runs for the leads screens, extracted verbatim
// from the original monolith.

import { supabase } from "../server/supabase.js";

async function getLeadAIIntelligenceHistory({ orgId, business, limit = 20 }) {
  const { data, error } = await supabase
    .from("lead_ai_intelligence_runs")
    .select("*")
    .eq("org_id", orgId)
    .eq("business", business)
    .neq("timeframe", "cumulative")
    .order("created_at", { ascending: false })
    .limit(limit);

  if (error) throw error;
  return data || [];
}

async function getLatestLeadAIIntelligenceRun({ orgId, business, timeframe }) {
  const { data, error } = await supabase
    .from("lead_ai_intelligence_runs")
    .select("*")
    .eq("org_id", orgId)
    .eq("business", business)
    .eq("timeframe", timeframe)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  return data || null;
}

export {
  getLatestLeadAIIntelligenceRun,
  getLeadAIIntelligenceHistory,
};
