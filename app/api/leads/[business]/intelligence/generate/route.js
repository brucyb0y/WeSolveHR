// POST /api/leads/:business/intelligence/generate — run AI lead intelligence
// for one timeframe and save the run.
//
// The timeframe is validated against an allow-list and falls back to "today"
// rather than erroring, matching the original.
//
// `all_history` is recorded differently on purpose: run_type
// "all_history_snapshot" with a human source_label, because the history page
// distinguishes a full snapshot from a windowed run when listing past runs.

import {
  supabase,
  DASHBOARD_ORG_ID,
  getBusinessCanonicalName,
  getBusinessLeadsData,
  generateLeadAIIntelligence,
} from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  readJsonBody,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const TIMEFRAMES = [
  "today",
  "yesterday",
  "this_week",
  "this_month",
  "all_history",
];

export const POST = withApiErrors(
  "POST /api/leads/[business]/intelligence/generate",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const p = await routeParams(ctx);
    const business = getBusinessCanonicalName(p.business);
    const orgId = orgIdForApi(user);

    const body = await readJsonBody(request);
    const timeframe = TIMEFRAMES.includes(body?.timeframe)
      ? body.timeframe
      : "today";

    try {
      const data = await getBusinessLeadsData(orgId, business, "all", "", 1, {});

      const aiSummary = await generateLeadAIIntelligence({
        business,
        timeframe,
        rows: data.businessRows || [],
      });

      const isSnapshot = timeframe === "all_history";

      const { data: savedRun, error } = await supabase
        .from("lead_ai_intelligence_runs")
        .insert([
          {
            org_id: orgId,
            business,
            timeframe,
            run_type: isSnapshot ? "all_history_snapshot" : "timeframe",
            source_label: isSnapshot
              ? "All available past transcripts snapshot"
              : timeframe,
            transcript_count: aiSummary?._meta?.transcript_count || 0,
            summary: aiSummary,
            created_by_user_id: user?.id || null,
          },
        ])
        .select()
        .maybeSingle();

      if (error) throw error;

      return apiSuccess(savedRun);
    } catch (error) {
      console.error("generate lead intelligence error:", error);
      return apiError(500, error.message || "Failed to generate intelligence");
    }
  },
);
