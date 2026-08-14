// POST /api/leads/:business/intelligence/generate-cumulative — synthesise a
// cumulative view from the last 20 saved intelligence runs.
//
// It summarises PRIOR RUNS, not transcripts, which is why transcript_count is
// stored as 0 — nothing was transcribed for this run and claiming otherwise
// would double-count on the history page.

import {
  supabase,
  getBusinessCanonicalName,
  getLeadAIIntelligenceHistory,
  generateCumulativeLeadAIIntelligence,
} from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const HISTORY_LIMIT = 20;

export const POST = withApiErrors(
  "POST /api/leads/[business]/intelligence/generate-cumulative",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const p = await routeParams(ctx);
    const business = getBusinessCanonicalName(p.business);
    const orgId = orgIdForApi(user);

    try {
      const runs = await getLeadAIIntelligenceHistory({
        orgId,
        business,
        limit: HISTORY_LIMIT,
      });

      const aiSummary = await generateCumulativeLeadAIIntelligence({
        business,
        runs,
      });

      const { data: savedRun, error } = await supabase
        .from("lead_ai_intelligence_runs")
        .insert([
          {
            org_id: orgId,
            business,
            timeframe: "cumulative",
            run_type: "cumulative",
            source_label: "Cumulative intelligence from saved prior AI runs",
            transcript_count: 0,
            summary: aiSummary,
            created_by_user_id: user?.id || null,
          },
        ])
        .select()
        .maybeSingle();

      if (error) throw error;

      return apiSuccess(savedRun);
    } catch (error) {
      console.error("generate cumulative intelligence error:", error);
      return apiError(500, error.message || "Failed to generate intelligence");
    }
  },
);
