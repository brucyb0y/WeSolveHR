// POST /api/clients/:clientId/report-summary/generate
//
// The internal "Regenerate" button behind the AI summary panel. Staff-only —
// /client-view never renders the control.
//
// `week_start` targets a SPECIFIC week when regenerating from a historical
// Week N tab. It is normalised through mondayStartOfUtcMs so any day in that
// week resolves to the same Monday key the summary is stored under; an
// unparseable value falls through to null rather than erroring, and the field
// is ignored entirely for the daily period.
//
// The upstream error message is surfaced (`error.message`) because it usually
// comes from the model call and is the only actionable detail the operator has.

import {
  supabase,
  CLIENT_REPORT_SUMMARY_PERIODS,
  mondayStartOfUtcMs,
  runClientReportSummary,
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

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/report-summary/generate",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);
    if (!clientId) return apiError(400, "Invalid client id");

    const orgId = orgIdForApi(user);
    const body = await readJsonBody(request);

    const period = CLIENT_REPORT_SUMMARY_PERIODS.includes(body?.period)
      ? body.period
      : "daily";

    let weekStartMs = null;
    if (period === "weekly" && body?.week_start) {
      const parsed = Date.parse(
        `${String(body.week_start).slice(0, 10)}T00:00:00Z`,
      );
      if (!Number.isNaN(parsed)) weekStartMs = mondayStartOfUtcMs(parsed);
    }

    const { data: client, error } = await supabase
      .from("clients")
      .select("id, org_id, name, company_name")
      .eq("org_id", orgId)
      .eq("id", clientId)
      .is("deleted_at", null)
      .maybeSingle();

    if (error) {
      console.error("report summary client lookup error:", error);
      return apiError(500, "Failed to generate summary");
    }
    if (!client) return apiError(404, "Client not found");

    try {
      const saved = await runClientReportSummary({
        orgId,
        client,
        period,
        userId: user?.id || null,
        weekStartMs,
      });
      return apiSuccess(saved);
    } catch (err) {
      console.error("report summary generate error:", err);
      return apiError(500, err.message || "Failed to generate summary");
    }
  },
);
