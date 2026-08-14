// POST /api/clients/:clientId/reports — create a weekly report.
//
// Needs at least ONE of period label / week start / summary: a report logged
// mid-week often has only a summary.
//
// Two flags with deliberately different defaults:
//   is_published      — false unless explicitly true (a new report is a draft);
//   is_client_visible — TRUE unless explicitly false (`!== false`), so a report
//                       is customer-visible once published unless someone opts
//                       out. Those defaults are opposite on purpose.

import {
  supabase,
  insertClientActivityLog,
  buildWeeklyReportPayloadFromBody,
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
  "POST /api/clients/[clientId]/reports",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);
    if (!clientId) return apiError(400, "Invalid client id");

    const body = await readJsonBody(request);
    const payload = buildWeeklyReportPayloadFromBody(body);

    if (!payload.period_label && !payload.week_start && !payload.summary) {
      return apiError(400, "Report period or summary is required");
    }

    // Accepts the string "true" as well as the boolean, matching the original —
    // this field has historically arrived from form posts.
    const published = body.is_published === true || body.is_published === "true";

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const now = new Date().toISOString();

    const { data, error } = await supabase
      .from("client_weekly_reports")
      .insert([
        {
          org_id: orgId,
          client_id: clientId,
          ...payload,
          is_published: published,
          published_at: published ? now : null,
          is_client_visible: body.is_client_visible !== false,
          is_active: true,
          created_by_user_id: actorUserId,
          last_updated_by_user_id: actorUserId,
          updated_at: now,
        },
      ])
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("create report error:", error);
      return apiError(500, "Failed to create report");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_report_created",
      entityType: "client_weekly_reports",
      entityId: data.id,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
