// PATCH /api/clients/:clientId/reports/:reportId
//
// FOUR mutually exclusive operations on one verb, checked in this order:
//   archive   -> soft delete
//   publish   -> is_published true + published_at stamped
//   unpublish -> is_published false + published_at cleared
//   otherwise -> a content edit, merged through the payload builder
//
// The order matters: an archive request must not also be treated as an edit.
// Publish/unpublish deliberately touch ONLY the publish fields, so publishing
// never rewrites the report body.

import {
  supabase,
  insertClientActivityLog,
  loadActiveClientChild,
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

export const PATCH = withApiErrors(
  "PATCH /api/clients/[clientId]/reports/[reportId]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const params = await routeParams(ctx);
    const clientId = Number(params.clientId);
    const reportId = Number(params.reportId);
    if (!clientId || !reportId) {
      return apiError(400, "Invalid client or report id");
    }

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const body = await readJsonBody(request);
    const now = new Date().toISOString();

    const existing = await loadActiveClientChild(
      "client_weekly_reports",
      orgId,
      clientId,
      reportId,
    );
    if (!existing) return apiError(404, "Report not found");

    let patch;
    if (body.archive === true) {
      patch = { is_active: false, deleted_at: now };
    } else if (body.publish === true) {
      patch = { is_published: true, published_at: now };
    } else if (body.unpublish === true) {
      patch = { is_published: false, published_at: null };
    } else {
      patch = buildWeeklyReportPayloadFromBody({ ...existing, ...body });
      // Only applied when the key was sent, so an unrelated edit cannot flip
      // a report's client visibility.
      if (body.is_client_visible !== undefined) {
        patch.is_client_visible = body.is_client_visible !== false;
      }
    }

    patch.updated_at = now;
    patch.last_updated_by_user_id = actorUserId;

    const { data, error } = await supabase
      .from("client_weekly_reports")
      .update(patch)
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("id", reportId)
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("update report error:", error);
      return apiError(500, "Failed to update report");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: body.archive
        ? "client_report_archived"
        : "client_report_updated",
      entityType: "client_weekly_reports",
      entityId: reportId,
      oldValue: existing,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
