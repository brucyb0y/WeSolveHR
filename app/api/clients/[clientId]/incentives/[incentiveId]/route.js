// PATCH /api/clients/:clientId/incentives/:incentiveId
//
// Payload-builder family: the update branch merges {...existing, ...body}
// before rebuilding, so patching `status` alone cannot reset `amount` to 0.

import {
  supabase,
  insertClientActivityLog,
  loadActiveClientChild,
  buildIncentivePayloadFromBody,
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
  "PATCH /api/clients/[clientId]/incentives/[incentiveId]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const params = await routeParams(ctx);
    const clientId = Number(params.clientId);
    const incentiveId = Number(params.incentiveId);
    if (!clientId || !incentiveId) {
      return apiError(400, "Invalid client or incentive id");
    }

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const body = await readJsonBody(request);

    const existing = await loadActiveClientChild(
      "client_incentives",
      orgId,
      clientId,
      incentiveId,
    );
    if (!existing) return apiError(404, "Incentive not found");

    const now = new Date().toISOString();
    const patch =
      body.archive === true
        ? { is_active: false, deleted_at: now }
        : buildIncentivePayloadFromBody({ ...existing, ...body });

    patch.updated_at = now;
    patch.last_updated_by_user_id = actorUserId;

    const { data, error } = await supabase
      .from("client_incentives")
      .update(patch)
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("id", incentiveId)
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("update incentive error:", error);
      return apiError(500, "Failed to update incentive");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: body.archive
        ? "client_incentive_archived"
        : "client_incentive_updated",
      entityType: "client_incentives",
      entityId: incentiveId,
      oldValue: existing,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
