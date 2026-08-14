// POST /api/clients/:clientId/incentives — log an incentive.
//
// Internal-only data (attribution / commission); it never reaches the customer
// dashboard. Field mapping is shared with PATCH via
// buildIncentivePayloadFromBody.

import {
  supabase,
  insertClientActivityLog,
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

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/incentives",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);
    if (!clientId) return apiError(400, "Invalid client id");

    const payload = buildIncentivePayloadFromBody(await readJsonBody(request));
    if (!payload.title) return apiError(400, "Incentive title is required");

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;

    const { data, error } = await supabase
      .from("client_incentives")
      .insert([
        {
          org_id: orgId,
          client_id: clientId,
          ...payload,
          is_active: true,
          created_by_user_id: actorUserId,
          last_updated_by_user_id: actorUserId,
          updated_at: new Date().toISOString(),
        },
      ])
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("create incentive error:", error);
      return apiError(500, "Failed to create incentive");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_incentive_created",
      entityType: "client_incentives",
      entityId: data.id,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
