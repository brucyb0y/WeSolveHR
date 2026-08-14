// PATCH /api/clients/:clientId/campaigns/:campaignId
//
// Archive and update share the verb, as elsewhere. The update branch rebuilds
// the payload from `{...existing, ...body}` rather than from the body alone —
// that is what makes a partial PATCH safe here: buildCampaignPayloadFromBody
// applies defaults to every field it knows, so feeding it only the changed keys
// would reset the untouched ones to their defaults.

import {
  supabase,
  insertClientActivityLog,
  loadActiveClientChild,
  buildCampaignPayloadFromBody,
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
  "PATCH /api/clients/[clientId]/campaigns/[campaignId]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const params = await routeParams(ctx);
    const clientId = Number(params.clientId);
    const campaignId = Number(params.campaignId);
    if (!clientId || !campaignId) {
      return apiError(400, "Invalid client or campaign id");
    }

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const body = await readJsonBody(request);

    const existing = await loadActiveClientChild(
      "client_campaigns",
      orgId,
      clientId,
      campaignId,
    );
    if (!existing) return apiError(404, "Campaign not found");

    const now = new Date().toISOString();
    const patch =
      body.archive === true
        ? { is_active: false, deleted_at: now }
        : buildCampaignPayloadFromBody({ ...existing, ...body });

    patch.updated_at = now;
    patch.last_updated_by_user_id = actorUserId;

    const { data, error } = await supabase
      .from("client_campaigns")
      .update(patch)
      .eq("org_id", orgId)
      .eq("client_id", clientId)
      .eq("id", campaignId)
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("update campaign error:", error);
      return apiError(500, "Failed to update campaign");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: body.archive
        ? "client_campaign_archived"
        : "client_campaign_updated",
      entityType: "client_campaigns",
      entityId: campaignId,
      oldValue: existing,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
