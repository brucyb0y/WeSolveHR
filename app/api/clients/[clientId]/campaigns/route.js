// POST /api/clients/:clientId/campaigns — create a campaign.
//
// Field mapping and validation live in buildCampaignPayloadFromBody (shared
// with the PATCH route), so the two cannot disagree about what a campaign is.

import {
  supabase,
  insertClientActivityLog,
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

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/campaigns",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);
    if (!clientId) return apiError(400, "Invalid client id");

    const payload = buildCampaignPayloadFromBody(await readJsonBody(request));
    if (!payload.name) return apiError(400, "Campaign name is required");

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;

    const { data, error } = await supabase
      .from("client_campaigns")
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
      console.error("create campaign error:", error);
      return apiError(500, "Failed to create campaign");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_campaign_created",
      entityType: "client_campaigns",
      entityId: data.id,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
