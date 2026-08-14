// POST /api/clients/:clientId/milestones/:milestoneId/archive
//
// Soft delete. Work items pointing at this milestone are deliberately left
// alone — the confirm text says so ("Work items will remain, but the milestone
// will be hidden"), so clearing their milestone_id here would contradict it.

import { supabase, insertClientActivityLog } from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/milestones/[milestoneId]/archive",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const params = await routeParams(ctx);
    const clientId = Number(params.clientId);
    const milestoneId = Number(params.milestoneId);
    if (!clientId || !milestoneId) {
      return apiError(400, "Invalid client or milestone id");
    }

    const orgId = orgIdForApi(user);
    const now = new Date().toISOString();

    const { data, error } = await supabase
      .from("client_milestones")
      .update({ is_active: false, deleted_at: now, updated_at: now })
      .eq("id", milestoneId)
      .eq("client_id", clientId)
      .eq("org_id", orgId)
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("archive milestone error:", error);
      return apiError(500, "Failed to archive milestone");
    }
    if (!data) return apiError(404, "Milestone not found");

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId: user?.id || null,
      action: "client_milestone_archived",
      entityType: "client_milestones",
      entityId: milestoneId,
      oldValue: data,
    });

    return apiSuccess(data);
  },
);
