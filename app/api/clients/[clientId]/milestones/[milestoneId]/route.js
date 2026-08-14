// PUT / PATCH /api/clients/:clientId/milestones/:milestoneId
//
// Key-by-key patch (`!== undefined`), NOT a merge through a payload builder —
// milestones have no build*PayloadFromBody, so only the keys actually sent are
// written and everything else is left alone.
//
// MilestonesTab sends PUT (its "Close" button sets status: "closed"); PATCH is
// exported too so the verb the rest of the API uses also works here.

import { supabase, insertClientActivityLog } from "@/lib/server/app";
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

const update = withApiErrors(
  "PUT /api/clients/[clientId]/milestones/[milestoneId]",
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
    const body = await readJsonBody(request);

    const payload = { updated_at: new Date().toISOString() };
    if (body.title !== undefined) payload.title = String(body.title || "").trim();
    if (body.due_date !== undefined) payload.due_date = body.due_date || null;
    if (body.status !== undefined) payload.status = body.status || "planned";
    if (body.notes !== undefined) payload.notes = body.notes || null;

    // Only rejected when title was SENT and is blank — omitting it is fine.
    if (payload.title !== undefined && !payload.title) {
      return apiError(400, "Milestone title is required");
    }

    const { data, error } = await supabase
      .from("client_milestones")
      .update(payload)
      .eq("id", milestoneId)
      .eq("client_id", clientId)
      .eq("org_id", orgId)
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("update milestone error:", error);
      return apiError(500, "Failed to update milestone");
    }
    if (!data) return apiError(404, "Milestone not found");

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId: user?.id || null,
      action: "client_milestone_updated",
      entityType: "client_milestones",
      entityId: milestoneId,
      newValue: data,
    });

    return apiSuccess(data);
  },
);

export const PUT = update;
export const PATCH = update;
