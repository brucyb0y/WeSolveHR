// POST /api/clients/:clientId/milestones — create a milestone.

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

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/milestones",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);
    const body = await readJsonBody(request);
    const title = String(body.title || "").trim();

    if (!clientId || !title) return apiError(400, "Milestone title is required");

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;

    const { data, error } = await supabase
      .from("client_milestones")
      .insert([
        {
          org_id: orgId,
          client_id: clientId,
          title,
          due_date: body.due_date || null,
          status: body.status || "planned",
          notes: body.notes || null,
          is_active: true,
          updated_at: new Date().toISOString(),
        },
      ])
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("create milestone error:", error);
      return apiError(500, "Failed to create milestone");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_milestone_created",
      entityType: "client_milestones",
      entityId: data.id,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
