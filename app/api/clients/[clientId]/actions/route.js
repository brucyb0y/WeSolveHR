import {
  supabase,
  DASHBOARD_ORG_ID,
  insertClientActivityLog,
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
  "POST /api/clients/[clientId]/actions",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);
    const body = await readJsonBody(request);
    const title = String(body.title || "").trim();
    if (!clientId || !title) return apiError(400, "Action title is required");

    const now = new Date().toISOString();
    const orgId = orgIdForApi(user);

    const { data, error } = await supabase
      .from("client_actions")
      .insert([
        {
          org_id: orgId,
          client_id: clientId,
          title,
          owner_type: body.owner_type || "WeSolve",
          owner_name: body.owner_name || null,
          due_date: body.due_date || null,
          status: body.status || "Open",
          priority: body.priority || "Medium",
          notes: body.notes || null,
          archived: false,
          created_at: now,
          updated_at: now,
        },
      ])
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("create action error:", error);
      return apiError(500, "Failed to create action");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId: user?.id || null,
      action: "client_action_created",
      entityType: "client_actions",
      entityId: data.id,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
