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

const SIDES = ["internal", "client_side"];
const PRIORITIES = ["low", "medium", "high"];

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/blockers",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);
    const body = await readJsonBody(request);
    const title = String(body.title || "").trim();

    if (!clientId || !title) return apiError(400, "Blocker title is required");

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;

    const { data, error } = await supabase
      .from("client_blockers")
      .insert([
        {
          org_id: orgId,
          client_id: clientId,
          title,
          description: String(body.description || "").trim() || null,
          blocker_side: SIDES.includes(body.blocker_side)
            ? body.blocker_side
            : "internal",
          priority: PRIORITIES.includes(body.priority)
            ? body.priority
            : "medium",
          resolution_status: "open",
          owner_user_id: body.owner_user_id ? Number(body.owner_user_id) : null,
          related_work_item_id: body.related_work_item_id
            ? Number(body.related_work_item_id)
            : null,
          is_active: true,
          created_by_user_id: actorUserId,
          last_updated_by_user_id: actorUserId,
          updated_at: new Date().toISOString(),
        },
      ])
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("create blocker error:", error);
      return apiError(500, "Failed to create blocker");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_blocker_created",
      entityType: "client_blockers",
      entityId: data.id,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
