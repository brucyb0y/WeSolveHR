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
const STATUSES = ["open", "in_progress", "resolved"];

export const PATCH = withApiErrors(
  "PATCH /api/clients/[clientId]/blockers/[blockerId]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const params = await routeParams(ctx);
    const clientId = Number(params.clientId);
    const blockerId = Number(params.blockerId);
    if (!clientId || !blockerId) {
      return apiError(400, "Invalid client or blocker id");
    }

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const body = await readJsonBody(request);
    const now = new Date().toISOString();

    const scope = (q) =>
      q.eq("org_id", orgId).eq("client_id", clientId).eq("id", blockerId);

    const { data: existing, error: existingError } = await scope(
      supabase.from("client_blockers").select("*"),
    )
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (existingError) {
      console.error("blocker lookup error:", existingError);
      return apiError(500, "Failed to update blocker");
    }
    if (!existing) return apiError(404, "Blocker not found");

    if (body.archive === true) {
      const { data, error } = await scope(
        supabase.from("client_blockers").update({
          is_active: false,
          deleted_at: now,
          updated_at: now,
          last_updated_by_user_id: actorUserId,
        }),
      )
        .select("*")
        .maybeSingle();

      if (error) {
        console.error("archive blocker error:", error);
        return apiError(500, "Failed to update blocker");
      }

      await insertClientActivityLog({
        orgId,
        clientId,
        actorUserId,
        action: "client_blocker_archived",
        entityType: "client_blockers",
        entityId: blockerId,
        oldValue: existing,
        newValue: data,
      });

      return apiSuccess(data);
    }

    const patch = { updated_at: now, last_updated_by_user_id: actorUserId };

    if (body.title !== undefined) {
      const title = String(body.title || "").trim();
      if (!title) return apiError(400, "Title is required");
      patch.title = title;
    }
    if (body.description !== undefined) {
      patch.description = String(body.description || "").trim() || null;
    }
    if (body.blocker_side !== undefined) {
      if (!SIDES.includes(body.blocker_side)) {
        return apiError(400, "Invalid blocker side");
      }
      patch.blocker_side = body.blocker_side;
    }
    if (body.priority !== undefined) {
      if (!PRIORITIES.includes(body.priority)) {
        return apiError(400, "Invalid priority");
      }
      patch.priority = body.priority;
    }
    if (body.resolution_status !== undefined) {
      if (!STATUSES.includes(body.resolution_status)) {
        return apiError(400, "Invalid resolution status");
      }
      patch.resolution_status = body.resolution_status;
      patch.resolved_at = body.resolution_status === "resolved" ? now : null;
    }
    if (body.owner_user_id !== undefined) {
      patch.owner_user_id = body.owner_user_id
        ? Number(body.owner_user_id)
        : null;
    }
    if (body.related_work_item_id !== undefined) {
      patch.related_work_item_id = body.related_work_item_id
        ? Number(body.related_work_item_id)
        : null;
    }

    const { data, error } = await scope(
      supabase.from("client_blockers").update(patch),
    )
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("update blocker error:", error);
      return apiError(500, "Failed to update blocker");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_blocker_updated",
      entityType: "client_blockers",
      entityId: blockerId,
      oldValue: existing,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
