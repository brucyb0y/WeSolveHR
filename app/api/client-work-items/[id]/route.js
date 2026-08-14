// GET / PATCH /api/client-work-items/:id
//
// THE DEPENDENCY GUARD IS THE POINT OF THIS ROUTE. A work item can declare
// another as a prerequisite, and this refuses to mark an item done while its
// dependency is not — that rule lives here rather than in the UI, so it holds
// no matter what calls the API.
//
// Two subtleties in how the dependency is resolved:
//   * `effectiveDependencyId` falls back to the STORED dependency when the body
//     omits the field. Otherwise a PATCH of just `{status:"done"}` would see no
//     dependency and skip the check entirely.
//   * "", null and undefined all mean "no dependency"; only a real value is
//     coerced with Number(). Truthiness alone would treat "" as a lookup.
//
// Self-dependency is rejected — an item that blocks itself could never be done.
//
// completed_at is derived from status, never taken from the body, so it cannot
// drift out of step (set on done, cleared on anything else).

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

const STATUSES = ["todo", "in_progress", "done"];
const PRIORITIES = ["low", "medium", "high"];

// "", null and undefined all mean "cleared"; anything else is an id.
const optionalId = (v) =>
  v === "" || v === undefined || v === null ? null : Number(v);

export const GET = withApiErrors(
  "GET /api/client-work-items/[id]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { id: raw } = await routeParams(ctx);
    const id = Number(raw);
    if (!id) return apiError(400, "Invalid work item id");

    const { data, error } = await supabase
      .from("client_work_items")
      .select("*")
      .eq("org_id", orgIdForApi(user))
      .eq("id", id)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (error) {
      console.error("get client work item error:", error);
      return apiError(500, "Failed to load work item");
    }
    if (!data) return apiError(404, "Work item not found");

    return apiSuccess(data);
  },
);

export const PATCH = withApiErrors(
  "PATCH /api/client-work-items/[id]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { id: raw } = await routeParams(ctx);
    const id = Number(raw);
    if (!id) return apiError(400, "Invalid work item id");

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const body = await readJsonBody(request);
    const now = new Date().toISOString();

    const { data: existing, error: existingError } = await supabase
      .from("client_work_items")
      .select("*")
      .eq("org_id", orgId)
      .eq("id", id)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (existingError) {
      console.error("work item lookup error:", existingError);
      return apiError(500, "Failed to update work item");
    }
    if (!existing) return apiError(404, "Work item not found");

    // ---- archive ----------------------------------------------------------
    if (body.archive === true) {
      const { data, error } = await supabase
        .from("client_work_items")
        .update({
          is_active: false,
          deleted_at: now,
          updated_at: now,
          last_updated_by_user_id: actorUserId,
        })
        .eq("org_id", orgId)
        .eq("id", id)
        .select("*")
        .maybeSingle();

      if (error) {
        console.error("archive work item error:", error);
        return apiError(500, "Failed to archive work item");
      }

      await insertClientActivityLog({
        orgId,
        clientId: existing.client_id,
        actorUserId,
        action: "work_item_archived",
        entityType: "client_work_items",
        entityId: id,
        oldValue: existing,
        newValue: data,
      });

      return apiSuccess(data);
    }

    // ---- validation -------------------------------------------------------
    if (body.status && !STATUSES.includes(body.status)) {
      return apiError(400, "Invalid status");
    }
    if (body.priority && !PRIORITIES.includes(body.priority)) {
      return apiError(400, "Invalid priority");
    }

    const dependencyId = optionalId(body.dependency_work_item_id);
    const milestoneId = optionalId(body.milestone_id);

    if (dependencyId && dependencyId === id) {
      return apiError(400, "A work item cannot depend on itself");
    }

    // Fall back to the stored dependency so a status-only PATCH is still
    // checked against it.
    const effectiveDependencyId =
      body.dependency_work_item_id === undefined
        ? existing.dependency_work_item_id || null
        : dependencyId;

    if (effectiveDependencyId) {
      const { data: dependency, error: dependencyError } = await supabase
        .from("client_work_items")
        .select("id, client_id, title, status")
        .eq("org_id", orgId)
        .eq("id", effectiveDependencyId)
        // Same client — an item cannot depend on another client's work.
        .eq("client_id", existing.client_id)
        .eq("is_active", true)
        .is("deleted_at", null)
        .maybeSingle();

      if (dependencyError) {
        console.error("dependency lookup error:", dependencyError);
        return apiError(500, "Failed to validate dependency");
      }
      if (!dependency) {
        return apiError(400, "Dependency work item not found for this client");
      }
      if (body.status === "done" && dependency.status !== "done") {
        return apiError(
          400,
          `Cannot mark done yet. Dependency is still not done: ${dependency.title}`,
        );
      }
    }

    // ---- patch ------------------------------------------------------------
    const patch = { updated_at: now, last_updated_by_user_id: actorUserId };

    if (body.title !== undefined) {
      const title = String(body.title || "").trim();
      if (!title) return apiError(400, "Title is required");
      patch.title = title;
    }
    if (body.description !== undefined) {
      patch.description = String(body.description || "").trim() || null;
    }
    if (body.owner_user_id !== undefined) {
      patch.owner_user_id = body.owner_user_id
        ? Number(body.owner_user_id)
        : null;
    }
    if (body.priority !== undefined) patch.priority = body.priority || "medium";
    if (body.due_date !== undefined) patch.due_date = body.due_date || null;
    if (body.dependency_work_item_id !== undefined) {
      patch.dependency_work_item_id = dependencyId;
    }
    if (body.milestone_id !== undefined) patch.milestone_id = milestoneId;
    if (body.status !== undefined) {
      patch.status = body.status;
      patch.completed_at = body.status === "done" ? now : null;
    }

    const { data, error } = await supabase
      .from("client_work_items")
      .update(patch)
      .eq("org_id", orgId)
      .eq("id", id)
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("update work item error:", error);
      return apiError(500, "Failed to update work item");
    }

    await insertClientActivityLog({
      orgId,
      clientId: existing.client_id,
      actorUserId,
      action: "work_item_updated",
      entityType: "client_work_items",
      entityId: id,
      oldValue: existing,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
