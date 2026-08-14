// POST /api/client-work-items — create a work item for a client.
//
// Lives at the top level rather than under /api/clients/:id because a work
// item's client is part of its BODY (client_id), not its path — the same
// endpoint serves the workspace and anything else that files work.
//
// New items always start status "todo": the field is not read from the body,
// so an item cannot be created already done.

import {
  supabase,
  insertClientActivityLog,
} from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  readJsonBody,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const PRIORITIES = ["low", "medium", "high"];

export const POST = withApiErrors(
  "POST /api/client-work-items",
  async (request) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const body = await readJsonBody(request);
    const clientId = Number(body.client_id);
    const title = String(body.title || "").trim();

    if (!clientId) return apiError(400, "Client is required");
    if (!title) return apiError(400, "Title is required");
    if (!PRIORITIES.includes(body.priority || "medium")) {
      return apiError(400, "Invalid priority");
    }

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;

    const { data, error } = await supabase
      .from("client_work_items")
      .insert([
        {
          org_id: orgId,
          client_id: clientId,
          title,
          description: body.description || null,
          owner_user_id: body.owner_user_id ? Number(body.owner_user_id) : null,
          dependency_work_item_id: body.dependency_work_item_id
            ? Number(body.dependency_work_item_id)
            : null,
          milestone_id: body.milestone_id ? Number(body.milestone_id) : null,
          priority: body.priority || "medium",
          status: "todo",
          due_date: body.due_date || null,
          is_active: true,
          created_by_user_id: actorUserId,
          last_updated_by_user_id: actorUserId,
          updated_at: new Date().toISOString(),
        },
      ])
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("create client work item error:", error);
      return apiError(500, "Failed to create work item");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "work_item_created",
      entityType: "client_work_items",
      entityId: data.id,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
