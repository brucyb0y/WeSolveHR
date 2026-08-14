// POST /api/clients/:clientId/updates — add a manual client update.
//
// `is_client_visible` is compared with === true, not coerced: an update is
// internal unless explicitly marked visible, so a stray truthy value (a
// non-empty string from a form, say) cannot publish it to the customer.

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
  "POST /api/clients/[clientId]/updates",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);
    if (!clientId) return apiError(400, "Invalid client id");

    const body = await readJsonBody(request);
    const updateText = String(body.update_text || "").trim();
    if (!updateText) return apiError(400, "Update text is required");

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;

    const { data, error } = await supabase
      .from("client_updates")
      .insert([
        {
          org_id: orgId,
          client_id: clientId,
          title: body.title || null,
          update_text: updateText,
          update_type: body.update_type || "general",
          related_work_item_id: body.related_work_item_id
            ? Number(body.related_work_item_id)
            : null,
          is_client_visible: body.is_client_visible === true,
          is_active: true,
          created_by_user_id: actorUserId,
          updated_at: new Date().toISOString(),
        },
      ])
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("create update error:", error);
      return apiError(500, "Failed to save update");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_update_created",
      entityType: "client_updates",
      entityId: data.id,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
