// PUT / PATCH /api/clients/:clientId/updates/:updateId
//
// NO CALLER IN THE REACT APP — UpdatesTab is add-only, matching the original
// page, which never offered an edit control for a client update. Converted for
// parity so the Express handler can be deleted, and normalised to the standard
// {ok, data} envelope (the Express version replied `{success, update}`); since
// nothing calls it, that change cannot break a client.

import { supabase } from "@/lib/server/app";
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
  "PUT /api/clients/[clientId]/updates/[updateId]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId, updateId } = await routeParams(ctx);
    const body = await readJsonBody(request);

    const updateText = String(body.update_text || "").trim();
    if (!updateText) return apiError(400, "Update text is required");

    const { data, error } = await supabase
      .from("client_updates")
      .update({
        update_text: updateText,
        related_work_item_id: body.related_work_item_id || null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", updateId)
      .eq("client_id", clientId)
      .eq("org_id", orgIdForApi(user))
      .select()
      .maybeSingle();

    if (error) {
      console.error("edit update error:", error);
      return apiError(500, "Failed to edit update");
    }
    if (!data) return apiError(404, "Update not found");

    return apiSuccess(data);
  },
);

export const PUT = update;
export const PATCH = update;
