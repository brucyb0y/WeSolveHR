// POST /api/clients/:clientId/actions/:actionId/archive
//
// Archiving is a soft delete: the row stays and gains archived=true plus an
// "Archived" status, so history and the activity timeline survive.
//
// Returns the standard {ok, data} envelope. The Express version replied
// `{success: true}`, which ActionsTab (its only caller) does not understand —
// it checks json.ok, so a successful archive reported "Failed to archive
// action" while having actually archived the row.

import { supabase } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/actions/[actionId]/archive",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { clientId, actionId } = await routeParams(ctx);

    const { data, error } = await supabase
      .from("client_actions")
      .update({
        archived: true,
        status: "Archived",
        updated_at: new Date().toISOString(),
      })
      .eq("id", actionId)
      .eq("client_id", clientId)
      .select()
      .maybeSingle();

    if (error) {
      console.error("archive action error:", error);
      return apiError(500, "Failed to archive action");
    }
    if (!data) return apiError(404, "Action not found");

    await supabase.from("client_updates").insert({
      client_id: clientId,
      update_text: `Action archived: ${data.title}`,
      update_type: "action",
    });

    return apiSuccess(data);
  },
);
