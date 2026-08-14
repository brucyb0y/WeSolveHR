// POST /api/clients/:clientId/updates/:updateId/archive
//
// Soft delete. Note the column is `archived_at` here, not `deleted_at` as on
// blockers/campaigns/meetings — client_updates uses its own name, so this is
// not a copy-paste of the other archive routes.
//
// No caller in the React app (UpdatesTab is add-only); converted for parity.

import { supabase } from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/updates/[updateId]/archive",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId, updateId } = await routeParams(ctx);
    const now = new Date().toISOString();

    const { data, error } = await supabase
      .from("client_updates")
      .update({ is_active: false, archived_at: now, updated_at: now })
      .eq("id", updateId)
      .eq("client_id", clientId)
      .eq("org_id", orgIdForApi(user))
      .select()
      .maybeSingle();

    if (error) {
      console.error("archive update error:", error);
      return apiError(500, "Failed to archive update");
    }
    if (!data) return apiError(404, "Update not found");

    return apiSuccess(data);
  },
);
