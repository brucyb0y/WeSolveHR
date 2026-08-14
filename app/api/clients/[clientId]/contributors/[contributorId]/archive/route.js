// POST /api/clients/:clientId/contributors/:contributorId/archive
//
// Soft delete via `archived` + status "Inactive" — client_contributors uses
// its own column names again (no is_active / deleted_at here), which is why
// the GET filters on `archived = false`.
//
// Envelope normalised to {ok, data}; the Express version replied {success}
// while TeamTab checks json.ok.

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
  "POST /api/clients/[clientId]/contributors/[contributorId]/archive",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { clientId, contributorId } = await routeParams(ctx);

    const { data, error } = await supabase
      .from("client_contributors")
      .update({
        archived: true,
        status: "Inactive",
        updated_at: new Date().toISOString(),
      })
      .eq("id", contributorId)
      .eq("client_id", clientId)
      .select()
      .maybeSingle();

    if (error) {
      console.error("archive contributor error:", error);
      return apiError(500, "Failed to archive contributor");
    }
    if (!data) return apiError(404, "Contributor not found");

    await supabase.from("client_updates").insert({
      client_id: clientId,
      update_text: `Contributor archived: ${data.name}`,
      update_type: "contributor",
    });

    return apiSuccess(data);
  },
);
