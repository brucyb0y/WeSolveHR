// DELETE /api/lead-voice-uploads/bulk-delete — remove several voice uploads.
//
// A POST-style body on a DELETE, matching the original: the id list is too
// large for a query string. `ids` is mapped through Number and filtered, so a
// malformed entry is dropped rather than poisoning the `.in()` clause — and an
// empty result 400s rather than issuing a delete with no filter.
//
// PRESERVED DEFECT: `req.session?.user?.org_id` was always undefined, so this
// always scoped to DASHBOARD_ORG_ID.

import { supabase, DASHBOARD_ORG_ID } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  readJsonBody,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const DELETE = withApiErrors(
  "DELETE /api/lead-voice-uploads/bulk-delete",
  async (request) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const body = await readJsonBody(request);
    const ids = Array.isArray(body.ids)
      ? body.ids.map(Number).filter(Boolean)
      : [];

    if (!ids.length) return apiError(400, "No voice message IDs provided");

    const { error } = await supabase
      .from("lead_voice_uploads")
      .delete()
      .eq("org_id", DASHBOARD_ORG_ID)
      .in("id", ids);

    if (error) {
      console.error("bulk delete voice uploads error:", error);
      return apiError(500, error.message || "Failed to delete voice messages");
    }

    return apiSuccess({ deleted: ids.length });
  },
);
