// DELETE /api/lead-voice-uploads/:id — remove one call summary.

import { supabase, DASHBOARD_ORG_ID } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const DELETE = withApiErrors(
  "DELETE /api/lead-voice-uploads/[id]",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { id: raw } = await routeParams(ctx);
    const id = Number(raw);
    if (!id) return apiError(400, "Invalid call summary ID");

    const { error } = await supabase
      .from("lead_voice_uploads")
      .delete()
      .eq("org_id", DASHBOARD_ORG_ID)
      .eq("id", id);

    if (error) {
      console.error("delete voice upload error:", error);
      return apiError(500, error.message || "Failed to delete call summary");
    }

    return apiSuccess({ id });
  },
);
