// Update a contributor.
//
// FIXED — the same two defects the actions route had:
//   * registered PUT only, while ContributorModal sends PATCH, so every edit
//     returned 405 and silently did nothing (verified before the fix);
//   * replied `{success, contributor}` where the modal checks `json.ok`.
// Both verbs are exported and the envelope is standard.

import { supabase } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
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
  "PATCH /api/clients/[clientId]/contributors/[contributorId]",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { clientId, contributorId } = await routeParams(ctx);
    const body = await readJsonBody(request);

    const { data, error } = await supabase
      .from("client_contributors")
      .update({
        person_type: body.person_type,
        user_id: body.user_id || null,
        name: body.name,
        email: body.email || null,
        phone: body.phone || null,
        role: body.role,
        can_update_work: !!body.can_update_work,
        can_view_client_dashboard: !!body.can_view_client_dashboard,
        status: body.status || "Active",
        notes: body.notes || null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", contributorId)
      .eq("client_id", clientId)
      .select()
      .maybeSingle();

    if (error) {
      console.error("update contributor error:", error);
      return apiError(500, "Failed to update contributor");
    }
    if (!data) return apiError(404, "Contributor not found");

    await supabase.from("client_updates").insert({
      client_id: clientId,
      update_text: `Contributor updated: ${data.name}`,
      update_type: "contributor",
    });

    return apiSuccess(data);
  },
);

export const PATCH = update;
export const PUT = update;
