// GET / POST /api/clients/:clientId/contributors
//
// Contributors are contractors and client-side people who are NOT WeSolve
// users — distinct from the employees on the Team tab, which come from the
// users table.
//
// ENVELOPE FIXED: the Express version replied `{success, contributors}` /
// `{success, contributor}` while ContributorModal checks `json.ok`, so a
// successful create reported failure. Both now return `{ok, data}`.

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

export const GET = withApiErrors(
  "GET /api/clients/[clientId]/contributors",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { clientId } = await routeParams(ctx);

    const { data, error } = await supabase
      .from("client_contributors")
      .select("*")
      .eq("client_id", clientId)
      .eq("archived", false)
      // Grouped by type first, then newest within each group.
      .order("person_type", { ascending: true })
      .order("created_at", { ascending: false });

    if (error) {
      console.error("load contributors error:", error);
      return apiError(500, "Failed to load contributors");
    }

    return apiSuccess(data || []);
  },
);

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/contributors",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { clientId } = await routeParams(ctx);
    const body = await readJsonBody(request);

    if (!body.person_type || !body.name || !body.role) {
      return apiError(400, "person_type, name and role are required");
    }

    const { data, error } = await supabase
      .from("client_contributors")
      .insert({
        client_id: clientId,
        person_type: body.person_type,
        user_id: body.user_id || null,
        name: body.name,
        email: body.email || null,
        phone: body.phone || null,
        role: body.role,
        // Permission flags are coerced to real booleans — they gate what a
        // non-employee can see and do.
        can_update_work: !!body.can_update_work,
        can_view_client_dashboard: !!body.can_view_client_dashboard,
        status: body.status || "Active",
        notes: body.notes || null,
      })
      .select()
      .maybeSingle();

    if (error) {
      console.error("create contributor error:", error);
      return apiError(500, "Failed to create contributor");
    }

    await supabase.from("client_updates").insert({
      client_id: clientId,
      update_text: `Contributor added: ${body.name} as ${body.role}`,
      update_type: "contributor",
    });

    return apiSuccess(data);
  },
);
