// GET /api/clients/nav-list — the top nav's client dropdown.
//
// Scoped to the CALLER's org (not DASHBOARD_ORG_ID), falling back to the
// dashboard org only when the user carries none — same precedence the Express
// handler used via req.loggedInUser?.org_id.

import { supabase } from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import { apiSuccess, apiError, withApiErrors } from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const GET = withApiErrors(
  "GET /api/clients/nav-list",
  async (request) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { data, error } = await supabase
      .from("clients")
      .select("id, name, company_name")
      .eq("org_id", orgIdForApi(user))
      .eq("is_active", true)
      .is("deleted_at", null)
      .order("name", { ascending: true });

    if (error) {
      console.error("GET /api/clients/nav-list error:", error);
      return apiError(500, "Failed to load clients");
    }

    return apiSuccess(
      (data || []).map((c) => ({
        id: c.id,
        // Falls through name -> company_name -> "Client #id" so the dropdown
        // never renders a blank row.
        name: c.name || c.company_name || `Client #${c.id}`,
      })),
    );
  },
);
