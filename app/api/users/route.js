// GET /api/users — active users in the dashboard org.
//
// Native route handler: no Express, no adapter. Replaces the forwarding shim
// that called app.get("/api/users") in lib/server/app.js.

import { supabase, DASHBOARD_ORG_ID } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import { apiSuccess, apiError, withApiErrors } from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const GET = withApiErrors("GET /api/users", async (request) => {
  const { user, response } = await requireApiUser(request);
  if (response) return response;

  const { data, error } = await supabase
    .from("users")
    .select("id, org_id, name, role, is_active")
    // Scoped to the dashboard org, not the caller's — the original queried
    // DASHBOARD_ORG_ID directly, and this list feeds assignee pickers that
    // must stay identical across orgs.
    .eq("org_id", DASHBOARD_ORG_ID)
    .eq("is_active", true)
    .order("name", { ascending: true });

  if (error) {
    console.error("GET /api/users error:", error);
    return apiError(500, "Failed to load users");
  }

  return apiSuccess(data || []);
});
