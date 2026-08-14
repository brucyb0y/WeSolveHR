// POST /api/clients/:clientId/leads — create a lead for this client.
//
// Client leads are stored in the shared business-leads tables under a synthetic
// business key ("client:<id>"), which resolveClientLeadBusiness derives. That
// is why creation goes through createBusinessLead rather than a direct insert:
// the same validation, dedupe and column mapping apply as for static lead
// businesses.
//
// createBusinessLead attaches a statusCode to its errors (e.g. duplicate
// phone/email), so those surface with their own status rather than a blanket
// 500.
//
// FIXED HERE: the Express version only checked `if (!clientId)`, so any
// truthy id passed — POST /api/clients/999999/leads returned 200 and inserted a
// row with client_id 999999 that no client owned. resolveClientLeadBusiness
// derives "client:<id>" without touching the clients table, so nothing further
// down caught it either. The client is now looked up first and a bad id 404s,
// matching .../goals and .../client-view-link, which already did this.

import {
  supabase,
  insertClientActivityLog,
  resolveClientLeadBusiness,
  createBusinessLead,
} from "@/lib/server/app";
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

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/leads",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);
    if (!clientId) return apiError(400, "Invalid client id");

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const body = await readJsonBody(request);

    // Confirm the client exists in this org before deriving its lead business,
    // otherwise the insert creates an orphan row.
    const { data: client, error: clientError } = await supabase
      .from("clients")
      .select("id")
      .eq("org_id", orgId)
      .eq("id", clientId)
      .is("deleted_at", null)
      .maybeSingle();

    if (clientError) {
      console.error("client lookup before lead create failed:", clientError);
      return apiError(500, "Failed to create lead");
    }
    if (!client) return apiError(404, "Client not found");

    try {
      const business = await resolveClientLeadBusiness(orgId, clientId);
      const lead = await createBusinessLead({ orgId, business, body });

      await insertClientActivityLog({
        orgId,
        clientId,
        actorUserId,
        action: "client_lead_created",
        entityType: "business_leads",
        entityId: lead?.id || null,
        newValue: lead,
      });

      return apiSuccess(lead);
    } catch (err) {
      console.error("create client lead error:", err);
      return apiError(err.statusCode || 500, err.message || "Failed to create lead");
    }
  },
);
