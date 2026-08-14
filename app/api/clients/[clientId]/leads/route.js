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

import {
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
