import {
  DASHBOARD_ORG_ID,
  getBusinessLeadById,
  updateBusinessLead,
  deleteBusinessLead,
} from "@/lib/server/app";
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

async function ids(ctx) {
  const p = await routeParams(ctx);
  return { business: p.business, leadId: Number(p.id) };
}

export const GET = withApiErrors(
  "GET /api/business-leads/[business]/[id]",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { business, leadId } = await ids(ctx);
    if (!leadId) return apiError(400, "Invalid lead ID");

    const data = await getBusinessLeadById({
      orgId: DASHBOARD_ORG_ID,
      business,
      leadId,
    });
    if (!data) return apiError(404, "Lead not found");

    return apiSuccess(data);
  },
);

export const PUT = withApiErrors(
  "PUT /api/business-leads/[business]/[id]",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { business, leadId } = await ids(ctx);
    if (!leadId) return apiError(400, "Invalid lead ID");

    try {
      const data = await updateBusinessLead({
        orgId: DASHBOARD_ORG_ID,
        business,
        leadId,
        body: await readJsonBody(request),
      });
      return apiSuccess(data);
    } catch (error) {
      console.error("update business lead error:", error);
      return apiError(error.statusCode || 500, error.message || "Failed to update lead");
    }
  },
);

export const DELETE = withApiErrors(
  "DELETE /api/business-leads/[business]/[id]",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { business, leadId } = await ids(ctx);
    if (!leadId) return apiError(400, "Invalid lead ID");

    try {
      return apiSuccess(
        await deleteBusinessLead({ orgId: DASHBOARD_ORG_ID, business, leadId }),
      );
    } catch (error) {
      console.error("delete business lead error:", error);
      return apiError(error.statusCode || 500, error.message || "Failed to delete lead");
    }
  },
);
