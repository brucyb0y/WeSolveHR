import {
  DASHBOARD_ORG_ID,
  updateBusinessLeadStatus,
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

export const PATCH = withApiErrors(
  "PATCH /api/business-leads/[business]/[id]/status",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const p = await routeParams(ctx);
    const leadId = Number(p.id);
    if (!leadId) return apiError(400, "Invalid business lead ID");

    const body = await readJsonBody(request);

    try {
      const data = await updateBusinessLeadStatus({
        business: p.business,
        leadId,
        orgId: DASHBOARD_ORG_ID,
        status: String(body.status || "").trim(),
      });
      return apiSuccess(data);
    } catch (error) {
      console.error("update business lead status error:", error);
      return apiError(error.statusCode || 500, error.message || "Failed to update status");
    }
  },
);
