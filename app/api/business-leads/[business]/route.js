import { DASHBOARD_ORG_ID, createBusinessLead } from "@/lib/server/app";
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

export const POST = withApiErrors(
  "POST /api/business-leads/[business]",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { business } = await routeParams(ctx);

    try {
      const data = await createBusinessLead({
        orgId: DASHBOARD_ORG_ID,
        business,
        body: await readJsonBody(request),
      });
      return apiSuccess(data);
    } catch (error) {
      console.error("create business lead error:", error);
      return apiError(
        error.statusCode || 500,
        error.message || "Failed to create lead",
      );
    }
  },
);
