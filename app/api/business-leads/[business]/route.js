// POST /api/business-leads/:business — create a lead in a static business.
//
// Delegates to createBusinessLead, the same engine the client-lead route uses,
// so dedupe/validation/column-mapping stay identical across both surfaces.
//
// createBusinessLead attaches `statusCode` to its errors (duplicate phone,
// unknown business), so those surface with their own status rather than a
// blanket 500.
//
// PRESERVED DEFECT: `req.session?.user?.org_id` was always undefined, so this
// always used DASHBOARD_ORG_ID.

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
