// POST /api/leads/:id/reject — reject a transcribed voice upload.
//
// Same [business]-folder / lead-id naming gotcha as the approve route.
//
// `reason` is optional: the original trims it and passes it through without
// requiring it, so a rejection with no stated reason is allowed.

import { DASHBOARD_ORG_ID, rejectLeadVoiceUpload } from "@/lib/server/app";
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
  "POST /api/leads/[id]/reject",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const p = await routeParams(ctx);
    const leadVoiceId = Number(p.business);
    if (!leadVoiceId) return apiError(400, "Invalid lead voice ID");

    const body = await readJsonBody(request);

    try {
      const data = await rejectLeadVoiceUpload({
        leadVoiceId,
        orgId: DASHBOARD_ORG_ID,
        userId: null,
        reason: String(body.reason || "").trim(),
      });
      return apiSuccess(data);
    } catch (error) {
      console.error("reject lead voice error:", error);
      return apiError(500, error.message || "Failed to reject lead");
    }
  },
);
