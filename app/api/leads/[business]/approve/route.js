// POST /api/leads/:id/approve — approve a transcribed voice upload.
//
// NAMING GOTCHA: the folder is [business] but the value is a LEAD VOICE ID.
// Express registered this as "/api/leads/:id/approve"; Next requires one
// dynamic segment name per level, and this segment's siblings (transcribe,
// transcript, intelligence/*) genuinely are businesses — so the folder keeps
// that name while this route reads it as an id. Same URL shape either way.
//
// PRESERVED DEFECT: verifiedBy came from `req.session?.user?.phone ?? .name`,
// both always undefined (the session stores `userId`), so every approval was
// recorded as "admin". userId was likewise always null. Kept rather than
// silently changing who the audit trail credits.

import {
  DASHBOARD_ORG_ID,
  approveLeadVoiceUpload,
} from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/leads/[id]/approve",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const p = await routeParams(ctx);
    const leadVoiceId = Number(p.business);
    if (!leadVoiceId) return apiError(400, "Invalid lead voice ID");

    try {
      const data = await approveLeadVoiceUpload({
        leadVoiceId,
        orgId: DASHBOARD_ORG_ID,
        userId: null,
        verifiedBy: "admin",
        verifiedAt: new Date().toISOString(),
      });
      return apiSuccess(data);
    } catch (error) {
      console.error("approve lead voice error:", error);
      return apiError(500, error.message || "Failed to approve lead");
    }
  },
);
