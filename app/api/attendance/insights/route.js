// GET /api/attendance/insights — aggregate attendance insights.
//
// The Express file registered this path TWICE (two functionally identical
// handlers). Express serves the first match, so the second was unreachable
// dead code; collapsing to one native route resolves that by construction.

import { getAttendanceInsightsData } from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import { apiSuccess, apiError, withApiErrors } from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const GET = withApiErrors(
  "GET /api/attendance/insights",
  async (request) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    try {
      return apiSuccess(await getAttendanceInsightsData(orgIdForApi(user)));
    } catch (error) {
      console.error("GET /api/attendance/insights error:", error);
      return apiError(500, "Failed to load attendance insights");
    }
  },
);
