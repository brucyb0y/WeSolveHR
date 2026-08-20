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
