// GET /api/attendance — attendance board data for the caller's org.

import { getAttendancePageData } from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import { apiSuccess, apiError, withApiErrors } from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const GET = withApiErrors("GET /api/attendance", async (request) => {
  const { user, response } = await requireApiUser(request);
  if (response) return response;

  try {
    return apiSuccess(await getAttendancePageData(orgIdForApi(user)));
  } catch (error) {
    console.error("GET /api/attendance error:", error);
    return apiError(500, "Failed to load attendance");
  }
});
