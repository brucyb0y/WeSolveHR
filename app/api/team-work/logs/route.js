// GET /api/team-work/logs — recent activity for the team work board.
//
// Same 40-row window the board's initial payload uses, so refreshing the log
// panel alone cannot show a different amount of history than the page load did.

import { getRecentTeamWorkLogs } from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import { apiSuccess, apiError, withApiErrors } from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const GET = withApiErrors("GET /api/team-work/logs", async (request) => {
  const { user, response } = await requireApiUser(request);
  if (response) return response;

  try {
    const logs = await getRecentTeamWorkLogs(orgIdForApi(user), 40);
    return apiSuccess(logs);
  } catch (error) {
    console.error("GET /api/team-work/logs error:", error);
    return apiError(500, "Failed to load logs");
  }
});
