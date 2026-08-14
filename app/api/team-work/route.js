// GET /api/team-work — the team work board for one day.
//
// `date` must be YYYY-MM-DD; anything else silently falls back to today in the
// app timezone rather than erroring, matching the original — the board should
// always render something.
//
// Logs are bundled into the same response so the page needs one request, not
// two; /api/team-work/logs exists for refreshing them alone.

import {
  loadTeamWorkData,
  getRecentTeamWorkLogs,
  getTodayDateStringInTimeZone,
} from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  searchParamsToQuery,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const RECENT_LOG_LIMIT = 40;

export const GET = withApiErrors("GET /api/team-work", async (request) => {
  const { user, response } = await requireApiUser(request);
  if (response) return response;

  const orgId = orgIdForApi(user);
  let date = String(searchParamsToQuery(request).date || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) date = getTodayDateStringInTimeZone();

  try {
    const data = await loadTeamWorkData(orgId, date);
    const logs = await getRecentTeamWorkLogs(orgId, RECENT_LOG_LIMIT);
    return apiSuccess({ ...data, logs });
  } catch (error) {
    console.error("GET /api/team-work error:", error);
    return apiError(500, "Failed to load team work data");
  }
});
