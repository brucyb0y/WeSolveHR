// GET /api/logs — filtered activity log feed for the logs console.
//
// All five filters are optional and passed straight through to
// getLogsPageData, which treats undefined as "no filter". Reading them off the
// query object (rather than the URL directly) keeps the exact shape the Express
// handler forwarded.
//
// The error message is surfaced to the client when the loader supplies one —
// the logs console shows it verbatim, and it is usually a bad date range the
// user can fix.

import { getLogsPageData } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  searchParamsToQuery,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const GET = withApiErrors("GET /api/logs", async (request) => {
  const { response } = await requireApiUser(request);
  if (response) return response;

  const query = searchParamsToQuery(request);

  try {
    const data = await getLogsPageData(null, {
      q: query.q,
      user: query.user,
      outcome: query.outcome,
      day: query.day,
      month: query.month,
    });
    return apiSuccess(data);
  } catch (error) {
    console.error("GET /api/logs error:", error);
    return apiError(500, error?.message || "Failed to load logs");
  }
});
