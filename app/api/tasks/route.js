// GET /api/tasks — the tasks console feed.
//
// `progressBucket` is REPEATED in the query string (the console sends one per
// checked bucket), so it must reach getTasksPageData as an array.
// searchParamsToQuery collapses repeats into arrays exactly as Express's
// req.query did — reading it with searchParams.get() would silently keep only
// the first bucket and quietly narrow every result set.
//
// Scoped to DASHBOARD_ORG_ID rather than the caller's org, matching the
// original: the console is a cross-org operations view.

import { getTasksPageData, DASHBOARD_ORG_ID } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  searchParamsToQuery,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const GET = withApiErrors("GET /api/tasks", async (request) => {
  const { response } = await requireApiUser(request);
  if (response) return response;

  const q = searchParamsToQuery(request);

  try {
    const rows = await getTasksPageData(
      {
        search: q.search,
        assignee: q.assignee,
        waitingOn: q.waitingOn,
        business: q.business,
        area: q.area,
        status: q.status,
        priority: q.priority,
        blocked: q.blocked,
        overdue: q.overdue,
        progressBucket: q.progressBucket,
      },
      DASHBOARD_ORG_ID,
    );
    return apiSuccess(rows);
  } catch (error) {
    console.error("GET /api/tasks error:", error);
    return apiError(500, "Failed to load tasks");
  }
});
