// POST /api/cron/generate-report-summaries — nightly AI summary job.
//
// NOT user-authenticated. This is called by a scheduler, so it is gated on a
// shared CRON_SECRET accepted from a header, the query string, or the body —
// all three, because different schedulers can only send one of them.
//
// FAIL-CLOSED: when CRON_SECRET is unset the route rejects everything rather
// than running unauthenticated. `!secret` is checked first for exactly that.

import { runDailyClientReportSummaries } from "@/lib/server/app";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  readJsonBody,
  searchParamsToQuery,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/cron/generate-report-summaries",
  async (request) => {
    const secret = process.env.CRON_SECRET;
    const body = await readJsonBody(request);
    const provided =
      request.headers.get("x-cron-secret") ||
      searchParamsToQuery(request).secret ||
      body?.secret;

    if (!secret || provided !== secret) return apiError(401, "Unauthorized");

    try {
      return apiSuccess(await runDailyClientReportSummaries());
    } catch (error) {
      console.error("cron generate report summaries error:", error);
      return apiError(500, error.message || "Failed to generate report summaries");
    }
  },
);
