// POST /api/rasset-leads/enrich — enrich a lead from its website / Maps URL.
//
// Thin wrapper: the scraping + LLM extraction lives in enrichLeadFromUrl, so
// both this and /api/business-leads/enrich-url stay in step. The only
// difference is that this route also forwards a Google Maps URL.
//
// The upstream message is surfaced because it names the actual failure (an
// unreachable site, a blocked scrape), which the operator can act on.

import { enrichLeadFromUrl } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  readJsonBody,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/rasset-leads/enrich",
  async (request) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const body = await readJsonBody(request);

    try {
      return apiSuccess(
        await enrichLeadFromUrl({
          url: String(body.website || "").trim(),
          googleMapsUrl: String(body.google_maps_url || "").trim(),
        }),
      );
    } catch (error) {
      console.error("rasset lead enrich error:", error);
      return apiError(500, error.message || "Failed to enrich Rasset lead");
    }
  },
);
