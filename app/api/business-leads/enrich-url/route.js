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
  "POST /api/business-leads/enrich-url",
  async (request) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const body = await readJsonBody(request);

    try {
      return apiSuccess(
        await enrichLeadFromUrl({
          url: String(body.url || "").trim(),
          googleMapsUrl: "",
        }),
      );
    } catch (error) {
      console.error("enrich url error:", error);
      return apiError(500, error.message || "Failed to enrich URL");
    }
  },
);
