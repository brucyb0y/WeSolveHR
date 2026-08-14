// GET /api/leads/voice/:id/debug — raw voice-upload row, for troubleshooting
// the transcription pipeline.

import { supabase, DASHBOARD_ORG_ID } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const GET = withApiErrors(
  "GET /api/leads/voice/[id]/debug",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { id: raw } = await routeParams(ctx);
    const id = Number(raw);

    const { data, error } = await supabase
      .from("lead_voice_uploads")
      .select("*")
      .eq("org_id", DASHBOARD_ORG_ID)
      .eq("id", id)
      .maybeSingle();

    if (error) return apiError(500, error.message);
    if (!data) return apiError(404, "Voice lead not found");

    return apiSuccess(data);
  },
);
