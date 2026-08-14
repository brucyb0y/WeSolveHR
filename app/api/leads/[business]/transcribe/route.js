// POST /api/leads/:id/transcribe — run Whisper over a stored voice upload.
//
// NAMING GOTCHA: the folder is [business] but the value is a LEAD VOICE ID —
// Express registered "/api/leads/:id/transcribe", and Next allows only one
// dynamic name per level (its siblings genuinely are businesses).
//
// STATUS IS A STATE MACHINE, and the failure path matters: the row is set to
// "transcribing" before the work starts, and on failure is returned to
// "pending_transcription" with the error recorded. Without that reset a failed
// run would leave the upload stuck in "transcribing" forever, invisible to
// retry.

import {
  supabase,
  DASHBOARD_ORG_ID,
  transcribeLeadVoiceUploadById,
} from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/leads/[id]/transcribe",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const p = await routeParams(ctx);
    const leadVoiceId = Number(p.business);
    if (!leadVoiceId) return apiError(400, "Invalid voice lead id");

    const orgId = DASHBOARD_ORG_ID;

    const { data: voice, error: voiceError } = await supabase
      .from("lead_voice_uploads")
      .select("*")
      .eq("org_id", orgId)
      .eq("id", leadVoiceId)
      .maybeSingle();

    if (voiceError) {
      console.error("transcribe lookup error:", voiceError);
      return apiError(500, "Failed to transcribe voice lead");
    }
    if (!voice) return apiError(404, "Voice lead not found");
    if (!voice.media_url) return apiError(400, "Voice lead has no media_url");

    await supabase
      .from("lead_voice_uploads")
      .update({ status: "transcribing", updated_at: new Date().toISOString() })
      .eq("org_id", orgId)
      .eq("id", leadVoiceId);

    try {
      const result = await transcribeLeadVoiceUploadById({
        leadVoiceId,
        orgId,
      });
      return apiSuccess(result || { message: "Transcribed" });
    } catch (error) {
      console.error("transcribe voice lead error:", error);
      // Release the "transcribing" state so the upload can be retried.
      await supabase
        .from("lead_voice_uploads")
        .update({
          status: "pending_transcription",
          transcription_error: String(error.message || error),
          updated_at: new Date().toISOString(),
        })
        .eq("id", leadVoiceId);
      return apiError(500, error.message || "Failed to transcribe voice lead");
    }
  },
);
