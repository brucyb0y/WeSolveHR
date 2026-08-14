// PATCH /api/leads/:id/transcript — save a human-reviewed transcript.
//
// Same [business]-folder / lead-voice-id naming gotcha as transcribe.
//
// cleaned_transcript is REQUIRED (this is the reviewed text, and saving an
// empty one would erase the review); translated_text and review_notes are
// optional and may legitimately be blank.

import { DASHBOARD_ORG_ID, updateLeadVoiceTranscript } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  readJsonBody,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const PATCH = withApiErrors(
  "PATCH /api/leads/[id]/transcript",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const p = await routeParams(ctx);
    const leadVoiceId = Number(p.business);
    if (!leadVoiceId) return apiError(400, "Invalid lead voice ID");

    const body = await readJsonBody(request);
    const cleanedTranscript = String(body.cleaned_transcript || "").trim();
    if (!cleanedTranscript) {
      return apiError(400, "Cleaned transcript is required");
    }

    try {
      return apiSuccess(
        await updateLeadVoiceTranscript({
          leadVoiceId,
          orgId: DASHBOARD_ORG_ID,
          cleanedTranscript,
          translatedText: String(body.translated_text || "").trim(),
          reviewNotes: String(body.review_notes || "").trim(),
        }),
      );
    } catch (error) {
      console.error("update transcript error:", error);
      return apiError(500, error.message || "Failed to update transcript");
    }
  },
);
