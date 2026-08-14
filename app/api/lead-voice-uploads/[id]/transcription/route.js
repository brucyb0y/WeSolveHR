// DELETE /api/lead-voice-uploads/:id/transcription — clear a transcription so
// the recording can be re-processed.
//
// This does NOT delete the upload: it blanks every derived field and returns
// the row to "pending_transcription", leaving media_url intact so the pipeline
// can run again. Deleting the row instead would lose the audio.
//
// Array fields are reset to [] rather than null — the readers iterate them.

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

export const DELETE = withApiErrors(
  "DELETE /api/lead-voice-uploads/[id]/transcription",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { id: raw } = await routeParams(ctx);
    const id = Number(raw);
    if (!id) return apiError(400, "Invalid voice lead ID");

    const { data, error } = await supabase
      .from("lead_voice_uploads")
      .update({
        raw_transcript: null,
        cleaned_transcript: null,
        translated_text: null,
        detected_language: null,
        conversation_rows: [],
        important_points: [],
        pain_points: [],
        follow_up_questions: [],
        review_notes: null,
        transcription_confidence: null,
        transcription_model: null,
        transcription_chunked: false,
        transcription_chunk_seconds: null,
        status: "pending_transcription",
        updated_at: new Date().toISOString(),
      })
      .eq("org_id", DASHBOARD_ORG_ID)
      .eq("id", id)
      .select()
      .maybeSingle();

    if (error) {
      console.error("delete transcription error:", error);
      return apiError(500, error.message || "Failed to delete transcription");
    }
    if (!data) return apiError(404, "Voice lead not found");

    return apiSuccess(data);
  },
);
