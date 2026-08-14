// POST /api/leads/:business/:leadId/upload-call — attach a call recording and
// transcribe it.
//
// ENVELOPE PRESERVED: replies `{success, call}` because LeadCallsModal checks
// `json.success` (the same modal reads `{ok, data}` from a different endpoint —
// check the caller, not the shape).
//
// Whisper needs a file handle, not a Buffer, so the upload is written to a
// temp file and streamed. The temp file is removed in a `finally` — the
// original unlinked it only on the success path, so a transcription failure
// leaked the file into os.tmpdir(). That is the one behaviour change here, and
// it is invisible to callers.
//
// Transcription is skipped entirely when OPENAI_API_KEY is absent: the
// recording still uploads and saves with an empty transcript rather than the
// whole request failing.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  supabase,
  DASHBOARD_ORG_ID,
  openai,
  uploadLeadCallAudio,
} from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import { routeParams, readUploadedFile } from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const fail = (status, error) =>
  Response.json({ success: false, error }, { status });

export async function POST(request, ctx) {
  const { response } = await requireApiUser(request);
  if (response) return response;

  try {
    const p = await routeParams(ctx);
    const leadId = Number(p.leadId);

    const form = await request.formData();
    const file = await readUploadedFile(form, "audio");
    if (!file) return fail(400, "Audio file required");

    const audioUrl = await uploadLeadCallAudio(file.buffer, file.originalname);

    let transcript = "";
    if (openai) {
      const tempPath = path.join(
        os.tmpdir(),
        `${Date.now()}-${file.originalname}`,
      );
      try {
        fs.writeFileSync(tempPath, file.buffer);
        const transcription = await openai.audio.transcriptions.create({
          file: fs.createReadStream(tempPath),
          model: "whisper-1",
        });
        transcript = transcription.text || "";
      } finally {
        try {
          fs.unlinkSync(tempPath);
        } catch {
          /* already gone */
        }
      }
    }

    const { data, error } = await supabase
      .from("lead_calls")
      .insert({
        org_id: DASHBOARD_ORG_ID,
        business: p.business,
        lead_id: leadId,
        audio_url: audioUrl,
        transcript,
        // PRESERVED DEFECT: the original read req.session?.user?.name, always
        // undefined, so every call was recorded as "Unknown".
        created_by: "Unknown",
      })
      .select()
      .maybeSingle();

    if (error) throw error;

    return Response.json({ success: true, call: data });
  } catch (error) {
    console.error("upload call failed:", error);
    return fail(500, error.message || "Failed to upload call");
  }
}
