// POST /api/clients/:clientId/leads/:leadId/note-audio — attach a voice note.
//
// FIRST MULTIPART ROUTE. Express used `upload.single("audio")` (multer), which
// handed the handler `{buffer, originalname, mimetype}`. Native handlers get a
// web `File`, so readUploadedFile() materialises the Buffer — the storage and
// transcription helpers take a Node Buffer and cannot consume a stream.
//
// Everything else in the body arrives as STRINGS because it is form-encoded,
// which is why booleans are compared against "true" as well as true.
//
// TRANSCRIPTION IS BEST EFFORT. A failure there is logged and the note still
// saves with its audio — losing the recording because the model was
// unavailable would be far worse than a missing transcript.
//
// The route also carries optional status/demo/reached-via changes (the quick
// update and status-note modals reuse this flow), and emits the same
// client_lead_status_changed events as the JSON PATCH path — otherwise a stage
// change made alongside a voice note would vanish from the funnel report.

import {
  supabase,
  normalizeText,
  insertClientActivityLog,
  resolveClientLeadBusiness,
  resolveLeadSource,
  appendLeadNote,
  uploadLeadNoteAudio,
  transcribeAudioBuffer,
  HINGLISH_TRANSCRIPTION_PROMPT,
  CLIENT_LEAD_PIPELINE_STAGES,
  CLIENT_LEAD_DEMO_STATUSES,
  REACH_VIA_CHANNELS,
} from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
  readUploadedFile,
  formToBody,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/leads/[leadId]/note-audio",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const p = await routeParams(ctx);
    const clientId = Number(p.clientId);
    const leadId = Number(p.leadId);
    if (!clientId || !leadId) {
      return apiError(400, "Invalid client or lead id");
    }

    const form = await request.formData();
    const file = await readUploadedFile(form, "audio");
    if (!file) return apiError(400, "No audio file uploaded");

    const body = formToBody(form);
    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;

    const business = await resolveClientLeadBusiness(orgId, clientId);
    const { tableName: leadTable, clientId: leadClientId } =
      resolveLeadSource(business);

    const scopeLeadQuery = (query) => {
      let scoped = query.eq("org_id", orgId).eq("id", leadId);
      if (leadClientId) scoped = scoped.eq("client_id", leadClientId);
      return scoped;
    };

    const { data: current, error: readError } = await scopeLeadQuery(
      supabase.from(leadTable).select("notes, pipeline_stage, demo_status"),
    ).maybeSingle();

    if (readError) {
      console.error("note-audio lead read error:", readError);
      return apiError(500, "Failed to save voice note");
    }
    if (!current) return apiError(404, "Lead not found");

    // ---- optional status changes carried with the note --------------------
    const statusPatch = {};

    if (body.pipeline_stage !== undefined) {
      const stage = normalizeText(body.pipeline_stage || "");
      if (!CLIENT_LEAD_PIPELINE_STAGES.map((s) => s.key).includes(stage)) {
        return apiError(400, "Invalid pipeline stage");
      }
      statusPatch.pipeline_stage = stage;
    }
    if (body.demo_status !== undefined) {
      const demo = normalizeText(body.demo_status || "");
      if (!CLIENT_LEAD_DEMO_STATUSES.map((s) => s.key).includes(demo)) {
        return apiError(400, "Invalid demo status");
      }
      statusPatch.demo_status = demo;
    }
    if (body.callback_date !== undefined) {
      const callbackDate = String(body.callback_date || "").trim();
      if (callbackDate && !/^\d{4}-\d{2}-\d{2}$/.test(callbackDate)) {
        return apiError(400, "Invalid callback date");
      }
      statusPatch.callback_date = callbackDate || null;
    }
    for (const c of REACH_VIA_CHANNELS) {
      if (body[c.column] !== undefined) {
        statusPatch[c.column] =
          body[c.column] === true || body[c.column] === "true";
      }
    }
    if (body.is_call_made !== undefined) {
      statusPatch.is_call_made =
        body.is_call_made === true || body.is_call_made === "true";
    }

    // 1) Store the audio so the note can be played back later.
    const audioUrl = await uploadLeadNoteAudio(
      file.buffer,
      file.originalname,
      file.mimetype,
    );

    // 2) Transcribe — best effort.
    let transcript = "";
    try {
      transcript = (
        await transcribeAudioBuffer({
          buffer: file.buffer,
          contentType: file.mimetype,
          fileName: file.originalname,
          prompt: HINGLISH_TRANSCRIPTION_PROMPT,
        })
      ).trim();
    } catch (transcribeError) {
      console.error("lead note audio transcription failed:", transcribeError);
    }

    // 3) Compose typed note + transcription.
    const typed = String(body.text || "").trim();
    const parts = [];
    if (typed) parts.push(typed);
    if (transcript) parts.push(`🗣 Transcription:\n${transcript}`);
    const noteText = parts.join("\n\n") || "🎙 Voice note";

    const notes = appendLeadNote(
      current.notes,
      noteText,
      user?.name || user?.email || null,
      { audio_url: audioUrl },
    );

    const { data, error } = await scopeLeadQuery(
      supabase
        .from(leadTable)
        .update({ ...statusPatch, notes, updated_at: new Date().toISOString() }),
    )
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("note-audio update error:", error);
      return apiError(500, "Failed to save voice note");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_lead_updated",
      entityType: leadTable,
      entityId: leadId,
      newValue: {
        add_voice_note: true,
        transcribed: Boolean(transcript),
        ...statusPatch,
      },
    });

    for (const field of ["pipeline_stage", "demo_status"]) {
      if (statusPatch[field] === undefined) continue;
      const from = current[field] || null;
      const to = statusPatch[field];
      if (from === to) continue;
      await insertClientActivityLog({
        orgId,
        clientId,
        actorUserId,
        action: "client_lead_status_changed",
        entityType: leadTable,
        entityId: leadId,
        oldValue: { field, value: from },
        newValue: { field, from, to },
      });
    }

    return apiSuccess(data);
  },
);
