// POST /api/clients/:clientId/leads/:leadId/call-recording — attach a call
// recording to a lead.
//
// Simpler than note-audio: the file is stored and its URL saved on the lead.
// No transcription, no note composition.
//
// The lead is scoped the same way as everywhere else — client_id is applied
// only when the business has one, since static lead businesses do not.

import {
  supabase,
  insertClientActivityLog,
  resolveClientLeadBusiness,
  resolveLeadSource,
  uploadLeadCallAudio,
} from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
  readUploadedFile,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/leads/[leadId]/call-recording",
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
    if (!file) return apiError(400, "Audio file is required");

    const orgId = orgIdForApi(user);

    // Default filename matches the original — the storage helper uses the
    // extension to pick a content type.
    const recordingUrl = await uploadLeadCallAudio(
      file.buffer,
      file.originalname || "call.mp3",
    );

    const business = await resolveClientLeadBusiness(orgId, clientId);
    const { tableName: leadTable, clientId: leadClientId } =
      resolveLeadSource(business);

    let query = supabase
      .from(leadTable)
      .update({
        call_recording_url: recordingUrl,
        updated_at: new Date().toISOString(),
      })
      .eq("org_id", orgId)
      .eq("id", leadId);
    if (leadClientId) query = query.eq("client_id", leadClientId);

    const { data, error } = await query.select("*").maybeSingle();

    if (error) {
      console.error("call recording update error:", error);
      return apiError(500, "Failed to save call recording");
    }
    if (!data) return apiError(404, "Lead not found");

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId: user?.id || null,
      action: "client_lead_call_recording_uploaded",
      entityType: leadTable,
      entityId: leadId,
      newValue: { call_recording_url: recordingUrl },
    });

    return apiSuccess(data);
  },
);
