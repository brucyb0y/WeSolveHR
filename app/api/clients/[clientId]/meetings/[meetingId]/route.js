// GET / PATCH /api/clients/:clientId/meetings/:meetingId
//
// GET deliberately does NOT filter on is_active — it is how the meeting editor
// loads a row, and an archived meeting must still be readable. PATCH does
// filter, so an archived meeting cannot be edited back into circulation.

import {
  supabase,
  insertClientActivityLog,
  buildClientMeetingPayloadFromBody,
} from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  readJsonBody,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const GET = withApiErrors(
  "GET /api/clients/[clientId]/meetings/[meetingId]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const params = await routeParams(ctx);
    const clientId = Number(params.clientId);
    const meetingId = Number(params.meetingId);
    if (!clientId || !meetingId) {
      return apiError(400, "Invalid client or meeting id");
    }

    const { data, error } = await supabase
      .from("client_meetings")
      .select("*")
      .eq("org_id", orgIdForApi(user))
      .eq("client_id", clientId)
      .eq("id", meetingId)
      .maybeSingle();

    if (error) {
      console.error("load meeting error:", error);
      return apiError(500, "Failed to load meeting");
    }
    if (!data) return apiError(404, "Meeting not found");

    return apiSuccess(data);
  },
);

export const PATCH = withApiErrors(
  "PATCH /api/clients/[clientId]/meetings/[meetingId]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const params = await routeParams(ctx);
    const clientId = Number(params.clientId);
    const meetingId = Number(params.meetingId);
    if (!clientId || !meetingId) {
      return apiError(400, "Invalid client or meeting id");
    }

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const body = await readJsonBody(request);
    const now = new Date().toISOString();

    const scope = (q) =>
      q.eq("org_id", orgId).eq("client_id", clientId).eq("id", meetingId);

    const { data: existing, error: existingError } = await scope(
      supabase.from("client_meetings").select("*"),
    )
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (existingError) {
      console.error("meeting lookup error:", existingError);
      return apiError(500, "Failed to update meeting");
    }
    if (!existing) return apiError(404, "Meeting not found");

    const patch =
      body.archive === true
        ? { is_active: false, deleted_at: now }
        : // Rebuilt from existing + body so a partial PATCH cannot reset the
          // MOM fields the payload builder defaults.
          buildClientMeetingPayloadFromBody({ ...existing, ...body });

    patch.updated_at = now;
    patch.last_updated_by_user_id = actorUserId;

    const { data, error } = await scope(
      supabase.from("client_meetings").update(patch),
    )
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("update meeting error:", error);
      return apiError(500, "Failed to update meeting");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: body.archive
        ? "client_meeting_archived"
        : "client_meeting_updated",
      entityType: "client_meetings",
      entityId: meetingId,
      oldValue: existing,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
