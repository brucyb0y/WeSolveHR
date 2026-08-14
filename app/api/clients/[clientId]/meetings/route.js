// POST /api/clients/:clientId/meetings — log a meeting.
//
// Requires a title OR a date, not both: a call logged in the moment often has
// only one of them, and the MOM fields get filled in later.

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

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/meetings",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);
    if (!clientId) return apiError(400, "Invalid client id");

    const payload = buildClientMeetingPayloadFromBody(
      await readJsonBody(request),
    );
    if (!payload.title && !payload.meeting_date) {
      return apiError(400, "Meeting title or date is required");
    }

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;

    const { data, error } = await supabase
      .from("client_meetings")
      .insert([
        {
          org_id: orgId,
          client_id: clientId,
          ...payload,
          is_active: true,
          created_by_user_id: actorUserId,
          last_updated_by_user_id: actorUserId,
          updated_at: new Date().toISOString(),
        },
      ])
      .select("*")
      .maybeSingle();

    if (error) {
      console.error("create meeting error:", error);
      return apiError(500, "Failed to create meeting");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_meeting_created",
      entityType: "client_meetings",
      entityId: data.id,
      newValue: data,
    });

    return apiSuccess(data);
  },
);
