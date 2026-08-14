// POST /api/clients/:clientId/leads/:leadId/log-call — take the "call in
// progress" lock on a lead.
//
// IDEMPOTENT BY DESIGN. If the lead is already marked, the existing values are
// returned unchanged rather than re-stamping call_time. A double click or a
// stale page must not overwrite who actually took the call, or when.
//
// This is the lock LeadQuickUpdateModal acquires when its dialog opens; both
// Save and Cancel release it by writing is_call_made:false through the normal
// PATCH route.

import {
  supabase,
  insertClientActivityLog,
  resolveClientLeadBusiness,
  resolveLeadSource,
} from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const SELECT = "is_call_made, call_time, call_made_by";

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/leads/[leadId]/log-call",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const p = await routeParams(ctx);
    const clientId = Number(p.clientId);
    const leadId = Number(p.leadId);
    if (!clientId || !leadId) {
      return apiError(400, "Invalid client or lead id");
    }

    const orgId = orgIdForApi(user);
    const business = await resolveClientLeadBusiness(orgId, clientId);
    const { tableName: leadTable, clientId: leadClientId } =
      resolveLeadSource(business);

    const scopeLeadQuery = (query) => {
      let scoped = query.eq("org_id", orgId).eq("id", leadId);
      if (leadClientId) scoped = scoped.eq("client_id", leadClientId);
      return scoped;
    };

    const { data: current, error: readError } = await scopeLeadQuery(
      supabase.from(leadTable).select(SELECT),
    ).maybeSingle();

    if (readError) {
      console.error("log-call read error:", readError);
      return apiError(500, "Failed to log call");
    }
    if (!current) return apiError(404, "Lead not found");

    // Already locked — no-op.
    if (current.is_call_made) return apiSuccess(current);

    const callMadeBy = user?.name || user?.email || "Unknown";
    const now = new Date().toISOString();

    const { data, error } = await scopeLeadQuery(
      supabase.from(leadTable).update({
        is_call_made: true,
        call_time: now,
        call_made_by: callMadeBy,
        updated_at: now,
      }),
    )
      .select(SELECT)
      .maybeSingle();

    if (error) {
      console.error("log-call update error:", error);
      return apiError(500, "Failed to log call");
    }
    if (!data) return apiError(404, "Lead not found");

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId: user?.id || null,
      action: "client_lead_call_logged",
      entityType: leadTable,
      entityId: leadId,
      newValue: { call_time: now, call_made_by: callMadeBy },
    });

    return apiSuccess(data);
  },
);
