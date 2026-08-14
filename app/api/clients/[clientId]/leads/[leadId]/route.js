// GET / PATCH / DELETE /api/clients/:clientId/leads/:leadId
//
// LEAD STORAGE IS NOT UNIFORM. A client mapped to an inline static business
// (e.g. Rasset) writes to that business's own table with no client_id filter;
// every other client uses per-client client_leads rows. resolveLeadSource()
// resolves which, and scopeLeadQuery() applies the client_id filter ONLY when
// there is one — adding it unconditionally would silently match zero rows for
// static businesses.
//
// PATCH HAS TWO PATHS:
//   * light  — every key is in LIGHT_KEYS (inline dropdowns, toggles, notes,
//              bulk assign). Validated and written directly here.
//   * full   — anything else goes through the shared updateBusinessLead engine,
//              which the lead form uses.
// Both record one `client_lead_status_changed` event per changed status
// dimension. That is not bookkeeping: the funnel report is built from those
// events, so a form-driven stage change that skipped them would silently
// vanish from the funnel while the lead itself moved.

import {
  supabase,
  normalizeText,
  insertClientActivityLog,
  resolveClientLeadBusiness,
  resolveLeadSource,
  tableHasClientLeadColumns,
  appendLeadNote,
  updateBusinessLead,
  getBusinessLeadById,
  CLIENT_LEAD_PIPELINE_STAGES,
  CLIENT_LEAD_OUTREACH_STATUSES,
  CLIENT_LEAD_DEMO_STATUSES,
  CLIENT_LEAD_CATEGORY_TYPES,
  REACH_VIA_CHANNELS,
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

const TRACKED_STATUS_FIELDS = [
  "pipeline_stage",
  "outreach_status",
  "demo_status",
];

async function resolveIds(ctx) {
  const params = await routeParams(ctx);
  return {
    clientId: Number(params.clientId),
    leadId: Number(params.leadId),
  };
}

export const GET = withApiErrors(
  "GET /api/clients/[clientId]/leads/[leadId]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId, leadId } = await resolveIds(ctx);
    if (!clientId || !leadId) {
      return apiError(400, "Invalid client or lead id");
    }

    const orgId = orgIdForApi(user);
    const business = await resolveClientLeadBusiness(orgId, clientId);
    const lead = await getBusinessLeadById({ orgId, business, leadId });

    if (!lead) return apiError(404, "Lead not found");
    return apiSuccess(lead);
  },
);

export const PATCH = withApiErrors(
  "PATCH /api/clients/[clientId]/leads/[leadId]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId, leadId } = await resolveIds(ctx);
    if (!clientId || !leadId) {
      return apiError(400, "Invalid client or lead id");
    }

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;
    const body = await readJsonBody(request);

    const business = await resolveClientLeadBusiness(orgId, clientId);
    const { tableName: leadTable, clientId: leadClientId } =
      resolveLeadSource(business);

    const scopeLeadQuery = (query) => {
      let scoped = query.eq("org_id", orgId).eq("id", leadId);
      if (leadClientId) scoped = scoped.eq("client_id", leadClientId);
      return scoped;
    };

    const logStatusTransitions = async (previous, next) => {
      for (const field of TRACKED_STATUS_FIELDS) {
        const from = previous[field] || null;
        const to = next[field];
        if (!to || from === to) continue;
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
    };

    const LIGHT_KEYS = [
      "pipeline_stage",
      "is_client_visible",
      "outreach_status",
      "demo_status",
      "is_starred",
      "is_call_made",
      "call_recording_url",
      "notes",
      "add_note",
      "callback_date",
      "assigned_to",
      "phone_assigned_to",
      "email_assigned_to",
      "category_type",
      ...REACH_VIA_CHANNELS.map((c) => c.column),
    ];

    const keys = Object.keys(body);
    const isLightPatch =
      keys.length > 0 && keys.every((k) => LIGHT_KEYS.includes(k));

    // ---- light path -------------------------------------------------------
    if (isLightPatch) {
      const lightPatch = { updated_at: new Date().toISOString() };

      const enumField = (key, list, message) => {
        const value = normalizeText(body[key] || "");
        if (!list.map((s) => s.key).includes(value)) return message;
        lightPatch[key] = value;
        return null;
      };

      if (body.pipeline_stage !== undefined) {
        const err = enumField(
          "pipeline_stage",
          CLIENT_LEAD_PIPELINE_STAGES,
          "Invalid pipeline stage",
        );
        if (err) return apiError(400, err);
      }
      if (body.outreach_status !== undefined) {
        const err = enumField(
          "outreach_status",
          CLIENT_LEAD_OUTREACH_STATUSES,
          "Invalid outreach status",
        );
        if (err) return apiError(400, err);
      }
      if (body.demo_status !== undefined) {
        const err = enumField(
          "demo_status",
          CLIENT_LEAD_DEMO_STATUSES,
          "Invalid demo status",
        );
        if (err) return apiError(400, err);
      }
      if (body.category_type !== undefined) {
        const value = normalizeText(body.category_type || "");
        if (
          value &&
          !CLIENT_LEAD_CATEGORY_TYPES.map((c) => c.key).includes(value)
        ) {
          return apiError(400, "Invalid category type");
        }
        // Empty clears the category.
        lightPatch.category_type = value || null;
      }

      if (body.callback_date !== undefined) {
        const callbackDate = String(body.callback_date || "").trim();
        if (callbackDate && !/^\d{4}-\d{2}-\d{2}$/.test(callbackDate)) {
          return apiError(400, "Invalid callback date");
        }
        lightPatch.callback_date = callbackDate || null;
      }

      for (const key of ["assigned_to", "phone_assigned_to", "email_assigned_to"]) {
        if (body[key] !== undefined) {
          lightPatch[key] = String(body[key] || "").trim() || null;
        }
      }

      // Booleans accept the string "true" as well — these arrive from both
      // JSON and multipart (the voice-note upload) call sites.
      for (const key of ["is_client_visible", "is_starred", "is_call_made"]) {
        if (body[key] !== undefined) {
          lightPatch[key] = body[key] === true || body[key] === "true";
        }
      }
      for (const c of REACH_VIA_CHANNELS) {
        if (body[c.column] !== undefined) {
          lightPatch[c.column] =
            body[c.column] === true || body[c.column] === "true";
        }
      }

      if (body.call_recording_url !== undefined) {
        lightPatch.call_recording_url =
          String(body.call_recording_url || "").trim() || null;
      }
      if (body.notes !== undefined) {
        lightPatch.notes = String(body.notes || "").trim() || null;
      }

      // add_note APPENDS to the history rather than replacing `notes`.
      if (body.add_note !== undefined && String(body.add_note).trim()) {
        const { data: current, error: readError } = await scopeLeadQuery(
          supabase.from(leadTable).select("notes"),
        ).maybeSingle();

        if (readError) {
          console.error("lead note read error:", readError);
          return apiError(500, "Failed to update lead");
        }
        if (!current) return apiError(404, "Lead not found");

        lightPatch.notes = appendLeadNote(
          current.notes,
          body.add_note,
          user?.name || user?.email || null,
        );
      }

      // Read the previous status values BEFORE the update, or the transition
      // events would compare a value against itself.
      const changingFields = TRACKED_STATUS_FIELDS.filter(
        (f) => lightPatch[f] !== undefined,
      );
      let previousStatus = {};
      if (changingFields.length) {
        const { data: prev, error: prevError } = await scopeLeadQuery(
          supabase.from(leadTable).select(changingFields.join(", ")),
        ).maybeSingle();
        if (prevError) {
          console.error("lead status read error:", prevError);
          return apiError(500, "Failed to update lead");
        }
        previousStatus = prev || {};
      }

      const { data, error } = await scopeLeadQuery(
        supabase.from(leadTable).update(lightPatch),
      )
        .select("*")
        .maybeSingle();

      if (error) {
        console.error("lead light update error:", error);
        return apiError(500, "Failed to update lead");
      }
      if (!data) return apiError(404, "Lead not found");

      await insertClientActivityLog({
        orgId,
        clientId,
        actorUserId,
        action: "client_lead_updated",
        entityType: leadTable,
        entityId: leadId,
        newValue: lightPatch,
      });

      await logStatusTransitions(previousStatus, lightPatch);

      return apiSuccess(data);
    }

    // ---- full path --------------------------------------------------------
    let previousFullStatus = {};
    if (tableHasClientLeadColumns(leadTable)) {
      const { data: prevFull, error: prevFullError } = await scopeLeadQuery(
        supabase
          .from(leadTable)
          .select("pipeline_stage, outreach_status, demo_status"),
      ).maybeSingle();
      if (prevFullError) {
        console.error("lead full status read error:", prevFullError);
        return apiError(500, "Failed to update lead");
      }
      previousFullStatus = prevFull || {};
    }

    try {
      const lead = await updateBusinessLead({ orgId, business, leadId, body });

      await insertClientActivityLog({
        orgId,
        clientId,
        actorUserId,
        action: "client_lead_updated",
        entityType: leadTable,
        entityId: leadId,
        newValue: lead,
      });

      await logStatusTransitions(previousFullStatus, lead || {});

      return apiSuccess(lead);
    } catch (err) {
      console.error("lead full update error:", err);
      return apiError(err.statusCode || 500, err.message || "Failed to update lead");
    }
  },
);

export const DELETE = withApiErrors(
  "DELETE /api/clients/[clientId]/leads/[leadId]",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId, leadId } = await resolveIds(ctx);
    if (!clientId || !leadId) {
      return apiError(400, "Invalid client or lead id");
    }

    const orgId = orgIdForApi(user);
    const business = await resolveClientLeadBusiness(orgId, clientId);
    const { tableName: leadTable, clientId: leadClientId } =
      resolveLeadSource(business);

    // A HARD delete, matching the original — leads have no soft-delete column.
    let deleteQuery = supabase
      .from(leadTable)
      .delete()
      .eq("org_id", orgId)
      .eq("id", leadId);
    if (leadClientId) deleteQuery = deleteQuery.eq("client_id", leadClientId);

    const { data, error } = await deleteQuery.select("*").maybeSingle();

    if (error) {
      console.error("lead delete error:", error);
      return apiError(500, "Failed to delete lead");
    }
    if (!data) return apiError(404, "Lead not found");

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId: user?.id || null,
      action: "client_lead_deleted",
      entityType: leadTable,
      entityId: leadId,
      oldValue: data,
    });

    return apiSuccess(data);
  },
);
