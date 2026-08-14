// PATCH /api/business-leads/:business/:id/quick-toggle — inline checkbox /
// stage edits from the leads table.
//
// The field name is checked against an ALLOW-LIST before being used as a
// column. Without that, the body could name any column and turn this into an
// arbitrary write.
//
// Value coercion depends on the field: `lead_stage` is free text, everything
// else is a checkbox and is compared with `=== true` so a truthy string cannot
// tick a box that was not ticked.

import {
  supabase,
  DASHBOARD_ORG_ID,
  getBusinessCanonicalName,
  getBusinessLeadTableName,
} from "@/lib/server/app";
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

const ALLOWED_FIELDS = ["l2_done", "qualified", "lead_stage"];

export const PATCH = withApiErrors(
  "PATCH /api/business-leads/[business]/[id]/quick-toggle",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const p = await routeParams(ctx);
    const business = getBusinessCanonicalName(p.business);
    const tableName = getBusinessLeadTableName(business);
    const leadId = Number(p.id);

    if (!tableName || !leadId) return apiError(400, "Invalid lead");

    const body = await readJsonBody(request);
    const field = String(body.field || "").trim();
    if (!ALLOWED_FIELDS.includes(field)) {
      return apiError(400, "Invalid checkbox field");
    }

    const value =
      field === "lead_stage"
        ? String(body.value || "").trim()
        : body.value === true;

    const { data, error } = await supabase
      .from(tableName)
      .update({ [field]: value, updated_at: new Date().toISOString() })
      .eq("org_id", DASHBOARD_ORG_ID)
      .eq("id", leadId)
      .select()
      .maybeSingle();

    if (error) {
      console.error("quick toggle error:", error);
      return apiError(500, error.message || "Failed to update lead checkbox");
    }
    if (!data) return apiError(404, "Lead not found");

    return apiSuccess(data);
  },
);
