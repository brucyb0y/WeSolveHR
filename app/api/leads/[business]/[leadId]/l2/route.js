// POST /api/leads/:business/:leadId/l2 — save the L2 qualification call.
//
// ENVELOPE DELIBERATELY NOT NORMALISED. This replies `{success, lead}`, and
// LeadCallsModal checks `json.success` — unlike the actions/contributors
// routes, whose callers check `json.ok` and where the `{success}` shape was
// the bug. Always check what the CALLER reads before changing an envelope.
//
// Fields are applied only when present (`!== undefined`), and several are
// renamed on the way in — spoke_to_name -> contact_name, capability ->
// manufacturing_capabilities — so the request body and the column names are
// not the same vocabulary.
//
// l2_done is forced true: completing this form IS the L2 being done.

import {
  supabase,
  DASHBOARD_ORG_ID,
  getBusinessCanonicalName,
  getBusinessLeadTableName,
} from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import { readJsonBody, routeParams } from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const fail = (status, error) =>
  Response.json({ success: false, error }, { status });

export async function POST(request, ctx) {
  const { response } = await requireApiUser(request);
  if (response) return response;

  try {
    const p = await routeParams(ctx);
    const business = getBusinessCanonicalName(p.business);
    const tableName = getBusinessLeadTableName(business);
    const leadId = Number(p.leadId);

    if (!tableName) return fail(400, "Invalid business");

    const body = await readJsonBody(request);
    const updatePayload = { updated_at: new Date().toISOString() };

    // [body key, column] — the two vocabularies differ.
    const FIELDS = [
      ["spoke_to_name", "contact_name"],
      ["designation", "contact_designation"],
      ["industry", "industry"],
      ["capability", "manufacturing_capabilities"],
      ["behavior", "behavior"],
      ["call_outcome", "last_call_outcome"],
      ["notes", "notes"],
    ];
    for (const [key, column] of FIELDS) {
      if (body[key] !== undefined) updatePayload[column] = body[key];
    }
    updatePayload.l2_done = true;

    const { data, error } = await supabase
      .from(tableName)
      .update(updatePayload)
      .eq("org_id", DASHBOARD_ORG_ID)
      .eq("id", leadId)
      .select()
      .maybeSingle();

    if (error) {
      console.error("save l2 failed:", error);
      return fail(500, error.message);
    }
    if (!data) return fail(404, "Lead not found");

    return Response.json({ success: true, lead: data });
  } catch (error) {
    console.error("save l2 failed:", error);
    return fail(500, error.message || "Failed to save L2");
  }
}
