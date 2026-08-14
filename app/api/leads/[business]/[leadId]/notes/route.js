// POST /api/leads/:business/:leadId/notes — append a note to a business lead.
//
// Keeps the `{success, note}` envelope of the original. No React caller was
// found for this endpoint, so there is nothing to migrate to the standard
// shape and no benefit in changing it.
//
// Columns are `note_text` and `created_by` (a NAME string, not a user id).
//
// PRESERVED DEFECT: `created_by` came from `req.session?.user?.name`, which was
// always undefined because the session stores `userId` — so every note was
// authored by "Unknown". Kept as-is rather than quietly changing what gets
// written; worth fixing deliberately if this endpoint is ever used.

import { supabase, DASHBOARD_ORG_ID } from "@/lib/server/app";
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
    const leadId = Number(p.leadId);
    const body = await readJsonBody(request);
    const noteText = String(body.note || "").trim();

    if (!noteText) return fail(400, "Note required");

    const { data, error } = await supabase
      .from("lead_notes")
      .insert({
        org_id: DASHBOARD_ORG_ID,
        business: p.business,
        lead_id: leadId,
        note_text: noteText,
        created_by: "Unknown",
      })
      .select()
      .maybeSingle();

    if (error) {
      console.error("add lead note failed:", error);
      return fail(500, error.message);
    }

    return Response.json({ success: true, note: data });
  } catch (error) {
    console.error("add lead note failed:", error);
    return fail(500, error.message || "Failed to add note");
  }
}
