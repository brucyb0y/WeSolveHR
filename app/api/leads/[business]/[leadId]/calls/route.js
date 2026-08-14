// GET /api/leads/:business/:leadId/calls — call history for a lead.
//
// ENVELOPE PRESERVED: replies `{success, calls}`. LeadCallsModal reads
// `json.calls` after checking `json.success`, so this must NOT be normalised
// to `{ok, data}` — the same shape that was a bug on the actions routes is the
// contract here. Check the caller, not the shape.
//
// Not scoped by org: lead_calls has no org_id column; business + lead_id is the
// key. Matches the original.

import { supabase } from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import { routeParams } from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request, ctx) {
  const { response } = await requireApiUser(request);
  if (response) return response;

  try {
    const p = await routeParams(ctx);

    const { data, error } = await supabase
      .from("lead_calls")
      .select("*")
      .eq("business", p.business)
      .eq("lead_id", Number(p.leadId))
      .order("created_at", { ascending: false });

    if (error) throw error;

    return Response.json({ success: true, calls: data || [] });
  } catch (error) {
    console.error("load lead calls error:", error);
    return Response.json(
      { success: false, error: error.message || "Failed to load calls" },
      { status: 500 },
    );
  }
}
