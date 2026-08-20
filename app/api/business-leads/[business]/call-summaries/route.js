import {
  supabase,
  DASHBOARD_ORG_ID,
  getBusinessCanonicalName,
  getLeadPhoneKey,
} from "@/lib/server/app";
import { requireApiUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
  searchParamsToQuery,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const COLUMNS =
  "id, business, lead_phone, sender_phone, status, raw_transcript, cleaned_transcript, translated_text, conversation_rows, transcription_model, transcription_confidence, important_points, pain_points, follow_up_questions, review_notes, media_url, created_at, updated_at, verified_by, verified_at, spoke_to_name";

export const GET = withApiErrors(
  "GET /api/business-leads/[business]/call-summaries",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { business: rawBusiness } = await routeParams(ctx);
    const business = getBusinessCanonicalName(rawBusiness);
    if (!business) return apiError(400, "Invalid business");

    const phoneKey = getLeadPhoneKey(
      String(searchParamsToQuery(request).phone || "").trim(),
    );
    if (!phoneKey) return apiError(400, "Phone is required");

    const { data, error } = await supabase
      .from("lead_voice_uploads")
      .select(COLUMNS)
      .eq("org_id", DASHBOARD_ORG_ID)
      .eq("business", business)
      .order("created_at", { ascending: false });

    if (error) {
      console.error("call summaries error:", error);
      return apiError(500, error.message || "Failed to load call summaries");
    }

    return apiSuccess(
      (data || []).filter((row) => getLeadPhoneKey(row.lead_phone) === phoneKey),
    );
  },
);
