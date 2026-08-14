// GET /api/business-leads/:business/check-phone — duplicate-phone lookup.
//
// Comparison is on a NORMALISED key, not the raw string: the same number is
// stored with different punctuation and country prefixes across imports, so a
// direct `.eq("phone", …)` would miss most real duplicates. That is also why
// the rows are filtered in JS rather than in the query.
//
// An empty/unparseable phone returns `{duplicate:false}` rather than an error —
// the form calls this on every keystroke and must not show errors mid-typing.
//
// PRESERVED DEFECT: the original read `req.session?.user?.org_id`, but the
// session stores `userId`, never `user` — so this ALWAYS fell back to
// DASHBOARD_ORG_ID. Kept as DASHBOARD_ORG_ID rather than silently switching to
// the caller's org, which would change behaviour in a multi-org deployment.

import {
  supabase,
  DASHBOARD_ORG_ID,
  getBusinessCanonicalName,
  getBusinessLeadTableName,
  normalizeLeadPhone,
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

export const GET = withApiErrors(
  "GET /api/business-leads/[business]/check-phone",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { business: rawBusiness } = await routeParams(ctx);
    const business = getBusinessCanonicalName(rawBusiness);
    const tableName = getBusinessLeadTableName(business);
    if (!tableName) return apiError(400, "Invalid business");

    const rawPhone = searchParamsToQuery(request).phone || "";
    if (!normalizeLeadPhone(rawPhone)) return apiSuccess({ duplicate: false });

    const { data, error } = await supabase
      .from(tableName)
      .select(
        "id, phone, company, business_name, contact_name, city, state, status, lead_stage",
      )
      .eq("org_id", DASHBOARD_ORG_ID);

    if (error) {
      console.error("check-phone error:", error);
      return apiError(500, error.message || "Failed to check phone");
    }

    const phoneKey = getLeadPhoneKey(rawPhone);
    const duplicate = (data || []).find(
      (row) => getLeadPhoneKey(row.phone) === phoneKey,
    );

    return apiSuccess({ duplicate: !!duplicate, lead: duplicate || null });
  },
);
