import { supabase, ACCOUNT_FIELD_OPTIONS } from "@/lib/server/app";
import { requireSessionUser } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  readJsonBody,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export const POST = withApiErrors(
  "POST /api/account/profile-field",
  async (request) => {
    const { user, response } = await requireSessionUser();
    if (response) return response;

    const body = await readJsonBody(request);
    const field = String(body.field || "").trim();
    const value = String(body.value || "").trim();

    const options = ACCOUNT_FIELD_OPTIONS[field];
    if (!options) return apiError(400, "Invalid field");
    if (value && !options.includes(value)) {
      return apiError(400, `Invalid ${field}`);
    }

    const { data, error } = await supabase
      .from("users")
      .update({ [field]: value || null })
      .eq("id", user.id)
      .eq("org_id", user.org_id)
      .select(`id, ${field}`)
      .maybeSingle();

    if (error) {
      console.error("update profile field error:", error);
      return apiError(500, error.message || "Failed to update profile field");
    }
    if (!data) return apiError(404, "User not found");

    return apiSuccess(data);
  },
);
