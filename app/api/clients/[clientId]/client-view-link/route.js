import { supabase, insertClientActivityLog, generateClientViewToken } from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";
import {
  apiSuccess,
  apiError,
  withApiErrors,
  routeParams,
} from "@/lib/api/respond";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function baseUrlFor(request) {
  const h = request.headers;
  const host = h.get("x-forwarded-host") || h.get("host");
  if (!host) return new URL(request.url).origin;
  const proto =
    h.get("x-forwarded-proto") || new URL(request.url).protocol.replace(":", "");
  return `${proto}://${host}`;
}

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/client-view-link",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);
    if (!clientId) return apiError(400, "Invalid client id");

    const orgId = orgIdForApi(user);

    const { data: existingClient, error: loadError } = await supabase
      .from("clients")
      .select("id, org_id, client_view_token, client_view_enabled")
      .eq("org_id", orgId)
      .eq("id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (loadError) {
      console.error("client view link lookup error:", loadError);
      return apiError(500, "Failed to create client view link");
    }
    if (!existingClient) return apiError(404, "Client not found");

    const token = existingClient.client_view_token || generateClientViewToken();

    const { data, error } = await supabase
      .from("clients")
      .update({ client_view_token: token, client_view_enabled: true })
      .eq("org_id", orgId)
      .eq("id", clientId)
      .select("id, client_view_token")
      .maybeSingle();

    if (error) {
      console.error("client view link update error:", error);
      return apiError(500, "Failed to create client view link");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId: user?.id || null,
      action: "client_view_link_enabled",
      entityType: "clients",
      entityId: clientId,
      newValue: data,
    });

    return apiSuccess({
      url: `${baseUrlFor(request)}/client-view/${data.client_view_token}`,
      token: data.client_view_token,
    });
  },
);
