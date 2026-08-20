import { supabase } from "@/lib/server/app";
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

const update = withApiErrors(
  "PATCH /api/clients/[clientId]/actions/[actionId]",
  async (request, ctx) => {
    const { response } = await requireApiUser(request);
    if (response) return response;

    const { clientId, actionId } = await routeParams(ctx);
    const body = await readJsonBody(request);

    const { data, error } = await supabase
      .from("client_actions")
      .update({
        title: body.title,
        owner_type: body.owner_type,
        owner_name: body.owner_name || null,
        due_date: body.due_date || null,
        status: body.status || "Open",
        priority: body.priority || "Medium",
        notes: body.notes || null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", actionId)
      .eq("client_id", clientId)
      .select()
      .maybeSingle();

    if (error) {
      console.error("update action error:", error);
      return apiError(500, "Failed to update action");
    }
    if (!data) return apiError(404, "Action not found");

    await supabase.from("client_updates").insert({
      client_id: clientId,
      update_text: `Action updated: ${data.title}`,
      update_type: "action",
    });

    return apiSuccess(data);
  },
);

export const PATCH = update;
export const PUT = update;
