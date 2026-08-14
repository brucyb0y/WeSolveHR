// POST /api/clients/:id/goals — upsert the curated Goals block.
//
// Staff-only write; the client sees the result read-only on their external
// dashboard, which is why every save records who made it.
//
// The caps are deliberate and preserved exactly: title 300 chars, value 100,
// notes 20 000, and at most 100 goals. They bound what the client-facing panel
// can be made to render, so relaxing any of them changes what a customer sees.
//
// Rows where BOTH title and value are blank are dropped, so the editor's
// trailing empty row never becomes a stored goal.
//
// Upsert keys on (org_id, client_id) — one goals row per client, so re-saving
// updates in place rather than accumulating history.

import {
  supabase,
  DASHBOARD_ORG_ID,
  insertClientActivityLog,
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

export const POST = withApiErrors(
  "POST /api/clients/[clientId]/goals",
  async (request, ctx) => {
    const { user, response } = await requireApiUser(request);
    if (response) return response;

    const { clientId: rawId } = await routeParams(ctx);
    const clientId = Number(rawId);
    if (!clientId) return apiError(400, "Invalid client id");

    const orgId = orgIdForApi(user);
    const body = await readJsonBody(request);

    const rawItems = Array.isArray(body?.goals_json) ? body.goals_json : [];
    const goalsJson = rawItems
      .map((g) => ({
        title: String(g?.title ?? "")
          .trim()
          .slice(0, 300),
        value: String(g?.value ?? "")
          .trim()
          .slice(0, 100),
      }))
      .filter((g) => g.title || g.value)
      .slice(0, 100);

    const notes = String(body?.notes ?? "").slice(0, 20000);

    // Confirm the client exists in this org before writing — a bad id must 404
    // rather than create an orphan goals row.
    const { data: client, error: clientError } = await supabase
      .from("clients")
      .select("id")
      .eq("org_id", orgId)
      .eq("id", clientId)
      .is("deleted_at", null)
      .maybeSingle();

    if (clientError) {
      console.error("goals client lookup error:", clientError);
      return apiError(500, "Failed to save goals");
    }
    if (!client) return apiError(404, "Client not found");

    const { data: saved, error } = await supabase
      .from("client_goals")
      .upsert(
        [
          {
            org_id: orgId,
            client_id: clientId,
            goals_json: goalsJson,
            notes,
            updated_by_user_id: user?.id || null,
            updated_at: new Date().toISOString(),
          },
        ],
        { onConflict: "org_id,client_id" },
      )
      .select()
      .single();

    if (error) {
      console.error("goals upsert error:", error);
      return apiError(500, error.message || "Failed to save goals");
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId: user?.id || null,
      action: "client_goals_updated",
      entityType: "client_goals",
      entityId: saved?.id || null,
    });

    return apiSuccess(saved);
  },
);
