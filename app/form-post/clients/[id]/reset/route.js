// POST /clients/:id/reset — ported from lib/server/app.js lines 43739-43900.
// Reached through the rewrite in middleware.js.

import { assertRewritten, readFormBody, redirectTo } from "@/lib/server/form-post.js";
import { requireDashboardAuthApi } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import { supabase } from "@/lib/server/supabase.js";
import { escapeHtml } from "@/lib/ui/html.js";
import { insertClientActivityLog } from "@/lib/data/activity-log.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request, context) {
  const blocked = assertRewritten(request);
  if (blocked) return blocked;

  const params = await context.params;
  const formBody = await readFormBody(request);
  const auth = await requireDashboardAuthApi(request);
  if (auth.response) return auth.response;
  const user = auth.user;

  try {
    const orgId = user?.org_id || DASHBOARD_ORG_ID;
    const actorUserId = user?.id || null;
    const clientId = Number(params.id);
    const body = formBody || {};
    const now = new Date().toISOString();

    if (!clientId) {
      return new Response("Invalid client id", { status: 400 });
    }

    if (String(body.confirm_text || "").trim() !== "RESET") {
      return new Response("Please type RESET to confirm.", { status: 400 });
    }

    const { data: client, error: clientError } = await supabase
      .from("clients")
      .select("id, org_id")
      .eq("org_id", orgId)
      .eq("id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (clientError) throw clientError;

    if (!client) {
      return new Response("Client not found", { status: 404 });
    }

    const resetSummary = {};

    if (body.reset_work_items === "on") {
      const { data, error } = await supabase
        .from("client_work_items")
        .update({
          is_active: false,
          deleted_at: now,
          updated_at: now,
        })
        .eq("org_id", orgId)
        .eq("client_id", clientId)
        .eq("is_active", true)
        .select("id");

      if (error) throw error;
      resetSummary.work_items = data?.length || 0;
    }

    if (body.reset_updates === "on") {
      const { data, error } = await supabase
        .from("client_updates")
        .update({
          is_active: false,
          archived_at: now,
          updated_at: now,
        })
        .eq("client_id", clientId)
        .eq("is_active", true)
        .select("id");

      if (error) throw error;
      resetSummary.updates = data?.length || 0;
    }

    if (body.reset_actions === "on") {
      const { data, error } = await supabase
        .from("client_actions")
        .update({
          archived: true,
          status: "Archived",
          updated_at: now,
        })
        .eq("client_id", clientId)
        .eq("archived", false)
        .select("id");

      if (error) throw error;
      resetSummary.actions = data?.length || 0;
    }

    if (body.reset_contributors === "on") {
      const { data, error } = await supabase
        .from("client_contributors")
        .update({
          archived: true,
          status: "Inactive",
          updated_at: now,
        })
        .eq("client_id", clientId)
        .eq("archived", false)
        .select("id");

      if (error) throw error;
      resetSummary.contributors = data?.length || 0;
    }

    if (body.reset_milestones === "on") {
      const { data, error } = await supabase
        .from("client_milestones")
        .update({
          is_active: false,
          deleted_at: now,
          updated_at: now,
        })
        .eq("org_id", orgId)
        .eq("client_id", clientId)
        .eq("is_active", true)
        .select("id");

      if (error) throw error;
      resetSummary.milestones = data?.length || 0;
    }

    if (body.reset_documents === "on") {
      const { data, error } = await supabase
        .from("client_documents")
        .update({
          is_active: false,
          deleted_at: now,
        })
        .eq("client_id", clientId)
        .eq("is_active", true)
        .select("id");

      if (error) throw error;
      resetSummary.documents = data?.length || 0;
    }

    if (body.reset_activity_logs === "on") {
      const { data, error } = await supabase
        .from("client_activity_logs")
        .delete()
        .eq("org_id", orgId)
        .eq("client_id", clientId)
        .select("id");

      if (error) throw error;
      resetSummary.activity_logs = data?.length || 0;
    }

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_workspace_reset",
      entityType: "clients",
      entityId: clientId,
      newValue: resetSummary,
    });

    return redirectTo(request, "/clients");
  } catch (error) {
    console.error("POST /clients/:id/reset error:", error);
    return new Response(`Failed to reset client: ${escapeHtml(error?.message || String(error))}`, { status: 500 });
  }
}
