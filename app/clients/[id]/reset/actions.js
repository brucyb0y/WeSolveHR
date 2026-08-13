"use server";

// Server Action replacing app.post("/clients/:id/reset").
//
// The archive logic is unchanged, table for table: work items, milestones and
// documents are soft-deleted; updates are archived; actions and contributors
// are flagged archived with a status change; activity logs are hard-deleted.
// The "RESET" confirmation gate and the client_workspace_reset audit entry are
// also unchanged.
//
// One deliberate difference: the old handler answered a bad confirmation with a
// 400 and a bare text body, replacing the whole page. Here the message comes
// back as action state and renders inline on the form, so the user keeps their
// checkbox selections. The gate itself is identical.

import { redirect } from "next/navigation";
import {
  supabase,
  insertClientActivityLog,
} from "@/lib/server/app.js";
import { requireDashboardUser, orgIdFor } from "@/lib/auth";

const isChecked = (formData, name) => formData.get(name) === "on";

export async function resetClientWorkspaceAction(clientId, _prevState, formData) {
  try {
    const user = await requireDashboardUser();
    const orgId = orgIdFor(user);
    const actorUserId = user?.id || null;
    const id = Number(clientId);
    const now = new Date().toISOString();

    if (!id) return { error: "Invalid client id" };

    if (String(formData.get("confirm_text") || "").trim() !== "RESET") {
      return { error: "Please type RESET to confirm." };
    }

    const { data: client, error: clientError } = await supabase
      .from("clients")
      .select("id, org_id")
      .eq("org_id", orgId)
      .eq("id", id)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (clientError) throw clientError;
    if (!client) return { error: "Client not found" };

    const resetSummary = {};

    if (isChecked(formData, "reset_work_items")) {
      const { data, error } = await supabase
        .from("client_work_items")
        .update({ is_active: false, deleted_at: now, updated_at: now })
        .eq("org_id", orgId)
        .eq("client_id", id)
        .eq("is_active", true)
        .select("id");

      if (error) throw error;
      resetSummary.work_items = data?.length || 0;
    }

    if (isChecked(formData, "reset_updates")) {
      const { data, error } = await supabase
        .from("client_updates")
        .update({ is_active: false, archived_at: now, updated_at: now })
        .eq("client_id", id)
        .eq("is_active", true)
        .select("id");

      if (error) throw error;
      resetSummary.updates = data?.length || 0;
    }

    if (isChecked(formData, "reset_actions")) {
      const { data, error } = await supabase
        .from("client_actions")
        .update({ archived: true, status: "Archived", updated_at: now })
        .eq("client_id", id)
        .eq("archived", false)
        .select("id");

      if (error) throw error;
      resetSummary.actions = data?.length || 0;
    }

    if (isChecked(formData, "reset_contributors")) {
      const { data, error } = await supabase
        .from("client_contributors")
        .update({ archived: true, status: "Inactive", updated_at: now })
        .eq("client_id", id)
        .eq("archived", false)
        .select("id");

      if (error) throw error;
      resetSummary.contributors = data?.length || 0;
    }

    if (isChecked(formData, "reset_milestones")) {
      const { data, error } = await supabase
        .from("client_milestones")
        .update({ is_active: false, deleted_at: now, updated_at: now })
        .eq("org_id", orgId)
        .eq("client_id", id)
        .eq("is_active", true)
        .select("id");

      if (error) throw error;
      resetSummary.milestones = data?.length || 0;
    }

    if (isChecked(formData, "reset_documents")) {
      const { data, error } = await supabase
        .from("client_documents")
        .update({ is_active: false, deleted_at: now })
        .eq("client_id", id)
        .eq("is_active", true)
        .select("id");

      if (error) throw error;
      resetSummary.documents = data?.length || 0;
    }

    if (isChecked(formData, "reset_activity_logs")) {
      const { data, error } = await supabase
        .from("client_activity_logs")
        .delete()
        .eq("org_id", orgId)
        .eq("client_id", id)
        .select("id");

      if (error) throw error;
      resetSummary.activity_logs = data?.length || 0;
    }

    await insertClientActivityLog({
      orgId,
      clientId: id,
      actorUserId,
      action: "client_workspace_reset",
      entityType: "clients",
      entityId: id,
      newValue: resetSummary,
    });
  } catch (error) {
    console.error("resetClientWorkspaceAction error:", error);
    return {
      error: `Failed to reset client: ${error?.message || String(error)}`,
    };
  }

  redirect("/clients");
}
