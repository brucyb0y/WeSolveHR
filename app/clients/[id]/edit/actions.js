"use server";

// Server Action replacing app.post("/clients/:id/edit").
//
// The update row, the slug normalisation, the three-contact upsert (including
// the "had an id but all fields cleared -> archive it" branch) and the
// client_updated activity log are all unchanged. Only the error surface
// differs: the old handler replaced the page with a bare 400/500 body, this
// returns the message as action state so the form keeps the user's input.

import { redirect } from "next/navigation";
import {
  supabase,
  normalizeSlug,
  parseUserIdList,
  insertClientActivityLog,
} from "@/lib/server/app.js";
import { requireDashboardUser, orgIdFor } from "@/lib/auth";

export async function updateClientAction(clientId, _prevState, formData) {
  const get = (key) => String(formData.get(key) || "").trim();

  try {
    const user = await requireDashboardUser();
    const orgId = orgIdFor(user);
    const actorUserId = user?.id || null;
    const id = Number(clientId);

    const name = get("name");

    if (!id) return { error: "Invalid client id" };
    if (!name) return { error: "Client name is required" };

    const oldResult = await supabase
      .from("clients")
      .select("*")
      .eq("org_id", orgId)
      .eq("id", id)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (oldResult.error) throw oldResult.error;
    if (!oldResult.data) return { error: "Client not found" };

    const updateRow = {
      name,
      company_name: formData.get("company_name") || null,
      slug: normalizeSlug(formData.get("slug") || name),
      google_drive_folder_url: formData.get("google_drive_folder_url") || null,
      // Empty -> null so the workspace can test for presence rather than "".
      notebook_url: String(formData.get("notebook_url") || "").trim() || null,
      status: formData.get("status") || "active",
      health_status: formData.get("health_status") || "healthy",
      start_date: formData.get("start_date") || null,
      description: formData.get("description") || null,
      account_manager_user_id: formData.get("account_manager_user_id")
        ? Number(formData.get("account_manager_user_id"))
        : null,
      project_manager_user_id: formData.get("project_manager_user_id")
        ? Number(formData.get("project_manager_user_id"))
        : null,
      gtm_associate_user_ids: parseUserIdList(
        formData.getAll("gtm_associate_user_ids"),
      ),
      updated_by_user_id: actorUserId,
      updated_at: new Date().toISOString(),
    };

    const { data, error } = await supabase
      .from("clients")
      .update(updateRow)
      .eq("org_id", orgId)
      .eq("id", id)
      .select("*")
      .maybeSingle();

    if (error) throw error;

    async function upsertContact(index, isPrimary) {
      const contactId = formData.get(`contact_${index}_id`)
        ? Number(formData.get(`contact_${index}_id`))
        : null;

      const contactName = get(`contact_${index}_name`);
      const contactEmail = get(`contact_${index}_email`);
      const contactPhone = get(`contact_${index}_phone`);
      const contactRole = get(`contact_${index}_role`);

      const hasAnyContactData =
        contactName || contactEmail || contactPhone || contactRole;

      if (!hasAnyContactData && !contactId) return null;

      // Existing contact with every field cleared -> archive it.
      if (contactId && !hasAnyContactData) {
        const now = new Date().toISOString();
        const { error: archiveError } = await supabase
          .from("client_contacts")
          .update({ is_active: false, deleted_at: now, updated_at: now })
          .eq("id", contactId)
          .eq("client_id", id);

        if (archiveError) throw archiveError;
        return null;
      }

      const contactRow = {
        org_id: orgId,
        client_id: id,
        name: contactName || "Unnamed Contact",
        email: contactEmail || null,
        phone: contactPhone || null,
        role: contactRole || null,
        is_primary: isPrimary,
        is_active: true,
      };

      if (contactId) {
        const { error: updateError } = await supabase
          .from("client_contacts")
          .update(contactRow)
          .eq("id", contactId)
          .eq("client_id", id);

        if (updateError) throw updateError;
        return null;
      }

      const { error: insertError } = await supabase
        .from("client_contacts")
        .insert([contactRow]);

      if (insertError) throw insertError;
      return null;
    }

    await upsertContact(1, true);
    await upsertContact(2, false);
    await upsertContact(3, false);

    await insertClientActivityLog({
      orgId,
      clientId: id,
      actorUserId,
      action: "client_updated",
      entityType: "clients",
      entityId: id,
      oldValue: oldResult.data,
      newValue: data,
    });
  } catch (error) {
    console.error("updateClientAction error:", error);
    return {
      error: `Failed to update client: ${error?.message || String(error)}`,
    };
  }

  redirect(`/clients/${clientId}`);
}
