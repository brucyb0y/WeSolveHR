// POST /clients/:id/edit — ported from lib/server/app.js lines 43410-43563.
// middleware.js rewrites the form's POST here so the browser keeps posting
// to /clients/:id/edit, which the App Router cannot host beside its page.

import { assertRewritten, readFormBody, redirectTo } from "@/lib/server/form-post.js";
import { requireDashboardAuthApi } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import { supabase } from "@/lib/server/supabase.js";
import { escapeHtml } from "@/lib/ui/html.js";
import { insertClientActivityLog } from "@/lib/data/activity-log.js";
import { normalizeSlug, parseUserIdList } from "@/lib/server/parse.js";

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

    const name = String(body.name || "").trim();

    if (!clientId) {
      return new Response("Invalid client id", { status: 400 });
    }

    if (!name) {
      return new Response("Client name is required", { status: 400 });
    }

    const oldResult = await supabase
      .from("clients")
      .select("*")
      .eq("org_id", orgId)
      .eq("id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle();

    if (oldResult.error) throw oldResult.error;

    if (!oldResult.data) {
      return new Response("Client not found", { status: 404 });
    }

    const updateRow = {
      name,
      company_name: body.company_name || null,
      slug: normalizeSlug(body.slug || name),
      google_drive_folder_url: body.google_drive_folder_url || null,
      status: body.status || "active",
      health_status: body.health_status || "healthy",
      start_date: body.start_date || null,
      description: body.description || null,
      account_manager_user_id: body.account_manager_user_id
        ? Number(body.account_manager_user_id)
        : null,
      project_manager_user_id: body.project_manager_user_id
        ? Number(body.project_manager_user_id)
        : null,
      gtm_associate_user_ids: parseUserIdList(body.gtm_associate_user_ids),
      updated_by_user_id: actorUserId,
      updated_at: new Date().toISOString(),
    };

    const { data, error } = await supabase
      .from("clients")
      .update(updateRow)
      .eq("org_id", orgId)
      .eq("id", clientId)
      .select("*")
      .maybeSingle();

    if (error) throw error;

    async function upsertContactFromBody(index, isPrimary = false) {
      const contactId = body[`contact_${index}_id`]
        ? Number(body[`contact_${index}_id`])
        : null;

      const contactName = String(body[`contact_${index}_name`] || "").trim();
      const contactEmail = String(body[`contact_${index}_email`] || "").trim();
      const contactPhone = String(body[`contact_${index}_phone`] || "").trim();
      const contactRole = String(body[`contact_${index}_role`] || "").trim();

      const hasAnyContactData =
        contactName || contactEmail || contactPhone || contactRole;

      if (!hasAnyContactData && !contactId) {
        return null;
      }

      if (contactId && !hasAnyContactData) {
        const { error: archiveError } = await supabase
          .from("client_contacts")
          .update({
            is_active: false,
            deleted_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
          })
          .eq("id", contactId)
          .eq("client_id", clientId);

        if (archiveError) throw archiveError;
        return null;
      }

      const contactRow = {
        org_id: orgId,
        client_id: clientId,
        name: contactName || "Unnamed Contact",
        email: contactEmail || null,
        phone: contactPhone || null,
        role: contactRole || null,
        is_primary: isPrimary,
        is_active: true,
      };

      if (contactId) {
        const { data: updatedContact, error: contactUpdateError } =
          await supabase
            .from("client_contacts")
            .update(contactRow)
            .eq("id", contactId)
            .eq("client_id", clientId)
            .select("*")
            .maybeSingle();

        if (contactUpdateError) throw contactUpdateError;
        return updatedContact;
      }

      const { data: newContact, error: contactInsertError } = await supabase
        .from("client_contacts")
        .insert([contactRow])
        .select("*")
        .maybeSingle();

      if (contactInsertError) throw contactInsertError;
      return newContact;
    }

    await upsertContactFromBody(1, true);
    await upsertContactFromBody(2, false);
    await upsertContactFromBody(3, false);

    await insertClientActivityLog({
      orgId,
      clientId,
      actorUserId,
      action: "client_updated",
      entityType: "clients",
      entityId: clientId,
      oldValue: oldResult.data,
      newValue: data,
    });

    return redirectTo(request, `/clients/${clientId}`);
  } catch (error) {
    console.error("POST /clients/:id/edit error:", error);
    return new Response(`Failed to update client: ${escapeHtml(error?.message || String(error))}`, { status: 500 });
  }
}
