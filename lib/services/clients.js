// Clients data service. Server-side data access for the clients pages, ported
// from the handlers in lib/server/app.js. Queries are kept identical to the
// originals so page output is unchanged.

import { supabase } from "@/lib/db/supabase.js";

// Clients list + summary cards for GET /clients. Mirrors the handler in
// lib/server/app.js (same select, the client_services lookup, and the derived
// summary). open_work_count / waiting_count / last_update_text are placeholders
// in the original too.
export async function getClientsList(orgId) {
  const { data: clients, error } = await supabase
    .from("clients")
    .select(
      `
        id,
        name,
        company_name,
        slug,
        status,
        health_status,
        start_date,
        description,
        account_manager_user_id,
        project_manager_user_id,
        created_at,
        account_manager:users!clients_account_manager_user_id_fkey(name),
        project_manager:users!clients_project_manager_user_id_fkey(name)
      `,
    )
    .eq("org_id", orgId)
    .eq("is_active", true)
    .is("deleted_at", null)
    .order("created_at", { ascending: false });

  if (error) {
    console.error("clients list error:", error);
    throw error;
  }

  const clientIds = (clients || []).map((c) => c.id);

  let serviceRows = [];
  if (clientIds.length) {
    const { data: servicesData, error: servicesError } = await supabase
      .from("client_services")
      .select(
        `
          client_id,
          services(name)
        `,
      )
      .in("client_id", clientIds)
      .eq("is_active", true)
      .is("deleted_at", null);

    if (servicesError) {
      console.error("client services list error:", servicesError);
    } else {
      serviceRows = servicesData || [];
    }
  }

  const serviceMap = {};
  for (const row of serviceRows) {
    if (!serviceMap[row.client_id]) serviceMap[row.client_id] = [];
    if (row.services?.name) serviceMap[row.client_id].push(row.services.name);
  }

  const clientsList = (clients || []).map((client) => ({
    ...client,
    service_names: serviceMap[client.id] || [],
    account_manager_name: client.account_manager?.name || "",
    project_manager_name: client.project_manager?.name || "",
    open_work_count: 0,
    waiting_count: 0,
    last_update_text: "-",
  }));

  const summary = {
    total: clientsList.length,
    active: clientsList.filter((c) => c.status === "active").length,
    waiting: 0,
    atRisk: clientsList.filter((c) => c.health_status === "at_risk").length,
  };

  return { clients: clientsList, summary };
}

// ---------------------------------------------------------------------------
// Shared helpers (ported verbatim from lib/server/app.js) used by the client
// mutations below and reused by other client pages as they migrate.
// ---------------------------------------------------------------------------

export function normalizeSlug(input) {
  return String(input || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export function parseUserIdList(value) {
  const raw = Array.isArray(value) ? value : value == null ? [] : [value];
  const ids = [];
  for (const v of raw) {
    const n = Number(v);
    if (Number.isInteger(n) && n > 0 && !ids.includes(n)) ids.push(n);
  }
  return ids;
}

export async function insertClientActivityLog({
  orgId,
  clientId,
  actorUserId,
  action,
  entityType = null,
  entityId = null,
  oldValue = null,
  newValue = null,
}) {
  const { error } = await supabase.from("client_activity_logs").insert([
    {
      org_id: orgId,
      client_id: clientId,
      actor_user_id: actorUserId || null,
      action,
      entity_type: entityType,
      entity_id: entityId,
      old_value: oldValue,
      new_value: newValue,
    },
  ]);

  if (error) {
    console.error("insertClientActivityLog error:", error);
  }
}

// Client + users (for the AM/PM/GTM selects) + contacts, for GET /clients/:id/edit.
export async function getClientForEdit(orgId, clientId) {
  const [clientResult, usersResult, contactsResult] = await Promise.all([
    supabase
      .from("clients")
      .select("*")
      .eq("org_id", orgId)
      .eq("id", clientId)
      .eq("is_active", true)
      .is("deleted_at", null)
      .maybeSingle(),

    supabase
      .from("users")
      .select("id, name")
      .eq("org_id", orgId)
      .eq("is_active", true)
      .order("name", { ascending: true }),

    supabase
      .from("client_contacts")
      .select("*")
      .eq("client_id", clientId)
      .order("is_primary", { ascending: false })
      .order("created_at", { ascending: true }),
  ]);

  if (clientResult.error) throw clientResult.error;
  if (usersResult.error) throw usersResult.error;
  if (contactsResult.error) throw contactsResult.error;

  return {
    client: clientResult.data,
    users: usersResult.data || [],
    contacts: contactsResult.data || [],
  };
}

// Save handler for POST /clients/:id/edit — updates the client row, upserts the
// three contact slots, and writes an activity log. `body` is the submitted form
// values as a plain object (gtm_associate_user_ids as an array).
export async function updateClient({ orgId, actorUserId, clientId, body }) {
  const name = String(body.name || "").trim();

  if (!clientId) throw new Error("Invalid client id");
  if (!name) throw new Error("Client name is required");

  const oldResult = await supabase
    .from("clients")
    .select("*")
    .eq("org_id", orgId)
    .eq("id", clientId)
    .eq("is_active", true)
    .is("deleted_at", null)
    .maybeSingle();

  if (oldResult.error) throw oldResult.error;
  if (!oldResult.data) throw new Error("Client not found");

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
      const { data: updatedContact, error: contactUpdateError } = await supabase
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
}

// Handler for POST /clients/:id/reset — archives the selected workspace data
// sets and writes an activity log. `body` is the submitted form values; each
// checkbox is "on" when checked (absent otherwise), matching the original.
export async function resetClientWorkspace({
  orgId,
  actorUserId,
  clientId,
  body,
}) {
  const now = new Date().toISOString();

  if (!clientId) throw new Error("Invalid client id");

  if (String(body.confirm_text || "").trim() !== "RESET") {
    throw new Error("Please type RESET to confirm.");
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
  if (!client) throw new Error("Client not found");

  const resetSummary = {};

  if (body.reset_work_items === "on") {
    const { data, error } = await supabase
      .from("client_work_items")
      .update({ is_active: false, deleted_at: now, updated_at: now })
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
      .update({ is_active: false, archived_at: now, updated_at: now })
      .eq("client_id", clientId)
      .eq("is_active", true)
      .select("id");
    if (error) throw error;
    resetSummary.updates = data?.length || 0;
  }

  if (body.reset_actions === "on") {
    const { data, error } = await supabase
      .from("client_actions")
      .update({ archived: true, status: "Archived", updated_at: now })
      .eq("client_id", clientId)
      .eq("archived", false)
      .select("id");
    if (error) throw error;
    resetSummary.actions = data?.length || 0;
  }

  if (body.reset_contributors === "on") {
    const { data, error } = await supabase
      .from("client_contributors")
      .update({ archived: true, status: "Inactive", updated_at: now })
      .eq("client_id", clientId)
      .eq("archived", false)
      .select("id");
    if (error) throw error;
    resetSummary.contributors = data?.length || 0;
  }

  if (body.reset_milestones === "on") {
    const { data, error } = await supabase
      .from("client_milestones")
      .update({ is_active: false, deleted_at: now, updated_at: now })
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
      .update({ is_active: false, deleted_at: now })
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
}
