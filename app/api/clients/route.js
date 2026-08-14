// POST /api/clients — create a client from the "New Client" form.
//
// FORM POST, NOT A JSON API: form-encoded body, plain-text errors, and a
// redirect to the new client's workspace on success. Same shape as
// .../documents; wrapping it in {ok, data} would break the form.
//
// SLUG COLLISIONS RETRY RATHER THAN FAIL. The slug is derived from the client
// name (the form has no slug field), so two clients called the same thing would
// otherwise bounce the user with an error they cannot act on. Instead the
// insert retries up to 5 times appending -2, -3 … and only a non-unique-
// violation error (or exhausting the attempts) gives up. 23505 is Postgres's
// unique-violation code.
//
// Services and contacts are best-effort: their failures are logged but do NOT
// fail the request, because the client row already exists and reporting an
// error would imply nothing was created. That is the original's behaviour.

import {
  supabase,
  DASHBOARD_ORG_ID,
  insertClientActivityLog,
  generateClientViewToken,
  normalizeSlug,
  parseUserIdList,
  ensureArray,
} from "@/lib/server/app";
import { requireApiUser, orgIdForApi } from "@/lib/api/auth";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const MAX_SLUG_ATTEMPTS = 5;
const UNIQUE_VIOLATION = "23505";

const text = (status, body) =>
  new Response(body, { status, headers: { "Content-Type": "text/plain" } });

export async function POST(request) {
  const { user, response } = await requireApiUser(request);
  if (response) return response;

  try {
    const form = await request.formData();
    const get = (k) => {
      const v = form.get(k);
      return v == null ? "" : String(v);
    };

    const orgId = orgIdForApi(user);
    const actorUserId = user?.id || null;

    const name = get("name").trim();
    const companyName = get("company_name").trim();
    const slug = normalizeSlug(get("slug") || name);
    const googleDriveFolderUrl = get("google_drive_folder_url").trim();

    if (
      googleDriveFolderUrl &&
      !googleDriveFolderUrl.startsWith("https://drive.google.com/")
    ) {
      return text(400, "Please enter a valid Google Drive folder link");
    }
    if (!name) return text(400, "Client name is required");
    if (!slug) return text(400, "Slug is required");

    const clientRow = {
      org_id: orgId,
      name,
      company_name: companyName || null,
      slug,
      google_drive_folder_url: googleDriveFolderUrl || null,
      status: get("status") || "active",
      health_status: get("health_status") || "healthy",
      start_date: get("start_date") || null,
      description: get("description") || null,
      account_manager_user_id: get("account_manager_user_id") || null,
      project_manager_user_id: get("project_manager_user_id") || null,
      gtm_associate_user_ids: parseUserIdList(
        form.getAll("gtm_associate_user_ids"),
      ),
      created_by_user_id: actorUserId,
      updated_by_user_id: actorUserId,
      client_view_token: generateClientViewToken(),
      client_view_enabled: false,
    };

    let client = null;
    for (let attempt = 1; attempt <= MAX_SLUG_ATTEMPTS; attempt += 1) {
      const { data, error } = await supabase
        .from("clients")
        .insert([
          { ...clientRow, slug: attempt === 1 ? slug : `${slug}-${attempt}` },
        ])
        .select("*")
        .maybeSingle();

      if (!error) {
        client = data;
        break;
      }
      console.error("client insert error:", error);
      if (error.code !== UNIQUE_VIOLATION || attempt === MAX_SLUG_ATTEMPTS) {
        return text(500, "Failed to create client");
      }
    }
    if (!client) return text(500, "Failed to create client");

    // ---- services (best effort) -------------------------------------------
    const selectedServices = ensureArray(form.getAll("services"))
      .map((x) => String(x).trim())
      .filter(Boolean);

    if (selectedServices.length) {
      const { data: serviceRows, error: serviceLookupError } = await supabase
        .from("services")
        .select("id, name")
        .eq("org_id", orgId)
        .in("name", selectedServices);

      if (serviceLookupError) {
        console.error("service lookup error:", serviceLookupError);
      } else if (serviceRows?.length) {
        const { error: clientServiceError } = await supabase
          .from("client_services")
          .insert(
            serviceRows.map((service) => ({
              org_id: orgId,
              client_id: client.id,
              service_id: service.id,
            })),
          );
        if (clientServiceError) {
          console.error("client services insert error:", clientServiceError);
        }
      }
    }

    // ---- contacts (best effort) -------------------------------------------
    const contactRows = [];
    const pushContact = (prefix, isPrimary) => {
      const n = get(`${prefix}name`);
      const e = get(`${prefix}email`);
      const p = get(`${prefix}phone`);
      const r = get(`${prefix}role`);
      if (n || e || p || r) {
        contactRows.push({
          org_id: orgId,
          client_id: client.id,
          name: n || null,
          email: e || null,
          phone: p || null,
          role: r || null,
          is_primary: isPrimary,
        });
      }
    };
    pushContact("contact_", true);
    pushContact("contact_2_", false);

    if (contactRows.length) {
      const { error: contactsError } = await supabase
        .from("client_contacts")
        .insert(contactRows);
      if (contactsError) {
        console.error("client contacts insert error:", contactsError);
      }
    }

    await insertClientActivityLog({
      orgId,
      clientId: client.id,
      actorUserId,
      action: "client_created",
      entityType: "clients",
      entityId: client.id,
      newValue: client,
    });

    return Response.redirect(
      new URL(`/clients/${client.id}`, request.url),
      303,
    );
  } catch (error) {
    console.error("POST /api/clients fatal error:", error);
    return text(500, "Failed to create client");
  }
}
