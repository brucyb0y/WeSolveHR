// Data behind GET /clients/:id/edit, copied verbatim from the original
// handler (lib/server/app.js lines 43043-43408).

import { supabase } from "../server/supabase.js";
import { DASHBOARD_ORG_ID } from "../server/constants.js";

async function getClientEditData({ user, params }) {

    const orgId = user?.org_id || DASHBOARD_ORG_ID;
    const clientId = Number(params.id);

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
    const client = clientResult.data;
    const users = usersResult.data || [];
    const contacts = contactsResult.data || [];
    const primaryContact = contacts[0] || {};
    const secondContact = contacts[1] || {};
    const thirdContact = contacts[2] || {};
    if (!client) {
      return { __halt: { status: 404, body: "Client not found" } };
    }

  return { client, users, primaryContact, secondContact, thirdContact };
}

export { getClientEditData };
