// /clients/:id/edit — replaces the inline HTML in app.get("/clients/:id/edit").
// The POST half is now ./actions.js.

import { notFound } from "next/navigation";
import TopNav from "@/components/TopNav";
import { requireDashboardUser, orgIdFor } from "@/lib/auth";
import { supabase } from "@/lib/server/app.js";
import EditClientForm from "./EditClientForm";
import styles from "./edit-client.module.css";

export const metadata = { title: "Edit Client | WeSolveHR" };
export const dynamic = "force-dynamic";

export default async function EditClientPage({ params }) {
  const user = await requireDashboardUser();
  const orgId = orgIdFor(user);
  const { id } = await params;
  const clientId = Number(id);

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
  if (!client) notFound();

  return (
    <>
      <TopNav active="clients" user={user} />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Edit Client</div>
            <h1>{client.name || "Client"}</h1>
            <div className={styles.subtitle}>
              Update client basics, status, health, ownership, and Drive folder.
            </div>
          </div>
          <a className={styles.btn} href={`/clients/${client.id}`}>
            ← Back to Client
          </a>
        </div>

        <EditClientForm
          client={client}
          users={usersResult.data || []}
          contacts={contactsResult.data || []}
        />
      </div>
    </>
  );
}
