// Edit Client (Server Component). Replaces GET /clients/:id/edit: authenticate,
// load the client + users + contacts, and render a prefilled form whose submit
// is the updateClientAction Server Action (replacing POST /clients/:id/edit).

import { notFound, redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import GtmMultiselect from "@/components/GtmMultiselect.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import { getClientForEdit } from "@/lib/services/clients.js";
import { updateClientAction } from "./actions.js";
import styles from "./edit-client.module.css";

export const metadata = { title: "Edit Client | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const STATUS_OPTIONS = [
  { value: "active", label: "Active" },
  { value: "paused", label: "Paused" },
  { value: "onboarding", label: "Onboarding" },
  { value: "completed", label: "Completed" },
  { value: "inactive", label: "Inactive" },
];

const HEALTH_OPTIONS = [
  { value: "healthy", label: "Healthy" },
  { value: "watch", label: "Watch" },
  { value: "at_risk", label: "At Risk" },
];

// The three contact slots rendered in order; index 0 is the primary contact.
const CONTACT_FIELDS = ["name", "email", "phone", "role"];

function ContactGrid({ slot, contact, primary }) {
  const prefix = `contact_${slot}`;
  const labelBase = primary ? "Primary Contact" : `Contact ${slot}`;
  return (
    <div className={styles.grid} style={slot > 1 ? { marginTop: 18 } : undefined}>
      {CONTACT_FIELDS.map((f) => (
        <div className={styles.field} key={f}>
          <label>
            {labelBase} {f.charAt(0).toUpperCase() + f.slice(1)}
          </label>
          <input name={`${prefix}_${f}`} defaultValue={contact[f] || ""} />
        </div>
      ))}
    </div>
  );
}

export default async function EditClientPage({ params }) {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const { id } = await params;
  const clientId = Number(id);
  const orgId = user.org_id || DASHBOARD_ORG_ID;

  const { client, users, contacts } = await getClientForEdit(orgId, clientId);
  if (!client) {
    notFound();
  }

  const [primaryContact = {}, secondContact = {}, thirdContact = {}] = contacts;

  return (
    <>
      <TopNav active="clients" />

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

        <form action={updateClientAction.bind(null, client.id)}>
          <div className={styles.panel}>
            <h2>Basic Info</h2>

            <div className={styles.grid}>
              <div className={styles.field}>
                <label>Client Name</label>
                <input name="name" defaultValue={client.name || ""} required />
              </div>

              <div className={styles.field}>
                <label>Company Name</label>
                <input
                  name="company_name"
                  defaultValue={client.company_name || ""}
                />
              </div>

              <div className={styles.field}>
                <label>Slug</label>
                <input name="slug" defaultValue={client.slug || ""} />
              </div>

              <div className={styles.field}>
                <label>Google Drive Folder Link</label>
                <input
                  name="google_drive_folder_url"
                  defaultValue={client.google_drive_folder_url || ""}
                  required
                />
              </div>

              <div className={styles.field}>
                <label>Status</label>
                <select name="status" defaultValue={client.status || "active"}>
                  {STATUS_OPTIONS.map((o) => (
                    <option value={o.value} key={o.value}>
                      {o.label}
                    </option>
                  ))}
                </select>
              </div>

              <div className={styles.field}>
                <label>Health</label>
                <select
                  name="health_status"
                  defaultValue={client.health_status || "healthy"}
                >
                  {HEALTH_OPTIONS.map((o) => (
                    <option value={o.value} key={o.value}>
                      {o.label}
                    </option>
                  ))}
                </select>
              </div>

              <div className={styles.field}>
                <label>Start Date</label>
                <input
                  type="date"
                  name="start_date"
                  defaultValue={client.start_date || ""}
                />
              </div>

              <div className={styles.field}>
                <label>Account Manager</label>
                <select
                  name="account_manager_user_id"
                  defaultValue={
                    client.account_manager_user_id
                      ? String(client.account_manager_user_id)
                      : ""
                  }
                >
                  <option value="">Select account manager</option>
                  {users.map((u) => (
                    <option value={String(u.id)} key={u.id}>
                      {u.name}
                    </option>
                  ))}
                </select>
              </div>

              <div className={styles.field}>
                <label>Project Manager</label>
                <select
                  name="project_manager_user_id"
                  defaultValue={
                    client.project_manager_user_id
                      ? String(client.project_manager_user_id)
                      : ""
                  }
                >
                  <option value="">Select project manager</option>
                  {users.map((u) => (
                    <option value={String(u.id)} key={u.id}>
                      {u.name}
                    </option>
                  ))}
                </select>
              </div>

              <GtmMultiselect
                users={users}
                selectedIds={client.gtm_associate_user_ids}
              />
            </div>

            <div className={styles.field} style={{ marginTop: 14 }}>
              <label>Description</label>
              <textarea name="description" defaultValue={client.description || ""} />
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Client Contacts</h2>

            <input
              type="hidden"
              name="contact_1_id"
              defaultValue={primaryContact.id || ""}
            />
            <input
              type="hidden"
              name="contact_2_id"
              defaultValue={secondContact.id || ""}
            />
            <input
              type="hidden"
              name="contact_3_id"
              defaultValue={thirdContact.id || ""}
            />

            <ContactGrid slot={1} contact={primaryContact} primary />
            <ContactGrid slot={2} contact={secondContact} />
            <ContactGrid slot={3} contact={thirdContact} />
          </div>

          <div className={styles.panel}>
            <div className={styles.actions}>
              <a className={styles.btn} href={`/clients/${client.id}`}>
                Cancel
              </a>
              <button className={`${styles.btn} ${styles.btnPrimary}`} type="submit">
                Save Client
              </button>
            </div>
          </div>
        </form>
      </div>
    </>
  );
}
