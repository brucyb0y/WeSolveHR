// /clients/new — replaces renderNewClientPage() + app.get("/clients/new").
//
// Kept as a plain browser form POSTing to /api/clients, exactly as before: that
// endpoint already validates, inserts the client, its services and contacts,
// writes the activity log and redirects to /clients/:id. No client component is
// needed, so this page ships zero JavaScript.

import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import styles from "./new-client.module.css";

export const metadata = { title: "New Client | WeSolveHR" };
export const dynamic = "force-dynamic";

const SERVICES = [
  "Tech",
  "Sales",
  "Marketing",
  "GTM",
  "Design",
  "QA",
  "Operations",
  "Support",
];

const CONTACT_FIELDS = [
  { label: "Name", name: "contact_name", placeholder: "Client contact name" },
  { label: "Email", name: "contact_email", placeholder: "email@example.com" },
  { label: "Phone", name: "contact_phone", placeholder: "+1..." },
  { label: "Role", name: "contact_role", placeholder: "Founder / CEO / PM" },
];

export default async function NewClientPage() {
  const user = await requireDashboardUser();

  return (
    <>
      <TopNav active="clients" user={user} />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Create Client</div>
            <h1>New Client</h1>
            <div className={styles.subtitle}>
              This page is the starting shell. Database save will come after DB
              tables are added.
            </div>
          </div>
          <a className={styles.btn} href="/clients">
            ← Back to Clients
          </a>
        </div>

        <form method="POST" action="/api/clients">
          <div className={styles.panel}>
            <h2>Basic Info</h2>
            <div className={styles.grid}>
              <div className={styles.field}>
                <label>Client Name</label>
                <input name="name" placeholder="Example: Everloop" />
              </div>

              <div className={styles.field}>
                <label>Company Name</label>
                <input
                  name="company_name"
                  placeholder="Example: Everloop AI Inc."
                />
              </div>
            </div>

            <div className={`${styles.field} ${styles.fieldSpaced}`}>
              <label>Description</label>
              <textarea
                name="description"
                placeholder="Short internal description of this client relationship..."
              />
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Services</h2>
            <div className={styles.grid}>
              {SERVICES.map((service) => (
                <label key={service}>
                  <input type="checkbox" name="services" value={service} />{" "}
                  {service}
                </label>
              ))}
            </div>
          </div>

          <div className={styles.panel}>
            <h2>Primary Client Contact</h2>
            <div className={styles.grid}>
              {CONTACT_FIELDS.map((field) => (
                <div className={styles.field} key={field.name}>
                  <label>{field.label}</label>
                  <input name={field.name} placeholder={field.placeholder} />
                </div>
              ))}
            </div>
          </div>

          <div className={styles.panel}>
            <div className={styles.actions}>
              <a className={styles.btn} href="/clients">
                Cancel
              </a>
              <button
                className={`${styles.btn} ${styles.btnPrimary}`}
                type="submit"
              >
                Create Client
              </button>
            </div>
          </div>
        </form>
      </div>
    </>
  );
}
