// New Client form (Server Component). Replaces GET /clients/new +
// renderNewClientPage(). It's a static form that POSTs (urlencoded) to
// /api/clients, which is still served by the dispatch shim — so the submit
// behavior is unchanged and the page needs no client-side JavaScript.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import styles from "./new-client.module.css";

export const metadata = { title: "New Client | WeSolveHR" };
export const runtime = "nodejs";
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
  { name: "contact_name", label: "Name", placeholder: "Client contact name" },
  { name: "contact_email", label: "Email", placeholder: "email@example.com" },
  { name: "contact_phone", label: "Phone", placeholder: "+1..." },
  { name: "contact_role", label: "Role", placeholder: "Founder / CEO / PM" },
];

export default async function NewClientPage() {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  return (
    <>
      <TopNav active="clients" />

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
                <input name="company_name" placeholder="Example: Everloop AI Inc." />
              </div>
            </div>

            <div className={styles.field} style={{ marginTop: 14 }}>
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
                  <input type="checkbox" name="services" value={service} /> {service}
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
              <button className={`${styles.btn} ${styles.btnPrimary}`} type="submit">
                Create Client
              </button>
            </div>
          </div>
        </form>
      </div>
    </>
  );
}
