// Clients list (Server Component). Replaces GET /clients +
// renderClientsListPage(): authenticate, load the clients + summary on the
// server, and hand the table to a small client island (ClientsTable) that owns
// the per-row actions menu.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import { getClientsList } from "@/lib/services/clients.js";
import ClientsTable from "./ClientsTable.jsx";
import styles from "./clients.module.css";

export const metadata = { title: "Clients | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const STAT_CARDS = [
  { label: "Total Clients", field: "total" },
  { label: "Active", field: "active" },
  { label: "Waiting on Client", field: "waiting" },
  { label: "At Risk", field: "atRisk" },
];

export default async function ClientsPage() {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const { clients, summary } = await getClientsList(
    user.org_id || DASHBOARD_ORG_ID,
  );

  return (
    <>
      <TopNav active="clients" />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Client Workspace</div>
            <h1>Clients</h1>
            <div className={styles.subtitle}>
              Internal consulting CRM layer for client work, updates, actions,
              documents, and progress.
            </div>
          </div>

          <a className={styles.actionBtn} href="/clients/new">
            + New Client
          </a>
        </div>

        <div className={styles.stats}>
          {STAT_CARDS.map((card) => (
            <div className={styles.statCard} key={card.field}>
              <div className={styles.statLabel}>{card.label}</div>
              <div className={styles.statValue}>{summary[card.field] || 0}</div>
            </div>
          ))}
        </div>

        <div className={styles.panel}>
          <ClientsTable clients={clients} />
        </div>
      </div>
    </>
  );
}
