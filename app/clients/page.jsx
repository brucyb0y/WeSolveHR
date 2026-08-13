// /clients — replaces renderClientsListPage() + app.get("/clients").

import TopNav from "@/components/TopNav";
import { requireDashboardUser, orgIdFor } from "@/lib/auth";
import { getClientsListData } from "@/lib/data/clients";
import ClientsTable from "./ClientsTable";
import styles from "./clients.module.css";

export const metadata = { title: "Clients | WeSolveHR" };
export const dynamic = "force-dynamic";

const COLUMNS = [
  "Client",
  "Services",
  "Project Manager",
  "Status",
  "Health",
  "Open Work",
  "Waiting",
  "Last Update",
  "Actions",
];

const STAT_CARDS = [
  { label: "Total Clients", key: "total" },
  { label: "Active", key: "active" },
  { label: "Waiting on Client", key: "waiting" },
  { label: "At Risk", key: "atRisk" },
];

export default async function ClientsPage() {
  const user = await requireDashboardUser();
  const { clients, summary } = await getClientsListData(orgIdFor(user));

  return (
    <>
      <TopNav active="clients" user={user} />

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
            <div className={styles.statCard} key={card.key}>
              <div className={styles.statLabel}>{card.label}</div>
              <div className={styles.statValue}>{summary[card.key] || 0}</div>
            </div>
          ))}
        </div>

        <div className={styles.panel}>
          <table>
            <thead>
              <tr>
                {COLUMNS.map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <ClientsTable clients={clients} />
          </table>
        </div>
      </div>
    </>
  );
}
