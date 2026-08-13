// /logs — replaces the inline HTML in app.get("/logs").
//
// The page shell is server-rendered; the console itself is a client component
// because it polls. /api/logs stays as-is (it returns JSON, not HTML).

import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import LogsConsole from "./LogsConsole";
import styles from "./logs.module.css";

export const metadata = { title: "Logs" };
export const dynamic = "force-dynamic";

export default async function LogsPage() {
  const user = await requireDashboardUser();

  return (
    <>
      <TopNav active="logs" user={user} />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Message Logging</div>
            <h1>WeSolveHR // Logs Console</h1>
            <div className={styles.subtitle}>
              Inbound command visibility for tracing, debugging, and audit
              review
            </div>
          </div>
        </div>

        <LogsConsole />
      </div>
    </>
  );
}
