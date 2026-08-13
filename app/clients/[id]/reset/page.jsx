// /clients/:id/reset — replaces the inline HTML in app.get("/clients/:id/reset").
// The POST half is now ./actions.js.

import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import ResetForm from "./ResetForm";
import styles from "./reset.module.css";

export const metadata = { title: "Reset Client | WeSolveHR" };
export const dynamic = "force-dynamic";

export default async function ResetClientPage({ params }) {
  const user = await requireDashboardUser();
  const { id } = await params;
  const clientId = Number(id);

  return (
    <>
      <TopNav active="clients" user={user} />

      <div className={styles.wrap}>
        <div className={styles.panel}>
          <h1>Reset Client Workspace</h1>
          <div className={styles.muted}>
            This will archive workspace data for this client. It will not delete
            the client, Google Drive folder, contacts, services, or client view
            token.
          </div>

          <div className={styles.dangerBox}>
            <strong>Important:</strong> This is meant for cleaning a test/demo
            client workspace. Existing work items, updates, actions,
            contributors, milestones, documents, and activity logs will be
            hidden/archived.
          </div>

          <ResetForm clientId={clientId} />
        </div>
      </div>
    </>
  );
}
