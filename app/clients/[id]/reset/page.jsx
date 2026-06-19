// Reset Client Workspace (Server Component). Replaces GET /clients/:id/reset:
// a confirm form whose submit is the resetClientAction Server Action (replacing
// POST /clients/:id/reset). No data load is needed for the form itself.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { resetClientAction } from "./actions.js";
import styles from "./reset-client.module.css";

export const metadata = { title: "Reset Client | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const RESET_OPTIONS = [
  { name: "reset_work_items", label: "Archive work items" },
  { name: "reset_updates", label: "Archive updates" },
  { name: "reset_actions", label: "Archive actions" },
  { name: "reset_contributors", label: "Archive contributors" },
  { name: "reset_milestones", label: "Archive milestones" },
  { name: "reset_documents", label: "Archive document records" },
  { name: "reset_activity_logs", label: "Archive activity logs" },
];

export default async function ResetClientPage({ params }) {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const { id } = await params;
  const clientId = Number(id);

  return (
    <>
      <TopNav active="clients" />

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
            client workspace. Existing work items, updates, actions, contributors,
            milestones, documents, and activity logs will be hidden/archived.
          </div>

          <form action={resetClientAction.bind(null, clientId)}>
            <div className={styles.checkList}>
              {RESET_OPTIONS.map((opt) => (
                <label key={opt.name}>
                  <input type="checkbox" name={opt.name} defaultChecked />
                  {opt.label}
                </label>
              ))}
            </div>

            <input
              className={styles.confirmInput}
              name="confirm_text"
              placeholder="Type RESET to confirm"
            />

            <div className={styles.actions}>
              <a className={styles.btn} href={`/clients/${clientId}`}>
                Cancel
              </a>
              <button className={`${styles.btn} ${styles.btnDanger}`} type="submit">
                Reset Selected Data
              </button>
            </div>
          </form>
        </div>
      </div>
    </>
  );
}
