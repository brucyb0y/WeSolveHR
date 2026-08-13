// /help — replaces renderHelpPage() + app.get("/help").
//
// Session-only auth, as before (requireUserLogin, not requireDashboardAuth), so
// basic-auth-only visitors are bounced to /login exactly as they are today.
//
// The command guide itself lives in lib/data/help-sections.js.

import TopNav from "@/components/TopNav";
import { requireUser } from "@/lib/auth";
import { isManagerOrAdmin } from "@/lib/server/app.js";
import HelpContent from "./HelpContent";
import styles from "./help.module.css";

export const metadata = { title: "Help & Commands | WeSolveHR" };
export const dynamic = "force-dynamic";

export default async function HelpPage() {
  const user = await requireUser();

  return (
    <>
      <TopNav active="help" user={user} />

      <div className={styles.wrap}>
        <HelpContent isAdmin={isManagerOrAdmin(user)} />
      </div>
    </>
  );
}
