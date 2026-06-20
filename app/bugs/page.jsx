// Stage 0 Bug Board (Server Component). Replaces GET /bugs +
// renderStage0BugBoardPage(): authenticate, load the board on the server
// (org-scoped to DASHBOARD_ORG_ID as the original did), then hand it to the
// BugBoard client island that owns the create form and per-card updates.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import { getStage0BugBoardData } from "@/lib/services/bugs.js";
import BugBoard from "./BugBoard.jsx";
import styles from "./bugs.module.css";

export const metadata = { title: "Stage 0 Bug Board | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const STAT_CARDS = [
  { label: "Total", field: "total" },
  { label: "P0", field: "p0" },
  { label: "P1", field: "p1" },
  { label: "P2", field: "p2" },
  { label: "Open", field: "open" },
  { label: "In Progress", field: "in_progress" },
  { label: "Blocked", field: "blocked" },
];

export default async function BugsPage() {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const data = await getStage0BugBoardData(DASHBOARD_ORG_ID);
  const summary = data?.summary || {};
  const columns = data?.columns || [];
  const users = data?.users || [];

  return (
    <>
      <TopNav active="bugs" />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>Stage 0 Stability</div>
            <h1>Bug Board</h1>
            <div className={styles.subtitle}>
              Parsing, idempotency, Twilio, DB failures, dashboard/logs, infra,
              unknown issues.
            </div>
          </div>
        </div>

        <div className={styles.stats}>
          {STAT_CARDS.map((card) => (
            <div className={styles.statCard} key={card.field}>
              <div className={styles.statLabel}>{card.label}</div>
              <div className={styles.statValue}>{summary[card.field] ?? 0}</div>
            </div>
          ))}
        </div>

        <BugBoard columns={columns} users={users} />
      </div>
    </>
  );
}
