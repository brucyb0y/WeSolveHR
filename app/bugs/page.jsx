// /bugs — replaces renderStage0BugBoardPage() + app.get("/bugs").
//
// The board data is loaded here; the two interactive pieces (create panel, per
// card selects) are client components. Badge class names are computed here so
// those components never have to import from lib/server/app.js.

import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import {
  DASHBOARD_ORG_ID,
  getStage0BugBoardData,
  bugSeverityBadgeClass,
  bugStatusBadgeClass,
  STAGE0_BUG_COLUMNS,
  STAGE0_BUG_SEVERITIES,
  STAGE0_BUG_STATUSES,
} from "@/lib/server/app.js";
import BugCard from "./BugCard";
import CreateBugForm from "./CreateBugForm";
import styles from "./bugs.module.css";

export const metadata = { title: "Stage 0 Bug Board" };
export const dynamic = "force-dynamic";

const STAT_CARDS = [
  { label: "Total", key: "total" },
  { label: "P0", key: "p0" },
  { label: "P1", key: "p1" },
  { label: "P2", key: "p2" },
  { label: "Open", key: "open" },
  { label: "In Progress", key: "in_progress" },
  { label: "Blocked", key: "blocked" },
];

export default async function BugsPage() {
  const user = await requireDashboardUser();

  // The original page scoped to DASHBOARD_ORG_ID regardless of the acting user.
  const data = await getStage0BugBoardData(DASHBOARD_ORG_ID);

  const summary = data?.summary || {};
  const columns = data?.columns || [];
  const users = data?.users || [];

  return (
    <>
      <TopNav active="bugs" user={user} />

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
            <div className={styles.statCard} key={card.key}>
              <div className={styles.statLabel}>{card.label}</div>
              <div className={styles.statValue}>{summary[card.key] ?? 0}</div>
            </div>
          ))}
        </div>

        <CreateBugForm columns={STAGE0_BUG_COLUMNS} />

        <div className={styles.board}>
          {columns.map((column) => (
            <div className={styles.boardCol} key={column.name}>
              <div className={styles.boardColHead}>
                <div className={styles.boardColTitle}>{column.name}</div>
                <div className={styles.boardColCount}>{column.count}</div>
              </div>
              <div className={styles.boardColBody}>
                {(column.items || []).length ? (
                  column.items.map((bug) => (
                    <BugCard
                      key={bug.id}
                      bug={{
                        ...bug,
                        severityBadgeClass: bugSeverityBadgeClass(bug.severity),
                        statusBadgeClass: bugStatusBadgeClass(bug.status),
                      }}
                      users={users}
                      columns={STAGE0_BUG_COLUMNS}
                      severities={STAGE0_BUG_SEVERITIES}
                      statuses={STAGE0_BUG_STATUSES}
                    />
                  ))
                ) : (
                  <div className={styles.emptyCol}>No bugs here</div>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
    </>
  );
}
