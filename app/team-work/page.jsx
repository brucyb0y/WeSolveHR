// /team-work — replaces the inline HTML in app.get("/team-work").
//
// The initial grid, members, columns and logs are loaded here and handed to the
// client board as props. That replaces the old `var STATE = <json blob>`
// bootstrap, so the manual "</" escaping the handler did before inlining JSON
// into a <script> tag is no longer needed.

import TopNav from "@/components/TopNav";
import { requireDashboardUser, orgIdFor } from "@/lib/auth";
import {
  loadTeamWorkData,
  getRecentTeamWorkLogs,
  getTodayDateStringInTimeZone,
} from "@/lib/server/app.js";
import TeamWorkBoard from "./TeamWorkBoard";
import styles from "./team-work.module.css";

export const metadata = { title: "Team Work" };
export const dynamic = "force-dynamic";

const LOG_LIMIT = 40;

export default async function TeamWorkPage({ searchParams }) {
  const user = await requireDashboardUser();
  const orgId = orgIdFor(user);
  const sp = await searchParams;

  const today = getTodayDateStringInTimeZone();
  let initialDate = String(sp?.date || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(initialDate)) initialDate = today;

  let data;
  try {
    data = await loadTeamWorkData(orgId, initialDate);
  } catch (error) {
    console.error("/team-work load error:", error);
    data = {
      date: initialDate,
      tablesMissing: false,
      columns: [],
      members: [],
      hours: {},
    };
  }

  const logs = await getRecentTeamWorkLogs(orgId, LOG_LIMIT);

  return (
    <>
      <TopNav active="team-work" user={user} />

      <div className={styles.wrap}>
        <TeamWorkBoard initialState={{ ...data, logs }} today={today} />
      </div>
    </>
  );
}
