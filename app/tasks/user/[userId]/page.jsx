// /tasks/user/:userId — replaces renderUserTaskWorkspacePage() +
// app.get("/tasks/user/:userId").
//
// Server-side formatting (renderUserWorkspaceHistoryLine, formatDateTime,
// badgeClass) is applied here and the results handed down as plain strings, so
// the client list component imports nothing from lib/server/app.js.

import { notFound } from "next/navigation";
import TopNav from "@/components/TopNav";
import { requireDashboardUser } from "@/lib/auth";
import {
  DASHBOARD_ORG_ID,
  getUserTaskWorkspaceData,
  renderUserWorkspaceHistoryLine,
  formatDateTime,
  badgeClass,
} from "@/lib/server/app.js";
import WorkspaceList from "./WorkspaceList";
import styles from "./workspace.module.css";

export const dynamic = "force-dynamic";

const TABS = [
  { key: "pending", label: "Pending" },
  { key: "blocked", label: "Blocked" },
  { key: "blocked_on_me", label: "Blocked on me" },
  { key: "done_today", label: "Done today" },
  { key: "deleted", label: "Deleted" },
  { key: "progress_updates", label: "Progress updates" },
];

const STAT_CARDS = [
  { label: "Pending", key: "pending" },
  { label: "Blocked", key: "blocked" },
  { label: "Done today", key: "done_today" },
  { label: "Deleted", key: "deleted" },
];

export async function generateMetadata({ params }) {
  const { userId } = await params;
  const data = await getUserTaskWorkspaceData({
    userId: Number(userId),
    orgId: DASHBOARD_ORG_ID,
    tab: "pending",
  });
  return { title: `${data?.user?.name || "User"} Tasks` };
}

export default async function UserTaskWorkspacePage({ params, searchParams }) {
  const user = await requireDashboardUser();
  const { userId: rawUserId } = await params;
  const sp = await searchParams;

  const userId = Number(rawUserId);
  if (!Number.isFinite(userId)) notFound();

  const selectedTab = String(sp?.tab || "pending").trim();

  const data = await getUserTaskWorkspaceData({
    userId,
    orgId: DASHBOARD_ORG_ID,
    tab: selectedTab,
  });

  if (!data) notFound();

  const counts = data.counts || {};
  const subject = data.user;
  const isProgressTab = selectedTab === "progress_updates";
  const rawItems = data.tabs?.[selectedTab] || [];

  const items = isProgressTab
    ? rawItems.map((item) => ({
        ...item,
        line: renderUserWorkspaceHistoryLine(item),
        created_at_text: formatDateTime(item.created_at),
      }))
    : rawItems.map((task) => ({
        ...task,
        statusBadgeClass: badgeClass(task.status),
        latest_update_at_text: task.latest_update_at
          ? formatDateTime(task.latest_update_at)
          : "-",
        mini_history: Array.isArray(task.mini_history)
          ? task.mini_history.map((item) => ({
              ...item,
              line: renderUserWorkspaceHistoryLine(item),
              created_at_text: formatDateTime(item.created_at),
            }))
          : [],
      }));

  return (
    <>
      <TopNav active="tasks" user={user} />

      <div className={styles.wrap}>
        <div className={styles.topbar}>
          <div>
            <div className={styles.eyebrow}>User Task Workspace</div>
            <h1>{subject?.name || "Unknown user"}</h1>
            <div className={styles.subtitle}>
              Focused task workspace for one user
            </div>
          </div>
          <a className={styles.backLink} href="/tasks">
            ← Back to Tasks
          </a>
        </div>

        <div className={styles.stats}>
          {STAT_CARDS.map((card) => (
            <div className={styles.statCard} key={card.key}>
              <div className={styles.statLabel}>{card.label}</div>
              <div className={styles.statValue}>{counts[card.key] || 0}</div>
            </div>
          ))}
        </div>

        <div className={styles.workspaceChipRow}>
          {TABS.map((tab) => (
            <a
              key={tab.key}
              href={`/tasks/user/${subject.id}?tab=${tab.key}`}
              className={`${styles.workspaceChip} ${
                selectedTab === tab.key ? styles.active : ""
              }`}
            >
              {tab.label} ({counts[tab.key] || 0})
            </a>
          ))}
        </div>

        <div className={styles.workspaceList}>
          <WorkspaceList items={items} isProgressTab={isProgressTab} />
        </div>
      </div>
    </>
  );
}
