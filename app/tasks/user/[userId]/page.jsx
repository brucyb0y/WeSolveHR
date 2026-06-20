// User task workspace (Server Component). Replaces GET /tasks/user/:userId:
// authenticate, resolve the user id + tab, load the workspace data on the server
// (org-scoped to DASHBOARD_ORG_ID like the original), then hand it to the client
// workspace island (cards + task-detail modal + auto-refresh).

import { notFound, redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import { getUserTaskWorkspaceData } from "@/lib/services/tasks.js";
import UserTaskWorkspace from "./UserTaskWorkspace.jsx";

export const metadata = { title: "User Tasks | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export default async function UserTaskWorkspacePage({ params, searchParams }) {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const { userId: rawUserId } = await params;
  const sp = await searchParams;
  const userId = Number(rawUserId);
  if (!Number.isFinite(userId)) {
    notFound();
  }

  const tab = String(sp.tab || "pending").trim();
  const data = await getUserTaskWorkspaceData({
    userId,
    orgId: DASHBOARD_ORG_ID,
    tab,
  });

  if (!data) {
    notFound();
  }

  return (
    <>
      <TopNav active="tasks" />
      <UserTaskWorkspace data={data} />
    </>
  );
}
