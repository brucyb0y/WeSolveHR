// GET /tasks/user/:userId — ported from lib/server/app.js lines 45190-45222.

import { cache } from "react";
import { notFound, unstable_rethrow } from "next/navigation";
import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import { getUserTaskWorkspaceData } from "@/lib/data/tasks.js";
import { escapeHtml } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderUserTaskWorkspacePage } from "./UserTaskWorkspacePage.js";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./user-task-workspace.css";

export const dynamic = "force-dynamic";

// cache() keeps generateMetadata and the page body on one fetch per request.
const loadWorkspace = cache((userId, tab) =>
  getUserTaskWorkspaceData({ userId, orgId: DASHBOARD_ORG_ID, tab }),
);

function parseParams(params, searchParams) {
  return {
    userId: Number(params.userId),
    tab: String(searchParams.tab || "pending").trim(),
  };
}

export async function generateMetadata({ params, searchParams }) {
  const { userId, tab } = parseParams(await params, await searchParams);
  if (!Number.isFinite(userId)) return { title: "Tasks" };
  const data = await loadWorkspace(userId, tab);
  return { title: `${data?.user?.name || "User"} Tasks` };
}

export default async function UserTaskWorkspacePage({ params, searchParams }) {
  await requireDashboardAuthPage();
  const { userId, tab } = parseParams(await params, await searchParams);

  if (!Number.isFinite(userId)) return <RawHtml html="Invalid user id" />;

  try {
    const data = await loadWorkspace(userId, tab);
    if (!data) notFound();
    return <RawHtml html={renderUserTaskWorkspacePage(data)} />;
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("User task workspace page error:", error);
    return (
      <RawHtml
        html={`
          ${renderTopNav("tasks")}
          <pre>${escapeHtml(error?.stack || error?.message || String(error))}</pre>
        `}
      />
    );
  }
}
