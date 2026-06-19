// Tasks page (Server Component). Replaces the `GET /tasks` Express handler.
// Any logged-in active user may view tasks (no manager gate, unlike /dashboard),
// so we only require a session. All task data is fetched client-side from the
// /api/* routes, so this RSC just authenticates and mounts the console.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import TasksConsole from "./TasksConsole.jsx";

export const metadata = { title: "Tasks | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export default async function TasksPage() {
  const user = await getSessionUser();

  if (!user) {
    redirect("/login");
  }

  return (
    <>
      <TopNav active="tasks" />
      <TasksConsole />
    </>
  );
}
