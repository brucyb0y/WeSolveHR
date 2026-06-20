// Logs console (Server Component). Replaces the GET /logs handler: authenticate,
// then mount the client console. All log data is fetched client-side from
// /api/logs (served by the dispatch shim), so this RSC only gates access.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import LogsConsole from "./LogsConsole.jsx";

export const metadata = { title: "Logs | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export default async function LogsPage() {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  return (
    <>
      <TopNav active="logs" />
      <LogsConsole />
    </>
  );
}
