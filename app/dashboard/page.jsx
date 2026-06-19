// Dashboard page (Server Component). Replaces the `GET /dashboard` Express
// handler: authenticate via session, bounce non-managers to /my-dashboard, then
// compute the overview data on the server and hand it to the interactive body.

import { redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser, isManagerOrAdmin } from "@/lib/services/auth.js";
import { getDashboardData, DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import DashboardContent from "./DashboardContent.jsx";

export const metadata = { title: "Dashboard | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export default async function DashboardPage() {
  const user = await getSessionUser();

  if (!user) {
    redirect("/login");
  }

  if (!isManagerOrAdmin(user)) {
    redirect("/my-dashboard");
  }

  const data = await getDashboardData(DASHBOARD_ORG_ID);

  return (
    <>
      <TopNav active="dashboard" />
      <DashboardContent data={data} />
    </>
  );
}
