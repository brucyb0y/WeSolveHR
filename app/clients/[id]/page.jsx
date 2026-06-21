// Client workspace (Server Component). Replaces GET /clients/:id: authenticate,
// resolve the selected tab + lead query from the URL, load the data on the
// server (lib/services/clientWorkspace.js), compute the derived view model, and
// hand both to the client console. All /api/clients/:id/* mutation endpoints stay
// on the dispatch shim; every mutation reloads the page, exactly like the
// original, so the server-rendered tabs remain the source of truth.

import { notFound, redirect } from "next/navigation";
import TopNav from "@/components/TopNav.jsx";
import { getSessionUser } from "@/lib/services/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/services/dashboard.js";
import { getClientWorkspaceData } from "@/lib/services/clientWorkspace.js";
import { buildClientWorkspaceView } from "./workspaceView.js";
import ClientWorkspace from "./ClientWorkspace.jsx";
import "./client-workspace.css";

export const metadata = { title: "Client | WeSolveHR" };
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export default async function ClientWorkspacePage({ params, searchParams }) {
  const user = await getSessionUser();
  if (!user) {
    redirect("/login");
  }

  const { id } = await params;
  const sp = await searchParams;

  const clientId = Number(id);
  if (!clientId) {
    notFound();
  }

  const orgId = user.org_id || DASHBOARD_ORG_ID;
  const selectedTab = String(sp.tab || "overview");
  const selectedLeadTab = String(sp.leadTab || "all");
  const leadSearch = String(sp.search || "");
  const leadPage = Number(sp.page) || 1;

  const data = await getClientWorkspaceData({
    orgId,
    clientId,
    selectedTab,
    selectedLeadTab,
    leadSearch,
    leadPage,
  });

  if (!data) {
    notFound();
  }

  const view = buildClientWorkspaceView(data);

  return (
    <>
      <TopNav active="clients" />
      <div className="client-workspace-page">
        <ClientWorkspace data={data} view={view} />
      </div>
    </>
  );
}
