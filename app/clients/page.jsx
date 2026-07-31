// GET /clients — ported from lib/server/app.js lines 39256-39343.

import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import { getClientsListData } from "@/lib/data/clients.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderClientsListPage } from "./ClientsListPage.js";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./clients.css";

export const metadata = { title: "Clients | WeSolveHR" };
export const dynamic = "force-dynamic";

export default async function ClientsPage() {
  const user = await requireDashboardAuthPage();
  const orgId = user?.org_id || DASHBOARD_ORG_ID;
  const { clients, summary } = await getClientsListData(orgId);
  return <RawHtml html={renderClientsListPage({ clients, summary })} />;
}
