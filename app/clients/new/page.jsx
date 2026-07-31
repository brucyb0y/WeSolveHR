// GET /clients/new — ported from lib/server/app.js lines 39383-39385.

import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderNewClientPage } from "./NewClientPage.js";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./new-client.css";

export const metadata = { title: "New Client | WeSolveHR" };
export const dynamic = "force-dynamic";

export default async function NewClientPage() {
  await requireDashboardAuthPage();
  return <RawHtml html={renderNewClientPage()} />;
}
