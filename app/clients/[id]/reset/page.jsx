// GET /clients/:id/reset — ported from lib/server/app.js lines 43565-43737.
// The matching POST is app/form-post/clients/[id]/reset/route.js, reached
// through the rewrite in middleware.js.

import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderClientResetPage } from "./ClientResetPage.js";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./client-reset.css";

export const metadata = { title: "Reset Client | WeSolveHR" };
export const dynamic = "force-dynamic";

export default async function ClientResetPage({ params }) {
  await requireDashboardAuthPage();
  const { id } = await params;
  return <RawHtml html={renderClientResetPage({ clientId: Number(id) })} />;
}
