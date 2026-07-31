// GET /clients/:id/edit — ported from lib/server/app.js lines 43043-43408.
// The matching POST is app/form-post/clients/[id]/edit/route.js, reached through
// the rewrite in middleware.js so the form still posts to this same URL.

import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { getClientEditData } from "@/lib/data/client-edit.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderClientEditPage } from "./ClientEditPage.js";
import { unstable_rethrow } from "next/navigation";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./client-edit.css";

export const metadata = { title: "Edit Client | WeSolveHR" };
export const dynamic = "force-dynamic";

export default async function ClientEditPage({ params }) {
  const user = await requireDashboardAuthPage();
  try {
    const data = await getClientEditData({ user, params: await params });
    if (data?.__halt) return <RawHtml html={data.__halt.body} />;
    return <RawHtml html={renderClientEditPage(data)} />;
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("GET /clients/:id/edit error:", error);
    return <RawHtml html="Failed to load edit client page" />;
  }
}
