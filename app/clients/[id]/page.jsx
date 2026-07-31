// GET /clients/:id — ported from lib/server/app.js lines 42221-42678.

import { cache } from "react";
import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { getClientWorkspaceData } from "@/lib/data/client-workspace.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderClientWorkspacePage } from "./ClientWorkspacePage.js";
import { unstable_rethrow } from "next/navigation";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "@/lib/ui/css/sweetalert.css";
import "./client-workspace.css";

export const dynamic = "force-dynamic";

// The document title comes from the loaded client, so generateMetadata and the
// page body share one load per request via cache().
const load = cache(async (id, tab) => {
  const user = await requireDashboardAuthPage();
  return getClientWorkspaceData({ user, params: { id }, query: { tab } });
});

function readParams(params, searchParams) {
  return { id: params.id, tab: searchParams.tab };
}

export async function generateMetadata({ params, searchParams }) {
  const { id, tab } = readParams(await params, await searchParams);
  try {
    const data = await load(id, tab);
    // Raw name here — React escapes it on the way out, so passing the
    // already-escaped string would double-encode it.
    return { title: `${data?.client?.name || "Client"} | WeSolveHR` };
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    return { title: "Client | WeSolveHR" };
  }
}

export default async function ClientWorkspacePage({ params, searchParams }) {
  const { id, tab } = readParams(await params, await searchParams);

  try {
    const data = await load(id, tab);

    // The original short-circuited with 400/404/500 text bodies; the same text
    // is rendered here (Next answers 200 for a rendered page).
    if (data?.__halt) return <RawHtml html={data.__halt.body} />;

    // The SweetAlert <script> lives inside the rendered markup (see
    // ClientWorkspacePage.js) so the browser runs it during document parse.
    return <RawHtml html={renderClientWorkspacePage(data)} />;
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("GET /clients/:id fatal error:", error);
    return <RawHtml html="Failed to load client workspace" />;
  }
}
