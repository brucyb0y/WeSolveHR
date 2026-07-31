// GET /client-view/:token — ported from lib/server/app.js lines 42748-43041.
//
// Public, token-addressed read-only view: no dashboard auth, no top nav.

import { cache } from "react";
import { getClientViewData } from "@/lib/data/client-view.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderClientViewOnlyPage } from "./ClientViewOnlyPage.js";
import { unstable_rethrow } from "next/navigation";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "./client-view.css";

export const dynamic = "force-dynamic";

const load = cache((token, queryJson) =>
  getClientViewData({ params: { token }, query: JSON.parse(queryJson) }),
);

export async function generateMetadata({ params, searchParams }) {
  const { token } = await params;
  try {
    const data = await load(token, JSON.stringify(await searchParams));
    if (data?.__halt) return { title: "Project View" };
    return { title: `${data?.client?.name || "Client"} | Project View` };
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    return { title: "Project View" };
  }
}

export default async function ClientViewPage({ params, searchParams }) {
  const { token } = await params;

  try {
    const data = await load(token, JSON.stringify(await searchParams));
    if (data?.__halt) return <RawHtml html={data.__halt.body} />;
    return <RawHtml html={renderClientViewOnlyPage(data)} />;
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("GET /client-view/:token error:", error);
    return <RawHtml html="Failed to load client view" />;
  }
}
