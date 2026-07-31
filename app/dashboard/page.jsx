// GET /dashboard — ported from lib/server/app.js lines 43902-43953.

import { redirect, unstable_rethrow } from "next/navigation";
import { requireDashboardAuthPage } from "@/lib/server/auth.js";
import { DASHBOARD_ORG_ID } from "@/lib/server/constants.js";
import { isManagerOrAdmin } from "@/lib/server/users.js";
import { getDashboardData } from "@/lib/data/dashboard.js";
import { escapeHtml } from "@/lib/ui/html.js";
import { renderTopNav } from "@/lib/ui/nav.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderDashboardPage } from "./DashboardPage.js";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./dashboard.css";

// The error screen restyles <body>, so its CSS stays inline and only reaches
// the DOM when the error branch actually renders. Verbatim from the original
// handler (lib/server/app.js lines 43913-43936).
const DASHBOARD_ERROR_CSS = `
            body {
              font-family: Arial, sans-serif;
              background: #0f172a;
              color: white;
              padding: 40px;
            }
            .box {
              max-width: 800px;
              margin: 0 auto;
              padding: 24px;
              border-radius: 16px;
              background: rgba(255,255,255,0.06);
              border: 1px solid rgba(255,255,255,0.1);
            }
            pre {
              white-space: pre-wrap;
              word-break: break-word;
              color: #fca5a5;
            }
            a { color: #93c5fd; }
`;

export const metadata = { title: "Dashboard" };
export const dynamic = "force-dynamic";

export default async function DashboardPage() {
  const user = await requireDashboardAuthPage();

  if (user && !isManagerOrAdmin(user)) redirect("/my-dashboard");

  try {
    const data = await getDashboardData(DASHBOARD_ORG_ID);
    return <RawHtml html={renderDashboardPage(data)} />;
  } catch (error) {
    // redirect() and notFound() work by throwing; without this they would be
    // swallowed here and the page would render an error instead of navigating.
    unstable_rethrow(error);
    console.error("Dashboard error:", error);
    return (
      <RawHtml
        html={`
          <style>${DASHBOARD_ERROR_CSS}</style>
        ${renderTopNav("dashboard")}
          <div class="box">
            <h1>Dashboard failed to load</h1>
            <p>Check server logs and the details below.</p>
            <pre>${escapeHtml(error?.message || String(error))}</pre>
            <p><a href="/dashboard">Try again</a></p>
          </div>
    `}
      />
    );
  }
}
