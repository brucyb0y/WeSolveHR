// GET /help — ported from lib/server/app.js line 36781.

import { requireUserLoginPage } from "@/lib/server/auth.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderHelpPage } from "./HelpPage.js";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./help.css";

export const metadata = { title: "Help & Commands | WeSolveHR" };
// The only two pages that carried their own viewport meta were /help and
// /team-work; both keep it.
export const viewport = { width: "device-width", initialScale: 1 };
export const dynamic = "force-dynamic";

export default async function HelpPage() {
  const user = await requireUserLoginPage();
  return <RawHtml html={renderHelpPage(user || {})} />;
}
