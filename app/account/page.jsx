// GET /account — ported from lib/server/app.js lines 35384-36100.

import { requireUserLoginPage } from "@/lib/server/auth.js";
import { getAccountPageData } from "@/lib/data/account.js";
import RawHtml from "@/lib/ui/RawHtml.jsx";
import { renderAccountPage } from "./AccountPage.js";
// The original /account document pulled in only buildTopNavCss(); it never
// included the theme or base stylesheets, so neither is imported here.
import "@/lib/ui/css/top-nav.css";
import "./account.css";

export const metadata = { title: "My Account" };
export const dynamic = "force-dynamic";

export default async function AccountPage() {
  const user = await requireUserLoginPage();
  const data = await getAccountPageData({ user });
  if (data?.__halt) return <RawHtml html={data.__halt.body} />;
  return <RawHtml html={renderAccountPage(data)} />;
}
