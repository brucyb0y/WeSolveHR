// GET /login — ported from lib/server/app.js line 36785.
//
// The failure messages the Express POST handler used to render directly now
// arrive as ?error=<key> from app/form-post/login/route.js.

import RawHtml from "@/lib/ui/RawHtml.jsx";
import { loginErrorMessage } from "@/lib/server/login-errors.js";
import { renderLoginPage } from "./LoginPage.js";
import "@/lib/ui/css/theme.css";
import "@/lib/ui/css/base.css";
import "@/lib/ui/css/top-nav.css";
import "./login.css";

export const metadata = { title: "Login | WeSolveHR" };
export const dynamic = "force-dynamic";

export default async function LoginPage({ searchParams }) {
  const { error } = await searchParams;
  return <RawHtml html={renderLoginPage(loginErrorMessage(error))} />;
}
