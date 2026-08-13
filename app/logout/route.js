// /logout — hand-written, no longer dispatched through the Express adapter.
//
// Kept as a route handler rather than a page because the top nav links to it
// with a plain <a href="/logout">, and there is nothing to render. Matches the
// old handler exactly: drop the session, clear the cookie, 302 to /login.
//
// scripts/gen-routes.mjs skips this folder because a route.js already exists
// here that it did not generate.

import { NextResponse } from "next/server";
import { destroySession } from "@/lib/server/session.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export function GET(request) {
  const cookie = destroySession(request.headers.get("cookie"));
  const response = NextResponse.redirect(new URL("/login", request.url), 302);
  response.cookies.set(cookie.name, cookie.value, cookie.options);
  return response;
}
