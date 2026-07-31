// GET /logout — ported from lib/server/app.js lines 36911-36915.
//
// Stays a route handler rather than a page: it only destroys the session and
// redirects. Server Components cannot clear cookies, so the Set-Cookie has to
// come from a handler.

import { loadSession, commitSession } from "@/lib/server/session.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request) {
  const { session, meta } = loadSession(request.headers.get("cookie"));
  session.destroy(() => {});

  const setCookie = commitSession(session, meta);
  const headers = new Headers({
    location: new URL("/login", request.url).toString(),
  });
  if (setCookie) headers.append("set-cookie", setCookie);

  return new Response("", { status: 302, headers });
}
