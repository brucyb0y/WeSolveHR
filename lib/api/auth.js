// Auth for native API route handlers.
//
// Mirrors requireDashboardAuth from lib/server/app.js, with one deliberate
// difference: it NEVER redirects. The Express version redirected browsers to
// /login and only returned 401 JSON when the path began with /api/ — and when
// that branch mis-fired, clients ended up JSON.parsing the login page's HTML.
// Route handlers here are always API, so the answer is always JSON.
//
// Precedence matches the original exactly:
//   1. a real user session (cookie), looked up and required to still be active;
//   2. otherwise DASHBOARD_USERNAME / DASHBOARD_PASSWORD basic auth, resolving
//      to the org's first active admin/manager so handlers still have a user;
//   3. if those env vars are unset the dashboard is unprotected, which the
//      original also allowed — it warns rather than locking everyone out.
//
// Returns { user } on success or { response } holding a 401 to return as-is.

import { getSessionUser } from "@/lib/auth";
import { supabase, DASHBOARD_ORG_ID } from "@/lib/server/app";
import { apiError } from "./respond";

const UNAUTHENTICATED = () => apiError(401, "Not authenticated");

export async function requireApiUser(request) {
  // 1) Session cookie.
  const sessionUser = await getSessionUser();
  if (sessionUser) return { user: sessionUser };

  const username = process.env.DASHBOARD_USERNAME;
  const password = process.env.DASHBOARD_PASSWORD;

  // 3) Unprotected mode — same behaviour (and warning) as the original.
  if (!username || !password) {
    console.warn("Dashboard auth env vars missing; API is unprotected.");
    return { user: await fallbackAdmin() };
  }

  // 2) Basic auth.
  const header = request.headers.get("authorization") || "";
  if (!header.startsWith("Basic ")) return { response: UNAUTHENTICATED() };

  let decoded = "";
  try {
    decoded = Buffer.from(header.slice(6), "base64").toString("utf8");
  } catch {
    return { response: UNAUTHENTICATED() };
  }

  // Split on the FIRST colon only — passwords may contain colons.
  const i = decoded.indexOf(":");
  const inputUser = i >= 0 ? decoded.slice(0, i) : "";
  const inputPass = i >= 0 ? decoded.slice(i + 1) : "";

  if (inputUser !== username || inputPass !== password) {
    return { response: UNAUTHENTICATED() };
  }

  const user = await fallbackAdmin();
  if (!user) return { response: apiError(500, "Failed to resolve user") };
  return { user };
}

async function fallbackAdmin() {
  const { data, error } = await supabase
    .from("users")
    .select("*")
    .eq("org_id", DASHBOARD_ORG_ID)
    .eq("is_active", true)
    .in("role", ["admin", "manager"])
    .order("id", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("requireApiUser fallback admin lookup error:", error);
    return null;
  }
  return data || null;
}

export function orgIdForApi(user) {
  return user?.org_id || DASHBOARD_ORG_ID;
}
