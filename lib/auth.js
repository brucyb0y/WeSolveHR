// Auth for the React tree.
//
// These are the React Server Component equivalents of requireUserLogin and
// requireDashboardAuth in lib/server/app.js, with the same rules:
//
//   * the signed "connect.sid" session cookie is the primary credential;
//   * requireDashboardUser additionally accepts the DASHBOARD_USERNAME /
//     DASHBOARD_PASSWORD basic-auth pair and, when it matches, attaches the
//     lowest-id active admin/manager the way the Express version did;
//   * a page reached without credentials redirects to /login.
//
// Callers get the user row itself (not a req object), and pages fall back to
// DASHBOARD_ORG_ID when there is no user, exactly as the handlers did with
// `req.loggedInUser?.org_id || DASHBOARD_ORG_ID`.

import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { loadSession } from "@/lib/server/session.js";
import { supabase, DASHBOARD_ORG_ID } from "@/lib/server/app.js";

async function cookieHeader() {
  const h = await headers();
  return h.get("cookie");
}

// The logged-in user, or null. Never redirects — use this when a page renders
// differently for signed-out visitors instead of bouncing them.
export async function getSessionUser() {
  const { session } = loadSession(await cookieHeader());
  const userId = session?.userId;
  if (!userId) return null;

  const { data: user, error } = await supabase
    .from("users")
    .select("*")
    .eq("id", userId)
    .eq("is_active", true)
    .maybeSingle();

  if (error) {
    console.error("getSessionUser lookup error:", error);
    return null;
  }

  return user || null;
}

// Mirrors requireUserLogin: a real session is mandatory.
export async function requireUser() {
  const user = await getSessionUser();
  if (!user) redirect("/login");
  return user;
}

// Mirrors requireDashboardAuth: session first, then basic auth.
//
// Returns null (rather than redirecting) in the two cases the Express version
// called next() without a user: the basic-auth env vars are unset, or they
// matched but no admin/manager row exists to attach. Pages treat a null user as
// "org-scoped view with no personalisation", same as before.
export async function requireDashboardUser() {
  const sessionUser = await getSessionUser();
  if (sessionUser) return sessionUser;

  const username = process.env.DASHBOARD_USERNAME;
  const password = process.env.DASHBOARD_PASSWORD;

  if (!username || !password) {
    console.warn("Dashboard auth env vars missing; dashboard is unprotected.");
    return null;
  }

  const h = await headers();
  const header = h.get("authorization") || "";
  if (!header.startsWith("Basic ")) redirect("/login");

  let decoded = "";
  try {
    decoded = Buffer.from(header.slice(6), "base64").toString("utf8");
  } catch {
    redirect("/login");
  }

  const separatorIndex = decoded.indexOf(":");
  const inputUser = separatorIndex >= 0 ? decoded.slice(0, separatorIndex) : "";
  const inputPass = separatorIndex >= 0 ? decoded.slice(separatorIndex + 1) : "";

  if (inputUser !== username || inputPass !== password) redirect("/login");

  const { data: fallbackAdmin, error } = await supabase
    .from("users")
    .select("*")
    .eq("org_id", DASHBOARD_ORG_ID)
    .eq("is_active", true)
    .in("role", ["admin", "manager"])
    .order("id", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("requireDashboardUser fallback admin lookup error:", error);
    return null;
  }

  return fallbackAdmin || null;
}

// org_id for a page's queries, with the same fallback the handlers used.
export function orgIdFor(user) {
  return user?.org_id || DASHBOARD_ORG_ID;
}
