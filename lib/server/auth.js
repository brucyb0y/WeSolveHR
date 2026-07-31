// Authentication for App Router pages and route handlers.
//
// Ported from the Express middleware `requireUserLogin` / `requireDashboardAuth`
// in the original monolith (lib/server/app.js lines 15984-16106). The decision
// logic — session user first, dashboard basic-auth fallback second, redirect to
// /login otherwise — is unchanged. Only the transport differs: pages read
// next/headers and throw redirect(), route handlers return a Response.

import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { supabase } from "./supabase.js";
import { readSession, destroySessionById } from "./session.js";
import { DASHBOARD_ORG_ID } from "./constants.js";

async function requestCookieHeader() {
  const h = await headers();
  return h.get("cookie");
}

async function authorizationHeader() {
  const h = await headers();
  return h.get("authorization") || "";
}

// Shared core. Returns one of:
//   { ok: true, user }            authenticated (user may be null when the
//                                 dashboard basic-auth fallback found no admin)
//   { ok: false, reason: "..." }  caller decides how to reject
async function resolveDashboardUser() {
  const { session, sid } = readSession(await requestCookieHeader());
  const sessionUserId = session?.userId;

  // 1) Prefer a real user session if present.
  if (sessionUserId) {
    const { data: user, error } = await supabase
      .from("users")
      .select("*")
      .eq("id", sessionUserId)
      .eq("is_active", true)
      .maybeSingle();

    if (error) {
      console.error("requireDashboardAuth session lookup error:", error);
      return { ok: false, reason: "lookup-failed" };
    }

    if (user) return { ok: true, user };

    // Session exists but the user is no longer valid -> clear it.
    destroySessionById(sid);
    return { ok: false, reason: "stale-session" };
  }

  // 2) Fall back to the dashboard basic auth.
  const username = process.env.DASHBOARD_USERNAME;
  const password = process.env.DASHBOARD_PASSWORD;

  if (!username || !password) {
    console.warn("Dashboard auth env vars missing; dashboard is unprotected.");
    return { ok: true, user: null };
  }

  const header = await authorizationHeader();
  if (!header.startsWith("Basic ")) return { ok: false, reason: "no-credentials" };

  let decoded = "";
  try {
    decoded = Buffer.from(header.slice(6), "base64").toString("utf8");
  } catch {
    return { ok: false, reason: "no-credentials" };
  }

  const separatorIndex = decoded.indexOf(":");
  const inputUser = separatorIndex >= 0 ? decoded.slice(0, separatorIndex) : "";
  const inputPass = separatorIndex >= 0 ? decoded.slice(separatorIndex + 1) : "";
  if (inputUser !== username || inputPass !== password) {
    return { ok: false, reason: "bad-credentials" };
  }

  // Attach one admin user for legacy dashboard flows if available.
  const { data: fallbackAdmin, error: fallbackAdminError } = await supabase
    .from("users")
    .select("*")
    .eq("org_id", DASHBOARD_ORG_ID)
    .eq("is_active", true)
    .in("role", ["admin", "manager"])
    .order("id", { ascending: true })
    .limit(1)
    .maybeSingle();

  if (fallbackAdminError) {
    console.error(
      "requireDashboardAuth fallback admin lookup error:",
      fallbackAdminError,
    );
    return { ok: true, user: null };
  }

  return { ok: true, user: fallbackAdmin || null };
}

// ---------------------------------------------------------------------------
// Page guards. These either return the logged-in user or throw a redirect,
// so a page body can call them as its first statement.
// ---------------------------------------------------------------------------

export async function requireDashboardAuthPage() {
  const result = await resolveDashboardUser();
  if (result.ok) return result.user;
  if (result.reason === "lookup-failed") {
    throw new Error("Failed to validate logged in user");
  }
  redirect("/login");
}

export async function requireUserLoginPage() {
  const { session, sid } = readSession(await requestCookieHeader());
  const userId = session?.userId;
  if (!userId) redirect("/login");

  const { data: user } = await supabase
    .from("users")
    .select("*")
    .eq("id", userId)
    .maybeSingle();

  if (!user) {
    // The original middleware crashed here on an undefined `isApiRoute`
    // (lib/server/app.js:15999), which surfaced as a 500. A stale session on a
    // page route is meant to bounce to the login screen.
    destroySessionById(sid);
    redirect("/login");
  }

  return user;
}

// ---------------------------------------------------------------------------
// Route-handler guards. Return { user } on success, or a Response to return
// as-is on failure — 401 JSON for /api/*, a redirect otherwise.
// ---------------------------------------------------------------------------

export async function requireDashboardAuthApi(request) {
  const isApiRoute = new URL(request.url).pathname.startsWith("/api/");
  const result = await resolveDashboardUser();
  if (result.ok) return { user: result.user };

  if (result.reason === "lookup-failed") {
    return {
      response: new Response("Failed to validate logged in user", {
        status: 500,
      }),
    };
  }
  if (isApiRoute && result.reason !== "stale-session") {
    return {
      response: Response.json(
        { success: false, error: "Not authenticated" },
        { status: 401 },
      ),
    };
  }
  return { response: Response.redirect(new URL("/login", request.url), 302) };
}

export async function requireUserLoginApi(request) {
  const { session, sid } = readSession(await requestCookieHeader());
  const userId = session?.userId;
  const bounce = () => ({
    response: Response.redirect(new URL("/login", request.url), 302),
  });

  if (!userId) return bounce();

  const { data: user } = await supabase
    .from("users")
    .select("*")
    .eq("id", userId)
    .maybeSingle();

  if (!user) {
    destroySessionById(sid);
    if (new URL(request.url).pathname.startsWith("/api/")) {
      return {
        response: Response.json(
          { success: false, error: "Session expired" },
          { status: 401 },
        ),
      };
    }
    return bounce();
  }

  return { user };
}
