// Authentication service: phone+password login and post-login routing.
// Ported from the POST /login handler in lib/server/app.js (same phone-candidate
// matching and bcrypt verification) so login behavior is unchanged.

import bcrypt from "bcrypt";
import { supabase } from "@/lib/db/supabase.js";
import { normalizePhoneForLogin } from "@/lib/utils/phone.js";
import { getSessionUserId } from "@/lib/auth/session.js";

export function isManagerOrAdmin(user) {
  return user?.role === "admin" || user?.role === "manager";
}

// Resolve the active user behind the current session cookie, or null. Mirrors
// the session branch of requireDashboardAuth in lib/server/app.js (the legacy
// HTTP Basic fallback is intentionally dropped — the session flow is canonical
// for migrated pages). Callers redirect to /login when this returns null.
export async function getSessionUser() {
  const userId = await getSessionUserId();
  if (!userId) return null;

  const { data } = await supabase
    .from("users")
    .select("*")
    .eq("id", userId)
    .eq("is_active", true)
    .maybeSingle();

  return data || null;
}

export function getPostLoginRedirectPath(user) {
  return isManagerOrAdmin(user) ? "/dashboard" : "/my-dashboard";
}

// Verify credentials. Returns { ok: true, user } or { ok: false, error }.
export async function authenticate(rawPhoneInput, passwordInput) {
  const rawPhone = String(rawPhoneInput || "").trim();
  const password = String(passwordInput || "").trim();

  const normalizedPhone = normalizePhoneForLogin(rawPhone);
  const digitsOnly = normalizedPhone.replace(/\D/g, "");

  if (!normalizedPhone || !password) {
    return { ok: false, error: "Please enter phone number and password." };
  }

  const phoneCandidates = [
    normalizedPhone, // +12133081594
    digitsOnly, // 12133081594
    `whatsapp:${normalizedPhone}`, // whatsapp:+12133081594
    rawPhone, // whatever user typed
    rawPhone.replace(/^whatsapp:/i, "").trim(),
  ].filter(Boolean);

  const { data: users, error } = await supabase
    .from("users")
    .select("*")
    .in("phone_number", [...new Set(phoneCandidates)])
    .eq("is_active", true)
    .limit(1);

  if (error) {
    console.error("Login lookup error:", error);
    return { ok: false, error: "Unable to log in right now. Please try again." };
  }

  const user = users?.[0];

  if (!user || !user.password_hash) {
    return { ok: false, error: "Invalid phone number or password." };
  }

  const matches = await bcrypt.compare(password, user.password_hash);
  if (!matches) {
    return { ok: false, error: "Invalid phone number or password." };
  }

  // Best-effort last-login stamp (don't fail the login if this errors).
  await supabase
    .from("users")
    .update({ last_login_at: new Date().toISOString() })
    .eq("id", user.id);

  return { ok: true, user };
}
