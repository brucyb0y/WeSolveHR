// POST /login — ported from lib/server/app.js lines 36789-36858.
//
// Reached by the rewrite in middleware.js, so the browser still posts to
// /login. The one behavioural change from the Express original: a failed login
// redirects back to /login?error=<key> instead of answering the POST with a
// 400/401/500 body. The rendered page is the same (LOGIN_ERRORS holds the
// original strings verbatim); it just arrives via POST-redirect-GET, which also
// stops a browser refresh from resubmitting the password.

import bcrypt from "bcrypt";
import { supabase } from "@/lib/server/supabase.js";
import { getPostLoginRedirectPath } from "@/lib/server/users.js";
import { normalizePhoneForLogin } from "@/lib/server/phone.js";
import { loadSession, commitSession } from "@/lib/server/session.js";
import { assertRewritten, readFormBody, redirectTo } from "@/lib/server/form-post.js";
import { LOGIN_ERRORS } from "@/lib/server/login-errors.js";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request) {
  const blocked = assertRewritten(request);
  if (blocked) return blocked;

  const fail = (key) => redirectTo(request, "/login?error=" + key);

  try {
    const body = await readFormBody(request);
    const rawPhone = String(body.phone || "").trim();
    const password = String(body.password || "").trim();

    const normalizedPhone = normalizePhoneForLogin(rawPhone);
    const digitsOnly = normalizedPhone.replace(/\D/g, "");

    if (!normalizedPhone || !password) return fail(LOGIN_ERRORS.MISSING);

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
      return fail(LOGIN_ERRORS.UNAVAILABLE);
    }

    const user = users?.[0];

    if (!user || !user.password_hash) return fail(LOGIN_ERRORS.INVALID);

    const matches = await bcrypt.compare(password, user.password_hash);

    if (!matches) return fail(LOGIN_ERRORS.INVALID);

    const { session, meta } = loadSession(request.headers.get("cookie"));
    session.userId = user.id;

    await supabase
      .from("users")
      .update({
        last_login_at: new Date().toISOString(),
      })
      .eq("id", user.id);

    const setCookie = commitSession(session, meta);
    return redirectTo(request, getPostLoginRedirectPath(user), setCookie);
  } catch (err) {
    console.error("Login route error:", err);
    return fail(LOGIN_ERRORS.GENERIC);
  }
}
