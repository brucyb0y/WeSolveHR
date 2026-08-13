"use server";

// Server Action replacing the old app.post("/login") handler. The lookup rules,
// the phone-candidate list, the bcrypt comparison and the last_login_at write
// are unchanged; only the transport differs (FormData + action state instead of
// a urlencoded POST re-rendering the page with an error string).

import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import bcrypt from "bcrypt";
import { createSession } from "@/lib/server/session.js";
import {
  supabase,
  normalizePhoneForLogin,
  getPostLoginRedirectPath,
} from "@/lib/server/app.js";

export async function loginAction(_prevState, formData) {
  // redirect() signals by throwing, so the destination is resolved inside the
  // try and the actual redirect happens after it.
  let destination = null;

  try {
    const rawPhone = String(formData.get("phone") || "").trim();
    const password = String(formData.get("password") || "").trim();

    const normalizedPhone = normalizePhoneForLogin(rawPhone);
    const digitsOnly = normalizedPhone.replace(/\D/g, "");

    if (!normalizedPhone || !password) {
      return { error: "Please enter phone number and password." };
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
      return { error: "Unable to log in right now. Please try again." };
    }

    const user = users?.[0];

    if (!user || !user.password_hash) {
      return { error: "Invalid phone number or password." };
    }

    const matches = await bcrypt.compare(password, user.password_hash);

    if (!matches) {
      return { error: "Invalid phone number or password." };
    }

    const cookie = createSession({ userId: user.id });
    (await cookies()).set(cookie.name, cookie.value, cookie.options);

    await supabase
      .from("users")
      .update({ last_login_at: new Date().toISOString() })
      .eq("id", user.id);

    destination = getPostLoginRedirectPath(user);
  } catch (err) {
    console.error("Login action error:", err);
    return { error: "Something went wrong while logging in." };
  }

  redirect(destination);
}
