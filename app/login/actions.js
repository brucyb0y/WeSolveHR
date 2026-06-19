"use server";

// Login Server Action — the idiomatic Next.js replacement for the
// `POST /login` Express handler. Verifies credentials, starts a session via the
// shared cookie store, then redirects to the role-appropriate dashboard.

import { redirect } from "next/navigation";
import { authenticate, getPostLoginRedirectPath } from "@/lib/services/auth.js";
import { createUserSession } from "@/lib/auth/session.js";

export async function loginAction(_prevState, formData) {
  const phone = formData.get("phone");
  const password = formData.get("password");

  const result = await authenticate(phone, password);

  if (!result.ok) {
    // Returned to the form via useActionState; nothing was logged in.
    return { error: result.error };
  }

  await createUserSession(result.user.id);

  // redirect() throws a control-flow signal that Next handles — must run
  // outside any try/catch so it isn't swallowed.
  redirect(getPostLoginRedirectPath(result.user));
}
