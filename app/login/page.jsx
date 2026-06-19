// Login page (Server Component). Replaces the `GET /login` Express handler.
// If the visitor already has a valid session, send them straight to their
// role-appropriate dashboard; otherwise render the client login form.

import { redirect } from "next/navigation";
import { getSessionUserId } from "@/lib/auth/session.js";
import { supabase } from "@/lib/db/supabase.js";
import { getPostLoginRedirectPath } from "@/lib/services/auth.js";
import LoginForm from "./LoginForm.jsx";

export const metadata = { title: "Login | WeSolveHR" };
export const dynamic = "force-dynamic";

export default async function LoginPage() {
  const userId = await getSessionUserId();

  if (userId) {
    const { data: user } = await supabase
      .from("users")
      .select("id, role")
      .eq("id", userId)
      .eq("is_active", true)
      .maybeSingle();

    if (user) {
      redirect(getPostLoginRedirectPath(user));
    }
  }

  return <LoginForm />;
}
